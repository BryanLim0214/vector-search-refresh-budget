"""Shard summary representation and warm-started k-means computation.

Matches proposal/proposal.tex Section 4:
- Each shard summarizes its passages with c = 16 centers (24,576 bytes).
- Initial summary at setup is built by c-center k-means on the shard's initial vectors.
- Each epoch, a shard builds a candidate summary by c-center k-means on its current vectors,
  warm-started from its published centers (deterministic given seed).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import faiss
import numpy as np

from dvs.bus import DIM, SUMMARY_BYTES, SUMMARY_CENTERS


@dataclass
class ShardSummary:
    """A c-center summary of a shard's vectors (c x 384 float32 = 24,576 bytes)."""

    shard_id: int
    centers: np.ndarray  # shape (c, 384), dtype float32
    epoch_built: int = 0

    def __post_init__(self) -> None:
        self.centers = np.ascontiguousarray(self.centers, dtype=np.float32)
        if self.centers.ndim != 2 or self.centers.shape[1] != DIM:
            raise ValueError(f"Expected centers of shape (c, {DIM}), got {self.centers.shape}")

    @property
    def num_centers(self) -> int:
        return int(self.centers.shape[0])

    @property
    def byte_size(self) -> int:
        return SUMMARY_BYTES if self.num_centers == SUMMARY_CENTERS else int(self.centers.nbytes)

    def copy(self) -> "ShardSummary":
        return ShardSummary(
            shard_id=self.shard_id,
            centers=self.centers.copy(),
            epoch_built=self.epoch_built,
        )

    def query_scores(self, queries: np.ndarray) -> np.ndarray:
        """Return max dot product between each query and this summary's centers.

        Args:
            queries: shape (Q, 384) or (384,) float32 array.
        Returns:
            shape (Q,) float32 array (or scalar float if 1D input).
        """
        q = np.ascontiguousarray(queries, dtype=np.float32)
        if q.ndim == 1:
            dots = self.centers @ q
            return np.max(dots)
        dots = q @ self.centers.T  # (Q, c)
        return np.max(dots, axis=1)

    def nearest_center_distance(self, vectors: np.ndarray) -> np.ndarray:
        """Return inner-product distance (1 - max dot product) from each vector to nearest center."""
        v = np.ascontiguousarray(vectors, dtype=np.float32)
        if len(v) == 0:
            return np.empty(0, dtype=np.float32)
        max_dots = np.max(v @ self.centers.T, axis=1)
        return np.maximum(0.0, 1.0 - max_dots).astype(np.float32)


def _pad_vectors_if_needed(vectors: np.ndarray, k: int) -> np.ndarray:
    n = vectors.shape[0]
    if n == 0:
        return np.zeros((k, DIM), dtype=np.float32)
    if n >= k:
        return np.ascontiguousarray(vectors, dtype=np.float32)
    reps = (k + n - 1) // n
    tiled = np.tile(vectors, (reps, 1))[:k]
    return np.ascontiguousarray(tiled, dtype=np.float32)


def compute_summary_kmeans(
    vectors: np.ndarray,
    shard_id: int,
    c: int = SUMMARY_CENTERS,
    warm_start_centers: Optional[np.ndarray] = None,
    seed: int = 42,
    niter: int = 15,
    epoch_built: int = 0,
) -> ShardSummary:
    """Compute a c-center k-means summary on `vectors`, optionally warm-started from `warm_start_centers`.

    Uses faiss.Kmeans with fixed seed for deterministic clustering.
    """
    train_x = _pad_vectors_if_needed(vectors, c)
    km = faiss.Kmeans(
        d=DIM,
        k=c,
        niter=niter,
        seed=int(seed),
        spherical=True,
        verbose=False,
        min_points_per_centroid=1,
        max_points_per_centroid=max(train_x.shape[0], 100000),
    )
    if warm_start_centers is not None:
        init_c = np.ascontiguousarray(warm_start_centers, dtype=np.float32)
        if init_c.shape != (c, DIM):
            raise ValueError(f"warm_start_centers must have shape ({c}, {DIM}), got {init_c.shape}")
        km.train(train_x, init_centroids=init_c)
    else:
        km.train(train_x)
    centroids = np.ascontiguousarray(km.centroids, dtype=np.float32)
    return ShardSummary(shard_id=shard_id, centers=centroids, epoch_built=epoch_built)
