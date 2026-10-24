"""Shard placement, local HNSW index (M=16, ef_construction=200, ef=100), and raw vector storage.

Matches proposal/proposal.tex Section 4 & 5 and audit/AGY_TASK_B1_build.md:
- Initial 50,000 passages are grouped into S shards by k-means (faiss.Kmeans, fixed seed).
- New passages go to the shard with the nearest INITIAL center (no data movement).
- Hash sharding by ID is also supported as the reference check mentioned in proposal Section 5.
- Each shard holds an hnswlib inner-product index (M=16, ef_construction=200, ef=100, fixed seed,
  num_threads=1 for strict determinism) and its raw vectors.
"""

from __future__ import annotations

from typing import List, Optional, Tuple

import faiss
import hnswlib
import numpy as np

from dvs.bus import DIM, SUMMARY_CENTERS, TOP_K
from dvs.summary import ShardSummary, compute_summary_kmeans

PLACEMENT_MODES = frozenset({"nearest_initial", "time_segment", "time_segment_g"})


class Shard:
    """Single vector shard storing raw vectors, an HNSW inner-product index, and summaries."""

    def __init__(
        self,
        shard_id: int,
        initial_vectors: np.ndarray,
        initial_ids: np.ndarray,
        max_elements: int = 120_000,
        c: int = SUMMARY_CENTERS,
        m_hnsw: int = 16,
        ef_construction: int = 200,
        ef_search: int = 100,
        seed: int = 42,
        kmeans_niter: int = 15,
    ) -> None:
        self.shard_id = int(shard_id)
        self.c = int(c)
        self.m_hnsw = int(m_hnsw)
        self.ef_construction = int(ef_construction)
        self.ef_search = int(ef_search)
        self.seed = int(seed) + int(shard_id) * 1009
        self.kmeans_niter = int(kmeans_niter)

        init_vecs = np.ascontiguousarray(initial_vectors, dtype=np.float32)
        init_ids = np.ascontiguousarray(initial_ids, dtype=np.int64)
        if init_vecs.ndim != 2 or init_vecs.shape[1] != DIM:
            raise ValueError(f"Expected initial_vectors shape (N, {DIM}), got {init_vecs.shape}")
        if init_vecs.shape[0] != init_ids.shape[0]:
            raise ValueError("initial_vectors and initial_ids length mismatch")

        self._vectors_chunks: List[np.ndarray] = [init_vecs] if len(init_vecs) > 0 else []
        self._ids_chunks: List[np.ndarray] = [init_ids] if len(init_ids) > 0 else []
        self._vectors_cache: Optional[np.ndarray] = init_vecs
        self._ids_cache: Optional[np.ndarray] = init_ids

        # Index of the first vector inserted since the last published summary
        self.new_vectors_start_idx: int = int(len(init_vecs))
        self.epochs_since_refresh: int = 0

        # Build HNSW inner-product index (num_threads=1 ensures bit-for-bit determinism)
        capacity = max(int(max_elements), len(init_vecs) + 1024)
        self.max_elements = capacity
        self.index = hnswlib.Index(space="ip", dim=DIM)
        self.index.init_index(
            max_elements=capacity,
            ef_construction=self.ef_construction,
            M=self.m_hnsw,
            random_seed=self.seed,
        )
        self.index.set_ef(self.ef_search)
        self.index.set_num_threads(1)

        if len(init_vecs) > 0:
            internal_labels = np.arange(len(init_vecs), dtype=np.int64)
            self.index.add_items(init_vecs, internal_labels, num_threads=1)

        # Build initial summary and mark as both published and candidate at epoch -1 (setup)
        init_summary = compute_summary_kmeans(
            vectors=self.vectors,
            shard_id=self.shard_id,
            c=self.c,
            warm_start_centers=None,
            seed=self.seed,
            niter=self.kmeans_niter,
            epoch_built=-1,
        )
        self.published_summary: ShardSummary = init_summary.copy()
        self.candidate_summary: ShardSummary = init_summary.copy()
        self._candidate_dirty: bool = False
        self.sample_queries: Optional[np.ndarray] = None

    @property
    def size(self) -> int:
        return int(self.vectors.shape[0])

    @property
    def vectors(self) -> np.ndarray:
        if self._vectors_cache is None:
            if not self._vectors_chunks:
                self._vectors_cache = np.empty((0, DIM), dtype=np.float32)
            else:
                self._vectors_cache = np.ascontiguousarray(
                    np.vstack(self._vectors_chunks), dtype=np.float32
                )
        return self._vectors_cache

    @property
    def doc_ids(self) -> np.ndarray:
        if self._ids_cache is None:
            if not self._ids_chunks:
                self._ids_cache = np.empty(0, dtype=np.int64)
            else:
                self._ids_cache = np.ascontiguousarray(
                    np.concatenate(self._ids_chunks), dtype=np.int64
                )
        return self._ids_cache

    @property
    def new_vectors(self) -> np.ndarray:
        """Return vectors inserted since the currently published summary was built."""
        vecs = self.vectors
        return vecs[self.new_vectors_start_idx :]

    @property
    def num_new_vectors(self) -> int:
        return int(self.size - self.new_vectors_start_idx)

    def set_sample_queries(self, sample_queries: np.ndarray) -> None:
        self.sample_queries = np.ascontiguousarray(sample_queries, dtype=np.float32)

    def add_vectors(self, new_vecs: np.ndarray, new_ids: np.ndarray) -> None:
        """Insert newly arrived vectors into the local HNSW index and raw vector store."""
        if len(new_vecs) == 0:
            return
        nv = np.ascontiguousarray(new_vecs, dtype=np.float32)
        ni = np.ascontiguousarray(new_ids, dtype=np.int64)
        start_label = self.size
        end_label = start_label + len(nv)
        if end_label > self.max_elements:
            self.max_elements = max(end_label * 2, self.max_elements + 10_000)
            self.index.resize_index(self.max_elements)
        internal_labels = np.arange(start_label, end_label, dtype=np.int64)
        self.index.add_items(nv, internal_labels, num_threads=1)
        self._vectors_chunks.append(nv)
        self._ids_chunks.append(ni)
        self._vectors_cache = None
        self._ids_cache = None
        self._candidate_dirty = True

    def update_candidate_summary(self, epoch: int) -> ShardSummary:
        """Build or update the 16-center candidate summary warm-started from published centers."""
        if not self._candidate_dirty and self.candidate_summary.epoch_built == epoch:
            return self.candidate_summary
        # Only re-run k-means if new vectors have arrived since published summary (or if dirty)
        if self.num_new_vectors == 0 and not self._candidate_dirty:
            self.candidate_summary = ShardSummary(
                shard_id=self.shard_id,
                centers=self.published_summary.centers.copy(),
                epoch_built=epoch,
            )
            return self.candidate_summary

        epoch_seed = self.seed + (int(epoch) + 1) * 131
        self.candidate_summary = compute_summary_kmeans(
            vectors=self.vectors,
            shard_id=self.shard_id,
            c=self.c,
            warm_start_centers=self.published_summary.centers,
            seed=epoch_seed,
            niter=self.kmeans_niter,
            epoch_built=epoch,
        )
        self._candidate_dirty = False
        return self.candidate_summary

    def publish_candidate_summary(self, epoch: int) -> ShardSummary:
        """Promote current candidate summary to published summary and reset new-vector tracking."""
        self.update_candidate_summary(epoch)
        self.published_summary = self.candidate_summary.copy()
        self.new_vectors_start_idx = self.size
        self.epochs_since_refresh = 0
        return self.published_summary.copy()

    def search(
        self,
        query: np.ndarray,
        k: int = TOP_K,
        exact: bool = False,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Search shard for top-k neighbors of `query` (1D array of shape (384,)).

        Returns:
            (doc_ids, scores) sorted by descending dot-product score, up to min(k, shard.size).
        """
        n = self.size
        if n == 0:
            return np.empty(0, dtype=np.int64), np.empty(0, dtype=np.float32)
        k_eff = min(int(k), n)
        q = np.ascontiguousarray(query, dtype=np.float32).reshape(1, DIM)
        ids_arr = self.doc_ids

        if exact:
            scores_all = (self.vectors @ q[0]).astype(np.float32)
            if k_eff == n:
                top_idx = np.argsort(-scores_all, kind="mergesort")
            else:
                part = np.argpartition(-scores_all, k_eff - 1)[:k_eff]
                order = np.argsort(-scores_all[part], kind="mergesort")
                top_idx = part[order]
            return ids_arr[top_idx].copy(), scores_all[top_idx].copy()

        labels, dists = self.index.knn_query(q, k=k_eff, num_threads=1)
        internal_idx = labels[0].astype(np.int64)
        # hnswlib space='ip' defines distance = 1.0 - dot(q, v)
        scores = (1.0 - dists[0]).astype(np.float32)
        order = np.argsort(-scores, kind="mergesort")
        internal_idx = internal_idx[order]
        scores = scores[order]
        return ids_arr[internal_idx].copy(), scores

    def search_batch_scores_only(
        self,
        queries: np.ndarray,
        k: int = TOP_K,
        exact: bool = False,
    ) -> np.ndarray:
        """Return top-k scores of shape (len(queries), k) padded with -inf if size < k."""
        q_arr = np.ascontiguousarray(queries, dtype=np.float32)
        num_q = q_arr.shape[0]
        out = np.full((num_q, k), -np.inf, dtype=np.float32)
        n = self.size
        if n == 0:
            return out
        k_eff = min(int(k), n)
        if exact:
            dots = q_arr @ self.vectors.T  # (Q, N)
            part = np. sort(dots, axis=1)[:, ::-1][:, :k_eff]
            out[:, :k_eff] = part
            return out
        labels, dists = self.index.knn_query(q_arr, k=k_eff, num_threads=1)
        scores = (1.0 - dists).astype(np.float32)
        scores.sort(axis=1)
        out[:, :k_eff] = scores[:, ::-1]
        return out


class ShardRouterInitializer:
    """Partitions initial vectors into S shards and assigns streamed vectors by placement_mode."""

    def __init__(
        self,
        num_shards: int,
        initial_centroids: np.ndarray,
        sharding_mode: str = "kmeans",
        placement_mode: str = "nearest_initial",
        active_segments: int = 8,
    ) -> None:
        if placement_mode not in PLACEMENT_MODES:
            raise ValueError(
                f"Unknown placement_mode '{placement_mode}'. Expected one of {sorted(PLACEMENT_MODES)}"
            )
        if int(active_segments) <= 0:
            raise ValueError(f"active_segments must be >= 1, got {active_segments}")
        self.num_shards = int(num_shards)
        self.initial_centroids = np.ascontiguousarray(initial_centroids, dtype=np.float32)
        self.sharding_mode = sharding_mode
        self.placement_mode = str(placement_mode)
        self.active_segments = int(active_segments)

    def active_shards_for_epoch(self, epoch: int) -> np.ndarray:
        """Return sorted active shard IDs {(G*e + j) mod S : j = 0..G-1} for `epoch`."""
        e = int(epoch)
        g = self.active_segments
        s = self.num_shards
        return np.array(sorted({(g * e + j) % s for j in range(g)}), dtype=np.int64)

    def assign_shards(
        self,
        vectors: np.ndarray,
        doc_ids: np.ndarray,
        epoch: Optional[int] = None,
    ) -> np.ndarray:
        """Assign each vector to a shard in [0, S-1].

        - For initial setup (`epoch is None` or `epoch < 0`), partitions by `sharding_mode`
          ('kmeans' nearest initial center, or 'hash' doc_id % S).
        - For stream insertions (`epoch >= 0`):
          * Under `placement_mode == 'time_segment'`, assigns all inserts of epoch `e` to
            shard `e % S` (growing segment that fills by arrival time).
          * Under `placement_mode == 'time_segment_g'`, the active shards in epoch `e` are
            `{(G*e + j) mod S : j = 0..G-1}` where `G = active_segments`, and each insert
            goes to the active shard whose initial center has the highest dot product.
          * Under `placement_mode == 'nearest_initial'`, assigns by `sharding_mode` ('kmeans'
            nearest initial center, or 'hash' doc_id % S).
        """
        if len(vectors) == 0:
            return np.empty(0, dtype=np.int64)
        if epoch is not None and int(epoch) >= 0:
            if self.placement_mode == "time_segment":
                target_shard = int(epoch) % self.num_shards
                return np.full(len(vectors), target_shard, dtype=np.int64)
            if self.placement_mode == "time_segment_g":
                active_shards = self.active_shards_for_epoch(int(epoch))
                v = np.ascontiguousarray(vectors, dtype=np.float32)
                active_centroids = self.initial_centroids[active_shards]
                dots = v @ active_centroids.T
                best_local = np.argmax(dots, axis=1)
                return active_shards[best_local].astype(np.int64)
        if self.sharding_mode == "hash":
            return (np.asarray(doc_ids, dtype=np.int64) % self.num_shards).astype(np.int64)
        v = np.ascontiguousarray(vectors, dtype=np.float32)
        # Nearest initial center in Euclidean / inner-product space
        # ||v - c||^2 = ||v||^2 + ||c||^2 - 2 <v, c>; minimizing ||v - c||^2 over c
        c_norm_sq = np.sum(self.initial_centroids ** 2, axis=1, keepdims=True).T  # (1, S)
        dots = v @ self.initial_centroids.T  # (N, S)
        sq_dists = c_norm_sq - 2.0 * dots
        return np.argmin(sq_dists, axis=1).astype(np.int64)


def build_initial_shards(
    initial_vectors: np.ndarray,
    initial_ids: np.ndarray,
    num_shards: int = 32,
    c: int = SUMMARY_CENTERS,
    m_hnsw: int = 16,
    ef_construction: int = 200,
    ef_search: int = 100,
    seed: int = 42,
    sharding_mode: str = "kmeans",
    placement_mode: str = "nearest_initial",
    active_segments: int = 8,
    max_elements_per_shard: int = 120_000,
    kmeans_niter: int = 20,
    precomputed_centroids: Optional[np.ndarray] = None,
) -> Tuple[List[Shard], ShardRouterInitializer]:
    """Partition `initial_vectors` into `num_shards` shards and initialize each shard's HNSW index."""
    vecs = np.ascontiguousarray(initial_vectors, dtype=np.float32)
    ids = np.ascontiguousarray(initial_ids, dtype=np.int64)

    if precomputed_centroids is not None:
        init_centroids = np.ascontiguousarray(precomputed_centroids, dtype=np.float32)
    else:
        km = faiss.Kmeans(
            d=DIM,
            k=int(num_shards),
            niter=int(kmeans_niter),
            seed=int(seed),
            spherical=True,
            verbose=False,
            min_points_per_centroid=1,
            max_points_per_centroid=max(len(vecs), 100000),
        )
        km.train(vecs)
        init_centroids = np.ascontiguousarray(km.centroids, dtype=np.float32)

    router = ShardRouterInitializer(
        num_shards=num_shards,
        initial_centroids=init_centroids,
        sharding_mode=sharding_mode,
        placement_mode=placement_mode,
        active_segments=active_segments,
    )
    assignments = router.assign_shards(vecs, ids, epoch=-1)

    shards: List[Shard] = []
    for s in range(num_shards):
        mask = assignments == s
        s_vecs = vecs[mask]
        s_ids = ids[mask]
        shard = Shard(
            shard_id=s,
            initial_vectors=s_vecs,
            initial_ids=s_ids,
            max_elements=max_elements_per_shard,
            c=c,
            m_hnsw=m_hnsw,
            ef_construction=ef_construction,
            ef_search=ef_search,
            seed=seed,
        )
        shards.append(shard)

    return shards, router
