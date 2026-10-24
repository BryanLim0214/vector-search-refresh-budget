"""Stream arrival schedules (steady, cluster_order, query_shift, new_topic) and optional bursts.

Matches proposal/proposal.tex Section 5, audit/AGY_TASK_B1_build.md, and audit/AGY_TASK_B2_fixes_pilot.md:
- steady: seeded random arrival order over the streamed vectors.
- cluster_order: Big-ANN streaming runbook style; k-means with 64 clusters on the streamed
  vectors, inserting cluster by cluster (clusters ordered by seeded permutation, vectors within
  each cluster ordered by seeded permutation).
- query_shift: test queries drawn from the subset of the 1,000 dev test queries farthest from
  the 64 sample queries, defined by lowest mean dot-product similarity to the 64 sample queries:
    mean_sim(q) = (1 / 64) * sum_{j=1}^{64} <q, sample_q_j>.
  Specifically, the 1,000 test queries are sorted in ascending order of mean_sim(q), and each
  epoch draws its Q test queries from the farthest pool of size max(Q, int(far_pool_fraction * N_test))
  (default far_pool_fraction=0.25, i.e., the 250 farthest test queries for Q=200).
- new_topic: spherical k-means (faiss, fixed seed, 64 clusters) on ALL 100,000 vectors; put the
  clusters in a seeded random order; the initial set is the vectors of the first 32 clusters,
  and the stream is the vectors of the other 32 clusters, inserted cluster by cluster.
- bursts: optional flag that concentrates insertions into periodic high-rate burst epochs while
  preserving total insertions across the run.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

import faiss
import numpy as np

from dvs.bus import DIM, TOP_K

STREAM_MODES = frozenset({"steady", "cluster_order", "query_shift", "new_topic"})
NUM_STREAM_CLUSTERS: int = 64


@dataclass
class EpochBatch:
    """Data arriving and queries evaluated in a single simulation epoch."""

    epoch: int
    insert_vectors: np.ndarray  # shape (I_e, 384) float32
    insert_ids: np.ndarray  # shape (I_e,) int64
    test_queries: np.ndarray  # shape (Q, 384) float32
    test_query_ids: np.ndarray  # shape (Q,) int64


def partition_new_topic_stream(
    initial_vectors: np.ndarray,
    initial_ids: np.ndarray,
    stream_vectors: np.ndarray,
    stream_ids: np.ndarray,
    seed: int = 42,
    num_clusters: int = NUM_STREAM_CLUSTERS,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Partition all vectors into initial (first 32 clusters) and stream (other 32 clusters, cluster-by-cluster).

    Runs 64-cluster spherical k-means (faiss, fixed seed) on ALL vectors (`initial_vectors` +
    `stream_vectors`), orders the 64 clusters by a seeded random permutation, assigns the vectors
    of the first 32 clusters to the initial set, and orders the vectors of the remaining 32 clusters
    cluster by cluster (with seeded permutation within each cluster) as the stream.
    """
    all_vecs = np.ascontiguousarray(
        np.vstack([initial_vectors, stream_vectors]), dtype=np.float32
    )
    all_ids = np.ascontiguousarray(
        np.concatenate([initial_ids, stream_ids]), dtype=np.int64
    )
    n = len(all_vecs)
    if n == 0:
        return (
            np.empty((0, DIM), dtype=np.float32),
            np.empty(0, dtype=np.int64),
            np.empty((0, DIM), dtype=np.float32),
            np.empty(0, dtype=np.int64),
        )

    k_eff = max(2, min(int(num_clusters), n))
    half_k = k_eff // 2

    km = faiss.Kmeans(
        d=DIM,
        k=k_eff,
        niter=20,
        seed=int(seed),
        spherical=True,
        verbose=False,
        min_points_per_centroid=1,
        max_points_per_centroid=max(n, 100000),
    )
    km.train(all_vecs)
    centroids = np.ascontiguousarray(km.centroids, dtype=np.float32)
    c_norm_sq = np.sum(centroids ** 2, axis=1, keepdims=True).T
    sq_dists = c_norm_sq - 2.0 * (all_vecs @ centroids.T)
    labels = np.argmin(sq_dists, axis=1)

    rng = np.random.default_rng(int(seed))
    cluster_order = rng.permutation(k_eff)
    init_clusters = cluster_order[:half_k]
    stream_clusters = cluster_order[half_k:]

    # Initial set: vectors of the first 32 clusters (preserving index order in all_vecs)
    init_mask = np.isin(labels, init_clusters)
    init_indices = np.nonzero(init_mask)[0]

    # Stream set: vectors of the other 32 clusters, inserted cluster by cluster
    stream_ordered_indices: List[np.ndarray] = []
    for cid in stream_clusters:
        members = np.nonzero(labels == cid)[0]
        if len(members) > 0:
            shuffled_members = members[rng.permutation(len(members))]
            stream_ordered_indices.append(shuffled_members)
    stream_perm = (
        np.concatenate(stream_ordered_indices)
        if stream_ordered_indices
        else np.empty(0, dtype=np.int64)
    )

    return (
        np.ascontiguousarray(all_vecs[init_indices], dtype=np.float32),
        np.ascontiguousarray(all_ids[init_indices], dtype=np.int64),
        np.ascontiguousarray(all_vecs[stream_perm], dtype=np.float32),
        np.ascontiguousarray(all_ids[stream_perm], dtype=np.int64),
    )


def compute_new_topic_query_stats(
    initial_vectors: np.ndarray,
    stream_vectors: np.ndarray,
    test_queries: np.ndarray,
    k: int = TOP_K,
) -> Dict[str, float | int]:
    """Compute how many test queries have most of their true top-k neighbors in the streamed half."""
    n_init = int(len(initial_vectors))
    n_stream = int(len(stream_vectors))
    n_q = int(len(test_queries))
    if n_init + n_stream == 0 or n_q == 0:
        return {
            "initial_set_size": n_init,
            "stream_set_size": n_stream,
            "queries_with_most_neighbors_in_stream": 0,
            "queries_with_at_least_half_neighbors_in_stream": 0,
            "mean_stream_neighbors_in_top_k": 0.0,
        }
    all_vecs = np.ascontiguousarray(
        np.vstack([initial_vectors, stream_vectors]), dtype=np.float32
    )
    q = np.ascontiguousarray(test_queries, dtype=np.float32)
    k_eff = min(int(k), len(all_vecs))
    idx = faiss.IndexFlatIP(DIM)
    idx.add(all_vecs)
    _, indices = idx.search(q, k_eff)
    in_stream = indices >= n_init
    stream_counts = np.sum(in_stream, axis=1)
    return {
        "initial_set_size": n_init,
        "stream_set_size": n_stream,
        "queries_with_most_neighbors_in_stream": int(np.sum(stream_counts > (k_eff / 2.0))),
        "queries_with_at_least_half_neighbors_in_stream": int(np.sum(stream_counts >= (k_eff / 2.0))),
        "mean_stream_neighbors_in_top_k": float(np.mean(stream_counts)),
    }


def order_stream_passages(
    stream_vectors: np.ndarray,
    stream_ids: np.ndarray,
    mode: str = "steady",
    seed: int = 42,
    num_clusters: int = NUM_STREAM_CLUSTERS,
) -> Tuple[np.ndarray, np.ndarray]:
    """Return `(ordered_vectors, ordered_ids)` according to `mode`."""
    vecs = np.ascontiguousarray(stream_vectors, dtype=np.float32)
    ids = np.ascontiguousarray(stream_ids, dtype=np.int64)
    n = len(vecs)
    if n == 0:
        return vecs, ids

    if mode == "new_topic":
        # Already ordered cluster-by-cluster by partition_new_topic_stream
        return vecs, ids

    rng = np.random.default_rng(int(seed))

    if mode in ("steady", "query_shift"):
        perm = rng.permutation(n)
        return np.ascontiguousarray(vecs[perm]), np.ascontiguousarray(ids[perm])

    if mode == "cluster_order":
        k_eff = min(int(num_clusters), n)
        km = faiss.Kmeans(
            d=DIM,
            k=k_eff,
            niter=20,
            seed=int(seed),
            spherical=True,
            verbose=False,
            min_points_per_centroid=1,
            max_points_per_centroid=max(n, 100000),
        )
        km.train(vecs)
        centroids = np.ascontiguousarray(km.centroids, dtype=np.float32)
        c_norm_sq = np.sum(centroids ** 2, axis=1, keepdims=True).T
        sq_dists = c_norm_sq - 2.0 * (vecs @ centroids.T)
        labels = np.argmin(sq_dists, axis=1)

        cluster_order = rng.permutation(k_eff)
        ordered_indices: List[np.ndarray] = []
        for cid in cluster_order:
            members = np.nonzero(labels == cid)[0]
            if len(members) > 0:
                shuffled_members = members[rng.permutation(len(members))]
                ordered_indices.append(shuffled_members)
        full_perm = np.concatenate(ordered_indices)
        return np.ascontiguousarray(vecs[full_perm]), np.ascontiguousarray(ids[full_perm])

    raise ValueError(f"Unknown stream mode '{mode}'. Expected one of {sorted(STREAM_MODES)}")


def compute_query_shift_pool(
    test_queries: np.ndarray,
    test_query_ids: np.ndarray,
    sample_queries: np.ndarray,
    queries_per_epoch: int,
    far_pool_fraction: float = 0.25,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Select the subset of test queries farthest from `sample_queries` by mean dot-product similarity.

    Rule:
      1. For each test query q_i in the 1,000 dev queries, compute
         mean_sim(q_i) = mean_{j=1..64} (q_i . sample_queries[j]).
      2. Sort all test queries in ascending order of mean_sim(q_i) (lowest similarity = farthest).
      3. Keep the top `pool_size = max(queries_per_epoch, int(round(far_pool_fraction * N_test)))`
         farthest test queries as the query-shift sampling pool.

    Returns:
      (pool_queries, pool_ids, pool_mean_sims)
    """
    tq = np.ascontiguousarray(test_queries, dtype=np.float32)
    tids = np.ascontiguousarray(test_query_ids, dtype=np.int64)
    sq = np.ascontiguousarray(sample_queries, dtype=np.float32)

    sims = tq @ sq.T  # (N_test, Q_s)
    mean_sims = np.mean(sims, axis=1)  # (N_test,)
    order = np.argsort(mean_sims, kind="mergesort")  # ascending: farthest first

    n_test = len(tq)
    pool_size = min(n_test, max(int(queries_per_epoch), int(round(far_pool_fraction * n_test))))
    selected = order[:pool_size]
    return (
        np.ascontiguousarray(tq[selected]),
        np.ascontiguousarray(tids[selected]),
        np.ascontiguousarray(mean_sims[selected]),
    )


def compute_epoch_insert_counts(
    num_epochs: int,
    inserts_per_epoch: int,
    total_available: int,
    bursts: bool = False,
    consume_all_available: bool = False,
) -> List[int]:
    """Return number of insertions for each epoch (with optional bursty schedule)."""
    if consume_all_available:
        target_total = int(total_available)
    else:
        target_total = min(int(num_epochs) * int(inserts_per_epoch), int(total_available))
    if not bursts or num_epochs <= 1:
        base = target_total // num_epochs
        rem = target_total % num_epochs
        return [base + (1 if e < rem else 0) for e in range(num_epochs)]

    # Under bursts=True, every 5th epoch has weight 3.0 and other epochs have weight 0.5
    weights = np.array([3.0 if (e % 5 == 0) else 0.5 for e in range(num_epochs)], dtype=np.float64)
    raw_counts = weights / np.sum(weights) * target_total
    counts = np.floor(raw_counts).astype(int)
    diff = target_total - int(np.sum(counts))
    frac_order = np.argsort(-(raw_counts - counts), kind="mergesort")
    for i in range(diff):
        counts[frac_order[i]] += 1
    return counts.tolist()


def build_epoch_schedule(
    stream_vectors: np.ndarray,
    stream_ids: np.ndarray,
    test_queries: np.ndarray,
    test_query_ids: np.ndarray,
    sample_queries: np.ndarray,
    stream_mode: str = "steady",
    num_epochs: int = 50,
    inserts_per_epoch: int = 1000,
    queries_per_epoch: int = 200,
    seed: int = 42,
    bursts: bool = False,
    far_pool_fraction: float = 0.25,
) -> List[EpochBatch]:
    """Build the deterministic per-epoch schedule of passage insertions and test queries."""
    if stream_mode not in STREAM_MODES:
        raise ValueError(f"Unknown stream_mode '{stream_mode}'. Expected one of {sorted(STREAM_MODES)}")

    ordered_vecs, ordered_ids = order_stream_passages(
        stream_vectors=stream_vectors,
        stream_ids=stream_ids,
        mode=stream_mode,
        seed=seed,
    )

    if stream_mode == "query_shift":
        q_pool, q_id_pool, _ = compute_query_shift_pool(
            test_queries=test_queries,
            test_query_ids=test_query_ids,
            sample_queries=sample_queries,
            queries_per_epoch=queries_per_epoch,
            far_pool_fraction=far_pool_fraction,
        )
    else:
        q_pool = np.ascontiguousarray(test_queries, dtype=np.float32)
        q_id_pool = np.ascontiguousarray(test_query_ids, dtype=np.int64)

    insert_counts = compute_epoch_insert_counts(
        num_epochs=num_epochs,
        inserts_per_epoch=inserts_per_epoch,
        total_available=len(ordered_vecs),
        bursts=bursts,
        consume_all_available=(stream_mode == "new_topic"),
    )

    batches: List[EpochBatch] = []
    cursor = 0
    for epoch in range(num_epochs):
        n_ins = insert_counts[epoch]
        ins_vecs = ordered_vecs[cursor : cursor + n_ins]
        ins_ids = ordered_ids[cursor : cursor + n_ins]
        cursor += n_ins

        q_rng = np.random.default_rng(int(seed) + 10_000 + epoch)
        q_count = int(queries_per_epoch)
        if q_count <= len(q_pool):
            chosen_idx = q_rng.choice(len(q_pool), size=q_count, replace=False)
        else:
            chosen_idx = q_rng.choice(len(q_pool), size=q_count, replace=True)

        batches.append(
            EpochBatch(
                epoch=epoch,
                insert_vectors=np.ascontiguousarray(ins_vecs, dtype=np.float32),
                insert_ids=np.ascontiguousarray(ins_ids, dtype=np.int64),
                test_queries=np.ascontiguousarray(q_pool[chosen_idx], dtype=np.float32),
                test_query_ids=np.ascontiguousarray(q_id_pool[chosen_idx], dtype=np.int64),
            )
        )
    return batches
