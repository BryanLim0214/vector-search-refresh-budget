"""Evaluation metrics, exact Faiss ground truth, CPU timing, and Spearman rank correlation.

Matches proposal/proposal.tex Section 4 & 5 and audit/AGY_TASK_B1_build.md:
- Exact top-10 ground truth via faiss.IndexFlatIP over all vectors inserted so far (incremental add).
- Overall recall@10.
- Recent recall@10: fraction of true top-10 neighbors inserted in the last W epochs that are
  returned (queries with zero recent true top-10 neighbors are excluded from the mean and counted).
- Missed neighbors: count of true top-10 neighbors on skipped shards.
- CPU time per component via time.process_time().
- Spearman rank correlation between estimated and true recovered neighbors per refresh decision.
- Paired bootstrap 95% confidence intervals over queries.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

import faiss
import numpy as np
from scipy.stats import spearmanr

from dvs.bus import DIM, TOP_K
from dvs.coordinator import Coordinator
from dvs.shards import Shard


class GroundTruthIndex:
    """Exact inner-product ground-truth index (faiss.IndexFlatIP) updated incrementally."""

    def __init__(self) -> None:
        self.index = faiss.IndexFlatIP(DIM)
        self._doc_ids_chunks: List[np.ndarray] = []
        self._epochs_chunks: List[np.ndarray] = []
        self._shards_chunks: List[np.ndarray] = []
        self._doc_ids_cache: Optional[np.ndarray] = np.empty(0, dtype=np.int64)
        self._epochs_cache: Optional[np.ndarray] = np.empty(0, dtype=np.int64)
        self._shards_cache: Optional[np.ndarray] = np.empty(0, dtype=np.int64)

    @property
    def size(self) -> int:
        return int(self.index.ntotal)

    @property
    def doc_ids(self) -> np.ndarray:
        if self._doc_ids_cache is None:
            self._doc_ids_cache = (
                np.concatenate(self._doc_ids_chunks)
                if self._doc_ids_chunks
                else np.empty(0, dtype=np.int64)
            )
        return self._doc_ids_cache

    @property
    def inserted_epochs(self) -> np.ndarray:
        if self._epochs_cache is None:
            self._epochs_cache = (
                np.concatenate(self._epochs_chunks)
                if self._epochs_chunks
                else np.empty(0, dtype=np.int64)
            )
        return self._epochs_cache

    @property
    def shard_ids(self) -> np.ndarray:
        if self._shards_cache is None:
            self._shards_cache = (
                np.concatenate(self._shards_chunks)
                if self._shards_chunks
                else np.empty(0, dtype=np.int64)
            )
        return self._shards_cache

    def add(
        self,
        vectors: np.ndarray,
        doc_ids: np.ndarray,
        shard_ids: np.ndarray,
        epoch: int,
    ) -> None:
        """Incrementally add vectors with their document IDs, shard assignments, and insertion epoch."""
        if len(vectors) == 0:
            return
        vecs = np.ascontiguousarray(vectors, dtype=np.float32)
        ids = np.ascontiguousarray(doc_ids, dtype=np.int64)
        sids = np.ascontiguousarray(shard_ids, dtype=np.int64)
        ep = np.full(len(vecs), int(epoch), dtype=np.int64)

        self.index.add(vecs)
        self._doc_ids_chunks.append(ids)
        self._shards_chunks.append(sids)
        self._epochs_chunks.append(ep)
        self._doc_ids_cache = None
        self._shards_cache = None
        self._epochs_cache = None

    def search(
        self,
        queries: np.ndarray,
        k: int = TOP_K,
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """Return exact top-k `(gt_doc_ids, gt_scores, gt_epochs, gt_shards)` of shape `(Q, k_eff)`."""
        q = np.ascontiguousarray(queries, dtype=np.float32)
        if q.ndim == 1:
            q = q.reshape(1, DIM)
        k_eff = min(int(k), self.size)
        if k_eff == 0:
            empty_i = np.empty((len(q), 0), dtype=np.int64)
            empty_f = np.empty((len(q), 0), dtype=np.float32)
            return empty_i, empty_f, empty_i, empty_i

        scores, indices = self.index.search(q, k_eff)
        all_ids = self.doc_ids
        all_epochs = self.inserted_epochs
        all_shards = self.shard_ids

        gt_ids = all_ids[indices]
        gt_epochs = all_epochs[indices]
        gt_shards = all_shards[indices]
        return gt_ids, scores.astype(np.float32), gt_epochs, gt_shards


class CPUTimer:
    """Tracks CPU process_time per component within an epoch."""

    def __init__(self) -> None:
        self.times: Dict[str, float] = {
            "insert": 0.0,
            "summary": 0.0,
            "policy": 0.0,
            "query": 0.0,
        }

    @contextmanager
    def measure(self, component: str) -> Iterator[None]:
        t0 = time.process_time()
        try:
            yield
        finally:
            t1 = time.process_time()
            self.times[component] = self.times.get(component, 0.0) + max(0.0, t1 - t0)

    @property
    def total(self) -> float:
        return float(sum(self.times.values()))


@dataclass
class EpochMetricsResult:
    """Computed accuracy and diagnostic metrics for one epoch."""

    recall_at_10: float
    recent_recall_at_10: float
    num_queries_with_recent_neighbors: int
    num_queries_without_recent_neighbors: int
    missed_neighbors_mean: float
    missed_neighbors_total: int
    spearman_rho: float
    per_query_recall: np.ndarray
    per_query_recent_recall: np.ndarray  # NaN for queries without recent true top-10 neighbors
    per_query_missed_neighbors: np.ndarray


def evaluate_epoch_queries(
    epoch: int,
    returned_ids_list: Sequence[np.ndarray],
    searched_shards_mask: np.ndarray,
    gt_ids: np.ndarray,
    gt_epochs: np.ndarray,
    gt_shards: np.ndarray,
    recent_window_w: int = 2,
    k: int = TOP_K,
    pushed_id_set: Optional[set[int]] = None,
    spearman_rho: float = 0.0,
) -> EpochMetricsResult:
    """Compute overall recall@10, recent recall@10 (window W), and missed neighbors."""
    num_q = len(returned_ids_list)
    k_eff = gt_ids.shape[1] if gt_ids.ndim == 2 else 0
    if num_q == 0 or k_eff == 0:
        return EpochMetricsResult(
            recall_at_10=0.0,
            recent_recall_at_10=float("nan"),
            num_queries_with_recent_neighbors=0,
            num_queries_without_recent_neighbors=num_q,
            missed_neighbors_mean=0.0,
            missed_neighbors_total=0,
            spearman_rho=spearman_rho,
            per_query_recall=np.zeros(num_q, dtype=np.float64),
            per_query_recent_recall=np.full(num_q, np.nan, dtype=np.float64),
            per_query_missed_neighbors=np.zeros(num_q, dtype=np.int64),
        )

    min_recent_epoch = max(0, int(epoch) - int(recent_window_w) + 1)

    per_q_recall = np.zeros(num_q, dtype=np.float64)
    per_q_recent = np.full(num_q, np.nan, dtype=np.float64)
    per_q_missed = np.zeros(num_q, dtype=np.int64)

    with_recent = 0
    without_recent = 0

    for q_idx in range(num_q):
        ret_set = set(int(x) for x in returned_ids_list[q_idx][:k])
        true_ids_q = gt_ids[q_idx]
        true_epochs_q = gt_epochs[q_idx]
        true_shards_q = gt_shards[q_idx]

        hits = sum(1 for tid in true_ids_q if int(tid) in ret_set)
        per_q_recall[q_idx] = float(hits) / float(k_eff)

        # Recent true top-10 neighbors: inserted at epoch in [max(0, epoch - W + 1), epoch]
        recent_mask = (true_epochs_q >= min_recent_epoch) & (true_epochs_q <= epoch)
        recent_true_ids = true_ids_q[recent_mask]
        if len(recent_true_ids) == 0:
            without_recent += 1
        else:
            with_recent += 1
            recent_hits = sum(1 for tid in recent_true_ids if int(tid) in ret_set)
            per_q_recent[q_idx] = float(recent_hits) / float(len(recent_true_ids))

        # Missed neighbors: true top-10 neighbors on skipped shards (not searched for this query,
        # and not locally pushed to the coordinator)
        missed_q = 0
        for tid, t_shard in zip(true_ids_q, true_shards_q):
            s_int = int(t_shard)
            if not searched_shards_mask[q_idx, s_int]:
                if pushed_id_set is not None and int(tid) in pushed_id_set:
                    continue
                missed_q += 1
        per_q_missed[q_idx] = missed_q

    overall_recall = float(np.mean(per_q_recall))
    recent_recall = (
        float(np.nanmean(per_q_recent)) if with_recent > 0 else float("nan")
    )
    missed_total = int(np.sum(per_q_missed))
    missed_mean = float(np.mean(per_q_missed))

    return EpochMetricsResult(
        recall_at_10=overall_recall,
        recent_recall_at_10=recent_recall,
        num_queries_with_recent_neighbors=with_recent,
        num_queries_without_recent_neighbors=without_recent,
        missed_neighbors_mean=missed_mean,
        missed_neighbors_total=missed_total,
        spearman_rho=spearman_rho,
        per_query_recall=per_q_recall,
        per_query_recent_recall=per_q_recent,
        per_query_missed_neighbors=per_q_missed,
    )


def compute_true_recovered_neighbors_per_shard(
    shards: Sequence[Shard],
    coordinator: Coordinator,
    test_queries: np.ndarray,
    gt_ids: np.ndarray,
    gt_shards: np.ndarray,
) -> np.ndarray:
    """Compute true recovered neighbors across shards on `test_queries` prior to refresh.

    For each shard s, assuming other shards' published summaries stay fixed within the epoch,
    checks which test queries lift shard s into the top m under `shard.candidate_summary`,
    and counts how many true top-10 neighbors for those queries reside on shard s.
    """
    num_shards = len(shards)
    m_eff = min(num_shards, coordinator.m)
    if m_eff >= num_shards or len(test_queries) == 0:
        return np.zeros(num_shards, dtype=np.float64)

    ranked_shards, ranked_scores = coordinator.rank_shards_for_queries(test_queries)
    top_m_shards = ranked_shards[:, :m_eff]
    mth_scores = ranked_scores[:, m_eff - 1]
    mth_sids = ranked_shards[:, m_eff - 1]

    true_recovered = np.zeros(num_shards, dtype=np.float64)
    for s_id, shard in enumerate(shards):
        cand_scores_s = shard.candidate_summary.query_scores(test_queries)
        in_top_m_pub = np.any(top_m_shards == s_id, axis=1)
        beats_mth = (cand_scores_s > mth_scores) | (
            (cand_scores_s == mth_scores) & (s_id < mth_sids)
        )
        lift_mask = (~in_top_m_pub) & beats_mth
        if np.any(lift_mask):
            lifted_gt_shards = gt_shards[lift_mask]
            true_recovered[s_id] = float(np.sum(lifted_gt_shards == s_id))
    return true_recovered


def compute_spearman_correlation(
    estimated: Optional[np.ndarray],
    true_recovered: Optional[np.ndarray],
) -> float:
    """Return Spearman rank correlation between estimated and true recovered neighbors across shards."""
    if estimated is None or true_recovered is None:
        return 0.0
    est = np.asarray(estimated, dtype=np.float64)
    tru = np.asarray(true_recovered, dtype=np.float64)
    if len(est) < 2 or len(est) != len(tru):
        return 0.0
    if np.all(est == est[0]) or np.all(tru == tru[0]):
        return 0.0
    res = spearmanr(est, tru)
    rho = float(res.statistic)
    return 0.0 if np.isnan(rho) else rho


def paired_bootstrap_ci(
    values_a: np.ndarray,
    values_b: np.ndarray,
    n_boot: int = 2000,
    ci: float = 0.95,
    seed: int = 42,
) -> Dict[str, float]:
    """Compute paired bootstrap mean gap (A - B) and 95% confidence interval."""
    a = np.asarray(values_a, dtype=np.float64)
    b = np.asarray(values_b, dtype=np.float64)
    valid = np.isfinite(a) & np.isfinite(b)
    diffs = a[valid] - b[valid]
    if len(diffs) == 0:
        return {"mean_diff": float("nan"), "ci_low": float("nan"), "ci_high": float("nan"), "n": 0}
    rng = np.random.default_rng(int(seed))
    n = len(diffs)
    idx = rng.integers(0, n, size=(int(n_boot), n))
    boot_means = np.mean(diffs[idx], axis=1)
    alpha = (1.0 - ci) / 2.0
    low, high = np.quantile(boot_means, [alpha, 1.0 - alpha])
    return {
        "mean_diff": float(np.mean(diffs)),
        "ci_low": float(low),
        "ci_high": float(high),
        "n": int(n),
    }
