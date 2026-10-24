"""Coordinator holding published shard summaries, routing queries, and merging top-10 results.

Matches proposal/proposal.tex Section 4 and audit/AGY_TASK_B1_build.md:
- Holds each shard's last sent (published) summary (c=16 centers).
- Scores each shard for a query by the max dot product between the query and the shard's
  published centers.
- Routes each query to the top m shards (with deterministic tie-breaking by shard_id).
- Merges candidate results from searched shards (and any locally pushed vectors under push_vectors)
  into the final top 10 unique document IDs.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np

from dvs.bus import ByteBus, DIM, TOP_K
from dvs.shards import Shard
from dvs.summary import ShardSummary


class Coordinator:
    """Central query router holding published shard summaries and optional pushed vectors."""

    def __init__(
        self,
        shards: Sequence[Shard],
        bus: ByteBus,
        m: int = 2,
        k: int = TOP_K,
        exact_local_search: bool = False,
    ) -> None:
        self.num_shards = len(shards)
        self.bus = bus
        self.m = int(m)
        self.k = int(k)
        self.exact_local_search = bool(exact_local_search)

        # Published summaries held at coordinator (initialized from setup summaries)
        self.published_summaries: List[ShardSummary] = [
            s.published_summary.copy() for s in shards
        ]

        # Cumulative count of how often each shard was routed in the top m for past test queries (for QL)
        self.past_top_m_counts: np.ndarray = np.zeros(self.num_shards, dtype=np.int64)

        # Local buffer of pushed vectors at the coordinator (used by the push_vectors policy)
        self._pushed_vecs_list: List[np.ndarray] = []
        self._pushed_ids_list: List[np.ndarray] = []
        self._pushed_vecs_cache: Optional[np.ndarray] = np.empty((0, DIM), dtype=np.float32)
        self._pushed_ids_cache: Optional[np.ndarray] = np.empty(0, dtype=np.int64)
        self._pushed_id_set: set[int] = set()

    def receive_summary(self, epoch: int, shard: Shard) -> None:
        """Refresh a shard's published summary at the coordinator and log bytes on the bus."""
        new_summary = shard.publish_candidate_summary(epoch)
        self.published_summaries[shard.shard_id] = new_summary.copy()
        self.bus.log_summary_refresh(epoch=epoch, shard_id=shard.shard_id, c=new_summary.num_centers)

    def receive_pushed_vectors(
        self,
        epoch: int,
        vectors: np.ndarray,
        doc_ids: np.ndarray,
        shard_ids: np.ndarray,
    ) -> int:
        """Store pushed vectors locally at the coordinator and log 1,536 bytes each on the bus."""
        if len(vectors) == 0:
            return 0
        vecs = np.ascontiguousarray(vectors, dtype=np.float32)
        ids = np.ascontiguousarray(doc_ids, dtype=np.int64)
        sids = np.asarray(shard_ids, dtype=np.int64)

        kept_vecs = []
        kept_ids = []
        for v, did, sid in zip(vecs, ids, sids):
            did_int = int(did)
            self.bus.log_push_vector(epoch=epoch, shard_id=int(sid), doc_id=did_int)
            if did_int not in self._pushed_id_set:
                self._pushed_id_set.add(did_int)
                kept_vecs.append(v)
                kept_ids.append(did_int)

        if kept_vecs:
            self._pushed_vecs_list.append(np.asarray(kept_vecs, dtype=np.float32))
            self._pushed_ids_list.append(np.asarray(kept_ids, dtype=np.int64))
            self._pushed_vecs_cache = None
            self._pushed_ids_cache = None
        return len(vecs)

    @property
    def pushed_vectors(self) -> np.ndarray:
        if self._pushed_vecs_cache is None:
            if not self._pushed_vecs_list:
                self._pushed_vecs_cache = np.empty((0, DIM), dtype=np.float32)
            else:
                self._pushed_vecs_cache = np.ascontiguousarray(
                    np.vstack(self._pushed_vecs_list), dtype=np.float32
                )
        return self._pushed_vecs_cache

    @property
    def pushed_ids(self) -> np.ndarray:
        if self._pushed_ids_cache is None:
            if not self._pushed_ids_list:
                self._pushed_ids_cache = np.empty(0, dtype=np.int64)
            else:
                self._pushed_ids_cache = np.ascontiguousarray(
                    np.concatenate(self._pushed_ids_list), dtype=np.int64
                )
        return self._pushed_ids_cache

    def score_shards_for_queries(
        self,
        queries: np.ndarray,
        summaries: Optional[Sequence[ShardSummary]] = None,
    ) -> np.ndarray:
        """Compute (Q, S) matrix of shard scores = max dot product with each shard's centers."""
        q = np.ascontiguousarray(queries, dtype=np.float32)
        if q.ndim == 1:
            q = q.reshape(1, DIM)
        sums = self.published_summaries if summaries is None else summaries
        num_q = q.shape[0]
        scores = np.empty((num_q, len(sums)), dtype=np.float32)
        for s_idx, summary in enumerate(sums):
            scores[:, s_idx] = summary.query_scores(q)
        return scores

    def rank_shards_for_queries(
        self,
        queries: np.ndarray,
        summaries: Optional[Sequence[ShardSummary]] = None,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Return (ranked_shard_ids, ranked_scores) of shape (Q, S), descending by score.

        Uses stable mergesort so ties break deterministically by lowest shard_id.
        """
        scores = self.score_shards_for_queries(queries, summaries=summaries)
        ranked_shards = np.argsort(-scores, axis=1, kind="mergesort")
        ranked_scores = np.take_along_axis(scores, ranked_shards, axis=1)
        return ranked_shards, ranked_scores

    def route_queries(
        self,
        queries: np.ndarray,
        m: Optional[int] = None,
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Route queries to top-m shards using published summaries.

        Returns:
            top_m_shards: shape (Q, m_eff) int64 shard IDs
            ranked_shards: shape (Q, S) int64 all shard IDs in descending score order
            ranked_scores: shape (Q, S) float32 all shard scores in descending order
        """
        m_eff = min(self.num_shards, self.m if m is None else int(m))
        ranked_shards, ranked_scores = self.rank_shards_for_queries(queries)
        top_m_shards = ranked_shards[:, :m_eff]
        return top_m_shards, ranked_shards, ranked_scores

    @staticmethod
    def merge_candidates(
        candidate_ids_list: Sequence[np.ndarray],
        candidate_scores_list: Sequence[np.ndarray],
        k: int = TOP_K,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Merge candidate (doc_id, score) arrays into final top-k unique doc_ids."""
        if not candidate_ids_list:
            return np.empty(0, dtype=np.int64), np.empty(0, dtype=np.float32)
        all_ids = np.concatenate(candidate_ids_list)
        all_scores = np.concatenate(candidate_scores_list)
        if len(all_ids) == 0:
            return np.empty(0, dtype=np.int64), np.empty(0, dtype=np.float32)

        # Sort descending by score, breaking ties deterministically by doc_id
        order = np.lexsort((all_ids, -all_scores))
        sorted_ids = all_ids[order]
        sorted_scores = all_scores[order]

        # Deduplicate preserving first (highest-scoring) occurrence
        out_ids: List[int] = []
        out_scores: List[float] = []
        seen: set[int] = set()
        for did, sc in zip(sorted_ids, sorted_scores):
            did_int = int(did)
            if did_int not in seen:
                seen.add(did_int)
                out_ids.append(did_int)
                out_scores.append(float(sc))
                if len(out_ids) == k:
                    break
        return np.asarray(out_ids, dtype=np.int64), np.asarray(out_scores, dtype=np.float32)

    def execute_queries(
        self,
        epoch: int,
        queries: np.ndarray,
        shards: Sequence[Shard],
        m: Optional[int] = None,
        extra_shards_per_query: Optional[Dict[int, List[int]]] = None,
        record_popularity: bool = True,
    ) -> Tuple[List[np.ndarray], List[np.ndarray], np.ndarray]:
        """Execute a batch of test queries across top-m routed shards (plus any extra shards).

        Logs workload query bytes on `self.bus` for the base top-m shards and budgeted
        `extra_search` bytes for any `extra_shards_per_query`. Also searches locally pushed
        vectors at the coordinator (if any) without additional query network bytes.

        Returns:
            results_ids: list of length Q of top-k doc_id arrays
            results_scores: list of length Q of top-k score arrays
            searched_shards_mask: boolean array of shape (Q, S) indicating which shards were searched
        """
        q_arr = np.ascontiguousarray(queries, dtype=np.float32)
        num_q = q_arr.shape[0]
        m_eff = min(self.num_shards, self.m if m is None else int(m))
        top_m_shards, _, _ = self.route_queries(q_arr, m=m_eff)

        if record_popularity and m_eff > 0:
            counts = np.bincount(top_m_shards.ravel(), minlength=self.num_shards)
            self.past_top_m_counts += counts

        p_vecs = self.pushed_vectors
        p_ids = self.pushed_ids
        has_pushed = len(p_vecs) > 0

        results_ids: List[np.ndarray] = []
        results_scores: List[np.ndarray] = []
        searched_mask = np.zeros((num_q, self.num_shards), dtype=bool)

        for q_idx in range(num_q):
            q_vec = q_arr[q_idx]
            cand_ids: List[np.ndarray] = []
            cand_scores: List[np.ndarray] = []

            # Base top-m shard searches (workload traffic)
            for s_id in top_m_shards[q_idx]:
                s_int = int(s_id)
                searched_mask[q_idx, s_int] = True
                self.bus.log_query_search(epoch=epoch, shard_id=s_int, query_idx=q_idx)
                ids_s, scores_s = shards[s_int].search(
                    q_vec, k=self.k, exact=self.exact_local_search
                )
                cand_ids.append(ids_s)
                cand_scores.append(scores_s)

            # Extra shard searches funded by maintenance budget B (if policy is extra_search)
            if extra_shards_per_query and q_idx in extra_shards_per_query:
                for extra_sid in extra_shards_per_query[q_idx]:
                    s_int = int(extra_sid)
                    if not searched_mask[q_idx, s_int]:
                        searched_mask[q_idx, s_int] = True
                        self.bus.log_extra_search(epoch=epoch, shard_id=s_int, query_idx=q_idx)
                        ids_s, scores_s = shards[s_int].search(
                            q_vec, k=self.k, exact=self.exact_local_search
                        )
                        cand_ids.append(ids_s)
                        cand_scores.append(scores_s)

            # Local coordinator search over pushed vectors (if policy is push_vectors)
            if has_pushed:
                scores_p = (p_vecs @ q_vec).astype(np.float32)
                k_p = min(self.k, len(scores_p))
                if k_p == len(scores_p):
                    top_p = np.argsort(-scores_p, kind="mergesort")
                else:
                    part = np.argpartition(-scores_p, k_p - 1)[:k_p]
                    top_p = part[np.argsort(-scores_p[part], kind="mergesort")]
                cand_ids.append(p_ids[top_p])
                cand_scores.append(scores_p[top_p])

            merged_ids, merged_scores = self.merge_candidates(cand_ids, cand_scores, k=self.k)
            results_ids.append(merged_ids)
            results_scores.append(merged_scores)

        return results_ids, results_scores, searched_mask
