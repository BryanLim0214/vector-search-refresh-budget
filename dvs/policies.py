"""Refresh policies, equal-bytes alternatives, and reference policies.

Implements all 11 methods specified in proposal/proposal.tex Section 4, audit/AGY_TASK_B1_build.md,
and audit/AGY_TASK_B2_fixes_pilot.md:
Refresh rules (sending whole 24,576-byte summaries under budget B):
  1. round_robin
  2. data_change
  3. recovered_neighbor
  4. ql (Shokouhi 2007 query-log popularity)
  5. ss (Shokouhi 2007 shard size)
Equal-bytes alternatives (under budget B):
  6. extra_search (1,656 bytes per extra shard search, prioritized by smallest gap s_(m) - s_(m+1))
  7. push_vectors (1,544 bytes per pushed vector: 1,536-byte vector + 8-byte ID, newest vectors pushed first, searched at coordinator)
References (no byte limit, reported for context):
  8. never_refresh (published summaries never change after setup)
  9. always_fresh (unbounded summary refresh of all S shards every epoch; fresh-summary reference)
 10. oracle_routing (route each query to the m shards holding the most ground-truth top-10 neighbors)
 11. all_shards (search all S shards for every query)
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np

from dvs.bus import (
    ByteBus,
    PUSH_VECTOR_BYTES,
    SAMPLE_QUERIES_COUNT,
    SETUP_SAMPLE_VECTORS_PER_SHARD_BYTES,
    SHARD_SEARCH_BYTES,
    SUMMARY_BYTES,
    TOP_K,
    VECTOR_BYTES,
    recovered_neighbor_overhead_bytes,
)
from dvs.coordinator import Coordinator
from dvs.shards import Shard

REFRESH_POLICIES = frozenset({
    "round_robin",
    "data_change",
    "recovered_neighbor",
    "ql",
    "ss",
})

ALTERNATIVE_POLICIES = frozenset({
    "extra_search",
    "push_vectors",
})

REFERENCE_POLICIES = frozenset({
    "never_refresh",
    "always_fresh",
    "oracle_routing",
    "all_shards",
})

# Backwards-compatible alias
BOUND_POLICIES = REFERENCE_POLICIES

ALL_POLICIES = REFRESH_POLICIES | ALTERNATIVE_POLICIES | REFERENCE_POLICIES

AGING_PER_EPOCH: float = 0.5
STARVATION_CAP_EPOCHS: int = 8
AGING_MODES = frozenset({"starvation_cap", "additive"})


def compute_arithmetic_starvation_cap(num_shards: int, num_summaries_per_epoch: int) -> int:
    """Return starvation cap A = ceil(2 * S / k), where k is summaries a rule can send per epoch."""
    if num_summaries_per_epoch <= 0:
        return int(2 * num_shards)
    return int(math.ceil((2.0 * float(num_shards)) / float(num_summaries_per_epoch)))


def scale_scores_unit_interval(raw_scores: np.ndarray) -> np.ndarray:
    """Scale raw scores across shards to [0, 1]. Returns zeros if max == min."""
    scores = np.asarray(raw_scores, dtype=np.float64)
    if len(scores) == 0:
        return np.empty(0, dtype=np.float64)
    s_min = float(np.min(scores))
    s_max = float(np.max(scores))
    if s_max <= s_min + 1e-15:
        return np.zeros_like(scores, dtype=np.float64)
    return (scores - s_min) / (s_max - s_min)


@dataclass
class PolicyEpochDecision:
    """Record of a policy's actions and diagnostic scores for a single epoch."""

    policy: str
    epoch: int
    refreshed_shards: List[int] = field(default_factory=list)
    raw_scores: Optional[np.ndarray] = None
    scaled_scores: Optional[np.ndarray] = None
    final_priorities: Optional[np.ndarray] = None
    estimated_recovered_neighbors: Optional[np.ndarray] = None
    lift_in_flips_per_shard: Optional[np.ndarray] = None
    dropout_flips_per_shard: Optional[np.ndarray] = None
    extra_shards_per_query: Optional[Dict[int, List[int]]] = None
    num_extra_searches: int = 0
    num_pushed_vectors: int = 0
    overhead_bytes: int = 0
    num_forced_by_cap: int = 0
    effective_starvation_cap: int = 0


class PolicyController:
    """Executes per-epoch maintenance/adaptation under budget B."""

    def __init__(
        self,
        policy: str,
        budget_bytes: int,
        num_shards: int,
        m: int,
        num_epochs: int = 50,
        charge_setup_to_budget: bool = False,
        aging_mode: str = "starvation_cap",
        starvation_cap_epochs: Optional[int] = None,
        aging_per_epoch: float = AGING_PER_EPOCH,
    ) -> None:
        if policy not in ALL_POLICIES:
            raise ValueError(f"Unknown policy '{policy}'. Expected one of {sorted(ALL_POLICIES)}")
        if aging_mode not in AGING_MODES:
            raise ValueError(f"Unknown aging_mode '{aging_mode}'. Expected one of {sorted(AGING_MODES)}")
        self.policy = policy
        self.budget_bytes = int(budget_bytes)
        self.num_shards = int(num_shards)
        self.m = int(m)
        self.num_epochs = int(num_epochs)
        self.charge_setup_to_budget = bool(charge_setup_to_budget)
        self.aging_mode = str(aging_mode)
        self.starvation_cap_epochs: Optional[int] = (
            int(starvation_cap_epochs) if starvation_cap_epochs is not None else None
        )
        self.aging_per_epoch = float(aging_per_epoch)
        self._rr_cursor: int = 0

        # Track unpushed streamed vectors in arrival order: list of (doc_id, shard_id, vector)
        self._unpushed_stream_buffer: List[Tuple[int, int, np.ndarray]] = []

    def summaries_per_epoch(self, num_sample_queries: int = SAMPLE_QUERIES_COUNT) -> int:
        """Return k, the number of summaries this rule can send per epoch at budget B after its own overhead."""
        if self.policy == "always_fresh":
            return self.num_shards
        if self.policy not in REFRESH_POLICIES:
            return min(self.num_shards, max(0, self.budget_bytes // SUMMARY_BYTES))
        rem = self.budget_bytes
        if self.policy == "recovered_neighbor":
            m_eff = min(self.num_shards, self.m)
            rn_overhead = recovered_neighbor_overhead_bytes(
                num_shards=self.num_shards,
                m=m_eff,
                num_sample_queries=num_sample_queries,
            )
            amortized_setup = 0
            if self.charge_setup_to_budget:
                total_setup = SETUP_SAMPLE_VECTORS_PER_SHARD_BYTES * self.num_shards
                amortized_setup = (total_setup + self.num_epochs - 1) // self.num_epochs
            rem -= rn_overhead + amortized_setup
        return min(self.num_shards, max(0, rem // SUMMARY_BYTES))

    def effective_starvation_cap(self, num_sample_queries: int = SAMPLE_QUERIES_COUNT) -> int:
        """Return effective starvation cap A (fixed integer if set, else ceil(2 * S / k))."""
        if self.starvation_cap_epochs is not None:
            return int(self.starvation_cap_epochs)
        k = self.summaries_per_epoch(num_sample_queries=num_sample_queries)
        return compute_arithmetic_starvation_cap(self.num_shards, k)

    def record_new_arrivals(
        self,
        vectors: np.ndarray,
        doc_ids: np.ndarray,
        shard_ids: np.ndarray,
    ) -> None:
        """Record newly inserted stream vectors (in arrival order) for push_vectors."""
        if self.policy != "push_vectors":
            return
        for v, did, sid in zip(vectors, doc_ids, shard_ids):
            self._unpushed_stream_buffer.append(
                (int(did), int(sid), np.ascontiguousarray(v, dtype=np.float32))
            )

    def compute_recovered_neighbor_raw_scores(
        self,
        shards: Sequence[Shard],
        coordinator: Coordinator,
        sample_queries: np.ndarray,
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Compute raw recovered-neighbor scores, lift-in flips, and drop-out flips across shards.

        Follows proposal Section 4:
        1. Coordinator routes each of the Q_s=64 sample queries to its top m shards using
           published summaries, searches those top m shards for their 10 best scores, and
           determines each sample query's m-th shard score and 10th-best result score.
        2. Each shard compares its published summary and candidate summary (assuming other
           summaries stay fixed within the epoch).
        3. A shard scores only flips lifting it into the top m, weighted by how many of its
           NEW vectors beat that sample query's 10th-best result score.
        4. Drop-out flips (where a shard in the top m would drop out under its candidate
           summary) are logged, not scored.
        """
        q_s = np.ascontiguousarray(sample_queries, dtype=np.float32)
        num_qs = q_s.shape[0]
        m_eff = min(self.num_shards, self.m)

        # Published shard scores for sample queries: shape (Q_s, S)
        pub_scores = coordinator.score_shards_for_queries(q_s)
        ranked_shards, ranked_scores = coordinator.rank_shards_for_queries(q_s)
        top_m_shards = ranked_shards[:, :m_eff]

        # m-th shard score cutoff for each sample query
        mth_shard_scores = ranked_scores[:, m_eff - 1].copy()
        # (m+1)-th shard score cutoff (used to detect drop-out flips when m < S)
        if m_eff < self.num_shards:
            m_plus_1_scores = ranked_scores[:, m_eff].copy()
        else:
            m_plus_1_scores = np.full(num_qs, -np.inf, dtype=np.float32)

        # Search each sample query on its top m shards to get the 10th-best result score
        # First batch sample queries by shard to avoid redundant HNSW calls
        shard_to_q_indices: Dict[int, List[int]] = {s: [] for s in range(self.num_shards)}
        for q_idx in range(num_qs):
            for s_id in top_m_shards[q_idx]:
                shard_to_q_indices[int(s_id)].append(q_idx)

        per_query_candidate_scores: List[List[np.ndarray]] = [[] for _ in range(num_qs)]
        for s_id, q_indices in shard_to_q_indices.items():
            if not q_indices:
                continue
            sub_q = q_s[q_indices]
            batch_top_scores = shards[s_id].search_batch_scores_only(
                sub_q, k=TOP_K, exact=coordinator.exact_local_search
            )
            for local_i, q_idx in enumerate(q_indices):
                valid_scores = batch_top_scores[local_i]
                valid_scores = valid_scores[np.isfinite(valid_scores)]
                per_query_candidate_scores[q_idx].append(valid_scores)

        tenth_best_scores = np.full(num_qs, -np.inf, dtype=np.float32)
        for q_idx in range(num_qs):
            if per_query_candidate_scores[q_idx]:
                merged = np.concatenate(per_query_candidate_scores[q_idx])
                if len(merged) >= TOP_K:
                    part = np.partition(-merged, TOP_K - 1)
                    tenth_best_scores[q_idx] = float(-part[TOP_K - 1])
                elif len(merged) > 0:
                    tenth_best_scores[q_idx] = float(np.min(merged))

        # Each shard evaluates lift-in and drop-out flips using its candidate summary
        raw_scores = np.zeros(self.num_shards, dtype=np.float64)
        lift_in_flips = np.zeros(self.num_shards, dtype=np.int64)
        dropout_flips = np.zeros(self.num_shards, dtype=np.int64)

        for s_id, shard in enumerate(shards):
            cand_scores_s = shard.candidate_summary.query_scores(q_s)  # (Q_s,)
            in_top_m_pub = np.any(top_m_shards == s_id, axis=1)  # (Q_s,)

            # Lift-in flip: not in top m under published summary, and candidate score beats
            # the m-th published shard score (or ties with smaller shard_id)
            mth_sid = ranked_shards[:, m_eff - 1]
            beats_mth = (cand_scores_s > mth_shard_scores) | (
                (cand_scores_s == mth_shard_scores) & (s_id < mth_sid)
            )
            lift_mask = (~in_top_m_pub) & beats_mth
            lift_in_flips[s_id] = int(np.sum(lift_mask))

            # Drop-out flip: in top m under published summary, and candidate score falls below
            # the (m+1)-th published shard score (logged, not scored)
            if m_eff < self.num_shards:
                mp1_sid = ranked_shards[:, m_eff]
                falls_below = (cand_scores_s < m_plus_1_scores) | (
                    (cand_scores_s == m_plus_1_scores) & (s_id > mp1_sid)
                )
                drop_mask = in_top_m_pub & falls_below
                dropout_flips[s_id] = int(np.sum(drop_mask))

            # Weight each lift-in flip by how many of shard's NEW vectors beat tenth_best_scores[q]
            if lift_in_flips[s_id] > 0 and shard.num_new_vectors > 0:
                lifted_q_indices = np.nonzero(lift_mask)[0]
                lifted_queries = q_s[lifted_q_indices]  # (L, 384)
                lifted_cutoffs = tenth_best_scores[lifted_q_indices].reshape(-1, 1)  # (L, 1)
                new_vecs = shard.new_vectors  # (N_new, 384)
                dots = lifted_queries @ new_vecs.T  # (L, N_new)
                recovered_counts = np.sum(dots > lifted_cutoffs, axis=1)
                raw_scores[s_id] = float(np.sum(recovered_counts))

        return raw_scores, lift_in_flips, dropout_flips

    @staticmethod
    def compute_data_change_raw_scores(shards: Sequence[Shard]) -> np.ndarray:
        """Compute data_change score: (new_vectors / shard_size) * mean_distance_to_published."""
        raw = np.zeros(len(shards), dtype=np.float64)
        for s_id, shard in enumerate(shards):
            if shard.num_new_vectors == 0 or shard.size == 0:
                raw[s_id] = 0.0
                continue
            dists = shard.published_summary.nearest_center_distance(shard.new_vectors)
            mean_dist = float(np.mean(dists)) if len(dists) > 0 else 0.0
            frac_new = float(shard.num_new_vectors) / float(shard.size)
            raw[s_id] = frac_new * mean_dist
        return raw

    def plan_extra_searches(
        self,
        queries: np.ndarray,
        coordinator: Coordinator,
    ) -> Tuple[Dict[int, List[int]], int]:
        """Allocate budget B to extra shard searches (1,656 bytes each) on closest (m, m+1) gaps."""
        max_extra = self.budget_bytes // SHARD_SEARCH_BYTES
        if max_extra <= 0 or self.m >= self.num_shards:
            return {}, 0

        _, ranked_shards, ranked_scores = coordinator.route_queries(queries, m=self.m)
        num_q = queries.shape[0]
        m_eff = min(self.num_shards, self.m)

        # Build candidate (rank_offset, gap, q_idx, shard_id) tuples
        candidates: List[Tuple[int, float, int, int]] = []
        for rank_idx in range(m_eff, self.num_shards):
            rank_offset = rank_idx - m_eff
            for q_idx in range(num_q):
                gap = float(ranked_scores[q_idx, m_eff - 1] - ranked_scores[q_idx, rank_idx])
                s_id = int(ranked_shards[q_idx, rank_idx])
                candidates.append((rank_offset, gap, q_idx, s_id))

        candidates.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
        selected = candidates[:max_extra]

        extra_per_q: Dict[int, List[int]] = {}
        for _, _, q_idx, s_id in selected:
            extra_per_q.setdefault(q_idx, []).append(s_id)

        return extra_per_q, len(selected)

    def step_epoch(
        self,
        epoch: int,
        shards: Sequence[Shard],
        coordinator: Coordinator,
        bus: ByteBus,
        sample_queries: np.ndarray,
        epoch_test_queries: Optional[np.ndarray] = None,
        rn_estimates: Optional[Tuple[np.ndarray, np.ndarray, np.ndarray]] = None,
    ) -> PolicyEpochDecision:
        """Execute the policy's maintenance or pre-query adaptation for `epoch`."""
        decision = PolicyEpochDecision(policy=self.policy, epoch=epoch)

        # Every epoch, shards advance their age since last refresh by 1
        for shard in shards:
            shard.epochs_since_refresh += 1

        if self.policy in ("all_shards", "never_refresh", "oracle_routing"):
            return decision

        if self.policy == "extra_search":
            if epoch_test_queries is None:
                raise ValueError("epoch_test_queries must be provided for policy='extra_search'")
            extra_map, count = self.plan_extra_searches(epoch_test_queries, coordinator)
            decision.extra_shards_per_query = extra_map
            decision.num_extra_searches = count
            return decision

        if self.policy == "push_vectors":
            max_push = self.budget_bytes // PUSH_VECTOR_BYTES
            if max_push > 0 and self._unpushed_stream_buffer:
                # Push newest vectors first (from the end of the arrival buffer)
                n_take = min(max_push, len(self._unpushed_stream_buffer))
                picked = [self._unpushed_stream_buffer.pop() for _ in range(n_take)]
                p_ids = np.array([item[0] for item in picked], dtype=np.int64)
                p_sids = np.array([item[1] for item in picked], dtype=np.int64)
                p_vecs = np.vstack([item[2] for item in picked]).astype(np.float32)
                coordinator.receive_pushed_vectors(
                    epoch=epoch,
                    vectors=p_vecs,
                    doc_ids=p_ids,
                    shard_ids=p_sids,
                )
                decision.num_pushed_vectors = n_take
            return decision

        # Update candidate summaries on all shards (used by always_fresh and refresh rules)
        for shard in shards:
            shard.update_candidate_summary(epoch)

        if self.policy == "always_fresh":
            for shard in shards:
                coordinator.receive_summary(epoch=epoch, shard=shard)
                decision.refreshed_shards.append(shard.shard_id)
            return decision

        # Charge recovered-neighbor probing CPU time only to recovered_neighbor;
        # for other refresh policies, accept pre-computed rn_estimates (if provided outside timer)
        if self.policy == "recovered_neighbor":
            if rn_estimates is None:
                rn_raw, lift_flips, drop_flips = self.compute_recovered_neighbor_raw_scores(
                    shards=shards,
                    coordinator=coordinator,
                    sample_queries=sample_queries,
                )
            else:
                rn_raw, lift_flips, drop_flips = rn_estimates
            decision.estimated_recovered_neighbors = rn_raw.copy()
            decision.lift_in_flips_per_shard = lift_flips.copy()
            decision.dropout_flips_per_shard = drop_flips.copy()
        elif rn_estimates is not None:
            rn_raw, lift_flips, drop_flips = rn_estimates
            decision.estimated_recovered_neighbors = rn_raw.copy()
            decision.lift_in_flips_per_shard = lift_flips.copy()
            decision.dropout_flips_per_shard = drop_flips.copy()
        else:
            rn_raw = np.zeros(self.num_shards, dtype=np.float64)

        remaining_budget = self.budget_bytes

        if self.policy == "recovered_neighbor":
            m_eff = min(self.num_shards, self.m)
            rn_overhead = recovered_neighbor_overhead_bytes(
                num_shards=self.num_shards,
                m=m_eff,
                num_sample_queries=len(sample_queries),
            )
            amortized_setup = 0
            if self.charge_setup_to_budget:
                total_setup = SETUP_SAMPLE_VECTORS_PER_SHARD_BYTES * self.num_shards
                amortized_setup = (total_setup + self.num_epochs - 1) // self.num_epochs

            total_overhead = rn_overhead + amortized_setup
            if remaining_budget < total_overhead:
                # Budget cannot even cover full probing overhead; cannot send any summaries
                return decision

            bus.log_recovered_neighbor_overhead(
                epoch=epoch,
                num_shards=self.num_shards,
                m=m_eff,
                num_sample_queries=len(sample_queries),
            )
            if amortized_setup > 0:
                bus.log(
                    epoch=epoch,
                    category="setup_sample_vectors_amortized",
                    num_bytes=amortized_setup,
                    src="coordinator",
                    dst="shards",
                    charged_to_budget=True,
                )
            remaining_budget -= total_overhead
            decision.overhead_bytes = total_overhead
            raw_scores = rn_raw
        elif self.policy == "data_change":
            raw_scores = self.compute_data_change_raw_scores(shards)
        elif self.policy == "ql":
            raw_scores = coordinator.past_top_m_counts.astype(np.float64)
        elif self.policy == "ss":
            raw_scores = np.array([s.size for s in shards], dtype=np.float64)
        elif self.policy == "round_robin":
            raw_scores = np.zeros(self.num_shards, dtype=np.float64)
        else:
            raise ValueError(f"Unhandled refresh policy: {self.policy}")

        num_summaries_to_send = min(self.num_shards, max(0, remaining_budget // SUMMARY_BYTES))
        cap_epochs = (
            int(self.starvation_cap_epochs)
            if self.starvation_cap_epochs is not None
            else compute_arithmetic_starvation_cap(self.num_shards, num_summaries_to_send)
        )
        decision.effective_starvation_cap = cap_epochs
        ages = np.array([s.epochs_since_refresh for s in shards], dtype=np.float64)
        scaled = scale_scores_unit_interval(raw_scores)
        if self.aging_mode == "additive":
            priorities = scaled + self.aging_per_epoch * ages
        else:
            priorities = scaled.copy()

        decision.raw_scores = raw_scores.copy()
        decision.scaled_scores = scaled.copy()
        decision.final_priorities = priorities.copy()

        if num_summaries_to_send <= 0:
            return decision

        if self.policy == "round_robin":
            selected_shards: List[int] = []
            if self.aging_mode == "starvation_cap":
                for offset in range(self.num_shards):
                    cand_sid = (self._rr_cursor + offset) % self.num_shards
                    if (
                        shards[cand_sid].num_new_vectors > 0
                        and shards[cand_sid].epochs_since_refresh > cap_epochs
                    ):
                        selected_shards.append(cand_sid)
                        if len(selected_shards) == num_summaries_to_send:
                            break
            if len(selected_shards) < num_summaries_to_send:
                for offset in range(self.num_shards):
                    cand_sid = (self._rr_cursor + offset) % self.num_shards
                    if shards[cand_sid].num_new_vectors > 0 and cand_sid not in selected_shards:
                        selected_shards.append(cand_sid)
                        if len(selected_shards) == num_summaries_to_send:
                            break
            if len(selected_shards) < num_summaries_to_send:
                for offset in range(self.num_shards):
                    cand_sid = (self._rr_cursor + offset) % self.num_shards
                    if cand_sid not in selected_shards:
                        selected_shards.append(cand_sid)
                        if len(selected_shards) == num_summaries_to_send:
                            break
            self._rr_cursor = (self._rr_cursor + num_summaries_to_send) % self.num_shards
        else:
            has_new = np.array([1 if s.num_new_vectors > 0 else 0 for s in shards], dtype=np.int64)
            shard_ids = np.arange(self.num_shards, dtype=np.int64)
            if self.aging_mode == "starvation_cap":
                # Starvation cap: any shard waiting > A epochs (with new vectors) is refreshed first,
                # with ties broken by the rule's score (priorities), then shard_id.
                is_starved = (ages > cap_epochs).astype(np.int64)
                order = np.lexsort((shard_ids, -priorities, -is_starved, -has_new))
            else:
                # Legacy additive aging: priority = scaled_score + 0.5 * epochs_waited
                order = np.lexsort((shard_ids, -priorities, -has_new))
            selected_shards = [int(sid) for sid in order[:num_summaries_to_send]]

        forced_count = 0
        for sid in selected_shards:
            if (
                self.aging_mode == "starvation_cap"
                and shards[sid].num_new_vectors > 0
                and shards[sid].epochs_since_refresh > cap_epochs
            ):
                forced_count += 1
            coordinator.receive_summary(epoch=epoch, shard=shards[sid])
            decision.refreshed_shards.append(sid)
        decision.num_forced_by_cap = forced_count

        return decision
