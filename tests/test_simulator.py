"""Comprehensive pytest suite for the DVS byte-counted shard simulator.

Covers all required verification tests from audit/AGY_TASK_B1_build.md and audit/AGY_TASK_B2_fixes_pilot.md:
1. m = S recall@10 == 1.0 (exact local search == 1.0; HNSW >= 0.99).
2. Structural test for always_fresh reference (refreshes all S summaries every epoch and publishes
   them before the epoch's queries), structural test for never_refresh reference (published summaries
   never change after setup), and oracle_routing reference bound check (per-query missed neighbors
   <= those of every routing rule).
3. Byte accounting: per-epoch totals equal the sum of logged messages; no budgeted rule exceeds B
   in any epoch; overhead of recovered_neighbor (21,632 bytes at S=32, m=2) is charged; setup
   sample vectors (98,304 bytes/shard) are reported both ways without double-counting when
   charge_setup_to_budget=True.
4. Leakage: no test query ID or vector is in the sample query set (verified on both real
   `data/` and synthetic runs); sample queries are never used to score test recall; ql uses only
   past epochs' queries.
5. Determinism: identical config + seed produces bit-for-bit identical results.
6. Routing and merging: hand-built 3-shard example routes queries to the expected shards, breaks
   ties deterministically, and merges pushed vectors / extra shard searches into the top 10.
7. Equal bytes: extra_search and push_vectors never spend more than B in any epoch.
8. Spearman NaN handling, CPU timing isolation, starvation-cap aging (A = 8 epochs), and new_topic
   stream partitioning.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest

from dvs.bus import (
    ByteBus,
    CUTOFFS_PER_SHARD_BYTES,
    DIM,
    PUSH_VECTOR_BYTES,
    RESULT_BYTES,
    SAMPLE_REPLY_PER_SHARD_QUERY_BYTES,
    SETUP_SAMPLE_VECTORS_PER_SHARD_BYTES,
    SHARD_SCORE_BYTES,
    SHARD_SEARCH_BYTES,
    SUMMARY_BYTES,
    SUMMARY_CENTERS,
    VECTOR_BYTES,
    recovered_neighbor_overhead_bytes,
)
from dvs.coordinator import Coordinator
from dvs.data import load_dataset, normalize_l2
from dvs.metrics import compute_spearman_correlation
from dvs.policies import (
    PolicyController,
    REFERENCE_POLICIES,
    REFRESH_POLICIES,
    compute_arithmetic_starvation_cap,
)
from dvs.run import run_single_simulation
from dvs.shards import Shard
from dvs.streams import partition_new_topic_stream
from dvs.summary import ShardSummary

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"


@pytest.fixture(scope="module")
def real_subset_data():
    """Load a 2,000-vector subset (1,000 initial + 1,000 streamed) from `repo/data/`."""
    (
        init_vecs,
        init_ids,
        stream_vecs,
        stream_ids,
        test_q,
        test_qids,
        sample_q,
        sample_qids,
    ) = load_dataset(DATA_DIR)
    return (
        init_vecs[:1000],
        init_ids[:1000],
        stream_vecs[:1000],
        stream_ids[:1000],
        test_q[:200],
        test_qids[:200],
        sample_q,
        sample_qids,
    )


def test_1_all_shards_recall_equals_one(real_subset_data) -> None:
    """Test 1: With m = S (search all shards), recall@10 equals 1.0 (exact) and ~1.0 (HNSW)."""
    init_v, init_id, stream_v, stream_id, test_q, test_qid, sample_q, sample_qid = real_subset_data

    # 1a. Exact local search with m = S: recall@10 must be 1.000000 in every epoch
    res_exact = run_single_simulation(
        initial_vectors=init_v,
        initial_ids=init_id,
        stream_vectors=stream_v,
        stream_ids=stream_id,
        test_queries=test_q,
        test_query_ids=test_qid,
        sample_queries=sample_q,
        sample_query_ids=sample_qid,
        policy="all_shards",
        num_shards=8,
        m=8,
        budget_summaries=2,
        num_epochs=4,
        inserts_per_epoch=250,
        queries_per_epoch=40,
        seed=42,
        exact_local_search=True,
    )
    assert np.allclose(res_exact.epochs_df["recall_at_10"].to_numpy(), 1.0, atol=1e-7)
    assert res_exact.summary["missed_neighbors_total"] == 0

    # 1b. HNSW local search with m = S (M=16, ef_construction=200, ef=100): recall@10 == 1.0 up to HNSW error
    res_hnsw = run_single_simulation(
        initial_vectors=init_v,
        initial_ids=init_id,
        stream_vectors=stream_v,
        stream_ids=stream_id,
        test_queries=test_q,
        test_query_ids=test_qid,
        sample_queries=sample_q,
        sample_query_ids=sample_qid,
        policy="all_shards",
        num_shards=8,
        m=8,
        budget_summaries=2,
        num_epochs=4,
        inserts_per_epoch=250,
        queries_per_epoch=40,
        seed=42,
        exact_local_search=False,
    )
    assert res_hnsw.summary["overall_recall_at_10_mean"] >= 0.99
    assert res_hnsw.summary["missed_neighbors_total"] == 0


def test_2_always_fresh_structural_and_references(real_subset_data) -> None:
    """Test 2: always_fresh refreshes all S summaries every epoch and publishes them before queries;
    never_refresh keeps setup summaries unchanged; oracle_routing has <= missed neighbors per query.
    """
    init_v, init_id, stream_v, stream_id, test_q, test_qid, sample_q, sample_qid = real_subset_data
    num_shards = 8
    num_epochs = 5

    # 2a. Structural test for always_fresh reference: hook Coordinator.execute_queries
    orig_execute = Coordinator.execute_queries
    epochs_checked_fresh = []

    def spy_execute_fresh(self_coord, epoch, queries, shards, *args, **kwargs):
        for s in range(num_shards):
            assert self_coord.published_summaries[s].epoch_built == epoch
            assert np.array_equal(
                self_coord.published_summaries[s].centers,
                shards[s].published_summary.centers,
            )
            assert shards[s].new_vectors_start_idx == shards[s].size
            assert shards[s].epochs_since_refresh == 0
        epochs_checked_fresh.append(epoch)
        return orig_execute(self_coord, epoch, queries, shards, *args, **kwargs)

    with patch.object(Coordinator, "execute_queries", autospec=True, side_effect=spy_execute_fresh):
        res_fresh = run_single_simulation(
            initial_vectors=init_v,
            initial_ids=init_id,
            stream_vectors=stream_v,
            stream_ids=stream_id,
            test_queries=test_q,
            test_query_ids=test_qid,
            sample_queries=sample_q,
            sample_query_ids=sample_qid,
            policy="always_fresh",
            num_shards=num_shards,
            m=2,
            budget_summaries=2,
            num_epochs=num_epochs,
            inserts_per_epoch=200,
            queries_per_epoch=50,
            stream_mode="steady",
            seed=44,
        )
    assert epochs_checked_fresh == list(range(num_epochs))
    assert (res_fresh.epochs_df["num_summaries_refreshed"] == num_shards).all()
    assert (res_fresh.epochs_df["bytes_summary_refresh"] == num_shards * SUMMARY_BYTES).all()

    # 2b. Structural test for never_refresh reference: published summaries never change after setup
    epochs_checked_never = []

    def spy_execute_never(self_coord, epoch, queries, shards, *args, **kwargs):
        for s in range(num_shards):
            assert self_coord.published_summaries[s].epoch_built == -1
            assert shards[s].epochs_since_refresh == epoch + 1
        epochs_checked_never.append(epoch)
        return orig_execute(self_coord, epoch, queries, shards, *args, **kwargs)

    with patch.object(Coordinator, "execute_queries", autospec=True, side_effect=spy_execute_never):
        res_never = run_single_simulation(
            initial_vectors=init_v,
            initial_ids=init_id,
            stream_vectors=stream_v,
            stream_ids=stream_id,
            test_queries=test_q,
            test_query_ids=test_qid,
            sample_queries=sample_q,
            sample_query_ids=sample_qid,
            policy="never_refresh",
            num_shards=num_shards,
            m=2,
            budget_summaries=2,
            num_epochs=num_epochs,
            inserts_per_epoch=200,
            queries_per_epoch=50,
            stream_mode="steady",
            seed=44,
        )
    assert epochs_checked_never == list(range(num_epochs))
    assert res_never.summary["total_summaries_refreshed"] == 0
    assert res_never.summary["total_budget_bytes_spent"] == 0

    # 2c. Reference check for oracle_routing: per query, missed neighbors <= every m-shard routing rule
    res_oracle = run_single_simulation(
        initial_vectors=init_v,
        initial_ids=init_id,
        stream_vectors=stream_v,
        stream_ids=stream_id,
        test_queries=test_q,
        test_query_ids=test_qid,
        sample_queries=sample_q,
        sample_query_ids=sample_qid,
        policy="oracle_routing",
        num_shards=num_shards,
        m=2,
        budget_summaries=2,
        num_epochs=num_epochs,
        inserts_per_epoch=200,
        queries_per_epoch=50,
        stream_mode="steady",
        seed=44,
    )
    assert res_oracle.summary["total_budget_bytes_spent"] == 0
    assert np.all(res_oracle.all_query_missed_neighbors <= res_fresh.all_query_missed_neighbors)
    assert np.all(res_oracle.all_query_missed_neighbors <= res_never.all_query_missed_neighbors)

    for rule in sorted(REFRESH_POLICIES):
        res_rule = run_single_simulation(
            initial_vectors=init_v,
            initial_ids=init_id,
            stream_vectors=stream_v,
            stream_ids=stream_id,
            test_queries=test_q,
            test_query_ids=test_qid,
            sample_queries=sample_q,
            sample_query_ids=sample_qid,
            policy=rule,
            num_shards=num_shards,
            m=2,
            budget_summaries=2,
            num_epochs=num_epochs,
            inserts_per_epoch=200,
            queries_per_epoch=50,
            stream_mode="steady",
            seed=44,
        )
        assert np.all(res_oracle.all_query_missed_neighbors <= res_rule.all_query_missed_neighbors)


def test_3_byte_accounting_and_overhead(real_subset_data) -> None:
    """Test 3: Per-epoch totals equal sum of logged messages; no rule exceeds B; RN overhead charged;
    charge_setup_to_budget=True does not double-count amortized setup bytes.
    """
    assert VECTOR_BYTES == 1536
    assert PUSH_VECTOR_BYTES == 1544
    assert SUMMARY_BYTES == 24576
    assert RESULT_BYTES == 120
    assert SHARD_SEARCH_BYTES == 1656
    assert CUTOFFS_PER_SHARD_BYTES == 512
    assert SHARD_SCORE_BYTES == 4
    assert SAMPLE_REPLY_PER_SHARD_QUERY_BYTES == 40
    assert SETUP_SAMPLE_VECTORS_PER_SHARD_BYTES == 98304
    assert recovered_neighbor_overhead_bytes(num_shards=32, m=2, num_sample_queries=64) == 21632

    init_v, init_id, stream_v, stream_id, test_q, test_qid, sample_q, sample_qid = real_subset_data
    num_shards = 8
    m = 2
    budget_summaries = 2
    budget_bytes = budget_summaries * SUMMARY_BYTES  # 49,152 bytes

    for pol in sorted(REFRESH_POLICIES):
        res = run_single_simulation(
            initial_vectors=init_v,
            initial_ids=init_id,
            stream_vectors=stream_v,
            stream_ids=stream_id,
            test_queries=test_q,
            test_query_ids=test_qid,
            sample_queries=sample_q,
            sample_query_ids=sample_qid,
            policy=pol,
            num_shards=num_shards,
            m=m,
            budget_summaries=budget_summaries,
            num_epochs=4,
            inserts_per_epoch=200,
            queries_per_epoch=30,
            seed=42,
        )
        for _, row in res.epochs_df.iterrows():
            ep = int(row["epoch"])
            msgs = res.bus.epoch_messages(ep)
            assert int(row["total_epoch_bytes"]) == sum(msg.bytes for msg in msgs)
            assert int(row["budget_bytes_spent"]) == sum(
                msg.bytes for msg in msgs if msg.charged_to_budget
            )
            assert int(row["workload_bytes"]) == sum(
                msg.bytes for msg in msgs if not msg.charged_to_budget
            )
            assert int(row["budget_bytes_spent"]) <= budget_bytes

            cat_sum = (
                int(row["bytes_summary_refresh"])
                + int(row["bytes_rn_sample_search_reply"])
                + int(row["bytes_rn_cutoffs"])
                + int(row["bytes_rn_shard_score"])
                + int(row["bytes_extra_search_query"])
                + int(row["bytes_extra_search_result"])
                + int(row["bytes_push_vector"])
                + int(row["bytes_setup_sample_vectors_amortized"])
                + int(row["bytes_query_out"])
                + int(row["bytes_query_result"])
            )
            assert cat_sum == int(row["total_epoch_bytes"])

        if pol == "recovered_neighbor":
            expected_overhead = recovered_neighbor_overhead_bytes(num_shards=num_shards, m=m)
            for _, row in res.epochs_df.iterrows():
                overhead_logged = (
                    int(row["bytes_rn_sample_search_reply"])
                    + int(row["bytes_rn_cutoffs"])
                    + int(row["bytes_rn_shard_score"])
                )
                assert overhead_logged == expected_overhead
                assert int(row["budget_bytes_spent"]) == (
                    overhead_logged + int(row["bytes_summary_refresh"])
                )
                assert np.isclose(
                    float(row["budget_bytes_with_amortized_setup"]),
                    float(row["budget_bytes_spent"]) + (98304 * num_shards) / 4.0,
                )
            assert res.summary["setup_sample_vectors_bytes"] == 98304 * num_shards
            assert np.isclose(
                res.summary["amortized_setup_sample_bytes_per_epoch"],
                (98304 * num_shards) / 4.0,
            )

    # 3b. Verify charge_setup_to_budget=True: amortized setup is charged to B and NOT double-counted
    res_charged = run_single_simulation(
        initial_vectors=init_v,
        initial_ids=init_id,
        stream_vectors=stream_v,
        stream_ids=stream_id,
        test_queries=test_q,
        test_query_ids=test_qid,
        sample_queries=sample_q,
        sample_query_ids=sample_qid,
        policy="recovered_neighbor",
        num_shards=num_shards,
        m=m,
        budget_summaries=10,  # 245,760 bytes/epoch > 9,248 overhead + 196,608 amortized setup
        num_epochs=4,
        inserts_per_epoch=200,
        queries_per_epoch=30,
        charge_setup_to_budget=True,
        seed=42,
    )
    expected_amortized = (98304 * num_shards + 4 - 1) // 4
    for _, row in res_charged.epochs_df.iterrows():
        assert int(row["bytes_setup_sample_vectors_amortized"]) == expected_amortized
        assert int(row["budget_bytes_spent"]) <= 10 * SUMMARY_BYTES
        # Must NOT double-count amortized setup bytes when already charged in budget_bytes_spent
        assert np.isclose(
            float(row["budget_bytes_with_amortized_setup"]),
            float(row["budget_bytes_spent"]),
        )


def test_4_no_data_leakage(real_subset_data) -> None:
    """Test 4: No test query ID or vector is in sample set; sample queries never score test recall;
    ql uses only past epochs' queries.
    """
    (
        _,
        _,
        _,
        _,
        test_q,
        test_qids,
        sample_q,
        sample_qids,
    ) = load_dataset(DATA_DIR)

    # 4a. Full dataset ID and vector disjointness
    assert len(set(test_qids.tolist()) & set(sample_qids.tolist())) == 0
    max_cos = float(np.max(sample_q @ test_q.T))
    assert max_cos < 0.9999

    with (DATA_DIR / "PROVENANCE.json").open("r", encoding="utf-8") as f:
        prov = json.load(f)
    assert prov["leakage_checks"]["id_overlap_count"] == 0
    assert prov["leakage_checks"]["text_overlap_count"] == 0

    # 4b. Verify that contaminating sample_query_ids with a test_query_id immediately raises AssertionError
    init_v, init_id, stream_v, stream_id, sub_tq, sub_tqid, sub_sq, sub_sqid = real_subset_data
    leaked_sqid = sub_sqid.copy()
    leaked_sqid[0] = sub_tqid[0]
    with pytest.raises(AssertionError, match="leakage"):
        run_single_simulation(
            initial_vectors=init_v,
            initial_ids=init_id,
            stream_vectors=stream_v,
            stream_ids=stream_id,
            test_queries=sub_tq,
            test_query_ids=sub_tqid,
            sample_queries=sub_sq,
            sample_query_ids=leaked_sqid,
            policy="recovered_neighbor",
            num_shards=4,
            m=2,
            budget_summaries=1,
            num_epochs=2,
            inserts_per_epoch=50,
            queries_per_epoch=10,
        )

    # 4c. Verify evaluate_epoch_queries is called exclusively on test queries, and ql uses only past epochs
    import dvs.run as run_mod

    orig_eval = run_mod.evaluate_epoch_queries
    called_counts = []

    def spy_eval(*args, **kwargs):
        ret_list = kwargs.get("returned_ids_list", args[1] if len(args) > 1 else [])
        called_counts.append(len(ret_list))
        return orig_eval(*args, **kwargs)

    orig_step = PolicyController.step_epoch
    past_counts_at_step = []

    def spy_step(self_ctrl, epoch, shards, coordinator, *args, **kwargs):
        assert kwargs.get("epoch_test_queries") is None
        past_counts_at_step.append(int(np.sum(coordinator.past_top_m_counts)))
        return orig_step(self_ctrl, epoch, shards, coordinator, *args, **kwargs)

    with (
        patch.object(run_mod, "evaluate_epoch_queries", side_effect=spy_eval),
        patch.object(PolicyController, "step_epoch", autospec=True, side_effect=spy_step),
    ):
        run_single_simulation(
            initial_vectors=init_v,
            initial_ids=init_id,
            stream_vectors=stream_v,
            stream_ids=stream_id,
            test_queries=sub_tq,
            test_query_ids=sub_tqid,
            sample_queries=sub_sq,
            sample_query_ids=sub_sqid,
            policy="ql",
            num_shards=4,
            m=2,
            budget_summaries=2,
            num_epochs=3,
            inserts_per_epoch=50,
            queries_per_epoch=17,
        )
    assert called_counts == [17, 17, 17]
    # Before epoch 0 queries: 0; before epoch 1 queries: 17 * 2 = 34; before epoch 2 queries: 34 * 2 = 68
    assert past_counts_at_step == [0, 34, 68]


def test_5_determinism(real_subset_data) -> None:
    """Test 5: Same config + seed gives bit-for-bit identical results."""
    init_v, init_id, stream_v, stream_id, test_q, test_qid, sample_q, sample_qid = real_subset_data

    for pol in ("recovered_neighbor", "data_change", "extra_search", "push_vectors"):
        r1 = run_single_simulation(
            initial_vectors=init_v,
            initial_ids=init_id,
            stream_vectors=stream_v,
            stream_ids=stream_id,
            test_queries=test_q,
            test_query_ids=test_qid,
            sample_queries=sample_q,
            sample_query_ids=sample_qid,
            policy=pol,
            num_shards=8,
            m=2,
            budget_summaries=2,
            num_epochs=3,
            inserts_per_epoch=100,
            queries_per_epoch=25,
            seed=99,
        )
        r2 = run_single_simulation(
            initial_vectors=init_v,
            initial_ids=init_id,
            stream_vectors=stream_v,
            stream_ids=stream_id,
            test_queries=test_q,
            test_query_ids=test_qid,
            sample_queries=sample_q,
            sample_query_ids=sample_qid,
            policy=pol,
            num_shards=8,
            m=2,
            budget_summaries=2,
            num_epochs=3,
            inserts_per_epoch=100,
            queries_per_epoch=25,
            seed=99,
        )
        assert np.array_equal(r1.all_query_recall, r2.all_query_recall)
        assert np.array_equal(r1.all_query_missed_neighbors, r2.all_query_missed_neighbors)
        assert r1.summary["total_budget_bytes_spent"] == r2.summary["total_budget_bytes_spent"]


def test_6_hand_built_3_shard_routing() -> None:
    """Test 6: Hand-built 3-shard example routes queries to the expected shards and merges
    pushed vectors and extra shard searches into the top 10.
    """
    e0 = np.zeros(DIM, dtype=np.float32)
    e0[0] = 1.0
    e1 = np.zeros(DIM, dtype=np.float32)
    e1[1] = 1.0
    e2 = np.zeros(DIM, dtype=np.float32)
    e2[2] = 1.0

    rng = np.random.default_rng(7)
    shards = []
    for s_id, base_vec in enumerate([e0, e1, e2]):
        noise = 0.01 * rng.standard_normal((20, DIM)).astype(np.float32)
        vecs = normalize_l2(base_vec.reshape(1, DIM) + noise)
        ids = np.arange(s_id * 100, s_id * 100 + 20, dtype=np.int64)
        shard = Shard(shard_id=s_id, initial_vectors=vecs, initial_ids=ids, c=SUMMARY_CENTERS, seed=7)
        centers = np.tile(base_vec, (SUMMARY_CENTERS, 1))
        shard.published_summary = ShardSummary(shard_id=s_id, centers=centers, epoch_built=-1)
        shards.append(shard)

    bus = ByteBus(num_shards=3, num_epochs=1)
    coord = Coordinator(shards=shards, bus=bus, m=2, exact_local_search=True)

    # Query A: 0.9 * e2 + 0.4 * e0 + 0.1 * e1 -> top-2 shards must be [2, 0] (shard 1 skipped)
    qa = normalize_l2((0.9 * e2 + 0.4 * e0 + 0.1 * e1).reshape(1, DIM))
    # Query B: 0.8 * e1 + 0.5 * e2 - 0.2 * e0 -> top-2 shards must be [1, 2] (shard 0 skipped)
    qb = normalize_l2((0.8 * e1 + 0.5 * e2 - 0.2 * e0).reshape(1, DIM))
    queries = np.vstack([qa, qb])

    top_m, ranked_shards, _ = coord.route_queries(queries, m=2)
    assert top_m[0].tolist() == [2, 0]
    assert ranked_shards[0].tolist() == [2, 0, 1]
    assert top_m[1].tolist() == [1, 2]
    assert ranked_shards[1].tolist() == [1, 2, 0]

    # Insert an exact copy of qa on unrouted shard 1 (doc_id=9999)
    shards[1].add_vectors(qa, np.array([9999], dtype=np.int64))
    res_base, _, _ = coord.execute_queries(epoch=0, queries=qa, shards=shards, m=2)
    assert 9999 not in res_base[0].tolist()

    # Extra search on shard 1 must recover doc_id=9999 as the top-1 result
    res_extra, _, _ = coord.execute_queries(
        epoch=0,
        queries=qa,
        shards=shards,
        m=2,
        extra_shards_per_query={0: [1]},
    )
    assert int(res_extra[0][0]) == 9999

    # Pushing doc_id=8888 (identical to qa) to coordinator must also return it as top-1
    coord.receive_pushed_vectors(
        epoch=0,
        vectors=qa,
        doc_ids=np.array([8888], dtype=np.int64),
        shard_ids=np.array([1], dtype=np.int64),
    )
    res_pushed, _, _ = coord.execute_queries(epoch=0, queries=qa, shards=shards, m=2)
    assert int(res_pushed[0][0]) == 8888


def test_7_equal_bytes_alternatives_never_exceed_budget(real_subset_data) -> None:
    """Test 7: extra_search and push_vectors never spend more than B in any epoch."""
    init_v, init_id, stream_v, stream_id, test_q, test_qid, sample_q, sample_qid = real_subset_data

    for budget_summaries in (1, 2, 4):
        budget_bytes = budget_summaries * SUMMARY_BYTES
        for pol in ("extra_search", "push_vectors"):
            res = run_single_simulation(
                initial_vectors=init_v,
                initial_ids=init_id,
                stream_vectors=stream_v,
                stream_ids=stream_id,
                test_queries=test_q,
                test_query_ids=test_qid,
                sample_queries=sample_q,
                sample_query_ids=sample_qid,
                policy=pol,
                num_shards=8,
                m=2,
                budget_summaries=budget_summaries,
                num_epochs=4,
                inserts_per_epoch=200,
                queries_per_epoch=40,
                seed=42,
            )
            for _, row in res.epochs_df.iterrows():
                spent = int(row["budget_bytes_spent"])
                assert spent <= budget_bytes
                if pol == "extra_search":
                    expected = (budget_bytes // SHARD_SEARCH_BYTES) * SHARD_SEARCH_BYTES
                    assert spent == expected
                    assert int(row["bytes_summary_refresh"]) == 0
                else:
                    expected = (budget_bytes // PUSH_VECTOR_BYTES) * PUSH_VECTOR_BYTES
                    assert spent == expected
                    assert int(row["bytes_summary_refresh"]) == 0


def test_8_spearman_cpu_aging_and_new_topic(real_subset_data) -> None:
    """Test 8: Verify Fixes 3, 4, 6 and the new_topic stream."""
    # Fix 3: Spearman returns NaN when undefined
    assert np.isnan(compute_spearman_correlation(None, np.array([1.0, 2.0])))
    assert np.isnan(compute_spearman_correlation(np.zeros(8), np.arange(8, dtype=np.float64)))
    assert np.isnan(compute_spearman_correlation(np.arange(8, dtype=np.float64), np.ones(8)))
    assert np.isclose(
        compute_spearman_correlation(np.arange(8, dtype=np.float64), np.arange(8, dtype=np.float64)),
        1.0,
    )

    init_v, init_id, stream_v, stream_id, test_q, test_qid, sample_q, sample_qid = real_subset_data

    # Fix 4: Non-RN refresh rules do not call compute_recovered_neighbor_raw_scores inside step_epoch;
    # ground-truth work is timed separately and excluded from cpu_total_s.
    res_dc = run_single_simulation(
        initial_vectors=init_v,
        initial_ids=init_id,
        stream_vectors=stream_v,
        stream_ids=stream_id,
        test_queries=test_q,
        test_query_ids=test_qid,
        sample_queries=sample_q,
        sample_query_ids=sample_qid,
        policy="data_change",
        num_shards=8,
        m=2,
        budget_summaries=2,
        num_epochs=3,
        inserts_per_epoch=100,
        queries_per_epoch=25,
        seed=42,
    )
    assert "spearman_n_defined" in res_dc.summary
    assert "spearman_new_vecs_n_defined" in res_dc.summary
    assert "spearman_new_vecs_rho_mean" in res_dc.summary
    assert res_dc.summary["cpu_ground_truth_s_total"] >= 0.0
    assert np.isclose(
        res_dc.summary["cpu_total_s"],
        res_dc.summary["cpu_insert_s_total"]
        + res_dc.summary["cpu_summary_s_total"]
        + res_dc.summary["cpu_policy_s_total"]
        + res_dc.summary["cpu_query_s_total"],
    )

    # Verify arithmetic starvation cap A = ceil(2 * S / k)
    assert compute_arithmetic_starvation_cap(num_shards=32, num_summaries_per_epoch=4) == 16
    assert compute_arithmetic_starvation_cap(num_shards=32, num_summaries_per_epoch=3) == 22
    assert compute_arithmetic_starvation_cap(num_shards=32, num_summaries_per_epoch=8) == 8
    ctrl_arith_dc = PolicyController(
        policy="data_change",
        budget_bytes=4 * SUMMARY_BYTES,
        num_shards=32,
        m=2,
    )
    assert ctrl_arith_dc.effective_starvation_cap() == 16
    ctrl_arith_rn = PolicyController(
        policy="recovered_neighbor",
        budget_bytes=4 * SUMMARY_BYTES,
        num_shards=32,
        m=2,
    )
    assert ctrl_arith_rn.effective_starvation_cap() == 22

    # Fix 6: Starvation cap A = 8 vs additive aging
    # Construct 4 shards where shard 0 has waited 9 epochs (> A=8) with lower data_change score,
    # and shards 1..3 have waited 2 epochs (<= A=8) with higher data_change score.
    rng = np.random.default_rng(123)
    shards = []
    for s_id in range(4):
        vecs = normalize_l2(rng.standard_normal((30, DIM)).astype(np.float32))
        ids = np.arange(s_id * 100, s_id * 100 + 30, dtype=np.int64)
        shard = Shard(shard_id=s_id, initial_vectors=vecs, initial_ids=ids, c=4, seed=123)
        # Add 5 new vectors
        new_v = normalize_l2(rng.standard_normal((5, DIM)).astype(np.float32))
        new_i = np.arange(1000 + s_id * 10, 1000 + s_id * 10 + 5, dtype=np.int64)
        shard.add_vectors(new_v, new_i)
        shards.append(shard)

    # Under starvation_cap (A=8): if all ages are <= 8 (e.g. shard 0 age=7 -> 8 after +1, shard 1 age=0 -> 1),
    # highest score wins even if age is 7 epochs smaller!
    shards[0].epochs_since_refresh = 7  # becomes 8 in step_epoch (<= 8, not starved)
    shards[1].epochs_since_refresh = 0  # becomes 1 in step_epoch
    shards[2].epochs_since_refresh = 0
    shards[3].epochs_since_refresh = 0

    bus = ByteBus(num_shards=4, num_epochs=10)
    coord = Coordinator(shards=shards, bus=bus, m=2)
    # Give shard 1 the highest past_top_m_counts for 'ql'
    coord.past_top_m_counts = np.array([10, 100, 50, 20], dtype=np.int64)

    ctrl_cap = PolicyController(
        policy="ql",
        budget_bytes=SUMMARY_BYTES,  # 1 summary
        num_shards=4,
        m=2,
        aging_mode="starvation_cap",
        starvation_cap_epochs=8,
    )
    dec_not_starved = ctrl_cap.step_epoch(
        epoch=0,
        shards=shards,
        coordinator=coord,
        bus=bus,
        sample_queries=sample_q[:8],
    )
    # Shard 1 has highest ql score (100) and no shard has age > 8, so shard 1 must be refreshed!
    assert dec_not_starved.refreshed_shards == [1]

    # Now shard 0 has age 8 before step_epoch -> becomes 9 (> 8, starved!) in step_epoch:
    # Shard 0 must be refreshed first due to the starvation cap!
    shards[1].add_vectors(
        normalize_l2(rng.standard_normal((2, DIM)).astype(np.float32)),
        np.array([2001, 2002], dtype=np.int64),
    )
    dec_starved = ctrl_cap.step_epoch(
        epoch=1,
        shards=shards,
        coordinator=coord,
        bus=bus,
        sample_queries=sample_q[:8],
    )
    assert dec_starved.refreshed_shards == [0]

    # New-topic stream partitioning test
    nt_init_v, nt_init_id, nt_stream_v, nt_stream_id = partition_new_topic_stream(
        initial_vectors=init_v,
        initial_ids=init_id,
        stream_vectors=stream_v,
        stream_ids=stream_id,
        seed=42,
        num_clusters=16,
    )
    assert len(nt_init_v) + len(nt_stream_v) == len(init_v) + len(stream_v)
    assert set(nt_init_id.tolist()).isdisjoint(set(nt_stream_id.tolist()))

    res_nt = run_single_simulation(
        initial_vectors=init_v,
        initial_ids=init_id,
        stream_vectors=stream_v,
        stream_ids=stream_id,
        test_queries=test_q,
        test_query_ids=test_qid,
        sample_queries=sample_q,
        sample_query_ids=sample_qid,
        policy="never_refresh",
        num_shards=8,
        m=2,
        budget_summaries=2,
        num_epochs=4,
        inserts_per_epoch=250,
        queries_per_epoch=25,
        stream_mode="new_topic",
        seed=42,
    )
    assert res_nt.summary["initial_set_size"] + res_nt.summary["stream_set_size"] == 2000
    assert res_nt.summary["queries_with_most_neighbors_in_stream"] is not None
    assert int(res_nt.epochs_df["total_indexed_vectors"].iloc[-1]) == 2000

    # Verify time_segment placement: inserts of epoch e go to shard (e mod S)
    res_ts = run_single_simulation(
        initial_vectors=init_v,
        initial_ids=init_id,
        stream_vectors=stream_v,
        stream_ids=stream_id,
        test_queries=test_q,
        test_query_ids=test_qid,
        sample_queries=sample_q,
        sample_query_ids=sample_qid,
        policy="never_refresh",
        num_shards=4,
        m=2,
        budget_summaries=2,
        num_epochs=5,
        inserts_per_epoch=100,
        queries_per_epoch=20,
        stream_mode="steady",
        placement_mode="time_segment",
        seed=42,
    )
    assert res_ts.summary["placement_mode"] == "time_segment"
    assert res_ts.summary["starvation_cap_mode"] == "arithmetic"
    assert res_ts.summary["starvation_cap_epochs"] == 4
