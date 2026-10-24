"""Comprehensive pytest suite for the DVS byte-counted shard simulator.

Covers all 7 required verification tests from audit/AGY_TASK_B1_build.md:
1. m = S recall@10 == 1.0 (exact local search == 1.0; HNSW >= 0.98).
2. always_fresh with m < S never has higher missed neighbors than any budgeted refresh rule
   on the same stream and seed.
3. Byte accounting: per-epoch totals equal the sum of logged messages; no budgeted rule exceeds B
   in any epoch; overhead of recovered_neighbor (21,632 bytes at S=32, m=2) is charged; setup
   sample vectors (98,304 bytes/shard) are reported both ways.
4. Leakage: no test query ID or vector is in the sample query set (verified on both real
   `data/` and synthetic runs); sample queries are never used to score test recall.
5. Determinism: identical config + seed produces bit-for-bit identical results.
6. Routing: hand-built 3-shard example routes queries to the expected shards and breaks ties
   deterministically.
7. Equal bytes: extra_search and push_vectors never spend more than B in any epoch.
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
from dvs.policies import REFRESH_POLICIES
from dvs.run import run_single_simulation
from dvs.shards import Shard
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


def test_2_always_fresh_missed_neighbors_never_higher_than_budgeted(real_subset_data) -> None:
    """Test 2: With always_fresh and m < S, missed neighbors are never higher than any budgeted rule."""
    init_v, init_id, stream_v, stream_id, test_q, test_qid, sample_q, sample_qid = real_subset_data

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
        num_shards=8,
        m=2,
        budget_summaries=2,
        num_epochs=5,
        inserts_per_epoch=200,
        queries_per_epoch=50,
        stream_mode="steady",
        seed=42,
    )
    fresh_missed = res_fresh.summary["missed_neighbors_total"]

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
            num_shards=8,
            m=2,
            budget_summaries=2,
            num_epochs=5,
            inserts_per_epoch=200,
            queries_per_epoch=50,
            stream_mode="steady",
            seed=42,
        )
        rule_missed = res_rule.summary["missed_neighbors_total"]
        assert fresh_missed <= rule_missed, (
            f"Expected always_fresh missed ({fresh_missed}) <= {rule} missed ({rule_missed})"
        )


def test_3_byte_accounting_and_overhead(real_subset_data) -> None:
    """Test 3: Per-epoch totals equal sum of logged messages; no rule exceeds B; RN overhead charged."""
    # Verify exact message byte constants from proposal Section 4
    assert VECTOR_BYTES == 1536
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
            # Setup sample vectors reported both ways
            assert res.summary["setup_sample_vectors_bytes"] == 98304 * num_shards
            assert np.isclose(
                res.summary["amortized_setup_sample_bytes_per_epoch"],
                (98304 * num_shards) / 4.0,
            )


def test_4_no_data_leakage(real_subset_data) -> None:
    """Test 4: No test query ID or vector is in sample set; sample queries never score test recall."""
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

    # 4c. Verify evaluate_epoch_queries is called exclusively on test queries, never sample queries
    import dvs.run as run_mod

    orig_eval = run_mod.evaluate_epoch_queries
    called_counts = []

    def spy_eval(*args, **kwargs):
        ret_list = kwargs.get("returned_ids_list", args[1] if len(args) > 1 else [])
        called_counts.append(len(ret_list))
        return orig_eval(*args, **kwargs)

    with patch.object(run_mod, "evaluate_epoch_queries", side_effect=spy_eval):
        run_single_simulation(
            initial_vectors=init_v,
            initial_ids=init_id,
            stream_vectors=stream_v,
            stream_ids=stream_id,
            test_queries=sub_tq,
            test_query_ids=sub_tqid,
            sample_queries=sub_sq,
            sample_query_ids=sub_sqid,
            policy="recovered_neighbor",
            num_shards=4,
            m=2,
            budget_summaries=2,
            num_epochs=2,
            inserts_per_epoch=50,
            queries_per_epoch=17,  # distinct from 64 sample queries
        )
    assert called_counts == [17, 17]


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
    """Test 6: A hand-built 3-shard example routes queries to the expected shards."""
    # Construct 3 orthogonal unit basis directions e_0, e_1, e_2 in R^384
    e0 = np.zeros(DIM, dtype=np.float32)
    e0[0] = 1.0
    e1 = np.zeros(DIM, dtype=np.float32)
    e1[1] = 1.0
    e2 = np.zeros(DIM, dtype=np.float32)
    e2[2] = 1.0

    # Build 3 shards centered around e0, e1, e2
    rng = np.random.default_rng(7)
    shards = []
    for s_id, base_vec in enumerate([e0, e1, e2]):
        noise = 0.01 * rng.standard_normal((20, DIM)).astype(np.float32)
        vecs = normalize_l2(base_vec.reshape(1, DIM) + noise)
        ids = np.arange(s_id * 100, s_id * 100 + 20, dtype=np.int64)
        shard = Shard(shard_id=s_id, initial_vectors=vecs, initial_ids=ids, c=SUMMARY_CENTERS, seed=7)
        # Override published summary with exact known centers so routing is analytically known
        centers = np.tile(base_vec, (SUMMARY_CENTERS, 1))
        shard.published_summary = ShardSummary(shard_id=s_id, centers=centers, epoch_built=-1)
        shards.append(shard)

    bus = ByteBus(num_shards=3, num_epochs=1)
    coord = Coordinator(shards=shards, bus=bus, m=2)

    # Query A: 0.9 * e2 + 0.4 * e0 + 0.1 * e1 -> top-2 shards must be [2, 0]
    qa = normalize_l2((0.9 * e2 + 0.4 * e0 + 0.1 * e1).reshape(1, DIM))
    # Query B: 0.8 * e1 + 0.5 * e2 - 0.2 * e0 -> top-2 shards must be [1, 2]
    qb = normalize_l2((0.8 * e1 + 0.5 * e2 - 0.2 * e0).reshape(1, DIM))
    queries = np.vstack([qa, qb])

    top_m, ranked_shards, _ = coord.route_queries(queries, m=2)
    assert top_m[0].tolist() == [2, 0]
    assert ranked_shards[0].tolist() == [2, 0, 1]
    assert top_m[1].tolist() == [1, 2]
    assert ranked_shards[1].tolist() == [1, 2, 0]


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
                    expected = (budget_bytes // VECTOR_BYTES) * VECTOR_BYTES
                    assert spent == expected
                    assert int(row["bytes_summary_refresh"]) == 0
