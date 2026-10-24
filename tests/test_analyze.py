"""Tests for `repo/analysis/analyze.py` (Task C2).

Covers using lightweight synthetic run folders (small numpy arrays):
1. Complete grid (M1=330, M2=330, sens=66) with known effects:
   - Expected per-stream PASS/FAIL outcomes (H1, H2, GATE_M2, H3, H4 per B)
   - Break-even Q crossing without interpolation using fixed Q=200 best refresh rule
   - Same-data best-refresh-rule selection notes with gap to second-best rule
   - Spearman comparison on shared refresh decisions only (mean, pooled, and n)
   - Absent `failed_runs.json` reported as "not found"
2. Missing seed folder -> listed in SUMMARY.md under "Missing or incomplete runs", test prints INCOMPLETE.
3. Missing `queries.npz` -> listed in SUMMARY.md under "Missing or incomplete runs", test prints INCOMPLETE.
4. `GATE_M2` failing on one stream (`steady`) -> `H3` reported as `NOT TESTED (GATE FAILED) / FAIL` on that stream only.
5. Config mismatch, duplicate run folder, and `failed_runs.json` unique run_id counting.
"""

from __future__ import annotations

import os

for _blas_env_var in (
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
):
    os.environ.setdefault(_blas_env_var, "1")

import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from analysis.analyze import (
    POLICY_ORDER,
    REFRESH_RULES,
    analyze_all,
    load_expected_grid,
)


def _target_recent_recall(
    exp_label: str,
    stream: str,
    b_sum: int,
    q_val: int,
    policy: str,
) -> float:
    """Deterministic target recent recall@10 in [0.1, 0.95] encoding known hypothesis effects."""
    if exp_label in ("M1", "M1_online"):
        # H1 at Q=200, B=4: always_fresh - never_refresh = 0.015 (< 0.05) -> PASS on both streams
        if policy == "never_refresh":
            return 0.500
        if policy == "always_fresh":
            return 0.515
        if policy == "oracle_routing":
            return 0.850
        if policy == "all_shards":
            return 0.920

        # Refresh rules on M1: data_change is best at Q=200 (0.540), recovered_neighbor is 2nd (0.515, gap +2.50 pp)
        if policy == "data_change":
            if stream == "steady":
                # Break-even crossing between Q=200 and Q=800 against extra_search (0.600) and extra_search_online (0.590):
                # Q=50: 0.520 (< 0.590), Q=200: 0.540 (< 0.590), Q=800: 0.640 (> 0.600)
                return {50: 0.520, 200: 0.540, 800: 0.640}[q_val]
            else:
                # new_topic: extra_search (0.600) and extra_search_online (0.590) ahead at all Q in {50, 200, 800}
                return {50: 0.510, 200: 0.540, 800: 0.570}[q_val]
        if policy == "recovered_neighbor":
            return 0.515
        if policy == "round_robin":
            return 0.505
        if policy == "ql":
            return 0.500
        if policy == "ss":
            return 0.495

        # Alternatives at M1 / M1_online: beat best refresh (0.540) at Q=200 on both streams
        if policy == "extra_search":
            return 0.600
        if policy == "extra_search_online":
            return 0.590
        if policy == "push_vectors":
            return 0.585

    # M2, sens (time_segment) and M3 (time_segment_g, G=8)
    # GATE_M2 / GATE_M3 at Q=200, B=4: always_fresh (0.650) - never_refresh (0.500) = +0.150 (>= 0.05) -> PASS on both streams
    if policy == "never_refresh":
        return 0.500
    if policy == "always_fresh":
        return 0.650
    if policy == "oracle_routing":
        return 0.860
    if policy == "all_shards":
        return 0.930

    # Refresh rules on M2 / M3:
    # recovered_neighbor is best refresh rule at all B (0.620 at B=2, 0.680 at B=4, 0.740 at B=8)
    if policy == "recovered_neighbor":
        return {2: 0.620, 4: 0.680, 8: 0.740}[b_sum]
    if policy == "data_change":
        # On steady at B=4: 0.590 (RN beats by +9.0 pp >= 5.0 pp -> H3 / H3b steady PASS)
        # On new_topic at B=4: 0.655 (RN beats by +2.5 pp < 5.0 pp -> H3 / H3b new_topic FAIL)
        if stream == "steady":
            return {2: 0.560, 4: 0.590, 8: 0.650}[b_sum]
        else:
            return {2: 0.580, 4: 0.655, 8: 0.670}[b_sum]
    if policy == "round_robin":
        return {2: 0.540, 4: 0.580, 8: 0.630}[b_sum]
    if policy == "ql":
        return {2: 0.530, 4: 0.570, 8: 0.620}[b_sum]
    if policy == "ss":
        return {2: 0.520, 4: 0.560, 8: 0.610}[b_sum]

    # Alternatives on M2 (H4) and M3 (H4b across B in {2, 4, 8}):
    # At B=2: extra_search (0.690) / extra_search_online (0.675) beats recovered_neighbor (0.620) and push_vectors (0.600) -> FAIL
    # At B=4: recovered_neighbor (0.680) beats extra_search (0.620), extra_search_online (0.605), and push_vectors (0.600) -> PASS
    # At B=8: recovered_neighbor (0.740) beats extra_search (0.660), extra_search_online (0.645), and push_vectors (0.630) -> PASS
    if policy == "extra_search":
        return {2: 0.690, 4: 0.620, 8: 0.660}[b_sum]
    if policy == "extra_search_online":
        return {2: 0.675, 4: 0.605, 8: 0.645}[b_sum]
    if policy == "push_vectors":
        return {2: 0.600, 4: 0.600, 8: 0.630}[b_sum]

    return 0.500


def _write_synthetic_run(
    run_dir: Path,
    run_id: str,
    exp_label: str,
    grid_spec: Dict[str, Any],
    stream_mode: str,
    b_sum: int,
    q_val: int,
    policy: str,
    seed: int,
    override_recent_mean: Optional[float] = None,
) -> None:
    """Create a complete synthetic run folder with small numpy arrays (n_queries=40, n_epochs=6)."""
    run_dir.mkdir(parents=True, exist_ok=True)
    n_queries = 40
    n_with_recent = 32
    n_without_recent = n_queries - n_with_recent

    target_rec = (
        override_recent_mean
        if override_recent_mean is not None
        else _target_recent_recall(exp_label, stream_mode, b_sum, q_val, policy)
    )
    # Small deterministic seed-level offset so across-seed variance is non-zero but tight
    seed_offset = (seed - 44) * 0.001
    q_idx = np.arange(n_with_recent, dtype=np.float64)
    q_jitter = ((q_idx % 5) - 2.0) * 0.002

    recent_valid = np.clip(target_rec + seed_offset + q_jitter, 0.0, 1.0).astype(np.float32)
    q_recent = np.empty(n_queries, dtype=np.float32)
    # Shared NaN mask across policies for the same (stream, Q, seed): last 8 queries have no recent neighbors
    q_recent[:n_with_recent] = recent_valid
    q_recent[n_with_recent:] = np.nan

    q_recall = np.clip(target_rec + 0.05 + seed_offset, 0.0, 1.0) * np.ones(n_queries, dtype=np.float32)
    q_missed = np.ones(n_queries, dtype=np.int16)

    np.savez_compressed(
        run_dir / "queries.npz",
        recall_at_10=q_recall,
        recent_recall_at_10=q_recent,
        missed_neighbors=q_missed,
    )

    # 6 epochs in epochs.parquet:
    # Epochs 0..3 have BOTH spearman_rho and spearman_rho_new_vecs defined.
    # Epoch 4 has ONLY spearman_rho_new_vecs defined (spearman_rho is NaN) to test same-decision filtering.
    # Epoch 5 has both NaN.
    if policy in REFRESH_RULES:
        sp_rn = [0.60 + 0.01 * (seed - 42), 0.62, 0.64, 0.66, float("nan"), float("nan")]
        sp_nv = [0.30 + 0.01 * (seed - 42), 0.32, 0.34, 0.36, 0.90, float("nan")]
    else:
        sp_rn = [float("nan")] * 6
        sp_nv = [float("nan")] * 6

    epochs_df = pd.DataFrame(
        {
            "run_id": [run_id] * 6,
            "epoch": list(range(6)),
            "policy": [policy] * 6,
            "seed": [seed] * 6,
            "spearman_rho": sp_rn,
            "spearman_rho_new_vecs": sp_nv,
        }
    )
    epochs_df.to_parquet(run_dir / "epochs.parquet", index=False)

    cap_mode = str(grid_spec["starvation_cap_mode"])
    cap_epochs = int(grid_spec["fixed_cap_epochs"]) if grid_spec["fixed_cap_epochs"] is not None else 16
    budget_bytes = int(b_sum) * 24576
    spent_bytes = 0 if policy in ("never_refresh", "oracle_routing", "all_shards") else budget_bytes * 50

    cfg_dict: Dict[str, Any] = {
        "policy": policy,
        "num_shards": int(grid_spec["num_shards"]),
        "c": int(grid_spec["c"]),
        "m": int(grid_spec["m"]),
        "budget_summaries": int(b_sum),
        "budget_bytes": budget_bytes,
        "num_epochs": int(grid_spec["num_epochs"]),
        "inserts_per_epoch": int(grid_spec["inserts_per_epoch"]),
        "queries_per_epoch": int(q_val),
        "recent_window_w": int(grid_spec["recent_window_w"]),
        "stream_mode": stream_mode,
        "placement_mode": str(grid_spec["placement_mode"]),
        "bursts": bool(grid_spec["bursts"]),
        "sharding_mode": str(grid_spec["sharding_mode"]),
        "aging_mode": str(grid_spec["aging_mode"]),
        "starvation_cap_mode": cap_mode,
        "starvation_cap_epochs": cap_epochs,
        "seed": int(seed),
        "exact_local_search": False,
        "charge_setup_to_budget": bool(grid_spec["charge_setup_to_budget"]),
    }
    if grid_spec.get("active_segments") is not None or grid_spec["placement_mode"] == "time_segment_g":
        cfg_dict["active_segments"] = int(
            grid_spec["active_segments"] if grid_spec.get("active_segments") is not None else 8
        )

    summary = {
        **cfg_dict,
        "run_id": run_id,
        "overall_recall_at_10_mean": float(np.mean(q_recall)),
        "recent_recall_at_10_query_mean": float(np.nanmean(q_recent)),
        "total_queries_evaluated": n_queries,
        "total_queries_with_recent_neighbors": n_with_recent,
        "total_queries_without_recent_neighbors": n_without_recent,
        "missed_neighbors_query_mean": 1.0,
        "missed_neighbors_total": n_queries,
        "spearman_rho_mean": float(np.nanmean(sp_rn)) if policy in REFRESH_RULES else float("nan"),
        "spearman_n_defined": 4 if policy in REFRESH_RULES else 0,
        "spearman_new_vecs_rho_mean": float(np.nanmean(sp_nv)) if policy in REFRESH_RULES else float("nan"),
        "spearman_new_vecs_n_defined": 5 if policy in REFRESH_RULES else 0,
        "total_budget_bytes_spent": spent_bytes,
        "total_budget_bytes_with_amortized_setup": float(spent_bytes),
        "total_workload_bytes": 100000,
        "total_summaries_refreshed": 150 if policy in REFRESH_RULES else 0,
        "total_forced_by_cap": 12 if policy in REFRESH_RULES else 0,
        "total_extra_searches": 500 if policy in ("extra_search", "extra_search_online") else 0,
        "total_pushed_vectors": 300 if policy == "push_vectors" else 0,
        "cpu_total_s": 1.25,
        "cpu_ground_truth_s_total": 0.50,
    }

    prov = {
        "run_id": run_id,
        "git_commit": "625f307",
        "config_sha256": f"sha_{exp_label}",
        "data_provenance_sha256": "data_sha",
        "package_versions": {"numpy": "1.26.0"},
        "cpu_info": "synthetic",
        "config": cfg_dict,
    }

    with (run_dir / "summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f)
    with (run_dir / "provenance.json").open("w", encoding="utf-8") as f:
        json.dump(prov, f)


@pytest.fixture(scope="module")
def synth_results_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Create a complete synthetic 1,116-run grid (M1=330, M2=330, sens=66, M1_online=30, M3=360) once for the module."""
    root = tmp_path_factory.mktemp("synth_results")
    main_dir = root / "main"
    configs_dir = REPO_ROOT / "configs"

    for exp_label, yaml_name in [
        ("M1", "main_M1.yaml"),
        ("M2", "main_M2.yaml"),
        ("sens", "main_sens.yaml"),
        ("M1_online", "main_M1_online.yaml"),
        ("M3", "main_M3.yaml"),
    ]:
        grid_spec = load_expected_grid(configs_dir / yaml_name, exp_label)
        exp_dir = main_dir / exp_label
        for (stream, _placement, b_sum, q_val, _cap_mode, pol, seed), rid in grid_spec[
            "expected_cells"
        ].items():
            _write_synthetic_run(
                run_dir=exp_dir / rid,
                run_id=rid,
                exp_label=exp_label,
                grid_spec=grid_spec,
                stream_mode=stream,
                b_sum=b_sum,
                q_val=q_val,
                policy=pol,
                seed=seed,
            )
    return root


def test_complete_grid_known_effects_and_breakeven_crossing(
    synth_results_root: Path,
    tmp_path: Path,
) -> None:
    """Verify full grid analysis: per-stream outcomes, H3 per-stream fail listing, H4 per-B winners,
    uninterpolated break-even Q crossing, same-data selection note, paired Spearman, and CLI options."""
    out_dir = tmp_path / "out_complete"
    res = analyze_all(results_root=synth_results_root, out_dir=out_dir)

    assert res["df_missing"].empty
    summary_md = (out_dir / "SUMMARY.md").read_text(encoding="utf-8")

    # 1. Missing or incomplete runs section shows 0 issues
    assert "## 2. Missing or incomplete runs" in summary_md
    assert "330 / 330 complete valid runs" in summary_md
    assert "66 / 66 complete valid runs" in summary_md

    # 2. Failed runs: when failed_runs.json is absent, says "not found" and does NOT print "0 failed runs"
    assert "`M1`: `failed_runs.json` not found" in summary_md
    assert "`M2`: `failed_runs.json` not found" in summary_md
    assert "`sens`: `failed_runs.json` not found" in summary_md
    assert "0 failed runs" not in summary_md

    # 3. Check hypothesis outcomes per stream (never collapsed into one overall PASS when a stream fails)
    df_hyp = res["df_hyp"].set_index("hypothesis")
    assert df_hyp.loc["H1", "outcome"] == "PASS (steady=PASS, new_topic=PASS)"
    assert df_hyp.loc["H2", "outcome"] == "steady=PASS, new_topic=PASS"
    assert df_hyp.loc["GATE_M2", "outcome"] == "steady=PASS, new_topic=PASS"
    # H3 passes on steady (+9 pp vs data_change) and FAILS on new_topic (+2.5 pp vs data_change)
    assert df_hyp.loc["H3", "outcome"] == "steady=PASS, new_topic=FAIL"

    # H4 per-B outcomes and winners: B=2 is FAIL (winner: extra_search), B=4 and B=8 are PASS (winner: recovered_neighbor)
    h4_outcome = str(df_hyp.loc["H4", "outcome"])
    assert "B=2:FAIL (winner: extra_search)" in h4_outcome
    assert "B=4:PASS (winner: recovered_neighbor)" in h4_outcome
    assert "B=8:PASS (winner: recovered_neighbor)" in h4_outcome

    # Every per-stream / per-B FAIL is listed under "### Hypotheses that FAIL"
    fail_sec = summary_md.split("### Hypotheses that FAIL")[1].split("## 2.")[0]
    assert "**H3 (new_topic)**: **FAIL**" in fail_sec
    assert "**H4 (steady, B=2)**: **FAIL** (winner: `extra_search`" in fail_sec
    assert "**H4 (new_topic, B=2)**: **FAIL** (winner: `extra_search`" in fail_sec

    # 4. Break-even Q: no interpolation, uses fixed Q=200 best refresh rule (`data_change`) at all Q
    df_be = res["df_breakeven"]
    assert set(df_be["best_refresh_policy"]) == {"data_change"}
    steady_be = str(df_be[df_be["stream_mode"] == "steady"]["breakeven_q"].iloc[0])
    nt_be = str(df_be[df_be["stream_mode"] == "new_topic"]["breakeven_q"].iloc[0])
    assert steady_be == "between Q = 200 and Q = 800 (rule: data_change, picked at Q = 200)"
    assert nt_be == "no crossing in {50, 200, 800}: extra_search ahead at all Q (rule: data_change, picked at Q = 200)"

    # 5. Best refresh rule selection note: says picked on same data + gap to second-best rule
    assert "picked on the same data" in summary_md
    assert "gap to second-best (`recovered_neighbor`): +2.50 pp" in summary_md

    # 6. Spearman: compared on the same refresh decisions only (4 epochs per seed * 5 seeds = 20 decisions, NOT 25)
    df_sp = res["df_spearman"]
    rn_m2_steady = df_sp[
        (df_sp["experiment"] == "M2")
        & (df_sp["stream_mode"] == "steady")
        & (df_sp["budget_summaries"] == 4)
        & (df_sp["policy"] == "recovered_neighbor")
    ].iloc[0]
    assert int(rn_m2_steady["spearman_rn_estimator_n"]) == 20
    assert int(rn_m2_steady["spearman_new_vectors_estimator_n"]) == 20
    # Epoch 4 had sp_nv=0.90 but sp_rn=NaN; excluding epoch 4 keeps new_vectors mean at ~0.35 instead of ~0.46
    assert np.isclose(rn_m2_steady["spearman_new_vectors_estimator_pooled"], 0.335, atol=1e-3)
    assert np.isclose(rn_m2_steady["spearman_rn_estimator_pooled"], 0.635, atol=1e-3)


def test_missing_seed_marks_test_incomplete(
    synth_results_root: Path,
    tmp_path: Path,
) -> None:
    """Deleting a single seed folder lists it under Missing or incomplete runs and marks affected test INCOMPLETE."""
    target_rid = (
        "never_refresh_S32_m2_B4_Q200_steady_nearest_initial_W2_kmeans_bursts0_setup0_starvation_cap_arith_seed46"
    )
    target_dir = synth_results_root / "main" / "M1" / target_rid
    backup_dir = tmp_path / "backup_seed46"
    shutil.move(str(target_dir), str(backup_dir))
    try:
        out_dir = tmp_path / "out_missing_seed"
        res = analyze_all(results_root=synth_results_root, out_dir=out_dir)
        summary_md = (out_dir / "SUMMARY.md").read_text(encoding="utf-8")

        # Listed by run_id in missing_runs.csv and SUMMARY.md under Missing or incomplete runs
        df_miss = res["df_missing"]
        assert target_rid in set(df_miss["run_id"])
        miss_sec = summary_md.split("## 2. Missing or incomplete runs")[1].split("### Failed Runs Record")[0]
        assert target_rid in miss_sec

        # H1 steady is INCOMPLETE (never PASS or FAIL) and names the missing run
        df_hyp = res["df_hyp"].set_index("hypothesis")
        h1_steady_val = str(df_hyp.loc["H1", "steady_value"])
        h1_outcome = str(df_hyp.loc["H1", "outcome"])
        assert h1_steady_val.startswith("INCOMPLETE")
        assert target_rid in h1_steady_val
        assert "PASS" not in h1_steady_val and "FAIL" not in h1_steady_val
        assert "steady=INCOMPLETE" in h1_outcome
    finally:
        shutil.move(str(backup_dir), str(target_dir))


def test_missing_queries_npz_listed_and_incomplete(
    synth_results_root: Path,
    tmp_path: Path,
) -> None:
    """A run folder missing queries.npz is listed as incomplete and marks affected tests INCOMPLETE."""
    target_rid = (
        "extra_search_S32_m2_B4_Q200_steady_nearest_initial_W2_kmeans_bursts0_setup0_starvation_cap_arith_seed42"
    )
    q_path = synth_results_root / "main" / "M1" / target_rid / "queries.npz"
    backup_q = tmp_path / "backup_queries.npz"
    shutil.move(str(q_path), str(backup_q))
    try:
        out_dir = tmp_path / "out_missing_q"
        res = analyze_all(results_root=synth_results_root, out_dir=out_dir)
        summary_md = (out_dir / "SUMMARY.md").read_text(encoding="utf-8")

        df_miss = res["df_missing"]
        row = df_miss[df_miss["run_id"] == target_rid].iloc[0]
        assert row["status"] == "incomplete"
        assert "missing queries.npz" in row["reason"]

        miss_sec = summary_md.split("## 2. Missing or incomplete runs")[1].split("### Failed Runs Record")[0]
        assert target_rid in miss_sec
        assert "missing queries.npz" in miss_sec

        # H2 steady is INCOMPLETE (never PASS or FAIL) and names the incomplete run
        df_hyp = res["df_hyp"].set_index("hypothesis")
        h2_steady_val = str(df_hyp.loc["H2", "steady_value"])
        assert h2_steady_val.startswith("INCOMPLETE")
        assert target_rid in h2_steady_val
        assert "missing queries.npz" in h2_steady_val
        assert "PASS" not in h2_steady_val and "FAIL" not in h2_steady_val
    finally:
        shutil.move(str(backup_q), str(q_path))


def test_gate_failing_on_one_stream_h3_not_tested_on_that_stream_only(
    synth_results_root: Path,
    tmp_path: Path,
) -> None:
    """When GATE_M2 fails on steady and passes on new_topic, H3 is NOT TESTED on steady only."""
    configs_dir = REPO_ROOT / "configs"
    grid_m2 = load_expected_grid(configs_dir / "main_M2.yaml", "M2")
    m2_dir = synth_results_root / "main" / "M2"

    # Lower always_fresh on M2 steady B=4 Q=200 to 0.520 (gap +2.0 pp < 5.0 pp -> GATE_M2 steady FAIL)
    # Also lower data_change on M2 new_topic B=4 Q=200 to 0.590 so H3 passes on new_topic (+9.0 pp)
    modified_dirs: Dict[Path, Path] = {}
    try:
        for seed in grid_m2["seeds"]:
            af_rid = grid_m2["expected_cells"][("steady", "time_segment", 4, 200, "arithmetic", "always_fresh", seed)]
            dc_rid = grid_m2["expected_cells"][("new_topic", "time_segment", 4, 200, "arithmetic", "data_change", seed)]

            for rid, stream, pol, new_mean in [
                (af_rid, "steady", "always_fresh", 0.520),
                (dc_rid, "new_topic", "data_change", 0.590),
            ]:
                orig_dir = m2_dir / rid
                bak_dir = tmp_path / f"bak_{rid}"
                shutil.move(str(orig_dir), str(bak_dir))
                modified_dirs[orig_dir] = bak_dir
                _write_synthetic_run(
                    run_dir=orig_dir,
                    run_id=rid,
                    exp_label="M2",
                    grid_spec=grid_m2,
                    stream_mode=stream,
                    b_sum=4,
                    q_val=200,
                    policy=pol,
                    seed=seed,
                    override_recent_mean=new_mean,
                )

        out_dir = tmp_path / "out_gate_fail_one"
        res = analyze_all(results_root=synth_results_root, out_dir=out_dir)
        summary_md = (out_dir / "SUMMARY.md").read_text(encoding="utf-8")

        df_hyp = res["df_hyp"].set_index("hypothesis")
        assert df_hyp.loc["GATE_M2", "outcome"] == "steady=FAIL, new_topic=PASS"
        assert df_hyp.loc["H3", "steady_value"] == "NOT TESTED (GATE FAILED) / FAIL"
        assert str(df_hyp.loc["H3", "new_topic_value"]).startswith("**PASS**")
        assert df_hyp.loc["H3", "outcome"] == "steady=NOT TESTED (GATE FAILED) / FAIL, new_topic=PASS"

        fail_sec = summary_md.split("### Hypotheses that FAIL")[1].split("## 2.")[0]
        assert "**GATE_M2 (steady)**: **FAIL**" in fail_sec
        assert "**H3 (steady)**: **NOT TESTED (GATE FAILED) / FAIL**" in fail_sec
    finally:
        for orig_dir, bak_dir in modified_dirs.items():
            if orig_dir.exists():
                shutil.rmtree(orig_dir)
            shutil.move(str(bak_dir), str(orig_dir))


def test_config_mismatch_duplicate_and_failed_runs_json_cli(
    synth_results_root: Path,
    tmp_path: Path,
) -> None:
    """Verify config mismatch flagging, duplicate run detection, failed_runs.json unique run_id counting,
    and CLI `--results-root` and `--out` execution."""
    configs_dir = REPO_ROOT / "configs"
    grid_sens = load_expected_grid(configs_dir / "main_sens.yaml", "sens")
    sens_dir = synth_results_root / "main" / "sens"
    m1_dir = synth_results_root / "main" / "M1"

    # 1. Create a duplicate run folder in sens for never_refresh steady B=4 Q=200 seed=42
    dup_rid = grid_sens["expected_cells"][("steady", "time_segment", 4, 200, "fixed", "never_refresh", 42)]
    dup_dir = sens_dir / f"{dup_rid}_dup_copy"
    shutil.copytree(sens_dir / dup_rid, dup_dir)

    # 2. Create a config-mismatched folder in sens (num_shards=16 != expected 32)
    bad_cfg_dir = sens_dir / "stray_bad_shards_run"
    shutil.copytree(sens_dir / dup_rid, bad_cfg_dir)
    s_path = bad_cfg_dir / "summary.json"
    summary_data = json.loads(s_path.read_text(encoding="utf-8"))
    summary_data["run_id"] = "stray_bad_shards_run"
    summary_data["num_shards"] = 16
    s_path.write_text(json.dumps(summary_data), encoding="utf-8")

    # 3. Write failed_runs.json in M1 with 3 attempt entries across 2 unique run_ids
    failed_path = m1_dir / "failed_runs.json"
    failed_entries = [
        {"run_id": "run_alpha_seed42", "error": "timeout", "attempt": 1},
        {"run_id": "run_alpha_seed42", "error": "timeout", "attempt": 2},
        {"run_id": "run_beta_seed43", "error": "transient", "attempt": 1},
    ]
    failed_path.write_text(json.dumps(failed_entries), encoding="utf-8")

    try:
        out_dir = tmp_path / "cli_out"
        cmd = [
            sys.executable,
            str(REPO_ROOT / "analysis" / "analyze.py"),
            "--results-root",
            str(synth_results_root),
            "--out",
            str(out_dir),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
        assert proc.returncode == 0

        summary_md = (out_dir / "SUMMARY.md").read_text(encoding="utf-8")
        df_miss = pd.read_csv(out_dir / "missing_runs.csv")

        # Duplicate and config mismatch are both flagged
        assert "duplicate" in set(df_miss["status"])
        assert "config_mismatch" in set(df_miss["status"])
        assert "stray_bad_shards_run" in set(df_miss["run_id"])
        assert dup_rid in set(df_miss["run_id"])

        # Failed runs section counts 2 unique run ids (not 3 attempts) and lists them
        assert "`M1`: 2 unique run id(s) across 3 attempt(s):" in summary_md
        assert "`run_alpha_seed42`" in summary_md
        assert "`run_beta_seed43`" in summary_md
    finally:
        if dup_dir.exists():
            shutil.rmtree(dup_dir)
        if bad_cfg_dir.exists():
            shutil.rmtree(bad_cfg_dir)
        if failed_path.exists():
            failed_path.unlink()


def test_addendum2_hypotheses_pred_m2_ties_and_lookahead(
    synth_results_root: Path,
    tmp_path: Path,
) -> None:
    """Verify Addendum 2 tests: PRED_M2_TIES (both non-zero FAIL and exact-tie PASS),
    GATE_M3, H3b, H2b (with break-even Q), H4b (per-B winners), and LOOKAHEAD intervals."""
    out_dir = tmp_path / "out_add2"
    res = analyze_all(results_root=synth_results_root, out_dir=out_dir)
    summary_md = (out_dir / "SUMMARY.md").read_text(encoding="utf-8")

    # 1. M1_online (30/30) and M3 (360/360) are reported in SUMMARY.md and output CSVs exist
    assert "30 / 30 complete valid runs" in summary_md
    assert "360 / 360 complete valid runs" in summary_md
    assert (out_dir / "m1_online_conditions.csv").exists()
    assert (out_dir / "m3_conditions.csv").exists()
    assert (out_dir / "breakeven_q_online.csv").exists()
    assert (out_dir / "lookahead_comparison.csv").exists()

    df_hyp = res["df_hyp"].set_index("hypothesis")

    # 2. GATE_M3, H3b, H2b, H4b outcomes on synth_results_root
    assert df_hyp.loc["GATE_M3", "outcome"] == "steady=PASS, new_topic=PASS"
    assert df_hyp.loc["H3b", "outcome"] == "steady=PASS, new_topic=FAIL"
    assert df_hyp.loc["H2b", "outcome"] == "steady=PASS, new_topic=PASS"

    h4b_outcome = str(df_hyp.loc["H4b", "outcome"])
    assert "B=2:FAIL (winner: extra_search_online)" in h4b_outcome
    assert "B=4:PASS (winner: recovered_neighbor)" in h4b_outcome
    assert "B=8:PASS (winner: recovered_neighbor)" in h4b_outcome

    # Break-even Q on H2b (extra_search_online vs fixed Q=200 best refresh rule `data_change`)
    df_be_on = res["df_breakeven_online"]
    assert set(df_be_on["best_refresh_policy"]) == {"data_change"}
    steady_be_on = str(df_be_on[df_be_on["stream_mode"] == "steady"]["breakeven_q"].iloc[0])
    nt_be_on = str(df_be_on[df_be_on["stream_mode"] == "new_topic"]["breakeven_q"].iloc[0])
    assert steady_be_on == "between Q = 200 and Q = 800 (rule: data_change, picked at Q = 200)"
    assert (
        nt_be_on
        == "no crossing in {50, 200, 800}: extra_search_online ahead at all Q (rule: data_change, picked at Q = 200)"
    )

    # 3. LOOKAHEAD intervals on M1+M1_online (+1.00 pp) and M3 (+1.50 pp)
    assert df_hyp.loc["LOOKAHEAD", "outcome"] == "steady=REPORTED, new_topic=REPORTED"
    df_la = res["df_lookahead"]
    assert len(df_la) == 12  # 2 streams * (3 Q in M1 + 3 B in M3)
    m1_la = df_la[df_la["experiment"] == "M1+M1_online"]
    m3_la = df_la[df_la["experiment"] == "M3"]
    assert np.allclose(m1_la["recent_diff_mean"].to_numpy(dtype=np.float64), 0.010, atol=1e-4)
    assert np.allclose(m3_la["recent_diff_mean"].to_numpy(dtype=np.float64), 0.015, atol=1e-4)

    # 4. PRED_M2_TIES: on default synth_results_root where M2 refresh rules differ from always_fresh (0.650),
    # PRED_M2_TIES is FAIL; when all 5 refresh rules match always_fresh (0.650) per query, PRED_M2_TIES is PASS (0.00 pp).
    assert df_hyp.loc["PRED_M2_TIES", "outcome"] == "steady=FAIL, new_topic=FAIL"
    assert res["pred_m2_ties"]["steady"]["max_abs_diff"] > 0.10

    configs_dir = REPO_ROOT / "configs"
    grid_m2 = load_expected_grid(configs_dir / "main_M2.yaml", "M2")
    m2_dir = synth_results_root / "main" / "M2"
    modified_dirs: Dict[Path, Path] = {}
    try:
        for (stream, _placement, b_sum, q_val, _cap_mode, pol, seed), rid in grid_m2[
            "expected_cells"
        ].items():
            if pol in REFRESH_RULES and stream == "steady":
                orig_dir = m2_dir / rid
                bak_dir = tmp_path / f"bak_tie_{rid}"
                shutil.move(str(orig_dir), str(bak_dir))
                modified_dirs[orig_dir] = bak_dir
                _write_synthetic_run(
                    run_dir=orig_dir,
                    run_id=rid,
                    exp_label="M2",
                    grid_spec=grid_m2,
                    stream_mode=stream,
                    b_sum=b_sum,
                    q_val=q_val,
                    policy=pol,
                    seed=seed,
                    override_recent_mean=0.650,
                )

        out_ties = tmp_path / "out_ties"
        res_ties = analyze_all(results_root=synth_results_root, out_dir=out_ties)
        df_hyp_ties = res_ties["df_hyp"].set_index("hypothesis")
        assert df_hyp_ties.loc["PRED_M2_TIES", "outcome"] == "steady=PASS, new_topic=FAIL"
        assert res_ties["pred_m2_ties"]["steady"]["max_abs_diff"] == 0.0
        assert "0.00 pp (0.000000" in str(df_hyp_ties.loc["PRED_M2_TIES", "steady_value"])
    finally:
        for orig_dir, bak_dir in modified_dirs.items():
            if orig_dir.exists():
                shutil.rmtree(orig_dir)
            shutil.move(str(bak_dir), str(orig_dir))


def test_gate_m3_failing_on_one_stream_h3b_not_tested_on_that_stream_only(
    synth_results_root: Path,
    tmp_path: Path,
) -> None:
    """When GATE_M3 fails on steady and passes on new_topic, H3b is NOT TESTED on steady only."""
    configs_dir = REPO_ROOT / "configs"
    grid_m3 = load_expected_grid(configs_dir / "main_M3.yaml", "M3")
    m3_dir = synth_results_root / "main" / "M3"

    modified_dirs: Dict[Path, Path] = {}
    try:
        for seed in grid_m3["seeds"]:
            af_rid = grid_m3["expected_cells"][
                ("steady", "time_segment_g", 4, 200, "arithmetic", "always_fresh", seed)
            ]
            dc_rid = grid_m3["expected_cells"][
                ("new_topic", "time_segment_g", 4, 200, "arithmetic", "data_change", seed)
            ]
            for rid, stream, pol, new_mean in [
                (af_rid, "steady", "always_fresh", 0.520),
                (dc_rid, "new_topic", "data_change", 0.590),
            ]:
                orig_dir = m3_dir / rid
                bak_dir = tmp_path / f"bak_m3_{rid}"
                shutil.move(str(orig_dir), str(bak_dir))
                modified_dirs[orig_dir] = bak_dir
                _write_synthetic_run(
                    run_dir=orig_dir,
                    run_id=rid,
                    exp_label="M3",
                    grid_spec=grid_m3,
                    stream_mode=stream,
                    b_sum=4,
                    q_val=200,
                    policy=pol,
                    seed=seed,
                    override_recent_mean=new_mean,
                )

        out_dir = tmp_path / "out_gate_m3_fail"
        res = analyze_all(results_root=synth_results_root, out_dir=out_dir)
        df_hyp = res["df_hyp"].set_index("hypothesis")
        assert df_hyp.loc["GATE_M3", "outcome"] == "steady=FAIL, new_topic=PASS"
        assert df_hyp.loc["H3b", "steady_value"] == "NOT TESTED (GATE FAILED) / FAIL"
        assert str(df_hyp.loc["H3b", "new_topic_value"]).startswith("**PASS**")
        assert df_hyp.loc["H3b", "outcome"] == "steady=NOT TESTED (GATE FAILED) / FAIL, new_topic=PASS"
    finally:
        for orig_dir, bak_dir in modified_dirs.items():
            if orig_dir.exists():
                shutil.rmtree(orig_dir)
            shutil.move(str(bak_dir), str(orig_dir))


def test_missing_m1_online_or_m3_run_marks_addendum2_tests_incomplete(
    synth_results_root: Path,
    tmp_path: Path,
) -> None:
    """Missing a run in M1_online or M3 lists it in missing_runs.csv and marks H2b, H4b, LOOKAHEAD INCOMPLETE."""
    configs_dir = REPO_ROOT / "configs"
    grid_m1_on = load_expected_grid(configs_dir / "main_M1_online.yaml", "M1_online")
    grid_m3 = load_expected_grid(configs_dir / "main_M3.yaml", "M3")

    m1_on_rid = grid_m1_on["expected_cells"][
        ("steady", "nearest_initial", 4, 200, "arithmetic", "extra_search_online", 42)
    ]
    m3_rid = grid_m3["expected_cells"][
        ("new_topic", "time_segment_g", 4, 200, "arithmetic", "extra_search_online", 43)
    ]

    m1_on_dir = synth_results_root / "main" / "M1_online" / m1_on_rid
    m3_dir = synth_results_root / "main" / "M3" / m3_rid
    bak_m1_on = tmp_path / "bak_m1_on"
    bak_m3 = tmp_path / "bak_m3"
    shutil.move(str(m1_on_dir), str(bak_m1_on))
    shutil.move(str(m3_dir), str(bak_m3))
    try:
        out_dir = tmp_path / "out_miss_add2"
        res = analyze_all(results_root=synth_results_root, out_dir=out_dir)
        df_miss = res["df_missing"]
        assert m1_on_rid in set(df_miss["run_id"])
        assert m3_rid in set(df_miss["run_id"])

        df_hyp = res["df_hyp"].set_index("hypothesis")
        assert str(df_hyp.loc["H2b", "steady_value"]).startswith("INCOMPLETE")
        assert m1_on_rid in str(df_hyp.loc["H2b", "steady_value"])
        assert "B=4:INCOMPLETE" in str(df_hyp.loc["H4b", "outcome"])
        assert m3_rid in str(df_hyp.loc["H4b", "new_topic_value"])
        assert str(df_hyp.loc["LOOKAHEAD", "steady_value"]).startswith("INCOMPLETE")
        assert str(df_hyp.loc["LOOKAHEAD", "new_topic_value"]).startswith("INCOMPLETE")
    finally:
        shutil.move(str(bak_m1_on), str(m1_on_dir))
        shutil.move(str(bak_m3), str(m3_dir))

