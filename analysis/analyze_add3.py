"""Analysis script for Addendum 3 (exploratory, post hoc): charged data_change on M3 (G = 8) and M1 (Q = 800).

Imports and reuses `paired_hierarchical_bootstrap_ci`, `hierarchical_bootstrap_ci`,
`load_expected_grid`, `scan_experiment_runs`, `build_condition_table`, and
`index_runs_by_condition` from `analysis/analyze.py` (no new statistics code).

Accepts:
- `--charged-dir` (defaults to `<results_root>/addendum3/M3_dc_charged`)
- `--m3-dir` (defaults to `<results_root>/main/M3`)
- `--replication-dir` (defaults to `<results_root>/addendum3/replication`)
- `--m1-charged-dir` (defaults to `<results_root>/addendum3/M1_dc_charged`)
- `--m1-dir` (defaults to `<results_root>/main/M1` if present, else `<repo_root>/../analysis_root/main/M1`)
- `--m1-online-dir` (defaults to `<results_root>/main/M1_online`)
- `--m1-replication-dir` (defaults to `<results_root>/addendum3/M1_replication`)
- `--out-dir` (defaults to `<results_root>/addendum3/out`)

Produces in `<out_dir>`:
- `replication_check.csv`
- `add3_conditions.csv`
- `add3_paired_comparisons.csv`
- `add3_dc_charged_epochs.csv`
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

import argparse
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from analysis.analyze import (
    build_condition_table,
    fmt_diff_pts,
    hierarchical_bootstrap_ci,
    index_runs_by_condition,
    load_expected_grid,
    paired_hierarchical_bootstrap_ci,
    scan_experiment_runs,
)
from dvs.bus import DC_SCORE_BYTES_PER_SHARD

PAIRING_IDENTITY_SUMMARY_KEYS: Tuple[str, ...] = (
    "total_queries_evaluated",
    "total_queries_with_recent_neighbors",
    "total_queries_without_recent_neighbors",
    "queries_with_most_neighbors_in_stream",
    "mean_stream_neighbors_in_top_10",
    "initial_set_size",
    "stream_set_size",
    "inserts_per_epoch",
    "num_epochs",
    "active_segments",
)


def _arrays_exact_equal(a: np.ndarray, b: np.ndarray) -> bool:
    """Compare two numpy arrays for exact equality after nan_to_num plus identical NaN masks."""
    if a.shape != b.shape or a.dtype != b.dtype:
        return False
    if np.issubdtype(a.dtype, np.floating):
        nan_a = np.isnan(a)
        nan_b = np.isnan(b)
        if not np.array_equal(nan_a, nan_b):
            return False
        return bool(np.array_equal(np.nan_to_num(a, nan=0.0), np.nan_to_num(b, nan=0.0)))
    return bool(np.array_equal(a, b))


def _summary_values_exact_equal(v1: Any, v2: Any) -> bool:
    """Compare two JSON-loaded summary values for exact equality with NaN-aware float handling."""
    if v1 is None or v2 is None:
        return v1 is v2
    if isinstance(v1, bool) or isinstance(v2, bool):
        return isinstance(v1, bool) and isinstance(v2, bool) and (v1 == v2)
    if isinstance(v1, (int, float, np.number)) and isinstance(v2, (int, float, np.number)):
        f1 = float(v1)
        f2 = float(v2)
        if np.isnan(f1) and np.isnan(f2):
            return True
        if np.isnan(f1) or np.isnan(f2):
            return False
        return v1 == v2
    return type(v1) is type(v2) and v1 == v2


def verify_replication_run(
    replication_dir: Path,
    m3_dir: Path,
    configs_dir: Path,
    rep_yaml: str = "add3_replication.yaml",
    rep_exp: str = "addendum3/replication",
    stored_yaml: str = "main_M3.yaml",
    stored_exp: str = "main/M3",
    target_cell: Tuple[str, str, int, int, str, str, int] = (
        "new_topic",
        "time_segment_g",
        4,
        200,
        "arithmetic",
        "data_change",
        42,
    ),
) -> pd.DataFrame:
    """Verify that the single replication run in `replication_dir` matches the stored target run
    in `m3_dir` (M3 or M1) on every non-CPU value:
    - all three arrays in `queries.npz` (`recall_at_10`, `recent_recall_at_10`, `missed_neighbors`),
    - every non-CPU column of `epochs.parquet`,
    - every non-CPU value in `summary.json` except `charge_dc_scores_to_budget`.
    Raises ValueError if the replication run is missing, invalid, or differs on any non-CPU value.
    """
    if replication_dir is None or not replication_dir.exists():
        raise ValueError(f"replication run missing: {replication_dir}")

    grid_rep = load_expected_grid(configs_dir / rep_yaml, rep_exp)
    scan_rep = scan_experiment_runs(replication_dir, grid_rep)
    if scan_rep["issues"] or len(scan_rep["valid_runs"]) != 1:
        reasons = "; ".join(f"{i['run_id']}: {i['status']} ({i['reason']})" for i in scan_rep["issues"])
        raise ValueError(f"replication run missing or invalid in {replication_dir}: {reasons}")

    rep_run = scan_rep["valid_runs"][0]
    rep_rid = str(rep_run["run_id"])
    rep_summary = rep_run["summary"]
    rep_run_dir = replication_dir / rep_rid
    rep_epochs = pd.read_parquet(rep_run_dir / "epochs.parquet").sort_values("epoch").reset_index(drop=True)

    if m3_dir is None or not m3_dir.exists():
        raise ValueError(f"Stored target directory not found: {m3_dir}")

    grid_stored = load_expected_grid(configs_dir / stored_yaml, stored_exp)
    expected_stored_rid = grid_stored["expected_cells"][target_cell]
    stored_run_dir = m3_dir / expected_stored_rid
    if not stored_run_dir.exists():
        raise ValueError(f"Stored target run directory not found: {stored_run_dir}")

    scan_stored = scan_experiment_runs(m3_dir, grid_stored)
    if target_cell not in scan_stored["valid_by_cell"]:
        raise ValueError(f"Stored run for cell {target_cell} ({expected_stored_rid}) is missing or invalid")

    stored_run = scan_stored["valid_by_cell"][target_cell]
    stored_rid = str(stored_run["run_id"])
    stored_summary = stored_run["summary"]
    stored_epochs = (
        pd.read_parquet(m3_dir / stored_rid / "epochs.parquet").sort_values("epoch").reset_index(drop=True)
    )

    # 1. Exact comparison of all three queries.npz arrays
    expected_q_keys = ("recall_at_10", "recent_recall_at_10", "missed_neighbors")
    with np.load(rep_run_dir / "queries.npz", allow_pickle=False) as qz_rep, np.load(
        m3_dir / stored_rid / "queries.npz", allow_pickle=False
    ) as qz_stored:
        q_keys_match = set(qz_rep.files) == set(expected_q_keys) and set(qz_stored.files) == set(expected_q_keys)
        overall_q_match = q_keys_match and _arrays_exact_equal(
            qz_rep["recall_at_10"], qz_stored["recall_at_10"]
        )
        recent_q_match = q_keys_match and _arrays_exact_equal(
            qz_rep["recent_recall_at_10"], qz_stored["recent_recall_at_10"]
        )
        missed_q_match = q_keys_match and _arrays_exact_equal(
            qz_rep["missed_neighbors"], qz_stored["missed_neighbors"]
        )
    queries_match = bool(overall_q_match and recent_q_match and missed_q_match)

    # 2. Exact comparison of every non-CPU column in epochs.parquet
    rep_non_cpu_cols = [c for c in rep_epochs.columns if not str(c).startswith("cpu_")]
    stored_non_cpu_cols = [c for c in stored_epochs.columns if not str(c).startswith("cpu_")]
    epochs_match = True
    epochs_diff_reason = ""
    if rep_non_cpu_cols != stored_non_cpu_cols:
        epochs_match = False
        epochs_diff_reason = f"non-CPU column list mismatch: {rep_non_cpu_cols} != {stored_non_cpu_cols}"
    else:
        try:
            pd.testing.assert_frame_equal(
                rep_epochs[rep_non_cpu_cols],
                stored_epochs[stored_non_cpu_cols],
                check_exact=True,
                check_dtype=True,
            )
        except AssertionError as exc:
            epochs_match = False
            epochs_diff_reason = str(exc)

    # 3. Exact comparison of every non-CPU value in summary.json except charge_dc_scores_to_budget
    rep_sum_keys = sorted(
        k for k in rep_summary.keys() if not str(k).startswith("cpu_") and k != "charge_dc_scores_to_budget"
    )
    stored_sum_keys = sorted(
        k
        for k in stored_summary.keys()
        if not str(k).startswith("cpu_") and k != "charge_dc_scores_to_budget"
    )
    summary_match = True
    summary_diff_keys: List[str] = []
    if rep_sum_keys != stored_sum_keys:
        summary_match = False
        summary_diff_keys.append(f"key_set_mismatch({sorted(set(rep_sum_keys) ^ set(stored_sum_keys))})")
    else:
        for k in rep_sum_keys:
            if not _summary_values_exact_equal(rep_summary[k], stored_summary[k]):
                summary_match = False
                summary_diff_keys.append(k)

    recent_match = bool(
        recent_q_match
        and _summary_values_exact_equal(
            rep_summary.get("recent_recall_at_10_query_mean"),
            stored_summary.get("recent_recall_at_10_query_mean"),
        )
    )
    overall_match = bool(
        overall_q_match
        and _summary_values_exact_equal(
            rep_summary.get("overall_recall_at_10_mean"),
            stored_summary.get("overall_recall_at_10_mean"),
        )
    )

    all_ok = bool(queries_match and epochs_match and summary_match and recent_match and overall_match)
    status = "PASS" if all_ok else "FAIL"
    df_rep = pd.DataFrame(
        [
            {
                "experiment": rep_exp,
                "stored_experiment": stored_exp,
                "replication_run_id": rep_rid,
                "stored_m3_run_id": stored_rid,
                "stored_run_id": stored_rid,
                "stream_mode": str(target_cell[0]),
                "placement_mode": str(target_cell[1]),
                "budget_summaries": int(target_cell[2]),
                "queries_per_epoch": int(target_cell[3]),
                "seed": int(target_cell[6]),
                "replication_recent_recall_at_10": float(rep_summary["recent_recall_at_10_query_mean"]),
                "stored_m3_recent_recall_at_10": float(stored_summary["recent_recall_at_10_query_mean"]),
                "recent_recall_exact_match": bool(recent_match),
                "replication_overall_recall_at_10": float(rep_summary["overall_recall_at_10_mean"]),
                "stored_m3_overall_recall_at_10": float(stored_summary["overall_recall_at_10_mean"]),
                "overall_recall_exact_match": bool(overall_match),
                "queries_npz_exact_match": bool(queries_match),
                "epoch_non_cpu_exact_match": bool(epochs_match),
                "summary_non_cpu_exact_match": bool(summary_match),
                "epoch_refresh_and_metrics_match": bool(epochs_match),
                "status": status,
            }
        ]
    )
    if not all_ok:
        raise ValueError(
            f"Replication check FAILED for {rep_rid} vs {stored_rid} ({stored_exp}): "
            f"queries_match={queries_match} (recent={recent_q_match}, overall={overall_q_match}, "
            f"missed={missed_q_match}), epochs_match={epochs_match} ({epochs_diff_reason}), "
            f"summary_match={summary_match} (diff_keys={summary_diff_keys})"
        )
    return df_rep


def _verify_paired_runs_identity(
    runs_a: Dict[int, Dict[str, Any]],
    runs_b: Dict[int, Dict[str, Any]],
) -> None:
    """Assert that paired runs share data_provenance_sha256 and schedule/ground-truth summary fields (L-5).
    Raises ValueError naming both run ids if any field differs.
    """
    for seed in sorted(runs_a.keys()):
        ra = runs_a[seed]
        rb = runs_b[seed]
        rid_a = str(ra["run_id"])
        rid_b = str(rb["run_id"])
        prov_a = ra.get("provenance", {})
        prov_b = rb.get("provenance", {})
        sha_a = prov_a.get("data_provenance_sha256")
        sha_b = prov_b.get("data_provenance_sha256")
        if not sha_a or not sha_b or sha_a != sha_b:
            raise ValueError(
                f"Paired run data_provenance_sha256 mismatch for seed {seed} between "
                f"{rid_a} ({sha_a!r}) and {rid_b} ({sha_b!r})"
            )
        sum_a = ra["summary"]
        sum_b = rb["summary"]
        for k in PAIRING_IDENTITY_SUMMARY_KEYS:
            va = sum_a.get(k)
            vb = sum_b.get(k)
            if not _summary_values_exact_equal(va, vb):
                raise ValueError(
                    f"Paired run summary[{k!r}] mismatch for seed {seed} between "
                    f"{rid_a} ({va!r}) and {rid_b} ({vb!r})"
                )


def build_charged_epochs_table(
    charged_batches: Sequence[Tuple[str, Path, Sequence[Dict[str, Any]]]],
) -> pd.DataFrame:
    """Build per-run epoch verification table for charged `data_change` runs (`add3_dc_charged_epochs.csv`),
    confirming k = B - 1 in {1, 3, 7}, 128 B overhead per epoch, and A = ceil(2S / k) in {64, 22, 10}.
    """
    expected_k_by_b = {2: 1, 4: 3, 8: 7}
    expected_cap_by_b = {2: 64, 4: 22, 8: 10}
    rows: List[Dict[str, Any]] = []

    for exp_label, exp_dir, runs in charged_batches:
        for r in sorted(
            runs,
            key=lambda x: (
                str(x["summary"]["stream_mode"]),
                int(x["summary"]["budget_summaries"]),
                int(x["summary"]["queries_per_epoch"]),
                int(x["summary"]["seed"]),
            ),
        ):
            s = r["summary"]
            rid = str(r["run_id"])
            b_sum = int(s["budget_summaries"])
            num_shards = int(s["num_shards"])
            expected_overhead = DC_SCORE_BYTES_PER_SHARD * num_shards
            expected_k = expected_k_by_b[b_sum]
            expected_cap = expected_cap_by_b[b_sum]

            ep_df = pd.read_parquet(exp_dir / rid / "epochs.parquet").sort_values("epoch").reset_index(drop=True)
            n_epochs = len(ep_df)
            k_series = ep_df["num_summaries_refreshed"].astype(int)
            refresh_bytes_series = ep_df["bytes_summary_refresh"].astype(int)
            budget_spent_series = ep_df["budget_bytes_spent"].astype(int)
            overhead_series = budget_spent_series - refresh_bytes_series

            k_min = int(k_series.min())
            k_max = int(k_series.max())
            overhead_min = int(overhead_series.min())
            overhead_max = int(overhead_series.max())
            cap_epochs = int(s["starvation_cap_epochs"])

            verified = (
                k_min == expected_k
                and k_max == expected_k
                and overhead_min == expected_overhead
                and overhead_max == expected_overhead
                and cap_epochs == expected_cap
            )

            rows.append(
                {
                    "experiment": exp_label,
                    "run_id": rid,
                    "placement_mode": str(s["placement_mode"]),
                    "stream_mode": str(s["stream_mode"]),
                    "budget_summaries": b_sum,
                    "queries_per_epoch": int(s["queries_per_epoch"]),
                    "seed": int(s["seed"]),
                    "num_epochs": n_epochs,
                    "charge_dc_scores_to_budget": bool(s.get("charge_dc_scores_to_budget", False)),
                    "expected_k_per_epoch": expected_k,
                    "min_summaries_refreshed_per_epoch": k_min,
                    "max_summaries_refreshed_per_epoch": k_max,
                    "total_summaries_refreshed": int(s["total_summaries_refreshed"]),
                    "total_forced_by_cap": int(s["total_forced_by_cap"]),
                    "expected_starvation_cap_epochs": expected_cap,
                    "starvation_cap_epochs": cap_epochs,
                    "expected_overhead_bytes_per_epoch": expected_overhead,
                    "min_overhead_bytes_per_epoch": overhead_min,
                    "max_overhead_bytes_per_epoch": overhead_max,
                    "epoch_budget_bytes_spent": int(budget_spent_series.iloc[0]),
                    "total_budget_bytes_spent": int(s["total_budget_bytes_spent"]),
                    "verified_k_and_overhead": bool(verified),
                }
            )

    return pd.DataFrame(rows)


def _augment_condition_table_with_t_ci(
    df_cond: pd.DataFrame,
    runs: Sequence[Dict[str, Any]],
    charge_dc_flag: bool,
) -> pd.DataFrame:
    """Attach 5-seed t-interval bounds and `charge_dc_scores_to_budget` to `build_condition_table` output."""
    if df_cond.empty:
        return df_cond
    idx = index_runs_by_condition(runs)
    rec_t_lows: List[float] = []
    rec_t_highs: List[float] = []
    ov_t_lows: List[float] = []
    ov_t_highs: List[float] = []
    k_per_epoch: List[float] = []

    for _, row in df_cond.iterrows():
        key = (
            str(row["stream_mode"]),
            str(row["placement_mode"]),
            int(row["budget_summaries"]),
            int(row["queries_per_epoch"]),
            str(row["starvation_cap_mode"]),
            str(row["policy"]),
        )
        seed_map = idx.get(key, {})
        r_sorted = [seed_map[s] for s in sorted(seed_map.keys())]
        ci_rec = hierarchical_bootstrap_ci([x["q_recent"] for x in r_sorted], seed=42)
        ci_ov = hierarchical_bootstrap_ci([x["q_recall"] for x in r_sorted], seed=42)
        rec_t_lows.append(ci_rec["t_ci_low"])
        rec_t_highs.append(ci_rec["t_ci_high"])
        ov_t_lows.append(ci_ov["t_ci_low"])
        ov_t_highs.append(ci_ov["t_ci_high"])
        n_ep = float(r_sorted[0]["summary"]["num_epochs"]) if r_sorted else 50.0
        k_per_epoch.append(float(row["summaries_refreshed_mean"]) / n_ep if n_ep > 0 else float("nan"))

    out = df_cond.copy()
    out["charge_dc_scores_to_budget"] = bool(charge_dc_flag)
    out["effective_k_per_epoch"] = k_per_epoch
    out["recent_recall_at_10_t_ci95_low"] = rec_t_lows
    out["recent_recall_at_10_t_ci95_high"] = rec_t_highs
    out["recall_at_10_t_ci95_low"] = ov_t_lows
    out["recall_at_10_t_ci95_high"] = ov_t_highs
    return out


def _build_paired_row(
    comp_group: str,
    comp_label: str,
    stream_mode: str,
    placement_mode: str,
    b_sum: int,
    q_val: int,
    baseline_pol: str,
    runs_a: Dict[int, Dict[str, Any]],
    runs_b: Dict[int, Dict[str, Any]],
    exp_seed_set: set[int],
) -> Dict[str, Any]:
    """Compute a single paired comparison row (X1, X2, X3, or X4) with L-5 identity verification."""
    policy_b_label = "uncharged_dc" if baseline_pol == "data_change" else baseline_pol
    if set(runs_a.keys()) != exp_seed_set or set(runs_b.keys()) != exp_seed_set:
        return {
            "comparison_group": comp_group,
            "comparison_label": comp_label,
            "stream_mode": stream_mode,
            "placement_mode": placement_mode,
            "budget_summaries": int(b_sum),
            "queries_per_epoch": int(q_val),
            "policy_a": "charged_dc",
            "policy_b": policy_b_label,
            "k_a": int(b_sum) - 1,
            "k_b": -1,
            "cap_epochs_a": -1,
            "cap_epochs_b": -1,
            "status": "INCOMPLETE",
            "n_seeds": 0,
            "n_paired_queries": 0,
            "recent_mean_a": float("nan"),
            "recent_mean_b": float("nan"),
            "recent_diff_mean": float("nan"),
            "recent_diff_boot_ci95_low": float("nan"),
            "recent_diff_boot_ci95_high": float("nan"),
            "recent_diff_t_ci95_low": float("nan"),
            "recent_diff_t_ci95_high": float("nan"),
            "recent_diff_within_ci95_low": float("nan"),
            "recent_diff_within_ci95_high": float("nan"),
            "overall_mean_a": float("nan"),
            "overall_mean_b": float("nan"),
            "overall_diff_mean": float("nan"),
            "overall_diff_boot_ci95_low": float("nan"),
            "overall_diff_boot_ci95_high": float("nan"),
            "overall_diff_t_ci95_low": float("nan"),
            "overall_diff_t_ci95_high": float("nan"),
            "overall_diff_within_ci95_low": float("nan"),
            "overall_diff_within_ci95_high": float("nan"),
            "budget_bytes_a_mean": float("nan"),
            "budget_bytes_b_mean": float("nan"),
            "prediction_id": "",
            "prediction_note": "",
        }

    _verify_paired_runs_identity(runs_a, runs_b)

    rec_a = {seed: r["q_recent"] for seed, r in runs_a.items()}
    rec_b = {seed: r["q_recent"] for seed, r in runs_b.items()}
    ov_a = {seed: r["q_recall"] for seed, r in runs_a.items()}
    ov_b = {seed: r["q_recall"] for seed, r in runs_b.items()}

    ci_rec = paired_hierarchical_bootstrap_ci(rec_a, rec_b, seed=42, check_nan_mask=True)
    ci_ov = paired_hierarchical_bootstrap_ci(ov_a, ov_b, seed=42, check_nan_mask=False)

    rec_mean_a = float(np.mean(np.concatenate([a[np.isfinite(a)] for a in rec_a.values()])))
    rec_mean_b = float(np.mean(np.concatenate([b[np.isfinite(b)] for b in rec_b.values()])))
    ov_mean_a = float(np.mean(np.concatenate(list(ov_a.values()))))
    ov_mean_b = float(np.mean(np.concatenate(list(ov_b.values()))))

    sample_a = next(iter(runs_a.values()))["summary"]
    sample_b = next(iter(runs_b.values()))["summary"]
    n_ep_a = int(sample_a["num_epochs"])
    n_ep_b = int(sample_b["num_epochs"])
    k_a = int(round(float(sample_a["total_summaries_refreshed"]) / n_ep_a)) if n_ep_a > 0 else 0
    k_b = int(round(float(sample_b["total_summaries_refreshed"]) / n_ep_b)) if n_ep_b > 0 else 0
    cap_a = int(sample_a["starvation_cap_epochs"])
    cap_b = int(sample_b["starvation_cap_epochs"])

    bytes_a = float(np.mean([r["summary"]["total_budget_bytes_spent"] for r in runs_a.values()]))
    bytes_b = float(np.mean([r["summary"]["total_budget_bytes_spent"] for r in runs_b.values()]))

    pred_id = ""
    pred_note = ""
    if comp_group == "X1" and stream_mode == "new_topic" and int(b_sum) == 4:
        pred_id = "P1"
        boot_gt_neg1pp = bool(ci_rec["ci_low"] > -0.01)
        t_gt_neg1pp = bool(ci_rec["t_ci_low"] > -0.01)
        disagree_str = (
            " (intervals agree)"
            if boot_gt_neg1pp == t_gt_neg1pp
            else " (DISAGREEMENT between bootstrap and 5-seed t-interval)"
        )
        pred_note = (
            f"P1 (exploratory, decided by two-stage paired bootstrap 95% CI lower bound > -1.00 pp: "
            f"{boot_gt_neg1pp}): "
            f"bootstrap={fmt_diff_pts(ci_rec['mean_diff'], ci_rec['ci_low'], ci_rec['ci_high'])} "
            f"(lower>-1pp: {boot_gt_neg1pp}); "
            f"5-seed t={fmt_diff_pts(ci_rec['mean_diff'], ci_rec['t_ci_low'], ci_rec['t_ci_high'])} "
            f"(lower>-1pp: {t_gt_neg1pp}){disagree_str}"
        )
    elif (
        comp_group == "X2"
        and comp_label == "charged_dc - push_vectors"
        and stream_mode == "new_topic"
        and int(b_sum) == 2
    ):
        pred_id = "P2"
        boot_not_above_zero = bool(ci_rec["ci_low"] <= 0.0)
        t_not_above_zero = bool(ci_rec["t_ci_low"] <= 0.0)
        disagree_str = (
            " (intervals agree)"
            if boot_not_above_zero == t_not_above_zero
            else " (DISAGREEMENT between bootstrap and 5-seed t-interval)"
        )
        pred_note = (
            f"P2 (exploratory, decided by two-stage paired bootstrap 95% CI lower bound <= 0.00 pp: "
            f"{boot_not_above_zero}): "
            f"bootstrap={fmt_diff_pts(ci_rec['mean_diff'], ci_rec['ci_low'], ci_rec['ci_high'])} "
            f"(lower<=0: {boot_not_above_zero}); "
            f"5-seed t={fmt_diff_pts(ci_rec['mean_diff'], ci_rec['t_ci_low'], ci_rec['t_ci_high'])} "
            f"(lower<=0: {t_not_above_zero}){disagree_str}"
        )

    return {
        "comparison_group": comp_group,
        "comparison_label": comp_label,
        "stream_mode": stream_mode,
        "placement_mode": placement_mode,
        "budget_summaries": int(b_sum),
        "queries_per_epoch": int(q_val),
        "policy_a": "charged_dc",
        "policy_b": policy_b_label,
        "k_a": k_a,
        "k_b": k_b,
        "cap_epochs_a": cap_a,
        "cap_epochs_b": cap_b,
        "status": "COMPLETE",
        "n_seeds": int(ci_rec["n_seeds"]),
        "n_paired_queries": int(ci_rec["n_paired_queries"]),
        "recent_mean_a": rec_mean_a,
        "recent_mean_b": rec_mean_b,
        "recent_diff_mean": float(ci_rec["mean_diff"]),
        "recent_diff_boot_ci95_low": float(ci_rec["ci_low"]),
        "recent_diff_boot_ci95_high": float(ci_rec["ci_high"]),
        "recent_diff_t_ci95_low": float(ci_rec["t_ci_low"]),
        "recent_diff_t_ci95_high": float(ci_rec["t_ci_high"]),
        "recent_diff_within_ci95_low": float(ci_rec["within_seed_ci_low"]),
        "recent_diff_within_ci95_high": float(ci_rec["within_seed_ci_high"]),
        "overall_mean_a": ov_mean_a,
        "overall_mean_b": ov_mean_b,
        "overall_diff_mean": float(ci_ov["mean_diff"]),
        "overall_diff_boot_ci95_low": float(ci_ov["ci_low"]),
        "overall_diff_boot_ci95_high": float(ci_ov["ci_high"]),
        "overall_diff_t_ci95_low": float(ci_ov["t_ci_low"]),
        "overall_diff_t_ci95_high": float(ci_ov["t_ci_high"]),
        "overall_diff_within_ci95_low": float(ci_ov["within_seed_ci_low"]),
        "overall_diff_within_ci95_high": float(ci_ov["within_seed_ci_high"]),
        "budget_bytes_a_mean": bytes_a,
        "budget_bytes_b_mean": bytes_b,
        "prediction_id": pred_id,
        "prediction_note": pred_note,
    }


def _resolve_default_m1_dir(m3_dir: Path) -> Path:
    """Resolve default stored M1 directory (sibling of m3_dir if present, else analysis_root/main/M1)."""
    cand_sibling = m3_dir.parent / "M1"
    if cand_sibling.exists():
        return cand_sibling
    cand_analysis = REPO_ROOT.parent / "analysis_root" / "main" / "M1"
    if cand_analysis.exists():
        return cand_analysis
    return REPO_ROOT.parent / "repo" / "results" / "main" / "M1"


def analyze_add3(
    charged_dir: Path,
    m3_dir: Path,
    out_dir: Path,
    replication_dir: Optional[Path] = None,
    configs_dir: Optional[Path] = None,
    m1_charged_dir: Optional[Path] = None,
    m1_dir: Optional[Path] = None,
    m1_online_dir: Optional[Path] = None,
    m1_replication_dir: Optional[Path] = None,
) -> Dict[str, pd.DataFrame]:
    """Run Addendum 3 analysis (M3 X1-X3 and M1 X4) and write CSV outputs to `out_dir`.
    Stops with ValueError (writing no outputs) if either replication run is missing or differs.
    """
    if configs_dir is None:
        configs_dir = REPO_ROOT / "configs"
    if replication_dir is None:
        replication_dir = charged_dir.parent / "replication"
    if m1_charged_dir is None:
        m1_charged_dir = charged_dir.parent / "M1_dc_charged"
    if m1_replication_dir is None:
        m1_replication_dir = charged_dir.parent / "M1_replication"
    if m1_dir is None:
        m1_dir = _resolve_default_m1_dir(m3_dir)
    if m1_online_dir is None:
        m1_online_dir = m3_dir.parent / "M1_online"

    # 1. Mandatory replication checks (M3 and M1) before creating or writing any output files
    df_rep_m3 = verify_replication_run(
        replication_dir=replication_dir,
        m3_dir=m3_dir,
        configs_dir=configs_dir,
        rep_yaml="add3_replication.yaml",
        rep_exp="addendum3/replication",
        stored_yaml="main_M3.yaml",
        stored_exp="main/M3",
        target_cell=("new_topic", "time_segment_g", 4, 200, "arithmetic", "data_change", 42),
    )
    df_rep_m1 = verify_replication_run(
        replication_dir=m1_replication_dir,
        m3_dir=m1_dir,
        configs_dir=configs_dir,
        rep_yaml="add3_M1_replication.yaml",
        rep_exp="addendum3/M1_replication",
        stored_yaml="main_M1.yaml",
        stored_exp="main/M1",
        target_cell=("new_topic", "nearest_initial", 4, 800, "arithmetic", "data_change", 42),
    )
    df_rep = pd.concat([df_rep_m3, df_rep_m1], ignore_index=True)

    # 2. Load and validate charged data_change runs and stored M3, M1, and M1_online runs
    grid_charged_m3 = load_expected_grid(configs_dir / "add3_M3_dc_charged.yaml", "addendum3/M3_dc_charged")
    scan_charged_m3 = scan_experiment_runs(charged_dir, grid_charged_m3)
    charged_m3_runs = scan_charged_m3["valid_runs"]

    grid_m3 = load_expected_grid(configs_dir / "main_M3.yaml", "main/M3")
    scan_m3 = scan_experiment_runs(m3_dir, grid_m3)
    m3_runs = scan_m3["valid_runs"]

    grid_charged_m1 = load_expected_grid(configs_dir / "add3_M1_dc_charged.yaml", "addendum3/M1_dc_charged")
    scan_charged_m1 = scan_experiment_runs(m1_charged_dir, grid_charged_m1)
    charged_m1_runs = scan_charged_m1["valid_runs"]

    grid_m1 = load_expected_grid(configs_dir / "main_M1.yaml", "main/M1")
    scan_m1 = scan_experiment_runs(m1_dir, grid_m1)
    m1_runs = scan_m1["valid_runs"]

    grid_m1_on = load_expected_grid(configs_dir / "main_M1_online.yaml", "main/M1_online")
    scan_m1_on = scan_experiment_runs(m1_online_dir, grid_m1_on)
    m1_on_runs = scan_m1_on["valid_runs"]

    # 3. Per-epoch refresh counts and bytes for charged data_change (M3 + M1)
    df_epochs = build_charged_epochs_table(
        [
            ("addendum3/M3_dc_charged", charged_dir, charged_m3_runs),
            ("addendum3/M1_dc_charged", m1_charged_dir, charged_m1_runs),
        ]
    )

    # 4. Per-condition means table (M3 charged + M3 stored + M1 charged + M1/M1_online stored at Q=800)
    expected_seeds = grid_charged_m3["seeds"]
    df_cond_charged_m3 = _augment_condition_table_with_t_ci(
        build_condition_table(charged_m3_runs, "addendum3/M3_dc_charged", expected_seeds=expected_seeds),
        charged_m3_runs,
        charge_dc_flag=True,
    )
    if not df_cond_charged_m3.empty:
        df_cond_charged_m3["policy_label"] = "charged_dc"

    m3_target_policies = {
        "data_change",
        "recovered_neighbor",
        "push_vectors",
        "extra_search_online",
        "never_refresh",
        "always_fresh",
    }
    m3_sub_runs = [r for r in m3_runs if str(r["summary"]["policy"]) in m3_target_policies]
    df_cond_m3 = _augment_condition_table_with_t_ci(
        build_condition_table(m3_sub_runs, "main/M3", expected_seeds=expected_seeds),
        m3_sub_runs,
        charge_dc_flag=False,
    )
    if not df_cond_m3.empty:
        df_cond_m3["policy_label"] = df_cond_m3["policy"].map(
            lambda p: "uncharged_dc" if p == "data_change" else str(p)
        )

    df_cond_charged_m1 = _augment_condition_table_with_t_ci(
        build_condition_table(charged_m1_runs, "addendum3/M1_dc_charged", expected_seeds=expected_seeds),
        charged_m1_runs,
        charge_dc_flag=True,
    )
    if not df_cond_charged_m1.empty:
        df_cond_charged_m1["policy_label"] = "charged_dc"

    m1_target_policies = {
        "data_change",
        "extra_search",
        "push_vectors",
        "recovered_neighbor",
        "never_refresh",
        "always_fresh",
    }
    m1_sub_runs = [
        r
        for r in m1_runs
        if str(r["summary"]["policy"]) in m1_target_policies
        and int(r["summary"]["queries_per_epoch"]) == 800
    ]
    df_cond_m1 = _augment_condition_table_with_t_ci(
        build_condition_table(m1_sub_runs, "main/M1", expected_seeds=expected_seeds),
        m1_sub_runs,
        charge_dc_flag=False,
    )
    if not df_cond_m1.empty:
        df_cond_m1["policy_label"] = df_cond_m1["policy"].map(
            lambda p: "uncharged_dc" if p == "data_change" else str(p)
        )

    m1_on_sub_runs = [r for r in m1_on_runs if int(r["summary"]["queries_per_epoch"]) == 800]
    df_cond_m1_on = _augment_condition_table_with_t_ci(
        build_condition_table(m1_on_sub_runs, "main/M1_online", expected_seeds=expected_seeds),
        m1_on_sub_runs,
        charge_dc_flag=False,
    )
    if not df_cond_m1_on.empty:
        df_cond_m1_on["policy_label"] = df_cond_m1_on["policy"].astype(str)

    df_conditions = pd.concat(
        [df_cond_charged_m3, df_cond_m3, df_cond_charged_m1, df_cond_m1, df_cond_m1_on],
        ignore_index=True,
    )

    # 5. Paired comparisons X1, X2, X3 (M3) and X4 (M1 at Q=800, B=4)
    idx_charged_m3 = index_runs_by_condition(charged_m3_runs)
    idx_m3 = index_runs_by_condition(m3_runs)
    idx_charged_m1 = index_runs_by_condition(charged_m1_runs)
    idx_m1 = index_runs_by_condition(m1_runs)
    idx_m1_on = index_runs_by_condition(m1_on_runs)

    exp_seed_set = set(int(s) for s in expected_seeds)
    comp_rows: List[Dict[str, Any]] = []

    m3_comparison_specs = [
        ("X1", "charged_dc - recovered_neighbor", "recovered_neighbor"),
        ("X2", "charged_dc - push_vectors", "push_vectors"),
        ("X2", "charged_dc - extra_search_online", "extra_search_online"),
        ("X3", "charged_dc - uncharged_dc", "data_change"),
    ]
    for comp_group, comp_label, baseline_pol in m3_comparison_specs:
        for stream_mode in grid_charged_m3["stream_modes"]:
            for b_sum in grid_charged_m3["budget_summaries"]:
                q_val = 200
                placement_mode = "time_segment_g"
                cap_mode = "arithmetic"
                runs_a = idx_charged_m3.get(
                    (stream_mode, placement_mode, int(b_sum), q_val, cap_mode, "data_change"), {}
                )
                runs_b = idx_m3.get(
                    (stream_mode, placement_mode, int(b_sum), q_val, cap_mode, baseline_pol), {}
                )
                comp_rows.append(
                    _build_paired_row(
                        comp_group=comp_group,
                        comp_label=comp_label,
                        stream_mode=stream_mode,
                        placement_mode=placement_mode,
                        b_sum=int(b_sum),
                        q_val=q_val,
                        baseline_pol=baseline_pol,
                        runs_a=runs_a,
                        runs_b=runs_b,
                        exp_seed_set=exp_seed_set,
                    )
                )

    m1_comparison_specs = [
        ("X4", "charged_dc - extra_search", "extra_search", idx_m1),
        ("X4", "charged_dc - push_vectors", "push_vectors", idx_m1),
        ("X4", "charged_dc - extra_search_online", "extra_search_online", idx_m1_on),
        ("X4", "charged_dc - uncharged_dc", "data_change", idx_m1),
    ]
    for comp_group, comp_label, baseline_pol, base_idx in m1_comparison_specs:
        for stream_mode in grid_charged_m1["stream_modes"]:
            b_sum = 4
            q_val = 800
            placement_mode = "nearest_initial"
            cap_mode = "arithmetic"
            runs_a = idx_charged_m1.get(
                (stream_mode, placement_mode, b_sum, q_val, cap_mode, "data_change"), {}
            )
            runs_b = base_idx.get(
                (stream_mode, placement_mode, b_sum, q_val, cap_mode, baseline_pol), {}
            )
            comp_rows.append(
                _build_paired_row(
                    comp_group=comp_group,
                    comp_label=comp_label,
                    stream_mode=stream_mode,
                    placement_mode=placement_mode,
                    b_sum=b_sum,
                    q_val=q_val,
                    baseline_pol=baseline_pol,
                    runs_a=runs_a,
                    runs_b=runs_b,
                    exp_seed_set=exp_seed_set,
                )
            )

    df_paired = pd.DataFrame(comp_rows)

    out_dir.mkdir(parents=True, exist_ok=True)
    df_rep.to_csv(out_dir / "replication_check.csv", index=False)
    df_epochs.to_csv(out_dir / "add3_dc_charged_epochs.csv", index=False)
    df_conditions.to_csv(out_dir / "add3_conditions.csv", index=False)
    df_paired.to_csv(out_dir / "add3_paired_comparisons.csv", index=False)

    return {
        "replication_check": df_rep,
        "add3_conditions": df_conditions,
        "add3_paired_comparisons": df_paired,
        "add3_dc_charged_epochs": df_epochs,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Analyze Addendum 3 (exploratory, post hoc): charged data_change on M3 (G = 8) and M1 (Q = 800)."
    )
    parser.add_argument(
        "--results-root",
        type=Path,
        default=REPO_ROOT / "results",
        help="Root results directory (defaults to <repo_root>/results).",
    )
    parser.add_argument(
        "--charged-dir",
        type=Path,
        default=None,
        help="Directory containing addendum3/M3_dc_charged runs (defaults to <results-root>/addendum3/M3_dc_charged).",
    )
    parser.add_argument(
        "--m3-dir",
        type=Path,
        default=None,
        help="Directory containing stored main/M3 runs (defaults to <results-root>/main/M3).",
    )
    parser.add_argument(
        "--replication-dir",
        type=Path,
        default=None,
        help="Directory containing addendum3/replication run (defaults to <results-root>/addendum3/replication).",
    )
    parser.add_argument(
        "--m1-charged-dir",
        type=Path,
        default=None,
        help="Directory containing addendum3/M1_dc_charged runs (defaults to <results-root>/addendum3/M1_dc_charged).",
    )
    parser.add_argument(
        "--m1-dir",
        type=Path,
        default=None,
        help="Directory containing stored main/M1 runs (defaults to <results-root>/main/M1 if present, else analysis_root/main/M1).",
    )
    parser.add_argument(
        "--m1-online-dir",
        type=Path,
        default=None,
        help="Directory containing stored main/M1_online runs (defaults to <results-root>/main/M1_online).",
    )
    parser.add_argument(
        "--m1-replication-dir",
        type=Path,
        default=None,
        help="Directory containing addendum3/M1_replication run (defaults to <results-root>/addendum3/M1_replication).",
    )
    parser.add_argument(
        "--out-dir",
        "--out",
        dest="out_dir",
        type=Path,
        default=None,
        help="Output directory for CSV tables (defaults to <results-root>/addendum3/out).",
    )
    parser.add_argument(
        "--configs-dir",
        type=Path,
        default=REPO_ROOT / "configs",
        help="Directory containing YAML configs (defaults to <repo_root>/configs).",
    )
    args = parser.parse_args()

    charged_dir = args.charged_dir or (args.results_root / "addendum3" / "M3_dc_charged")
    m3_dir = args.m3_dir or (args.results_root / "main" / "M3")
    replication_dir = args.replication_dir or (args.results_root / "addendum3" / "replication")
    m1_charged_dir = args.m1_charged_dir or (args.results_root / "addendum3" / "M1_dc_charged")
    m1_dir = args.m1_dir or _resolve_default_m1_dir(m3_dir)
    m1_online_dir = args.m1_online_dir or (args.results_root / "main" / "M1_online")
    m1_replication_dir = args.m1_replication_dir or (args.results_root / "addendum3" / "M1_replication")
    out_dir = args.out_dir or (args.results_root / "addendum3" / "out")

    analyze_add3(
        charged_dir=charged_dir,
        m3_dir=m3_dir,
        out_dir=out_dir,
        replication_dir=replication_dir,
        configs_dir=args.configs_dir,
        m1_charged_dir=m1_charged_dir,
        m1_dir=m1_dir,
        m1_online_dir=m1_online_dir,
        m1_replication_dir=m1_replication_dir,
    )


if __name__ == "__main__":
    main()
