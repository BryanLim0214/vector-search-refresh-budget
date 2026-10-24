"""Analysis script for main experiments M1, M2, and sensitivity (Step 4).

Reads all run outputs under:
- `<results_root>/main/M1` (or `<results_root>/M1`)
- `<results_root>/main/M2` (or `<results_root>/M2`)
- `<results_root>/main/sens` (or `<results_root>/sens`)

Validates found runs against the pre-registered grid in:
- `repo/configs/main_M1.yaml`
- `repo/configs/main_M2.yaml`
- `repo/configs/main_sens.yaml`

Produces in `<out_dir>` (defaults to `<results_root>/main`):
- `SUMMARY.md`
- `missing_runs.csv`
- `m1_conditions.csv`
- `m2_conditions.csv`
- `sens_conditions.csv`
- `hypothesis_tests.csv`
- `paired_comparisons.csv`
- `breakeven_q.csv`
- `spearman_comparison.csv`
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import yaml

REFRESH_RULES = ["round_robin", "data_change", "recovered_neighbor", "ql", "ss"]
ALTERNATIVES = ["extra_search", "push_vectors"]
REFERENCES = ["never_refresh", "always_fresh", "oracle_routing", "all_shards"]
POLICY_ORDER = REFERENCES + REFRESH_RULES + ALTERNATIVES

REQUIRED_RUN_FILES = ("summary.json", "provenance.json", "queries.npz", "epochs.parquet")
REQUIRED_SUMMARY_KEYS = (
    "run_id",
    "policy",
    "seed",
    "stream_mode",
    "placement_mode",
    "bursts",
    "sharding_mode",
    "aging_mode",
    "starvation_cap_mode",
    "starvation_cap_epochs",
    "num_shards",
    "c",
    "m",
    "budget_summaries",
    "num_epochs",
    "queries_per_epoch",
    "recent_window_w",
    "charge_setup_to_budget",
    "total_queries_without_recent_neighbors",
    "missed_neighbors_query_mean",
    "spearman_rho_mean",
    "spearman_n_defined",
    "spearman_new_vecs_rho_mean",
    "spearman_new_vecs_n_defined",
    "total_budget_bytes_spent",
    "total_budget_bytes_with_amortized_setup",
    "total_workload_bytes",
    "total_summaries_refreshed",
    "total_forced_by_cap",
    "total_extra_searches",
    "total_pushed_vectors",
    "cpu_total_s",
    "cpu_ground_truth_s_total",
)


def _as_list(val: Any) -> List[Any]:
    if isinstance(val, list):
        return val
    return [val]


def make_canonical_run_id(
    policy: str,
    num_shards: int,
    m: int,
    budget_summaries: int,
    queries_per_epoch: int,
    stream_mode: str,
    placement_mode: str,
    recent_window_w: int,
    sharding_mode: str,
    bursts: bool,
    charge_setup_to_budget: bool,
    aging_mode: str,
    starvation_cap_mode: str,
    fixed_cap_epochs: Optional[int],
    seed: int,
) -> str:
    cap_tag = "arith" if starvation_cap_mode == "arithmetic" else f"A{fixed_cap_epochs}"
    return (
        f"{policy}_S{num_shards}_m{m}_B{budget_summaries}_Q{queries_per_epoch}"
        f"_{stream_mode}_{placement_mode}_W{recent_window_w}_{sharding_mode}"
        f"_bursts{int(bursts)}_setup{int(charge_setup_to_budget)}"
        f"_{aging_mode}_{cap_tag}_seed{seed}"
    )


def load_expected_grid(config_path: Path, exp_label: str) -> Dict[str, Any]:
    """Load the expected run grid specification from a YAML config file."""
    raw_bytes = config_path.read_bytes()
    cfg = yaml.safe_load(raw_bytes.decode("utf-8"))

    policies = [str(p) for p in _as_list(cfg["policies"])]
    stream_modes = [str(s) for s in _as_list(cfg["stream_modes"])]
    budget_summaries = [int(b) for b in _as_list(cfg["budget_summaries"])]
    queries_per_epoch = [int(q) for q in _as_list(cfg["queries_per_epoch"])]
    seeds = [int(s) for s in _as_list(cfg["seeds"])]

    num_shards = int(_as_list(cfg["num_shards"])[0])
    c = int(cfg["c"])
    m = int(_as_list(cfg["m"])[0])
    num_epochs = int(cfg["num_epochs"])
    inserts_per_epoch = int(cfg["inserts_per_epoch"])
    recent_window_w = int(cfg["recent_window_w"])
    placement_mode = str(cfg["placement_mode"])
    bursts = bool(cfg["bursts"])
    sharding_mode = str(cfg["sharding_mode"])
    charge_setup_to_budget = bool(cfg["charge_setup_to_budget"])
    aging_mode = str(cfg["aging_mode"])

    raw_cap = cfg["starvation_cap_epochs"]
    if raw_cap is None or str(raw_cap).lower() in ("arithmetic", "auto", "none", "null"):
        starvation_cap_mode = "arithmetic"
        fixed_cap_epochs: Optional[int] = None
    else:
        starvation_cap_mode = "fixed"
        fixed_cap_epochs = int(raw_cap)

    # Cell key: (stream_mode, placement_mode, budget_summaries, queries_per_epoch, starvation_cap_mode, policy, seed)
    expected_cells: Dict[Tuple[str, str, int, int, str, str, int], str] = {}
    run_id_to_cell: Dict[str, Tuple[str, str, int, int, str, str, int]] = {}
    for stream in stream_modes:
        for b_sum in budget_summaries:
            for q_val in queries_per_epoch:
                for pol in policies:
                    for seed in seeds:
                        cell_key = (
                            stream,
                            placement_mode,
                            b_sum,
                            q_val,
                            starvation_cap_mode,
                            pol,
                            seed,
                        )
                        rid = make_canonical_run_id(
                            policy=pol,
                            num_shards=num_shards,
                            m=m,
                            budget_summaries=b_sum,
                            queries_per_epoch=q_val,
                            stream_mode=stream,
                            placement_mode=placement_mode,
                            recent_window_w=recent_window_w,
                            sharding_mode=sharding_mode,
                            bursts=bursts,
                            charge_setup_to_budget=charge_setup_to_budget,
                            aging_mode=aging_mode,
                            starvation_cap_mode=starvation_cap_mode,
                            fixed_cap_epochs=fixed_cap_epochs,
                            seed=seed,
                        )
                        expected_cells[cell_key] = rid
                        run_id_to_cell[rid] = cell_key

    return {
        "experiment": exp_label,
        "config_path": config_path,
        "policies": policies,
        "stream_modes": stream_modes,
        "budget_summaries": budget_summaries,
        "queries_per_epoch": queries_per_epoch,
        "seeds": seeds,
        "num_shards": num_shards,
        "c": c,
        "m": m,
        "num_epochs": num_epochs,
        "inserts_per_epoch": inserts_per_epoch,
        "recent_window_w": recent_window_w,
        "placement_mode": placement_mode,
        "bursts": bursts,
        "sharding_mode": sharding_mode,
        "charge_setup_to_budget": charge_setup_to_budget,
        "aging_mode": aging_mode,
        "starvation_cap_mode": starvation_cap_mode,
        "fixed_cap_epochs": fixed_cap_epochs,
        "expected_cells": expected_cells,
        "run_id_to_cell": run_id_to_cell,
    }


def hierarchical_bootstrap_ci(
    per_seed_arrays: Sequence[np.ndarray],
    n_boot: int = 2000,
    ci: float = 0.95,
    seed: int = 42,
) -> Dict[str, float]:
    """Bootstrap over queries within each seed, then across seeds.

    For each seed s, filters `per_seed_arrays[s]` to finite values.
    In each of `n_boot` replicates:
      1. Resamples K seeds with replacement from the K available seeds.
      2. Within each resampled (replicate, slot), draws an independent within-seed
         query resample of size N_s with replacement (fixing L-1).
      3. Computes the query-weighted mean across the resampled seeds.
    Also computes the fixed-seed (within-seed query resampling only) CI for reference.
    """
    cleaned: List[np.ndarray] = []
    for arr in per_seed_arrays:
        a = np.asarray(arr, dtype=np.float64)
        a = a[np.isfinite(a)]
        if len(a) > 0:
            cleaned.append(a)

    if not cleaned:
        return {
            "mean": float("nan"),
            "seed_mean": float("nan"),
            "ci_low": float("nan"),
            "ci_high": float("nan"),
            "within_seed_ci_low": float("nan"),
            "within_seed_ci_high": float("nan"),
            "n_queries": 0,
            "n_seeds": 0,
        }

    k_seeds = len(cleaned)
    all_concat = np.concatenate(cleaned)
    pooled_mean = float(np.mean(all_concat))
    seed_means = np.array([float(np.mean(a)) for a in cleaned], dtype=np.float64)
    seed_mean = float(np.mean(seed_means))
    counts = np.array([len(a) for a in cleaned], dtype=np.float64)

    rng = np.random.default_rng(int(seed))
    # Draw independent within-seed bootstrap sums for each (seed, replicate, slot)
    # so a seed drawn multiple times in the same replicate gets independent query resamples.
    within_slot_sums = np.empty((k_seeds, n_boot, k_seeds), dtype=np.float64)
    for s_idx, a in enumerate(cleaned):
        n_s = len(a)
        for slot in range(k_seeds):
            q_idx = rng.integers(0, n_s, size=(n_boot, n_s))
            within_slot_sums[s_idx, :, slot] = np.sum(a[q_idx], axis=1)

    # Fixed-seed (within-seed only) bootstrap means use slot 0
    within_only_means = np.sum(within_slot_sums[:, :, 0], axis=0) / float(np.sum(counts))

    alpha = (1.0 - ci) / 2.0
    w_low, w_high = np.quantile(within_only_means, [alpha, 1.0 - alpha])

    # Two-stage (across seeds + within seed) bootstrap
    if k_seeds > 1:
        seed_choices = rng.integers(0, k_seeds, size=(n_boot, k_seeds))
        rep_indices = np.arange(n_boot)[:, None]
        slot_indices = np.arange(k_seeds)[None, :]
        chosen_sums = within_slot_sums[seed_choices, rep_indices, slot_indices]  # (n_boot, k_seeds)
        chosen_counts = counts[seed_choices]  # (n_boot, k_seeds)
        hier_means = np.sum(chosen_sums, axis=1) / np.sum(chosen_counts, axis=1)
        ci_low, ci_high = np.quantile(hier_means, [alpha, 1.0 - alpha])
    else:
        # 1 seed: cannot estimate across-seed variation; do not silently substitute within-seed CI
        ci_low, ci_high = float("nan"), float("nan")

    return {
        "mean": pooled_mean,
        "seed_mean": seed_mean,
        "ci_low": float(ci_low),
        "ci_high": float(ci_high),
        "within_seed_ci_low": float(w_low),
        "within_seed_ci_high": float(w_high),
        "n_queries": int(len(all_concat)),
        "n_seeds": int(k_seeds),
    }


def paired_hierarchical_bootstrap_ci(
    arrays_a_by_seed: Dict[int, np.ndarray],
    arrays_b_by_seed: Dict[int, np.ndarray],
    n_boot: int = 2000,
    ci: float = 0.95,
    seed: int = 42,
    check_nan_mask: bool = True,
) -> Dict[str, float]:
    """Paired bootstrap over queries within each seed, then across seeds, for (A - B)."""
    common_seeds = sorted(set(arrays_a_by_seed.keys()) & set(arrays_b_by_seed.keys()))
    diff_arrays: List[np.ndarray] = []
    for s in common_seeds:
        a = np.asarray(arrays_a_by_seed[s], dtype=np.float64)
        b = np.asarray(arrays_b_by_seed[s], dtype=np.float64)
        if len(a) != len(b):
            raise AssertionError(
                f"Paired query array length mismatch on seed {s}: len(a)={len(a)} vs len(b)={len(b)}"
            )
        if check_nan_mask and not np.array_equal(np.isnan(a), np.isnan(b)):
            raise AssertionError(
                f"Paired query array NaN mask mismatch on seed {s}"
            )
        valid = np.isfinite(a) & np.isfinite(b)
        if np.any(valid):
            diff_arrays.append(a[valid] - b[valid])
    res = hierarchical_bootstrap_ci(diff_arrays, n_boot=n_boot, ci=ci, seed=seed)
    return {
        "mean_diff": res["mean"],
        "seed_mean_diff": res["seed_mean"],
        "ci_low": res["ci_low"],
        "ci_high": res["ci_high"],
        "within_seed_ci_low": res["within_seed_ci_low"],
        "within_seed_ci_high": res["within_seed_ci_high"],
        "n_paired_queries": res["n_queries"],
        "n_seeds": res["n_seeds"],
    }


def _validate_run_against_grid(
    summary: Dict[str, Any],
    prov: Dict[str, Any],
    grid_spec: Dict[str, Any],
) -> List[str]:
    """Check a run's summary and provenance config against the YAML grid specification."""
    mismatches: List[str] = []
    for req_key in REQUIRED_SUMMARY_KEYS:
        if req_key not in summary:
            mismatches.append(f"summary missing key '{req_key}'")
    if mismatches:
        return mismatches

    # Fixed parameters check
    fixed_checks = [
        ("num_shards", int(summary["num_shards"]), int(grid_spec["num_shards"])),
        ("c", int(summary["c"]), int(grid_spec["c"])),
        ("m", int(summary["m"]), int(grid_spec["m"])),
        ("num_epochs", int(summary["num_epochs"]), int(grid_spec["num_epochs"])),
        ("recent_window_w", int(summary["recent_window_w"]), int(grid_spec["recent_window_w"])),
        ("placement_mode", str(summary["placement_mode"]), str(grid_spec["placement_mode"])),
        ("bursts", bool(summary["bursts"]), bool(grid_spec["bursts"])),
        ("sharding_mode", str(summary["sharding_mode"]), str(grid_spec["sharding_mode"])),
        ("charge_setup_to_budget", bool(summary["charge_setup_to_budget"]), bool(grid_spec["charge_setup_to_budget"])),
        ("aging_mode", str(summary["aging_mode"]), str(grid_spec["aging_mode"])),
        ("starvation_cap_mode", str(summary["starvation_cap_mode"]), str(grid_spec["starvation_cap_mode"])),
    ]
    if "inserts_per_epoch" in summary:
        fixed_checks.append(
            ("inserts_per_epoch", int(summary["inserts_per_epoch"]), int(grid_spec["inserts_per_epoch"]))
        )
    if grid_spec["fixed_cap_epochs"] is not None:
        fixed_checks.append(
            ("starvation_cap_epochs", int(summary["starvation_cap_epochs"]), int(grid_spec["fixed_cap_epochs"]))
        )

    for field_name, actual_val, expected_val in fixed_checks:
        if actual_val != expected_val:
            mismatches.append(f"{field_name}={actual_val!r} != expected {expected_val!r}")

    # Domain checks for swept grid parameters
    if str(summary["policy"]) not in grid_spec["policies"]:
        mismatches.append(f"policy={summary['policy']!r} not in expected {grid_spec['policies']}")
    if str(summary["stream_mode"]) not in grid_spec["stream_modes"]:
        mismatches.append(f"stream_mode={summary['stream_mode']!r} not in expected {grid_spec['stream_modes']}")
    if int(summary["budget_summaries"]) not in grid_spec["budget_summaries"]:
        mismatches.append(
            f"budget_summaries={summary['budget_summaries']!r} not in expected {grid_spec['budget_summaries']}"
        )
    if int(summary["queries_per_epoch"]) not in grid_spec["queries_per_epoch"]:
        mismatches.append(
            f"queries_per_epoch={summary['queries_per_epoch']!r} not in expected {grid_spec['queries_per_epoch']}"
        )
    if int(summary["seed"]) not in grid_spec["seeds"]:
        mismatches.append(f"seed={summary['seed']!r} not in expected {grid_spec['seeds']}")

    # Check saved config inside provenance.json against summary and grid
    prov_cfg = prov.get("config")
    if not isinstance(prov_cfg, dict) or not prov_cfg:
        mismatches.append("provenance.json missing non-empty 'config' dict")
    else:
        for k in (
            "policy",
            "num_shards",
            "c",
            "m",
            "budget_summaries",
            "num_epochs",
            "queries_per_epoch",
            "recent_window_w",
            "stream_mode",
            "placement_mode",
            "bursts",
            "sharding_mode",
            "aging_mode",
            "starvation_cap_mode",
            "starvation_cap_epochs",
            "seed",
            "charge_setup_to_budget",
        ):
            if k in prov_cfg and k in summary and prov_cfg[k] != summary[k]:
                mismatches.append(f"provenance.config[{k!r}]={prov_cfg[k]!r} != summary[{k!r}]={summary[k]!r}")

    return mismatches


def scan_experiment_runs(
    exp_dir: Path,
    grid_spec: Dict[str, Any],
) -> Dict[str, Any]:
    """Scan `exp_dir`, validate all runs against `grid_spec`, and identify missing/incomplete/duplicate/mismatched runs."""
    exp_label = str(grid_spec["experiment"])
    expected_cells: Dict[Tuple[str, str, int, int, str, str, int], str] = grid_spec["expected_cells"]
    run_id_to_cell: Dict[str, Tuple[str, str, int, int, str, str, int]] = grid_spec["run_id_to_cell"]

    valid_by_cell: Dict[Tuple[str, str, int, int, str, str, int], Dict[str, Any]] = {}
    cell_issues: Dict[Tuple[str, str, int, int, str, str, int], List[Dict[str, Any]]] = {}
    all_issues: List[Dict[str, Any]] = []
    git_commits: set[str] = set()
    config_shas: set[str] = set()

    if exp_dir.exists():
        for run_dir in sorted(exp_dir.iterdir()):
            if not run_dir.is_dir():
                continue

            missing_files = [
                fname for fname in REQUIRED_RUN_FILES if not (run_dir / fname).exists()
            ]

            # Try to peek at summary.json or provenance.json to identify run_id and cell_key if possible
            peek_summary: Optional[Dict[str, Any]] = None
            peek_prov: Optional[Dict[str, Any]] = None
            summary_path = run_dir / "summary.json"
            prov_path = run_dir / "provenance.json"
            queries_path = run_dir / "queries.npz"
            epochs_path = run_dir / "epochs.parquet"

            if summary_path.exists():
                try:
                    with summary_path.open("r", encoding="utf-8") as f:
                        loaded_s = json.load(f)
                    if isinstance(loaded_s, dict) and loaded_s:
                        peek_summary = loaded_s
                except Exception:
                    peek_summary = None

            if prov_path.exists():
                try:
                    with prov_path.open("r", encoding="utf-8") as f:
                        loaded_p = json.load(f)
                    if isinstance(loaded_p, dict) and loaded_p:
                        peek_prov = loaded_p
                except Exception:
                    peek_prov = None

            detected_run_id = run_dir.name
            if peek_summary is not None and "run_id" in peek_summary:
                detected_run_id = str(peek_summary["run_id"])
            elif peek_prov is not None and "run_id" in peek_prov:
                detected_run_id = str(peek_prov["run_id"])

            inferred_cell: Optional[Tuple[str, str, int, int, str, str, int]] = run_id_to_cell.get(
                detected_run_id, run_id_to_cell.get(run_dir.name)
            )
            if inferred_cell is None:
                src_cfg = peek_summary if peek_summary is not None else (
                    peek_prov.get("config") if isinstance(peek_prov, dict) else None
                )
                if isinstance(src_cfg, dict):
                    try:
                        cand_cell = (
                            str(src_cfg["stream_mode"]),
                            str(src_cfg["placement_mode"]),
                            int(src_cfg["budget_summaries"]),
                            int(src_cfg["queries_per_epoch"]),
                            str(src_cfg["starvation_cap_mode"]),
                            str(src_cfg["policy"]),
                            int(src_cfg["seed"]),
                        )
                        if cand_cell in expected_cells:
                            inferred_cell = cand_cell
                    except Exception:
                        pass

            if missing_files:
                issue = {
                    "experiment": exp_label,
                    "run_id": detected_run_id,
                    "status": "incomplete",
                    "reason": f"missing {', '.join(missing_files)}",
                    "cell_key": inferred_cell,
                }
                all_issues.append(issue)
                if inferred_cell is not None:
                    cell_issues.setdefault(inferred_cell, []).append(issue)
                continue

            if peek_summary is None:
                issue = {
                    "experiment": exp_label,
                    "run_id": detected_run_id,
                    "status": "incomplete",
                    "reason": "invalid or empty summary.json",
                    "cell_key": inferred_cell,
                }
                all_issues.append(issue)
                if inferred_cell is not None:
                    cell_issues.setdefault(inferred_cell, []).append(issue)
                continue

            if peek_prov is None:
                issue = {
                    "experiment": exp_label,
                    "run_id": detected_run_id,
                    "status": "incomplete",
                    "reason": "invalid or empty provenance.json",
                    "cell_key": inferred_cell,
                }
                all_issues.append(issue)
                if inferred_cell is not None:
                    cell_issues.setdefault(inferred_cell, []).append(issue)
                continue

            # Parse queries.npz
            try:
                with np.load(queries_path, allow_pickle=False) as qz:
                    q_recall = qz["recall_at_10"].astype(np.float64)
                    q_recent = qz["recent_recall_at_10"].astype(np.float64)
                    q_missed = qz["missed_neighbors"].astype(np.int64)
            except Exception as exc:
                issue = {
                    "experiment": exp_label,
                    "run_id": detected_run_id,
                    "status": "incomplete",
                    "reason": f"corrupt queries.npz ({exc})",
                    "cell_key": inferred_cell,
                }
                all_issues.append(issue)
                if inferred_cell is not None:
                    cell_issues.setdefault(inferred_cell, []).append(issue)
                continue

            if (
                q_recall.ndim != 1
                or len(q_recall) == 0
                or len(q_recall) != len(q_recent)
                or len(q_recall) != len(q_missed)
            ):
                issue = {
                    "experiment": exp_label,
                    "run_id": detected_run_id,
                    "status": "incomplete",
                    "reason": (
                        f"empty or mismatched arrays in queries.npz "
                        f"(lengths: recall={len(q_recall)}, recent={len(q_recent)}, missed={len(q_missed)})"
                    ),
                    "cell_key": inferred_cell,
                }
                all_issues.append(issue)
                if inferred_cell is not None:
                    cell_issues.setdefault(inferred_cell, []).append(issue)
                continue

            if "total_queries_evaluated" in peek_summary and len(q_recall) != int(
                peek_summary["total_queries_evaluated"]
            ):
                issue = {
                    "experiment": exp_label,
                    "run_id": detected_run_id,
                    "status": "incomplete",
                    "reason": (
                        f"queries.npz length {len(q_recall)} != "
                        f"summary total_queries_evaluated {peek_summary['total_queries_evaluated']}"
                    ),
                    "cell_key": inferred_cell,
                }
                all_issues.append(issue)
                if inferred_cell is not None:
                    cell_issues.setdefault(inferred_cell, []).append(issue)
                continue

            # Parse epochs.parquet
            try:
                epochs_df = pd.read_parquet(epochs_path)
            except Exception as exc:
                issue = {
                    "experiment": exp_label,
                    "run_id": detected_run_id,
                    "status": "incomplete",
                    "reason": f"corrupt epochs.parquet ({exc})",
                    "cell_key": inferred_cell,
                }
                all_issues.append(issue)
                if inferred_cell is not None:
                    cell_issues.setdefault(inferred_cell, []).append(issue)
                continue

            if (
                epochs_df.empty
                or "spearman_rho" not in epochs_df.columns
                or "spearman_rho_new_vecs" not in epochs_df.columns
            ):
                issue = {
                    "experiment": exp_label,
                    "run_id": detected_run_id,
                    "status": "incomplete",
                    "reason": "epochs.parquet empty or missing spearman columns",
                    "cell_key": inferred_cell,
                }
                all_issues.append(issue)
                if inferred_cell is not None:
                    cell_issues.setdefault(inferred_cell, []).append(issue)
                continue

            # Validate against grid config values
            mismatches = _validate_run_against_grid(peek_summary, peek_prov, grid_spec)
            if mismatches:
                issue = {
                    "experiment": exp_label,
                    "run_id": detected_run_id,
                    "status": "config_mismatch",
                    "reason": "; ".join(mismatches),
                    "cell_key": inferred_cell,
                }
                all_issues.append(issue)
                if inferred_cell is not None:
                    cell_issues.setdefault(inferred_cell, []).append(issue)
                continue

            cell_key = (
                str(peek_summary["stream_mode"]),
                str(peek_summary["placement_mode"]),
                int(peek_summary["budget_summaries"]),
                int(peek_summary["queries_per_epoch"]),
                str(peek_summary["starvation_cap_mode"]),
                str(peek_summary["policy"]),
                int(peek_summary["seed"]),
            )
            if cell_key not in expected_cells:
                issue = {
                    "experiment": exp_label,
                    "run_id": detected_run_id,
                    "status": "config_mismatch",
                    "reason": f"run cell {cell_key} not in expected grid",
                    "cell_key": None,
                }
                all_issues.append(issue)
                continue

            # Check for duplicate runs mapping to the same grid cell
            if cell_key in valid_by_cell or any(
                i["status"] == "duplicate" for i in cell_issues.get(cell_key, [])
            ):
                if cell_key in valid_by_cell:
                    prev_run = valid_by_cell.pop(cell_key)
                    prev_issue = {
                        "experiment": exp_label,
                        "run_id": prev_run["run_id"],
                        "status": "duplicate",
                        "reason": (
                            f"duplicate run for cell {cell_key[5]} {cell_key[0]} "
                            f"B={cell_key[2]} Q={cell_key[3]} seed={cell_key[6]}"
                        ),
                        "cell_key": cell_key,
                    }
                    all_issues.append(prev_issue)
                    cell_issues.setdefault(cell_key, []).append(prev_issue)

                dup_issue = {
                    "experiment": exp_label,
                    "run_id": detected_run_id,
                    "status": "duplicate",
                    "reason": (
                        f"duplicate run for cell {cell_key[5]} {cell_key[0]} "
                        f"B={cell_key[2]} Q={cell_key[3]} seed={cell_key[6]}"
                    ),
                    "cell_key": cell_key,
                }
                all_issues.append(dup_issue)
                cell_issues.setdefault(cell_key, []).append(dup_issue)
                continue

            # If another folder for the same cell was earlier flagged as incomplete/mismatch, do not allow silent pass
            if cell_key in cell_issues:
                conflict_issue = {
                    "experiment": exp_label,
                    "run_id": detected_run_id,
                    "status": "duplicate",
                    "reason": f"conflicting folder alongside incomplete/mismatched folder for cell {cell_key}",
                    "cell_key": cell_key,
                }
                all_issues.append(conflict_issue)
                cell_issues[cell_key].append(conflict_issue)
                continue

            if "git_commit" in peek_prov and peek_prov["git_commit"]:
                git_commits.add(str(peek_prov["git_commit"]))
            if "config_sha256" in peek_prov and peek_prov["config_sha256"]:
                config_shas.add(str(peek_prov["config_sha256"]))

            ep_rn = pd.to_numeric(epochs_df["spearman_rho"], errors="coerce").to_numpy(dtype=np.float64)
            ep_nv = pd.to_numeric(epochs_df["spearman_rho_new_vecs"], errors="coerce").to_numpy(dtype=np.float64)

            valid_by_cell[cell_key] = {
                "run_id": detected_run_id,
                "summary": peek_summary,
                "provenance": peek_prov,
                "q_recall": q_recall,
                "q_recent": q_recent,
                "q_missed": q_missed,
                "ep_spearman_rn": ep_rn,
                "ep_spearman_nv": ep_nv,
            }

    # Check every expected cell in the grid; any cell neither in valid_by_cell nor in cell_issues is missing
    for cell_key, expected_rid in expected_cells.items():
        if cell_key not in valid_by_cell and cell_key not in cell_issues:
            miss_issue = {
                "experiment": exp_label,
                "run_id": expected_rid,
                "status": "missing",
                "reason": "missing run folder",
                "cell_key": cell_key,
            }
            all_issues.append(miss_issue)
            cell_issues[cell_key] = [miss_issue]

    # Flag provenance commit/config SHA mismatches within the experiment if multiple values exist
    prov_warnings: List[str] = []
    if len(git_commits) > 1:
        prov_warnings.append(f"multiple git_commit values in {exp_label}: {sorted(git_commits)}")
    if len(config_shas) > 1:
        prov_warnings.append(f"multiple config_sha256 values in {exp_label}: {sorted(config_shas)}")

    return {
        "experiment": exp_label,
        "grid_spec": grid_spec,
        "valid_runs": list(valid_by_cell.values()),
        "valid_by_cell": valid_by_cell,
        "cell_issues": cell_issues,
        "issues": all_issues,
        "git_commits": sorted(git_commits),
        "config_shas": sorted(config_shas),
        "prov_warnings": prov_warnings,
    }


def load_experiment_runs(exp_dir: Path, grid_spec: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Load complete and validated run outputs from `exp_dir`."""
    if grid_spec is not None:
        return scan_experiment_runs(exp_dir, grid_spec)["valid_runs"]
    runs: List[Dict[str, Any]] = []
    if not exp_dir.exists():
        return runs
    for run_dir in sorted(exp_dir.iterdir()):
        if not run_dir.is_dir():
            continue
        if any(not (run_dir / f).exists() for f in REQUIRED_RUN_FILES):
            continue
        with (run_dir / "summary.json").open("r", encoding="utf-8") as f:
            summary = json.load(f)
        with (run_dir / "provenance.json").open("r", encoding="utf-8") as f:
            prov = json.load(f)
        with np.load(run_dir / "queries.npz", allow_pickle=False) as qz:
            q_recall = qz["recall_at_10"].astype(np.float64)
            q_recent = qz["recent_recall_at_10"].astype(np.float64)
            q_missed = qz["missed_neighbors"].astype(np.int64)
        epochs_df = pd.read_parquet(run_dir / "epochs.parquet")
        ep_rn = pd.to_numeric(epochs_df["spearman_rho"], errors="coerce").to_numpy(dtype=np.float64)
        ep_nv = pd.to_numeric(epochs_df["spearman_rho_new_vecs"], errors="coerce").to_numpy(dtype=np.float64)
        runs.append(
            {
                "run_id": summary["run_id"],
                "summary": summary,
                "provenance": prov,
                "q_recall": q_recall,
                "q_recent": q_recent,
                "q_missed": q_missed,
                "ep_spearman_rn": ep_rn,
                "ep_spearman_nv": ep_nv,
            }
        )
    return runs


def check_condition_complete(
    scan_res: Dict[str, Any],
    stream_mode: str,
    placement_mode: str,
    b_sum: int,
    q_val: int,
    cap_mode: str,
    policies: Sequence[str],
) -> Tuple[bool, List[str]]:
    """Verify that every (policy, seed) in `policies x expected_seeds` is complete and valid."""
    expected_seeds: List[int] = scan_res["grid_spec"]["seeds"]
    valid_by_cell = scan_res["valid_by_cell"]
    cell_issues = scan_res["cell_issues"]
    expected_cells = scan_res["grid_spec"]["expected_cells"]

    missing_items: List[str] = []
    for pol in policies:
        for seed in expected_seeds:
            ck = (stream_mode, placement_mode, int(b_sum), int(q_val), cap_mode, pol, int(seed))
            if ck not in valid_by_cell:
                issues_for_ck = cell_issues.get(ck, [])
                if issues_for_ck:
                    for iss in issues_for_ck:
                        missing_items.append(f"{iss['run_id']} ({iss['status']}: {iss['reason']})")
                else:
                    rid = expected_cells.get(ck, f"{pol}_{stream_mode}_B{b_sum}_Q{q_val}_seed{seed}")
                    missing_items.append(f"{rid} (missing)")
    return (len(missing_items) == 0), missing_items


def format_incomplete_status(missing_items: Sequence[str], max_show: int = 4) -> str:
    """Format an INCOMPLETE status string naming the missing/incomplete runs."""
    if not missing_items:
        return "INCOMPLETE"
    shown = list(missing_items[:max_show])
    rem = len(missing_items) - len(shown)
    suffix = f", +{rem} more" if rem > 0 else ""
    return f"INCOMPLETE (missing: {'; '.join(shown)}{suffix})"


def _compute_paired_spearman_stats(r_sorted: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute Spearman rank correlations on the SAME refresh decisions (epochs where both are defined),
    as well as unrestricted statistics for each estimator."""
    rn_seed_means_both: List[float] = []
    nv_seed_means_both: List[float] = []
    rn_both_chunks: List[np.ndarray] = []
    nv_both_chunks: List[np.ndarray] = []

    rn_seed_means_all: List[float] = []
    nv_seed_means_all: List[float] = []
    rn_all_chunks: List[np.ndarray] = []
    nv_all_chunks: List[np.ndarray] = []

    for r in r_sorted:
        ep_rn = np.asarray(r["ep_spearman_rn"], dtype=np.float64)
        ep_nv = np.asarray(r["ep_spearman_nv"], dtype=np.float64)
        if len(ep_rn) != len(ep_nv):
            raise AssertionError(
                f"Epoch Spearman array length mismatch in {r['run_id']}: {len(ep_rn)} vs {len(ep_nv)}"
            )
        both_mask = np.isfinite(ep_rn) & np.isfinite(ep_nv)
        if np.any(both_mask):
            rn_b = ep_rn[both_mask]
            nv_b = ep_nv[both_mask]
            rn_seed_means_both.append(float(np.mean(rn_b)))
            nv_seed_means_both.append(float(np.mean(nv_b)))
            rn_both_chunks.append(rn_b)
            nv_both_chunks.append(nv_b)

        rn_mask = np.isfinite(ep_rn)
        if np.any(rn_mask):
            rn_a = ep_rn[rn_mask]
            rn_seed_means_all.append(float(np.mean(rn_a)))
            rn_all_chunks.append(rn_a)

        nv_mask = np.isfinite(ep_nv)
        if np.any(nv_mask):
            nv_a = ep_nv[nv_mask]
            nv_seed_means_all.append(float(np.mean(nv_a)))
            nv_all_chunks.append(nv_a)

    rn_both_concat = np.concatenate(rn_both_chunks) if rn_both_chunks else np.empty(0, dtype=np.float64)
    nv_both_concat = np.concatenate(nv_both_chunks) if nv_both_chunks else np.empty(0, dtype=np.float64)
    rn_all_concat = np.concatenate(rn_all_chunks) if rn_all_chunks else np.empty(0, dtype=np.float64)
    nv_all_concat = np.concatenate(nv_all_chunks) if nv_all_chunks else np.empty(0, dtype=np.float64)

    n_both = int(len(rn_both_concat))
    return {
        "spearman_paired_n": n_both,
        "spearman_rn_mean": float(np.mean(rn_seed_means_both)) if rn_seed_means_both else float("nan"),
        "spearman_rn_pooled": float(np.mean(rn_both_concat)) if n_both > 0 else float("nan"),
        "spearman_new_vecs_mean": float(np.mean(nv_seed_means_both)) if nv_seed_means_both else float("nan"),
        "spearman_new_vecs_pooled": float(np.mean(nv_both_concat)) if n_both > 0 else float("nan"),
        "spearman_rn_all_mean": float(np.mean(rn_seed_means_all)) if rn_seed_means_all else float("nan"),
        "spearman_rn_all_pooled": float(np.mean(rn_all_concat)) if len(rn_all_concat) > 0 else float("nan"),
        "spearman_rn_n_defined_total": int(len(rn_all_concat)),
        "spearman_new_vecs_all_mean": float(np.mean(nv_seed_means_all)) if nv_seed_means_all else float("nan"),
        "spearman_new_vecs_all_pooled": float(np.mean(nv_all_concat)) if len(nv_all_concat) > 0 else float("nan"),
        "spearman_new_vecs_n_defined_total": int(len(nv_all_concat)),
    }


def build_condition_table(
    runs: Sequence[Dict[str, Any]],
    exp_label: str,
    expected_seeds: Optional[Sequence[int]] = None,
) -> pd.DataFrame:
    """Aggregate runs by (stream_mode, placement_mode, budget_summaries, queries_per_epoch, starvation_cap_mode, policy)."""
    grouped: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = {}
    for r in runs:
        s = r["summary"]
        key = (
            str(s["stream_mode"]),
            str(s["placement_mode"]),
            int(s["budget_summaries"]),
            int(s["queries_per_epoch"]),
            str(s["starvation_cap_mode"]),
            int(s["starvation_cap_epochs"]),
            str(s["policy"]),
        )
        grouped.setdefault(key, []).append(r)

    rows: List[Dict[str, Any]] = []
    for key, r_list in grouped.items():
        stream_mode, placement_mode, b_sum, q_val, cap_mode, cap_epochs, policy = key
        r_sorted = sorted(r_list, key=lambda x: int(x["summary"]["seed"]))
        found_seeds = [int(x["summary"]["seed"]) for x in r_sorted]
        if len(found_seeds) != len(set(found_seeds)):
            raise ValueError(f"Duplicate seed in condition {key}: {found_seeds}")

        if expected_seeds is not None:
            exp_set = set(int(s) for s in expected_seeds)
            missing_seeds = sorted(exp_set - set(found_seeds))
            cond_status = "COMPLETE" if not missing_seeds else f"INCOMPLETE (missing seeds: {missing_seeds})"
        else:
            cond_status = "COMPLETE"

        recall_arrays = [x["q_recall"] for x in r_sorted]
        recent_arrays = [x["q_recent"] for x in r_sorted]

        ci_rec = hierarchical_bootstrap_ci(recent_arrays, seed=42)
        ci_ov = hierarchical_bootstrap_ci(recall_arrays, seed=42)

        summaries = [x["summary"] for x in r_sorted]
        sp_stats = _compute_paired_spearman_stats(r_sorted)
        n_without_recent = int(sum(int(s["total_queries_without_recent_neighbors"]) for s in summaries))

        rows.append(
            {
                "experiment": exp_label,
                "stream_mode": stream_mode,
                "placement_mode": placement_mode,
                "budget_summaries": b_sum,
                "queries_per_epoch": q_val,
                "starvation_cap_mode": cap_mode,
                "starvation_cap_epochs": cap_epochs,
                "policy": policy,
                "n_seeds": len(r_sorted),
                "status": cond_status,
                "recall_at_10_mean": ci_ov["mean"],
                "recall_at_10_ci95_low": ci_ov["ci_low"],
                "recall_at_10_ci95_high": ci_ov["ci_high"],
                "recent_recall_at_10_mean": ci_rec["mean"],
                "recent_recall_at_10_ci95_low": ci_rec["ci_low"],
                "recent_recall_at_10_ci95_high": ci_rec["ci_high"],
                "recent_recall_at_10_within_ci95_low": ci_rec["within_seed_ci_low"],
                "recent_recall_at_10_within_ci95_high": ci_rec["within_seed_ci_high"],
                "n_queries_with_recent": ci_rec["n_queries"],
                "n_queries_without_recent": n_without_recent,
                "missed_neighbors_mean": float(np.mean([s["missed_neighbors_query_mean"] for s in summaries])),
                "spearman_paired_n": sp_stats["spearman_paired_n"],
                "spearman_rn_mean": sp_stats["spearman_rn_mean"],
                "spearman_rn_pooled": sp_stats["spearman_rn_pooled"],
                "spearman_rn_all_mean": sp_stats["spearman_rn_all_mean"],
                "spearman_rn_all_pooled": sp_stats["spearman_rn_all_pooled"],
                "spearman_rn_n_defined_total": sp_stats["spearman_rn_n_defined_total"],
                "spearman_new_vecs_mean": sp_stats["spearman_new_vecs_mean"],
                "spearman_new_vecs_pooled": sp_stats["spearman_new_vecs_pooled"],
                "spearman_new_vecs_all_mean": sp_stats["spearman_new_vecs_all_mean"],
                "spearman_new_vecs_all_pooled": sp_stats["spearman_new_vecs_all_pooled"],
                "spearman_new_vecs_n_defined_total": sp_stats["spearman_new_vecs_n_defined_total"],
                "budget_bytes_spent_mean": float(np.mean([s["total_budget_bytes_spent"] for s in summaries])),
                "budget_bytes_with_amortized_setup_mean": float(
                    np.mean([s["total_budget_bytes_with_amortized_setup"] for s in summaries])
                ),
                "workload_bytes_mean": float(np.mean([s["total_workload_bytes"] for s in summaries])),
                "summaries_refreshed_mean": float(np.mean([s["total_summaries_refreshed"] for s in summaries])),
                "forced_by_cap_mean": float(np.mean([s["total_forced_by_cap"] for s in summaries])),
                "extra_searches_mean": float(np.mean([s["total_extra_searches"] for s in summaries])),
                "pushed_vectors_mean": float(np.mean([s["total_pushed_vectors"] for s in summaries])),
                "cpu_method_s_mean": float(np.mean([s["cpu_total_s"] for s in summaries])),
                "cpu_gt_s_mean": float(np.mean([s["cpu_ground_truth_s_total"] for s in summaries])),
            }
        )

    df = pd.DataFrame(rows)
    if not df.empty:
        pol_rank = {p: i for i, p in enumerate(POLICY_ORDER)}
        df["_pol_order"] = df["policy"].map(lambda x: pol_rank.get(x, 99))
        df = df.sort_values(
            ["stream_mode", "placement_mode", "budget_summaries", "queries_per_epoch", "starvation_cap_mode", "_pol_order"]
        ).drop(columns=["_pol_order"]).reset_index(drop=True)
    return df


def index_runs_by_condition(
    runs: Sequence[Dict[str, Any]],
) -> Dict[Tuple[str, str, int, int, str, str], Dict[int, Dict[str, Any]]]:
    """Map (stream_mode, placement_mode, B, Q, cap_mode, policy) -> {seed: run_dict}.
    Raises ValueError on any duplicate (key, seed)."""
    idx: Dict[Tuple[str, str, int, int, str, str], Dict[int, Dict[str, Any]]] = {}
    for r in runs:
        s = r["summary"]
        key = (
            str(s["stream_mode"]),
            str(s["placement_mode"]),
            int(s["budget_summaries"]),
            int(s["queries_per_epoch"]),
            str(s["starvation_cap_mode"]),
            str(s["policy"]),
        )
        seed_int = int(s["seed"])
        seed_map = idx.setdefault(key, {})
        if seed_int in seed_map:
            raise ValueError(f"Duplicate run for condition {key} seed {seed_int}: {r['run_id']}")
        seed_map[seed_int] = r
    return idx


def compare_pair(
    idx: Dict[Tuple[str, str, int, int, str, str], Dict[int, Dict[str, Any]]],
    stream_mode: str,
    placement_mode: str,
    b_sum: int,
    q_val: int,
    cap_mode: str,
    policy_a: str,
    policy_b: str,
    expected_seeds: Sequence[int] = (42, 43, 44, 45, 46),
    scan_res: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Compute paired bootstrap comparison (policy_a - policy_b) for recent and overall recall@10.
    Requires `set(runs_a) == set(runs_b) == set(expected_seeds)`; otherwise returns status INCOMPLETE."""
    runs_a = idx.get((stream_mode, placement_mode, b_sum, q_val, cap_mode, policy_a), {})
    runs_b = idx.get((stream_mode, placement_mode, b_sum, q_val, cap_mode, policy_b), {})
    exp_seed_set = set(int(s) for s in expected_seeds)
    missing_a = sorted(exp_seed_set - set(runs_a.keys()))
    missing_b = sorted(exp_seed_set - set(runs_b.keys()))

    if missing_a or missing_b or set(runs_a.keys()) != exp_seed_set or set(runs_b.keys()) != exp_seed_set:
        if scan_res is not None:
            _, missing_items = check_condition_complete(
                scan_res, stream_mode, placement_mode, b_sum, q_val, cap_mode, [policy_a, policy_b]
            )
            status_str = format_incomplete_status(missing_items)
        else:
            parts: List[str] = []
            if missing_a:
                parts.append(f"{policy_a} seeds {missing_a}")
            if missing_b:
                parts.append(f"{policy_b} seeds {missing_b}")
            status_str = f"INCOMPLETE (missing: {', '.join(parts)})"
        return {
            "stream_mode": stream_mode,
            "placement_mode": placement_mode,
            "budget_summaries": b_sum,
            "queries_per_epoch": q_val,
            "starvation_cap_mode": cap_mode,
            "policy_a": policy_a,
            "policy_b": policy_b,
            "status": status_str,
            "complete": False,
            "recent_diff_mean": float("nan"),
            "recent_diff_ci95_low": float("nan"),
            "recent_diff_ci95_high": float("nan"),
            "recent_diff_within_ci95_low": float("nan"),
            "recent_diff_within_ci95_high": float("nan"),
            "overall_diff_mean": float("nan"),
            "overall_diff_ci95_low": float("nan"),
            "overall_diff_ci95_high": float("nan"),
            "budget_bytes_a_mean": float("nan"),
            "budget_bytes_b_mean": float("nan"),
            "n_paired_queries": 0,
            "n_seeds": 0,
        }

    rec_a = {seed: r["q_recent"] for seed, r in runs_a.items()}
    rec_b = {seed: r["q_recent"] for seed, r in runs_b.items()}
    ov_a = {seed: r["q_recall"] for seed, r in runs_a.items()}
    ov_b = {seed: r["q_recall"] for seed, r in runs_b.items()}

    ci_rec = paired_hierarchical_bootstrap_ci(rec_a, rec_b, seed=42, check_nan_mask=True)
    ci_ov = paired_hierarchical_bootstrap_ci(ov_a, ov_b, seed=42, check_nan_mask=False)
    bytes_a = float(np.mean([r["summary"]["total_budget_bytes_spent"] for r in runs_a.values()]))
    bytes_b = float(np.mean([r["summary"]["total_budget_bytes_spent"] for r in runs_b.values()]))

    return {
        "stream_mode": stream_mode,
        "placement_mode": placement_mode,
        "budget_summaries": b_sum,
        "queries_per_epoch": q_val,
        "starvation_cap_mode": cap_mode,
        "policy_a": policy_a,
        "policy_b": policy_b,
        "status": "COMPLETE",
        "complete": True,
        "recent_diff_mean": ci_rec["mean_diff"],
        "recent_diff_ci95_low": ci_rec["ci_low"],
        "recent_diff_ci95_high": ci_rec["ci_high"],
        "recent_diff_within_ci95_low": ci_rec["within_seed_ci_low"],
        "recent_diff_within_ci95_high": ci_rec["within_seed_ci_high"],
        "overall_diff_mean": ci_ov["mean_diff"],
        "overall_diff_ci95_low": ci_ov["ci_low"],
        "overall_diff_ci95_high": ci_ov["ci_high"],
        "budget_bytes_a_mean": bytes_a,
        "budget_bytes_b_mean": bytes_b,
        "n_paired_queries": ci_rec["n_paired_queries"],
        "n_seeds": ci_rec["n_seeds"],
    }


def select_best_refresh_rule(
    df_cond: pd.DataFrame,
    stream_mode: str,
    placement_mode: str,
    b_sum: int,
    q_val: int,
    cap_mode: str,
) -> Dict[str, Any]:
    """Select the refresh rule with highest mean recent_recall_at_10 and compute gap to second-best rule."""
    sub = df_cond[
        (df_cond["stream_mode"] == stream_mode)
        & (df_cond["placement_mode"] == placement_mode)
        & (df_cond["budget_summaries"] == b_sum)
        & (df_cond["queries_per_epoch"] == q_val)
        & (df_cond["starvation_cap_mode"] == cap_mode)
        & (df_cond["policy"].isin(REFRESH_RULES))
    ].sort_values("recent_recall_at_10_mean", ascending=False)

    best_row = sub.iloc[0]
    second_row = sub.iloc[1]
    best_pol = str(best_row["policy"])
    second_pol = str(second_row["policy"])
    best_mean = float(best_row["recent_recall_at_10_mean"])
    second_mean = float(second_row["recent_recall_at_10_mean"])
    gap_pp = (best_mean - second_mean) * 100.0
    note = (
        f"Best refresh rule `{best_pol}` was picked on the same data "
        f"(highest mean recent recall@10 at stream=`{stream_mode}`, B={b_sum}, Q={q_val}); "
        f"gap to second-best (`{second_pol}`): {gap_pp:+.2f} pp ({best_mean:.4f} vs {second_mean:.4f})."
    )
    return {
        "best_policy": best_pol,
        "best_mean": best_mean,
        "second_policy": second_pol,
        "second_mean": second_mean,
        "gap_to_second_pp": gap_pp,
        "selection_note": note,
    }


def fmt_ci(mean: float, low: float, high: float, digits: int = 4) -> str:
    if not np.isfinite(mean):
        return "NaN"
    return f"{mean:.{digits}f} [{low:.{digits}f}, {high:.{digits}f}]"


def fmt_diff_pts(mean: float, low: float, high: float) -> str:
    if not np.isfinite(mean):
        return "NaN"
    return f"{mean * 100.0:+.2f} pp [{low * 100.0:+.2f}, {high * 100.0:+.2f}]"


def _parse_failed_runs_file(
    exp_dir: Path,
    valid_run_ids: set[str],
    missing_run_ids: set[str],
) -> Dict[str, Any]:
    """Read `failed_runs.json` if present; report 'not found' if absent; group unique run_ids."""
    f_path = exp_dir / "failed_runs.json"
    if not f_path.exists():
        return {
            "present": False,
            "status_text": "not found",
            "n_unique_runs": None,
            "n_attempts": None,
            "runs": [],
        }

    with f_path.open("r", encoding="utf-8") as f:
        raw_list = json.load(f)

    by_run_id: Dict[str, List[Dict[str, Any]]] = {}
    for entry in raw_list:
        if isinstance(entry, dict) and "run_id" in entry:
            rid = str(entry["run_id"])
            by_run_id.setdefault(rid, []).append(entry)
        else:
            rid = str(entry)
            by_run_id.setdefault(rid, []).append({"run_id": rid, "attempt": 1})

    run_records: List[Dict[str, Any]] = []
    for rid, attempts in by_run_id.items():
        max_attempt = max(int(a.get("attempt", 1)) for a in attempts)
        if rid in valid_run_ids and max_attempt == 1:
            outcome = "recovered on retry"
        elif rid in valid_run_ids:
            outcome = "completed after retry"
        elif rid in missing_run_ids or max_attempt >= 2:
            outcome = "failed after retry"
        else:
            outcome = "failed"
        run_records.append(
            {
                "run_id": rid,
                "attempts": len(attempts),
                "max_attempt": max_attempt,
                "outcome": outcome,
            }
        )

    return {
        "present": True,
        "status_text": "present",
        "n_unique_runs": len(run_records),
        "n_attempts": len(raw_list),
        "runs": run_records,
    }


def _resolve_experiment_dirs(results_root: Path) -> Tuple[Path, Path, Path, Path]:
    """Resolve `base_dir`, `m1_dir`, `m2_dir`, `sens_dir` from `results_root`."""
    if (results_root / "M1").exists() and not (results_root / "main" / "M1").exists():
        base_dir = results_root
    else:
        base_dir = results_root / "main"
    return base_dir, base_dir / "M1", base_dir / "M2", base_dir / "sens"


def analyze_all(
    results_root: Path,
    out_dir: Optional[Path] = None,
    configs_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    base_dir, m1_dir, m2_dir, sens_dir = _resolve_experiment_dirs(results_root)
    if out_dir is None:
        out_dir = base_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    if configs_dir is None:
        configs_dir = Path(__file__).resolve().parent.parent / "configs"

    grid_m1 = load_expected_grid(configs_dir / "main_M1.yaml", "M1")
    grid_m2 = load_expected_grid(configs_dir / "main_M2.yaml", "M2")
    grid_sens = load_expected_grid(configs_dir / "main_sens.yaml", "sens")

    scan_m1 = scan_experiment_runs(m1_dir, grid_m1)
    scan_m2 = scan_experiment_runs(m2_dir, grid_m2)
    scan_sens = scan_experiment_runs(sens_dir, grid_sens)

    m1_runs = scan_m1["valid_runs"]
    m2_runs = scan_m2["valid_runs"]
    sens_runs = scan_sens["valid_runs"]

    all_issues = scan_m1["issues"] + scan_m2["issues"] + scan_sens["issues"]
    df_missing = pd.DataFrame(
        [
            {
                "experiment": iss["experiment"],
                "run_id": iss["run_id"],
                "status": iss["status"],
                "reason": iss["reason"],
            }
            for iss in all_issues
        ],
        columns=["experiment", "run_id", "status", "reason"],
    )
    df_missing.to_csv(out_dir / "missing_runs.csv", index=False)

    df_m1 = build_condition_table(m1_runs, "M1", expected_seeds=grid_m1["seeds"])
    df_m2 = build_condition_table(m2_runs, "M2", expected_seeds=grid_m2["seeds"])
    df_sens = build_condition_table(sens_runs, "sens", expected_seeds=grid_sens["seeds"])

    df_m1.to_csv(out_dir / "m1_conditions.csv", index=False)
    df_m2.to_csv(out_dir / "m2_conditions.csv", index=False)
    df_sens.to_csv(out_dir / "sens_conditions.csv", index=False)

    idx_m1 = index_runs_by_condition(m1_runs)
    idx_m2 = index_runs_by_condition(m2_runs)
    idx_sens = index_runs_by_condition(sens_runs)

    paired_rows: List[Dict[str, Any]] = []
    hypothesis_rows: List[Dict[str, Any]] = []
    breakeven_rows: List[Dict[str, Any]] = []
    selection_notes: List[str] = []
    failed_hypothesis_items: List[str] = []
    incomplete_hypothesis_items: List[str] = []

    m1_seeds = grid_m1["seeds"]
    m2_seeds = grid_m2["seeds"]

    # --- H1 (M1, nearest_initial, Q=200, B=4): always_fresh - never_refresh < 0.05 on steady and new_topic ---
    h1_stream_outcomes: Dict[str, Dict[str, Any]] = {}
    h1_stream_disp: Dict[str, str] = {}
    for stream in ("steady", "new_topic"):
        comp = compare_pair(
            idx_m1,
            stream,
            "nearest_initial",
            4,
            200,
            "arithmetic",
            "always_fresh",
            "never_refresh",
            expected_seeds=m1_seeds,
            scan_res=scan_m1,
        )
        comp["experiment"] = "M1"
        comp["comparison_type"] = "H1_gate_M1"
        paired_rows.append(comp)
        if not comp["complete"]:
            comp["h1_status"] = comp["status"]
            h1_stream_disp[stream] = comp["status"]
            incomplete_hypothesis_items.append(f"- **H1 ({stream})**: **{comp['status']}**")
        else:
            stream_pass = bool(comp["recent_diff_mean"] < 0.05)
            comp["h1_status"] = "PASS" if stream_pass else "FAIL"
            val_str = (
                f"{fmt_diff_pts(comp['recent_diff_mean'], comp['recent_diff_ci95_low'], comp['recent_diff_ci95_high'])} "
                f"(bytes: AF={comp['budget_bytes_a_mean']:,.0f}, NR={comp['budget_bytes_b_mean']:,.0f}) "
                f"**[{comp['h1_status']}]**"
            )
            h1_stream_disp[stream] = val_str
            if not stream_pass:
                failed_hypothesis_items.append(f"- **H1 ({stream})**: **FAIL** ({val_str})")
        h1_stream_outcomes[stream] = comp

    if any(not c["complete"] for c in h1_stream_outcomes.values()):
        h1_outcome_str = (
            f"steady={h1_stream_outcomes['steady']['h1_status']}, "
            f"new_topic={h1_stream_outcomes['new_topic']['h1_status']}"
        )
    elif all(c["h1_status"] == "PASS" for c in h1_stream_outcomes.values()):
        h1_outcome_str = "PASS (steady=PASS, new_topic=PASS)"
    else:
        h1_outcome_str = (
            f"FAIL (steady={h1_stream_outcomes['steady']['h1_status']}, "
            f"new_topic={h1_stream_outcomes['new_topic']['h1_status']})"
        )

    hypothesis_rows.append(
        {
            "hypothesis": "H1",
            "experiment": "M1 (nearest_initial, Q=200, B=4)",
            "rule": "always_fresh - never_refresh recent recall@10 < 5.0 pp on both steady and new_topic",
            "steady_value": h1_stream_disp["steady"],
            "new_topic_value": h1_stream_disp["new_topic"],
            "outcome": h1_outcome_str,
        }
    )

    # --- H2 (M1, nearest_initial): extra_search and push_vectors beat best refresh rule at Q=200, B=4; break-even Q ---
    h2_checks: Dict[str, Dict[str, Any]] = {}
    h2_stream_disp: Dict[str, str] = {}
    for stream in ("steady", "new_topic"):
        q200_ok, q200_missing = check_condition_complete(
            scan_m1,
            stream,
            "nearest_initial",
            4,
            200,
            "arithmetic",
            REFRESH_RULES + ["extra_search", "push_vectors"],
        )
        if not q200_ok:
            inc_str = format_incomplete_status(q200_missing)
            h2_checks[stream] = {
                "complete": False,
                "status": inc_str,
                "best_refresh": None,
                "selection_note": f"H2 ({stream}, Q=200, B=4): {inc_str}",
                "es_vs_best": None,
                "pv_vs_best": None,
                "pass": False,
                "breakeven_q": inc_str,
            }
            selection_notes.append(f"H2 (`{stream}`, Q=200, B=4): {inc_str}")
            incomplete_hypothesis_items.append(f"- **H2 ({stream})**: **{inc_str}**")
            h2_stream_disp[stream] = inc_str
            breakeven_rows.append(
                {
                    "stream_mode": stream,
                    "queries_per_epoch": 200,
                    "status": inc_str,
                    "best_refresh_policy": "INCOMPLETE",
                    "second_best_refresh_policy": "INCOMPLETE",
                    "gap_to_second_best_pp": float("nan"),
                    "best_refresh_recent_recall": float("nan"),
                    "extra_search_recent_recall": float("nan"),
                    "push_vectors_recent_recall": float("nan"),
                    "refresh_minus_extra_search_mean": float("nan"),
                    "refresh_minus_extra_search_ci95_low": float("nan"),
                    "refresh_minus_extra_search_ci95_high": float("nan"),
                    "refresh_minus_push_vectors_mean": float("nan"),
                    "refresh_minus_push_vectors_ci95_low": float("nan"),
                    "refresh_minus_push_vectors_ci95_high": float("nan"),
                    "breakeven_q": inc_str,
                }
            )
            continue

        sel_q200 = select_best_refresh_rule(df_m1, stream, "nearest_initial", 4, 200, "arithmetic")
        best_refresh_pol = sel_q200["best_policy"]
        selection_notes.append(f"H2 (`{stream}`, Q=200, B=4): {sel_q200['selection_note']}")

        # Also record all 5 refresh rules against extra_search and push_vectors in paired_comparisons.csv (M-4)
        es_vs_best: Optional[Dict[str, Any]] = None
        pv_vs_best: Optional[Dict[str, Any]] = None
        for ref_pol in REFRESH_RULES:
            c_es = compare_pair(
                idx_m1,
                stream,
                "nearest_initial",
                4,
                200,
                "arithmetic",
                "extra_search",
                ref_pol,
                expected_seeds=m1_seeds,
                scan_res=scan_m1,
            )
            c_es["experiment"] = "M1"
            c_es["comparison_type"] = (
                "H2_extra_search_vs_best_refresh_Q200"
                if ref_pol == best_refresh_pol
                else "H2_extra_search_vs_refresh_Q200"
            )
            paired_rows.append(c_es)
            if ref_pol == best_refresh_pol:
                es_vs_best = c_es

            c_pv = compare_pair(
                idx_m1,
                stream,
                "nearest_initial",
                4,
                200,
                "arithmetic",
                "push_vectors",
                ref_pol,
                expected_seeds=m1_seeds,
                scan_res=scan_m1,
            )
            c_pv["experiment"] = "M1"
            c_pv["comparison_type"] = (
                "H2_push_vectors_vs_best_refresh_Q200"
                if ref_pol == best_refresh_pol
                else "H2_push_vectors_vs_refresh_Q200"
            )
            paired_rows.append(c_pv)
            if ref_pol == best_refresh_pol:
                pv_vs_best = c_pv

        assert es_vs_best is not None and pv_vs_best is not None
        stream_pass = bool(
            es_vs_best["recent_diff_mean"] > 0.0
            and es_vs_best["recent_diff_ci95_low"] > 0.0
            and pv_vs_best["recent_diff_mean"] > 0.0
            and pv_vs_best["recent_diff_ci95_low"] > 0.0
        )
        stream_status = "PASS" if stream_pass else "FAIL"

        # Break-even Q sweep across Q in {50, 200, 800} using the FIXED Q=200 best refresh rule `best_refresh_pol`
        q_gaps: List[Tuple[int, float]] = []
        sweep_missing: List[str] = []
        stream_be_rows: List[Dict[str, Any]] = []
        for q_val in (50, 200, 800):
            q_ok, q_miss = check_condition_complete(
                scan_m1,
                stream,
                "nearest_initial",
                4,
                q_val,
                "arithmetic",
                [best_refresh_pol, "extra_search", "push_vectors"],
            )
            if not q_ok:
                sweep_missing.extend(q_miss)
                stream_be_rows.append(
                    {
                        "stream_mode": stream,
                        "queries_per_epoch": q_val,
                        "status": format_incomplete_status(q_miss),
                        "best_refresh_policy": best_refresh_pol,
                        "second_best_refresh_policy": sel_q200["second_policy"],
                        "gap_to_second_best_pp": sel_q200["gap_to_second_pp"],
                        "best_refresh_recent_recall": float("nan"),
                        "extra_search_recent_recall": float("nan"),
                        "push_vectors_recent_recall": float("nan"),
                        "refresh_minus_extra_search_mean": float("nan"),
                        "refresh_minus_extra_search_ci95_low": float("nan"),
                        "refresh_minus_extra_search_ci95_high": float("nan"),
                        "refresh_minus_push_vectors_mean": float("nan"),
                        "refresh_minus_push_vectors_ci95_low": float("nan"),
                        "refresh_minus_push_vectors_ci95_high": float("nan"),
                    }
                )
                continue

            ref_vs_es = compare_pair(
                idx_m1,
                stream,
                "nearest_initial",
                4,
                q_val,
                "arithmetic",
                best_refresh_pol,
                "extra_search",
                expected_seeds=m1_seeds,
                scan_res=scan_m1,
            )
            ref_vs_es["experiment"] = "M1"
            ref_vs_es["comparison_type"] = f"H2_Q_sweep_Q{q_val}"
            paired_rows.append(ref_vs_es)

            ref_vs_pv = compare_pair(
                idx_m1,
                stream,
                "nearest_initial",
                4,
                q_val,
                "arithmetic",
                best_refresh_pol,
                "push_vectors",
                expected_seeds=m1_seeds,
                scan_res=scan_m1,
            )
            ref_vs_pv["experiment"] = "M1"
            ref_vs_pv["comparison_type"] = f"H2_Q_sweep_vs_push_Q{q_val}"
            paired_rows.append(ref_vs_pv)

            sub_cell = df_m1[
                (df_m1["stream_mode"] == stream)
                & (df_m1["placement_mode"] == "nearest_initial")
                & (df_m1["budget_summaries"] == 4)
                & (df_m1["queries_per_epoch"] == q_val)
                & (df_m1["starvation_cap_mode"] == "arithmetic")
            ]
            ref_rec = float(sub_cell[sub_cell["policy"] == best_refresh_pol]["recent_recall_at_10_mean"].iloc[0])
            es_rec = float(sub_cell[sub_cell["policy"] == "extra_search"]["recent_recall_at_10_mean"].iloc[0])
            pv_rec = float(sub_cell[sub_cell["policy"] == "push_vectors"]["recent_recall_at_10_mean"].iloc[0])

            q_gaps.append((q_val, float(ref_vs_es["recent_diff_mean"])))
            stream_be_rows.append(
                {
                    "stream_mode": stream,
                    "queries_per_epoch": q_val,
                    "status": "COMPLETE",
                    "best_refresh_policy": best_refresh_pol,
                    "second_best_refresh_policy": sel_q200["second_policy"],
                    "gap_to_second_best_pp": sel_q200["gap_to_second_pp"],
                    "best_refresh_recent_recall": ref_rec,
                    "extra_search_recent_recall": es_rec,
                    "push_vectors_recent_recall": pv_rec,
                    "refresh_minus_extra_search_mean": ref_vs_es["recent_diff_mean"],
                    "refresh_minus_extra_search_ci95_low": ref_vs_es["recent_diff_ci95_low"],
                    "refresh_minus_extra_search_ci95_high": ref_vs_es["recent_diff_ci95_high"],
                    "refresh_minus_push_vectors_mean": ref_vs_pv["recent_diff_mean"],
                    "refresh_minus_push_vectors_ci95_low": ref_vs_pv["recent_diff_ci95_low"],
                    "refresh_minus_push_vectors_ci95_high": ref_vs_pv["recent_diff_ci95_high"],
                }
            )

        if sweep_missing or len(q_gaps) < 3 or any(not np.isfinite(g) for _, g in q_gaps):
            breakeven_str = format_incomplete_status(sweep_missing)
        else:
            crossings: List[str] = []
            for (q1, g1), (q2, g2) in zip(q_gaps[:-1], q_gaps[1:]):
                if (g1 < 0.0 and g2 > 0.0) or (g1 > 0.0 and g2 < 0.0):
                    crossings.append(f"between Q = {q1} and Q = {q2}")
                elif g1 == 0.0:
                    crossings.append(f"at Q = {q1}")
                elif g2 == 0.0:
                    crossings.append(f"at Q = {q2}")
            if crossings:
                breakeven_str = (
                    f"{'; '.join(crossings)} "
                    f"(rule: {best_refresh_pol}, picked at Q = 200)"
                )
            elif all(g < 0.0 for _, g in q_gaps):
                breakeven_str = (
                    f"no crossing in {{50, 200, 800}}: extra_search ahead at all Q "
                    f"(rule: {best_refresh_pol}, picked at Q = 200)"
                )
            else:
                breakeven_str = (
                    f"no crossing in {{50, 200, 800}}: {best_refresh_pol} ahead at all Q "
                    f"(rule: {best_refresh_pol}, picked at Q = 200)"
                )

        for r_be in stream_be_rows:
            r_be["breakeven_q"] = breakeven_str
            breakeven_rows.append(r_be)

        h2_checks[stream] = {
            "complete": True,
            "status": stream_status,
            "best_refresh": best_refresh_pol,
            "second_refresh": sel_q200["second_policy"],
            "gap_to_second_pp": sel_q200["gap_to_second_pp"],
            "selection_note": sel_q200["selection_note"],
            "es_vs_best": es_vs_best,
            "pv_vs_best": pv_vs_best,
            "pass": stream_pass,
            "breakeven_q": breakeven_str,
        }

        disp_str = (
            f"**{stream_status}** — ES vs `{best_refresh_pol}` (picked on same data, "
            f"{sel_q200['gap_to_second_pp']:+.2f} pp vs 2nd `{sel_q200['second_policy']}`): "
            f"{fmt_diff_pts(es_vs_best['recent_diff_mean'], es_vs_best['recent_diff_ci95_low'], es_vs_best['recent_diff_ci95_high'])}; "
            f"PV vs `{best_refresh_pol}`: "
            f"{fmt_diff_pts(pv_vs_best['recent_diff_mean'], pv_vs_best['recent_diff_ci95_low'], pv_vs_best['recent_diff_ci95_high'])} "
            f"(bytes: ES={es_vs_best['budget_bytes_a_mean']:,.0f}, PV={pv_vs_best['budget_bytes_a_mean']:,.0f}, "
            f"{best_refresh_pol}={es_vs_best['budget_bytes_b_mean']:,.0f}); "
            f"Break-even: {breakeven_str}"
        )
        h2_stream_disp[stream] = disp_str
        if not stream_pass:
            failed_hypothesis_items.append(f"- **H2 ({stream})**: **FAIL** ({disp_str})")

    h2_outcome_str = f"steady={h2_checks['steady']['status']}, new_topic={h2_checks['new_topic']['status']}"
    hypothesis_rows.append(
        {
            "hypothesis": "H2",
            "experiment": "M1 (nearest_initial, Q=200, B=4 & Q sweep)",
            "rule": "extra_search and push_vectors beat best refresh rule at Q=200 (95% CI > 0) per stream",
            "steady_value": h2_stream_disp["steady"],
            "new_topic_value": h2_stream_disp["new_topic"],
            "outcome": h2_outcome_str,
        }
    )

    # --- GATE_M2 (M2, time_segment, Q=200, B=4): always_fresh - never_refresh >= 0.05 separately on steady and new_topic ---
    gate_m2_outcomes: Dict[str, Dict[str, Any]] = {}
    gate_m2_disp: Dict[str, str] = {}
    for stream in ("steady", "new_topic"):
        comp = compare_pair(
            idx_m2,
            stream,
            "time_segment",
            4,
            200,
            "arithmetic",
            "always_fresh",
            "never_refresh",
            expected_seeds=m2_seeds,
            scan_res=scan_m2,
        )
        comp["experiment"] = "M2"
        comp["comparison_type"] = "GATE_M2"
        paired_rows.append(comp)
        if not comp["complete"]:
            comp["gate_pass"] = False
            comp["gate_status"] = comp["status"]
            gate_m2_disp[stream] = comp["status"]
            incomplete_hypothesis_items.append(f"- **GATE_M2 ({stream})**: **{comp['status']}**")
        else:
            gate_pass = bool(comp["recent_diff_mean"] >= 0.05)
            comp["gate_pass"] = gate_pass
            comp["gate_status"] = "PASS" if gate_pass else "FAIL"
            val_str = (
                f"{fmt_diff_pts(comp['recent_diff_mean'], comp['recent_diff_ci95_low'], comp['recent_diff_ci95_high'])} "
                f"(bytes: AF={comp['budget_bytes_a_mean']:,.0f}, NR={comp['budget_bytes_b_mean']:,.0f}) "
                f"**[{comp['gate_status']}]**"
            )
            gate_m2_disp[stream] = val_str
            if not gate_pass:
                failed_hypothesis_items.append(f"- **GATE_M2 ({stream})**: **FAIL** ({val_str})")
        gate_m2_outcomes[stream] = comp

    gate_m2_outcome_str = (
        f"steady={gate_m2_outcomes['steady']['gate_status']}, "
        f"new_topic={gate_m2_outcomes['new_topic']['gate_status']}"
    )
    hypothesis_rows.append(
        {
            "hypothesis": "GATE_M2",
            "experiment": "M2 (time_segment, Q=200, B=4)",
            "rule": "always_fresh - never_refresh recent recall@10 >= 5.0 pp (evaluated per stream)",
            "steady_value": gate_m2_disp["steady"],
            "new_topic_value": gate_m2_disp["new_topic"],
            "outcome": gate_m2_outcome_str,
        }
    )

    # --- H3 (M2, time_segment, B=4, Q=200, tested only on streams where GATE_M2 passes):
    # recovered_neighbor beats round_robin, data_change, ql, ss by >= 5 points (95% CI > 0) ---
    h3_stream_details: Dict[str, Dict[str, Any]] = {}
    for stream in ("steady", "new_topic"):
        comps_vs_rn: Dict[str, Dict[str, Any]] = {}
        for other_rule in ("round_robin", "data_change", "ql", "ss"):
            comp = compare_pair(
                idx_m2,
                stream,
                "time_segment",
                4,
                200,
                "arithmetic",
                "recovered_neighbor",
                other_rule,
                expected_seeds=m2_seeds,
                scan_res=scan_m2,
            )
            comp["experiment"] = "M2"
            comp["comparison_type"] = "H3_rn_vs_refresh_rules_B4"
            paired_rows.append(comp)
            comps_vs_rn[other_rule] = comp

        gate_comp = gate_m2_outcomes[stream]
        h3_ok, h3_missing = check_condition_complete(
            scan_m2, stream, "time_segment", 4, 200, "arithmetic", REFRESH_RULES
        )
        if not gate_comp["complete"]:
            stream_h3_pass = False
            h3_status = gate_comp["status"]
            status_str = h3_status
            incomplete_hypothesis_items.append(f"- **H3 ({stream})**: **{h3_status}**")
        elif not gate_comp["gate_pass"]:
            stream_h3_pass = False
            h3_status = "NOT TESTED (GATE FAILED) / FAIL"
            status_str = "NOT TESTED (GATE FAILED) / FAIL"
            failed_hypothesis_items.append(
                f"- **H3 ({stream})**: **NOT TESTED (GATE FAILED) / FAIL** (GATE_M2 failed on `{stream}`)"
            )
        elif not h3_ok:
            stream_h3_pass = False
            h3_status = format_incomplete_status(h3_missing)
            status_str = h3_status
            incomplete_hypothesis_items.append(f"- **H3 ({stream})**: **{h3_status}**")
        else:
            stream_h3_pass = all(
                c["recent_diff_mean"] >= 0.05 and c["recent_diff_ci95_low"] > 0.0
                for c in comps_vs_rn.values()
            )
            h3_status = "PASS" if stream_h3_pass else "FAIL"
            min_other = min(comps_vs_rn.keys(), key=lambda k: comps_vs_rn[k]["recent_diff_mean"])
            mc = comps_vs_rn[min_other]
            status_str = (
                f"**{h3_status}** (min gap vs `{min_other}`: "
                f"{fmt_diff_pts(mc['recent_diff_mean'], mc['recent_diff_ci95_low'], mc['recent_diff_ci95_high'])}; "
                f"bytes: RN={mc['budget_bytes_a_mean']:,.0f}, {min_other}={mc['budget_bytes_b_mean']:,.0f})"
            )
            if not stream_h3_pass:
                failed_hypothesis_items.append(f"- **H3 ({stream})**: **FAIL** ({status_str})")

        h3_stream_details[stream] = {
            "gate_complete": gate_comp["complete"],
            "gate_passed": gate_comp["gate_pass"],
            "complete": gate_comp["complete"] and (not gate_comp["gate_pass"] or h3_ok),
            "pass": stream_h3_pass,
            "status": h3_status,
            "status_str": status_str,
            "comparisons": comps_vs_rn,
        }

    h3_outcome_str = (
        f"steady={h3_stream_details['steady']['status']}, "
        f"new_topic={h3_stream_details['new_topic']['status']}"
    )
    hypothesis_rows.append(
        {
            "hypothesis": "H3",
            "experiment": "M2 (time_segment, Q=200, B=4)",
            "rule": "If GATE_M2 passes on stream: recovered_neighbor beats round_robin, data_change, ql, ss by >= 5.0 pp (95% CI > 0)",
            "steady_value": h3_stream_details["steady"]["status_str"],
            "new_topic_value": h3_stream_details["new_topic"]["status_str"],
            "outcome": h3_outcome_str,
        }
    )

    # --- H4 (M2, time_segment, Q=200, B in {2, 4, 8}): best refresh rule vs extra_search vs push_vectors ---
    h4_details: Dict[str, List[str]] = {"steady": [], "new_topic": []}
    h4_per_b_outcomes: Dict[str, Dict[int, str]] = {"steady": {}, "new_topic": {}}
    h4_table_rows: List[Dict[str, Any]] = []

    for stream in ("steady", "new_topic"):
        for b_sum in (2, 4, 8):
            b_ok, b_missing = check_condition_complete(
                scan_m2,
                stream,
                "time_segment",
                b_sum,
                200,
                "arithmetic",
                REFRESH_RULES + ["extra_search", "push_vectors"],
            )
            if not b_ok:
                inc_str = format_incomplete_status(b_missing)
                h4_per_b_outcomes[stream][b_sum] = inc_str
                h4_details[stream].append(f"B={b_sum}: {inc_str}")
                selection_notes.append(f"H4 (`{stream}`, B={b_sum}, Q=200): {inc_str}")
                incomplete_hypothesis_items.append(f"- **H4 ({stream}, B={b_sum})**: **{inc_str}**")
                h4_table_rows.append(
                    {
                        "stream_mode": stream,
                        "budget_summaries": b_sum,
                        "status": inc_str,
                        "winner": "INCOMPLETE",
                        "best_refresh_policy": "INCOMPLETE",
                        "second_best_refresh_policy": "INCOMPLETE",
                        "gap_to_second_best_pp": float("nan"),
                        "ref_vs_es_str": inc_str,
                        "ref_vs_pv_str": inc_str,
                        "es_vs_pv_str": inc_str,
                        "bytes_str": "INCOMPLETE",
                    }
                )
                continue

            sel_b = select_best_refresh_rule(df_m2, stream, "time_segment", b_sum, 200, "arithmetic")
            best_ref_b = sel_b["best_policy"]
            selection_notes.append(f"H4 (`{stream}`, B={b_sum}, Q=200): {sel_b['selection_note']}")

            # Record all 5 refresh rules vs extra_search and push_vectors in paired_comparisons.csv (M-4)
            ref_vs_es: Optional[Dict[str, Any]] = None
            ref_vs_pv: Optional[Dict[str, Any]] = None
            for ref_pol in REFRESH_RULES:
                c_es = compare_pair(
                    idx_m2,
                    stream,
                    "time_segment",
                    b_sum,
                    200,
                    "arithmetic",
                    ref_pol,
                    "extra_search",
                    expected_seeds=m2_seeds,
                    scan_res=scan_m2,
                )
                c_es["experiment"] = "M2"
                c_es["comparison_type"] = (
                    f"H4_best_refresh_vs_extra_search_B{b_sum}"
                    if ref_pol == best_ref_b
                    else f"H4_refresh_vs_extra_search_B{b_sum}"
                )
                paired_rows.append(c_es)
                if ref_pol == best_ref_b:
                    ref_vs_es = c_es

                c_pv = compare_pair(
                    idx_m2,
                    stream,
                    "time_segment",
                    b_sum,
                    200,
                    "arithmetic",
                    ref_pol,
                    "push_vectors",
                    expected_seeds=m2_seeds,
                    scan_res=scan_m2,
                )
                c_pv["experiment"] = "M2"
                c_pv["comparison_type"] = (
                    f"H4_best_refresh_vs_push_vectors_B{b_sum}"
                    if ref_pol == best_ref_b
                    else f"H4_refresh_vs_push_vectors_B{b_sum}"
                )
                paired_rows.append(c_pv)
                if ref_pol == best_ref_b:
                    ref_vs_pv = c_pv

            es_vs_pv = compare_pair(
                idx_m2,
                stream,
                "time_segment",
                b_sum,
                200,
                "arithmetic",
                "extra_search",
                "push_vectors",
                expected_seeds=m2_seeds,
                scan_res=scan_m2,
            )
            es_vs_pv["experiment"] = "M2"
            es_vs_pv["comparison_type"] = f"H4_extra_search_vs_push_vectors_B{b_sum}"
            paired_rows.append(es_vs_pv)

            assert ref_vs_es is not None and ref_vs_pv is not None

            ref_beats_es = bool(ref_vs_es["recent_diff_mean"] > 0.0 and ref_vs_es["recent_diff_ci95_low"] > 0.0)
            es_beats_ref = bool(ref_vs_es["recent_diff_mean"] < 0.0 and ref_vs_es["recent_diff_ci95_high"] < 0.0)
            ref_beats_pv = bool(ref_vs_pv["recent_diff_mean"] > 0.0 and ref_vs_pv["recent_diff_ci95_low"] > 0.0)
            pv_beats_ref = bool(ref_vs_pv["recent_diff_mean"] < 0.0 and ref_vs_pv["recent_diff_ci95_high"] < 0.0)
            es_beats_pv = bool(es_vs_pv["recent_diff_mean"] > 0.0 and es_vs_pv["recent_diff_ci95_low"] > 0.0)
            pv_beats_es = bool(es_vs_pv["recent_diff_mean"] < 0.0 and es_vs_pv["recent_diff_ci95_high"] < 0.0)

            beats_both = ref_beats_es and ref_beats_pv
            b_status = "PASS" if beats_both else "FAIL"

            if beats_both:
                winner = best_ref_b
            elif es_beats_ref and es_beats_pv:
                winner = "extra_search"
            elif pv_beats_ref and pv_beats_es:
                winner = "push_vectors"
            else:
                sub_three = df_m2[
                    (df_m2["stream_mode"] == stream)
                    & (df_m2["placement_mode"] == "time_segment")
                    & (df_m2["budget_summaries"] == b_sum)
                    & (df_m2["queries_per_epoch"] == 200)
                    & (df_m2["starvation_cap_mode"] == "arithmetic")
                    & (df_m2["policy"].isin([best_ref_b, "extra_search", "push_vectors"]))
                ].sort_values("recent_recall_at_10_mean", ascending=False)
                top_pol = str(sub_three.iloc[0]["policy"])
                winner = f"no clear winner (highest mean: {top_pol})"

            h4_per_b_outcomes[stream][b_sum] = f"{b_status} (winner: {winner})"

            ref_vs_es_str = fmt_diff_pts(
                ref_vs_es["recent_diff_mean"],
                ref_vs_es["recent_diff_ci95_low"],
                ref_vs_es["recent_diff_ci95_high"],
            )
            ref_vs_pv_str = fmt_diff_pts(
                ref_vs_pv["recent_diff_mean"],
                ref_vs_pv["recent_diff_ci95_low"],
                ref_vs_pv["recent_diff_ci95_high"],
            )
            es_vs_pv_str = fmt_diff_pts(
                es_vs_pv["recent_diff_mean"],
                es_vs_pv["recent_diff_ci95_low"],
                es_vs_pv["recent_diff_ci95_high"],
            )
            bytes_str = (
                f"{best_ref_b}={ref_vs_es['budget_bytes_a_mean']:,.0f}, "
                f"ES={ref_vs_es['budget_bytes_b_mean']:,.0f}, "
                f"PV={ref_vs_pv['budget_bytes_b_mean']:,.0f}"
            )

            detail_entry = (
                f"B={b_sum}: **{b_status}** (winner: `{winner}`; "
                f"`{best_ref_b}` [picked on same data, {sel_b['gap_to_second_pp']:+.2f} pp vs 2nd `{sel_b['second_policy']}`] "
                f"vs ES {ref_vs_es_str}, vs PV {ref_vs_pv_str}, ES vs PV {es_vs_pv_str}; bytes: {bytes_str})"
            )
            h4_details[stream].append(detail_entry)
            if not beats_both:
                failed_hypothesis_items.append(
                    f"- **H4 ({stream}, B={b_sum})**: **FAIL** (winner: `{winner}`; "
                    f"`{best_ref_b}` vs ES {ref_vs_es_str}, vs PV {ref_vs_pv_str}, ES vs PV {es_vs_pv_str})"
                )

            h4_table_rows.append(
                {
                    "stream_mode": stream,
                    "budget_summaries": b_sum,
                    "status": b_status,
                    "winner": winner,
                    "best_refresh_policy": best_ref_b,
                    "second_best_refresh_policy": sel_b["second_policy"],
                    "gap_to_second_best_pp": sel_b["gap_to_second_pp"],
                    "ref_vs_es_str": ref_vs_es_str,
                    "ref_vs_pv_str": ref_vs_pv_str,
                    "es_vs_pv_str": es_vs_pv_str,
                    "bytes_str": bytes_str,
                }
            )

    h4_outcome_parts: List[str] = []
    for stream in ("steady", "new_topic"):
        b_parts = [f"B={b}:{h4_per_b_outcomes[stream][b]}" for b in (2, 4, 8)]
        h4_outcome_parts.append(f"{stream}=[{', '.join(b_parts)}]")
    h4_outcome_str = "; ".join(h4_outcome_parts)

    hypothesis_rows.append(
        {
            "hypothesis": "H4",
            "experiment": "M2 (time_segment, Q=200, B in {2, 4, 8})",
            "rule": "Equal-byte comparison of best refresh rule vs extra_search vs push_vectors per stream and per B in {2, 4, 8}",
            "steady_value": "; ".join(h4_details["steady"]),
            "new_topic_value": "; ".join(h4_details["new_topic"]),
            "outcome": h4_outcome_str,
        }
    )

    df_hyp = pd.DataFrame(hypothesis_rows)
    df_paired = pd.DataFrame(paired_rows)
    df_breakeven = pd.DataFrame(breakeven_rows)

    df_hyp.to_csv(out_dir / "hypothesis_tests.csv", index=False)
    df_paired.to_csv(out_dir / "paired_comparisons.csv", index=False)
    df_breakeven.to_csv(out_dir / "breakeven_q.csv", index=False)

    # Spearman comparison table: compare the two estimators on the SAME refresh decisions (epochs where both are defined)
    spearman_rows: List[Dict[str, Any]] = []
    for exp_label, df_cond in [("M1", df_m1), ("M2", df_m2), ("sens", df_sens)]:
        if df_cond.empty:
            continue
        sub_sp = df_cond[df_cond["policy"].isin(REFRESH_RULES)].copy()
        if sub_sp.empty:
            continue
        # Primary row: recovered_neighbor first, then other refresh rules
        sp_order = {"recovered_neighbor": 0, "round_robin": 1, "data_change": 2, "ql": 3, "ss": 4}
        sub_sp["_sp_rank"] = sub_sp["policy"].map(lambda p: sp_order.get(str(p), 99))
        sub_sp = sub_sp.sort_values(
            ["stream_mode", "placement_mode", "budget_summaries", "queries_per_epoch", "_sp_rank"]
        ).drop(columns=["_sp_rank"])
        for _, r in sub_sp.iterrows():
            spearman_rows.append(
                {
                    "experiment": exp_label,
                    "stream_mode": r["stream_mode"],
                    "placement_mode": r["placement_mode"],
                    "budget_summaries": r["budget_summaries"],
                    "queries_per_epoch": r["queries_per_epoch"],
                    "starvation_cap_mode": r["starvation_cap_mode"],
                    "starvation_cap_epochs": r["starvation_cap_epochs"],
                    "policy": r["policy"],
                    "role": "primary" if r["policy"] == "recovered_neighbor" else "secondary",
                    "spearman_paired_n": r["spearman_paired_n"],
                    "spearman_rn_estimator_mean": r["spearman_rn_mean"],
                    "spearman_rn_estimator_pooled": r["spearman_rn_pooled"],
                    "spearman_rn_estimator_n": r["spearman_paired_n"],
                    "spearman_new_vectors_estimator_mean": r["spearman_new_vecs_mean"],
                    "spearman_new_vectors_estimator_pooled": r["spearman_new_vecs_pooled"],
                    "spearman_new_vectors_estimator_n": r["spearman_paired_n"],
                    "spearman_rn_all_epochs_mean": r["spearman_rn_all_mean"],
                    "spearman_rn_all_epochs_pooled": r["spearman_rn_all_pooled"],
                    "spearman_rn_all_epochs_n": r["spearman_rn_n_defined_total"],
                    "spearman_new_vectors_all_epochs_mean": r["spearman_new_vecs_all_mean"],
                    "spearman_new_vectors_all_epochs_pooled": r["spearman_new_vecs_all_pooled"],
                    "spearman_new_vectors_all_epochs_n": r["spearman_new_vecs_n_defined_total"],
                    "forced_by_cap_mean": r["forced_by_cap_mean"],
                    "summaries_refreshed_mean": r["summaries_refreshed_mean"],
                }
            )
    df_spearman = pd.DataFrame(spearman_rows)
    df_spearman.to_csv(out_dir / "spearman_comparison.csv", index=False)

    # Check failed_runs.json across M1, M2, sens
    failed_runs_summary: Dict[str, Dict[str, Any]] = {}
    for name, d, scan_obj in [("M1", m1_dir, scan_m1), ("M2", m2_dir, scan_m2), ("sens", sens_dir, scan_sens)]:
        valid_rids = {r["run_id"] for r in scan_obj["valid_runs"]}
        missing_rids = {iss["run_id"] for iss in scan_obj["issues"]}
        failed_runs_summary[name] = _parse_failed_runs_file(d, valid_rids, missing_rids)

    write_summary_markdown(
        out_path=out_dir / "SUMMARY.md",
        df_m1=df_m1,
        df_m2=df_m2,
        df_sens=df_sens,
        df_hyp=df_hyp,
        df_breakeven=df_breakeven,
        df_spearman=df_spearman,
        h1_stream_outcomes=h1_stream_outcomes,
        h2_checks=h2_checks,
        gate_m2_outcomes=gate_m2_outcomes,
        h3_stream_details=h3_stream_details,
        h4_table_rows=h4_table_rows,
        selection_notes=selection_notes,
        failed_hypothesis_items=failed_hypothesis_items,
        incomplete_hypothesis_items=incomplete_hypothesis_items,
        scan_m1=scan_m1,
        scan_m2=scan_m2,
        scan_sens=scan_sens,
        failed_runs_summary=failed_runs_summary,
    )

    return {
        "df_m1": df_m1,
        "df_m2": df_m2,
        "df_sens": df_sens,
        "df_missing": df_missing,
        "df_hyp": df_hyp,
        "df_paired": df_paired,
        "df_breakeven": df_breakeven,
        "df_spearman": df_spearman,
        "failed_runs": failed_runs_summary,
        "scan_m1": scan_m1,
        "scan_m2": scan_m2,
        "scan_sens": scan_sens,
    }


def _render_condition_markdown_table(df: pd.DataFrame) -> List[str]:
    lines = [
        "| Stream | Placement | B | Q | Cap | Policy | Seeds | Status | Recall@10 (95% CI) | Recent Recall@10 (95% CI) | Queries With / Without Recent | Missed / Q | Spearman RN Mean / Pooled (n) | Spearman NewVecs Mean / Pooled (n) | Forced / Refreshed | Budget B (Amort B) | Workload B | Method CPU (s) | GT CPU (s) |",
        "|---|---|---:|---:|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    if df.empty:
        lines.append("| — | — | — | — | — | — | 0 | INCOMPLETE | NaN | NaN | 0 / 0 | NaN | NaN | NaN | NaN | NaN | NaN | NaN | NaN |")
        return lines

    for _, r in df.iterrows():
        n_sp = int(r["spearman_paired_n"])
        sp_rn = (
            f"{r['spearman_rn_mean']:.4f} / {r['spearman_rn_pooled']:.4f} ({n_sp})"
            if np.isfinite(r["spearman_rn_mean"])
            else f"NaN ({n_sp})"
        )
        sp_nv = (
            f"{r['spearman_new_vecs_mean']:.4f} / {r['spearman_new_vecs_pooled']:.4f} ({n_sp})"
            if np.isfinite(r["spearman_new_vecs_mean"])
            else f"NaN ({n_sp})"
        )
        cap_str = f"{r['starvation_cap_mode']}({int(r['starvation_cap_epochs'])})"
        lines.append(
            f"| `{r['stream_mode']}` | `{r['placement_mode']}` | {int(r['budget_summaries'])} | {int(r['queries_per_epoch'])} "
            f"| `{cap_str}` | `{r['policy']}` | {int(r['n_seeds'])} | {r['status']} "
            f"| {fmt_ci(r['recall_at_10_mean'], r['recall_at_10_ci95_low'], r['recall_at_10_ci95_high'])} "
            f"| {fmt_ci(r['recent_recall_at_10_mean'], r['recent_recall_at_10_ci95_low'], r['recent_recall_at_10_ci95_high'])} "
            f"| {int(r['n_queries_with_recent'])} / {int(r['n_queries_without_recent'])} "
            f"| {r['missed_neighbors_mean']:.4f} "
            f"| {sp_rn} | {sp_nv} "
            f"| {r['forced_by_cap_mean']:.1f} / {r['summaries_refreshed_mean']:.1f} "
            f"| {r['budget_bytes_spent_mean']:,.0f} ({r['budget_bytes_with_amortized_setup_mean']:,.0f}) "
            f"| {r['workload_bytes_mean']:,.0f} "
            f"| {r['cpu_method_s_mean']:.2f} | {r['cpu_gt_s_mean']:.2f} |"
        )
    return lines


def write_summary_markdown(
    out_path: Path,
    df_m1: pd.DataFrame,
    df_m2: pd.DataFrame,
    df_sens: pd.DataFrame,
    df_hyp: pd.DataFrame,
    df_breakeven: pd.DataFrame,
    df_spearman: pd.DataFrame,
    h1_stream_outcomes: Dict[str, Dict[str, Any]],
    h2_checks: Dict[str, Dict[str, Any]],
    gate_m2_outcomes: Dict[str, Dict[str, Any]],
    h3_stream_details: Dict[str, Dict[str, Any]],
    h4_table_rows: Sequence[Dict[str, Any]],
    selection_notes: Sequence[str],
    failed_hypothesis_items: Sequence[str],
    incomplete_hypothesis_items: Sequence[str],
    scan_m1: Dict[str, Any],
    scan_m2: Dict[str, Any],
    scan_sens: Dict[str, Any],
    failed_runs_summary: Dict[str, Dict[str, Any]],
) -> None:
    lines: List[str] = []
    lines.append("# Main Experiments Summary (`M1`, `M2`, `sens`)")
    lines.append("")
    lines.append("## 1. Pre-Registered Hypothesis Outcomes (`configs/prereg_addendum1.yaml`)")
    lines.append("")
    lines.append("| Hypothesis | Experiment / Condition | Decision Rule | `steady` Result (95% Paired Bootstrap CI) | `new_topic` Result (95% Paired Bootstrap CI) | Outcome |")
    lines.append("|---|---|---|---|---|---|")
    for _, row in df_hyp.iterrows():
        lines.append(
            f"| **{row['hypothesis']}** | {row['experiment']} | {row['rule']} | {row['steady_value']} | {row['new_topic_value']} | **{row['outcome']}** |"
        )
    lines.append("")

    lines.append("### Best Refresh Rule Selection Notes (Same-Data Selection)")
    lines.append("")
    for note in selection_notes:
        lines.append(f"- {note}")
    lines.append("")

    lines.append("### Hypotheses that FAIL")
    lines.append("")
    if failed_hypothesis_items:
        lines.extend(failed_hypothesis_items)
    elif incomplete_hypothesis_items:
        lines.append("- None confirmed FAIL (one or more tests are INCOMPLETE; see below).")
    else:
        lines.append("- None.")
    lines.append("")

    if incomplete_hypothesis_items:
        lines.append("### Incomplete Hypothesis Tests")
        lines.append("")
        lines.extend(incomplete_hypothesis_items)
        lines.append("")

    # Section 2: Missing or incomplete runs & Failed runs record
    lines.append("## 2. Missing or incomplete runs")
    lines.append("")
    for scan_obj in (scan_m1, scan_m2, scan_sens):
        exp_lbl = scan_obj["experiment"]
        n_exp = len(scan_obj["grid_spec"]["expected_cells"])
        n_val = len(scan_obj["valid_runs"])
        issues = scan_obj["issues"]
        commits_str = ", ".join(scan_obj["git_commits"]) if scan_obj["git_commits"] else "none"
        shas_str = ", ".join(scan_obj["config_shas"]) if scan_obj["config_shas"] else "none"
        lines.append(
            f"- **`{exp_lbl}`**: {n_val} / {n_exp} complete valid runs "
            f"(git_commit: `{commits_str}`, config_sha256: `{shas_str}`)."
        )
        for warn in scan_obj["prov_warnings"]:
            lines.append(f"  - PROVENANCE MISMATCH: {warn}")
        if issues:
            for iss in issues:
                lines.append(f"  - `{iss['run_id']}`: **{iss['status']}** ({iss['reason']})")
        else:
            lines.append("  - None (all expected grid runs present, complete, and config-verified).")
    lines.append("")

    lines.append("### Failed Runs Record (`failed_runs.json`)")
    lines.append("")
    for name in ("M1", "M2", "sens"):
        f_info = failed_runs_summary[name]
        if not f_info["present"]:
            lines.append(f"- `{name}`: `failed_runs.json` not found (experiment unfinished or absent).")
        elif f_info["n_unique_runs"] == 0:
            lines.append(f"- `{name}`: `failed_runs.json` present — 0 unique failed run ids.")
        else:
            lines.append(
                f"- `{name}`: {f_info['n_unique_runs']} unique run id(s) across {f_info['n_attempts']} attempt(s):"
            )
            for rec in f_info["runs"]:
                lines.append(
                    f"  - `{rec['run_id']}`: {rec['outcome']} ({rec['attempts']} attempt(s))"
                )
    lines.append("")

    # Section 3: Gate values (fixing L-4: M1 is H1 < 5.00 pp; M2 is GATE_M2 >= 5.00 pp)
    lines.append("## 3. Gate Values (`always_fresh - never_refresh` Recent Recall@10 at $Q=200, B=4$)")
    lines.append("")
    lines.append("| Experiment | Placement | Stream | Criterion | `always_fresh - never_refresh` (pp, 95% CI across seeds+queries) | Within-Seed 95% CI (pp) | Paired Queries ($n$) | Outcome |")
    lines.append("|---|---|---|---|---|---|---:|---|")
    for exp_name, criterion_str, outcomes_map, status_key in [
        ("M1", "H1 (< +5.00 pp)", h1_stream_outcomes, "h1_status"),
        ("M2", "GATE_M2 (>= +5.00 pp)", gate_m2_outcomes, "gate_status"),
    ]:
        for stream in ("steady", "new_topic"):
            c = outcomes_map[stream]
            if not c["complete"]:
                lines.append(
                    f"| `{exp_name}` | `{c['placement_mode']}` | `{stream}` | {criterion_str} "
                    f"| {c['status']} | NaN | 0 | **{c['status']}** |"
                )
            else:
                lines.append(
                    f"| `{exp_name}` | `{c['placement_mode']}` | `{stream}` | {criterion_str} "
                    f"| {fmt_diff_pts(c['recent_diff_mean'], c['recent_diff_ci95_low'], c['recent_diff_ci95_high'])} "
                    f"| [{c['recent_diff_within_ci95_low'] * 100.0:+.2f}, {c['recent_diff_within_ci95_high'] * 100.0:+.2f}] "
                    f"| {int(c['n_paired_queries'])} "
                    f"| **{c[status_key]}** |"
                )
    lines.append("")

    # Section 4: H3 pairwise gaps on M2 at B=4, Q=200
    lines.append("## 4. M2 Refresh-Rule Pairwise Comparisons (`recovered_neighbor` vs. Other Refresh Rules at $B=4, Q=200$)")
    lines.append("")
    lines.append("| Stream | `GATE_M2` Status | Baseline Rule | `recovered_neighbor - baseline` Recent Recall@10 (pp, 95% CI) | Overall Recall@10 Gap (pp, 95% CI) | Mean Budget Bytes (RN / Baseline) | Paired Queries ($n$) | Beats by $\\ge +5.00$ pp (95% CI $> 0$)? |")
    lines.append("|---|---|---|---|---|---|---:|---|")
    for stream in ("steady", "new_topic"):
        det = h3_stream_details[stream]
        gate_str = gate_m2_outcomes[stream]["gate_status"]
        for other_rule, c in det["comparisons"].items():
            if not det["gate_complete"]:
                cell_outcome = gate_str
                rec_str = c["status"]
                ov_str = "NaN"
                bytes_str = "NaN"
            elif not det["gate_passed"]:
                cell_outcome = "NOT TESTED (GATE FAILED) / FAIL"
                rec_str = fmt_diff_pts(c["recent_diff_mean"], c["recent_diff_ci95_low"], c["recent_diff_ci95_high"]) if c["complete"] else c["status"]
                ov_str = fmt_diff_pts(c["overall_diff_mean"], c["overall_diff_ci95_low"], c["overall_diff_ci95_high"]) if c["complete"] else "NaN"
                bytes_str = f"{c['budget_bytes_a_mean']:,.0f} / {c['budget_bytes_b_mean']:,.0f}" if c["complete"] else "NaN"
            elif not c["complete"]:
                cell_outcome = c["status"]
                rec_str = c["status"]
                ov_str = "NaN"
                bytes_str = "NaN"
            else:
                rule_ok = c["recent_diff_mean"] >= 0.05 and c["recent_diff_ci95_low"] > 0.0
                cell_outcome = "PASS" if rule_ok else "FAIL"
                rec_str = fmt_diff_pts(c["recent_diff_mean"], c["recent_diff_ci95_low"], c["recent_diff_ci95_high"])
                ov_str = fmt_diff_pts(c["overall_diff_mean"], c["overall_diff_ci95_low"], c["overall_diff_ci95_high"])
                bytes_str = f"{c['budget_bytes_a_mean']:,.0f} / {c['budget_bytes_b_mean']:,.0f}"
            lines.append(
                f"| `{stream}` | {gate_str} | `{other_rule}` "
                f"| {rec_str} | {ov_str} | {bytes_str} "
                f"| {int(c['n_paired_queries'])} "
                f"| **{cell_outcome}** |"
            )
    lines.append("")

    # Section 5: Break-even Q table (M1)
    lines.append("## 5. M1 Query-Rate Sweep ($Q \\in \\{50, 200, 800\\}, B=4$) & Break-Even $Q$")
    lines.append("")
    lines.append("| Stream | $Q$ | Fixed $Q=200$ Best Refresh Rule (Gap to 2nd) | Best Refresh Recent Recall@10 | `extra_search` Recent Recall@10 | `push_vectors` Recent Recall@10 | `best_refresh - extra_search` (pp, 95% CI) | `best_refresh - push_vectors` (pp, 95% CI) | Break-Even $Q$ (Refresh vs. `extra_search`) |")
    lines.append("|---|---:|---|---:|---:|---:|---|---|---|")
    for _, r in df_breakeven.iterrows():
        stream = str(r["stream_mode"])
        if r["status"] != "COMPLETE":
            lines.append(
                f"| `{stream}` | {int(r['queries_per_epoch'])} | `{r['best_refresh_policy']}` "
                f"| NaN | NaN | NaN | {r['status']} | {r['status']} | {r['breakeven_q']} |"
            )
        else:
            rule_lbl = (
                f"`{r['best_refresh_policy']}` "
                f"({r['gap_to_second_best_pp']:+.2f} pp vs `{r['second_best_refresh_policy']}`)"
            )
            lines.append(
                f"| `{stream}` | {int(r['queries_per_epoch'])} | {rule_lbl} "
                f"| {r['best_refresh_recent_recall']:.4f} "
                f"| {r['extra_search_recent_recall']:.4f} "
                f"| {r['push_vectors_recent_recall']:.4f} "
                f"| {fmt_diff_pts(r['refresh_minus_extra_search_mean'], r['refresh_minus_extra_search_ci95_low'], r['refresh_minus_extra_search_ci95_high'])} "
                f"| {fmt_diff_pts(r['refresh_minus_push_vectors_mean'], r['refresh_minus_push_vectors_ci95_low'], r['refresh_minus_push_vectors_ci95_high'])} "
                f"| {r['breakeven_q']} |"
            )
    lines.append("")

    # Section 6: H4 per-B comparison table
    lines.append("## 6. M2 Equal-Bytes Comparison Across Budget $B \\in \\{2, 4, 8\\}$ ($Q=200$, H4)")
    lines.append("")
    lines.append("| Stream | $B$ | Best Refresh Rule (Same-Data Selection Gap to 2nd) | Winner | `best_refresh - extra_search` (pp, 95% CI) | `best_refresh - push_vectors` (pp, 95% CI) | `extra_search - push_vectors` (pp, 95% CI) | Mean Budget Bytes | Outcome |")
    lines.append("|---|---:|---|---|---|---|---|---|---|")
    for r_h4 in h4_table_rows:
        if r_h4["status"].startswith("INCOMPLETE"):
            lines.append(
                f"| `{r_h4['stream_mode']}` | {int(r_h4['budget_summaries'])} | INCOMPLETE | INCOMPLETE "
                f"| {r_h4['status']} | {r_h4['status']} | {r_h4['status']} | INCOMPLETE | **{r_h4['status']}** |"
            )
        else:
            rule_lbl = (
                f"`{r_h4['best_refresh_policy']}` "
                f"({r_h4['gap_to_second_best_pp']:+.2f} pp vs `{r_h4['second_best_refresh_policy']}`)"
            )
            lines.append(
                f"| `{r_h4['stream_mode']}` | {int(r_h4['budget_summaries'])} | {rule_lbl} | `{r_h4['winner']}` "
                f"| {r_h4['ref_vs_es_str']} | {r_h4['ref_vs_pv_str']} | {r_h4['es_vs_pv_str']} "
                f"| {r_h4['bytes_str']} | **{r_h4['status']}** |"
            )
    lines.append("")

    # Section 7: Spearman estimator table (same refresh decisions only: mean, pooled, and n)
    lines.append("## 7. Estimator Rank Correlation on Same Refresh Decisions (`recovered_neighbor` Sample-Query Estimator vs. `new_vectors` Estimator at $B=4, Q=200$)")
    lines.append("")
    lines.append("| Experiment | Stream | Placement | Cap | Policy | Role | `recovered_neighbor` Estimator Spearman $\\rho$ Mean / Pooled ($n$) | `new_vectors` Since Last Refresh Spearman $\\rho$ Mean / Pooled ($n$) | Forced by Cap / Total Refreshed |")
    lines.append("|---|---|---|---|---|---|---:|---:|---:|")
    if not df_spearman.empty:
        sub_sp_b4 = df_spearman[(df_spearman["budget_summaries"] == 4) & (df_spearman["queries_per_epoch"] == 200)]
        for _, r in sub_sp_b4.iterrows():
            n_sp = int(r["spearman_paired_n"])
            rn_str = (
                f"mean={r['spearman_rn_estimator_mean']:.4f}, pooled={r['spearman_rn_estimator_pooled']:.4f} (n={n_sp})"
                if np.isfinite(r["spearman_rn_estimator_mean"])
                else f"NaN (n={n_sp})"
            )
            nv_str = (
                f"mean={r['spearman_new_vectors_estimator_mean']:.4f}, pooled={r['spearman_new_vectors_estimator_pooled']:.4f} (n={n_sp})"
                if np.isfinite(r["spearman_new_vectors_estimator_mean"])
                else f"NaN (n={n_sp})"
            )
            lines.append(
                f"| `{r['experiment']}` | `{r['stream_mode']}` | `{r['placement_mode']}` "
                f"| `{r['starvation_cap_mode']}({int(r['starvation_cap_epochs'])})` | `{r['policy']}` | {r['role']} "
                f"| {rn_str} | {nv_str} "
                f"| {r['forced_by_cap_mean']:.1f} / {r['summaries_refreshed_mean']:.1f} |"
            )
    lines.append("")

    # Full condition tables
    lines.append("## 8. Experiment M1 Condition Table (`nearest_initial`, 5 Seeds: 42–46)")
    lines.append("")
    lines.extend(_render_condition_markdown_table(df_m1))
    lines.append("")

    lines.append("## 9. Experiment M2 Condition Table (`time_segment`, 5 Seeds: 42–46)")
    lines.append("")
    lines.extend(_render_condition_markdown_table(df_m2))
    lines.append("")

    lines.append("## 10. Sensitivity Condition Table (`time_segment`, Fixed Cap $A=8$, $B=4, Q=200$, Seeds 42–44)")
    lines.append("")
    lines.extend(_render_condition_markdown_table(df_sens))
    lines.append("")

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze main DVS experiments M1, M2, and sens.")
    parser.add_argument(
        "--results-root",
        "--results-dir",
        dest="results_root",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "results",
        help="Root directory containing main/M1, main/M2, main/sens (or M1, M2, sens directly).",
    )
    parser.add_argument(
        "--out",
        "--out-dir",
        dest="out_dir",
        type=Path,
        default=None,
        help="Output directory for SUMMARY.md and CSV tables (defaults to <results-root>/main).",
    )
    parser.add_argument(
        "--configs-dir",
        dest="configs_dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "configs",
        help="Directory containing main_M1.yaml, main_M2.yaml, and main_sens.yaml.",
    )
    args = parser.parse_args()
    analyze_all(results_root=args.results_root, out_dir=args.out_dir, configs_dir=args.configs_dir)


if __name__ == "__main__":
    main()
