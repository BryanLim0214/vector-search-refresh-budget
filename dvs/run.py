"""Main simulation runner and experiment orchestrator.

Executes epoch-by-epoch streaming vector search simulations and writes:
- `results/<exp>/<run>/epochs.parquet`
- `results/<exp>/<run>/summary.json`
- `results/<exp>/<run>/provenance.json` (git commit, config SHA-256, package versions, CPU info)
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import platform
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd
import yaml

from dvs.bus import (
    ByteBus,
    SETUP_SAMPLE_VECTORS_PER_SHARD_BYTES,
    SUMMARY_BYTES,
    SUMMARY_CENTERS,
    TOP_K,
)
from dvs.coordinator import Coordinator
from dvs.data import load_dataset
from dvs.metrics import (
    CPUTimer,
    GroundTruthIndex,
    compute_spearman_correlation,
    compute_true_recovered_neighbors_per_shard,
    evaluate_epoch_queries,
    paired_bootstrap_ci,
)
from dvs.policies import BOUND_POLICIES, PolicyController
from dvs.shards import build_initial_shards
from dvs.streams import build_epoch_schedule


@dataclass
class SimulationRunResult:
    """In-memory result of a single simulation trajectory."""

    run_id: str
    config: Dict[str, Any]
    epochs_df: pd.DataFrame
    summary: Dict[str, Any]
    bus: ByteBus
    all_query_recall: np.ndarray
    all_query_recent_recall: np.ndarray
    all_query_missed_neighbors: np.ndarray


def get_git_commit(repo_root: Path) -> str:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=str(repo_root),
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        return out
    except Exception:
        return "uncommitted"


def get_package_versions() -> Dict[str, str]:
    pkgs = [
        "numpy",
        "scipy",
        "pandas",
        "pyarrow",
        "faiss-cpu",
        "chroma-hnswlib",
        "hnswlib",
        "scikit-learn",
        "PyYAML",
        "pytest",
        "tqdm",
    ]
    versions: Dict[str, str] = {}
    for p in pkgs:
        try:
            versions[p] = importlib.metadata.version(p)
        except importlib.metadata.PackageNotFoundError:
            continue
    return versions


def get_cpu_info() -> Dict[str, Any]:
    return {
        "platform": platform.platform(),
        "processor": platform.processor(),
        "machine": platform.machine(),
        "python_version": platform.python_version(),
        "cpu_count": os.cpu_count(),
    }


def run_single_simulation(
    initial_vectors: np.ndarray,
    initial_ids: np.ndarray,
    stream_vectors: np.ndarray,
    stream_ids: np.ndarray,
    test_queries: np.ndarray,
    test_query_ids: np.ndarray,
    sample_queries: np.ndarray,
    sample_query_ids: np.ndarray,
    policy: str = "recovered_neighbor",
    num_shards: int = 32,
    c: int = SUMMARY_CENTERS,
    m: int = 2,
    budget_summaries: int = 4,
    budget_bytes_override: Optional[int] = None,
    num_epochs: int = 50,
    inserts_per_epoch: int = 1000,
    queries_per_epoch: int = 200,
    recent_window_w: int = 2,
    stream_mode: str = "steady",
    bursts: bool = False,
    sharding_mode: str = "kmeans",
    seed: int = 42,
    exact_local_search: bool = False,
    charge_setup_to_budget: bool = False,
    m_hnsw: int = 16,
    ef_construction: int = 200,
    ef_search: int = 100,
    run_id: str = "run",
) -> SimulationRunResult:
    """Run one complete streaming simulation for a single policy and seed."""
    # Strict leakage assertion before simulation begins
    test_qid_set = set(np.asarray(test_query_ids, dtype=np.int64).tolist())
    sample_qid_set = set(np.asarray(sample_query_ids, dtype=np.int64).tolist())
    if not test_qid_set.isdisjoint(sample_qid_set):
        raise AssertionError("Data leakage detected: test_query_ids and sample_query_ids overlap!")
    if len(sample_queries) > 0 and len(test_queries) > 0:
        max_cos = float(np.max(sample_queries @ test_queries.T))
        if max_cos >= 0.9999:
            raise AssertionError(
                f"Data leakage detected: a sample query vector is identical to a test query vector (cos={max_cos})"
            )

    budget_bytes = (
        int(budget_bytes_override)
        if budget_bytes_override is not None
        else int(budget_summaries) * SUMMARY_BYTES
    )
    m_routed = int(num_shards) if policy == "all_shards" else int(m)

    bus = ByteBus(num_shards=num_shards, num_epochs=num_epochs)

    # 1. Build initial shards and ground-truth index at setup (epoch -1)
    max_elems = len(initial_vectors) + num_epochs * inserts_per_epoch + 1024
    shards, router = build_initial_shards(
        initial_vectors=initial_vectors,
        initial_ids=initial_ids,
        num_shards=num_shards,
        c=c,
        m_hnsw=m_hnsw,
        ef_construction=ef_construction,
        ef_search=ef_search,
        seed=seed,
        sharding_mode=sharding_mode,
        max_elements_per_shard=max_elems,
    )
    bus.log_setup_initial_summaries(num_shards=num_shards, c=c)

    # Send sample query vectors at setup for recovered_neighbor (reported both as setup and amortized)
    for s in shards:
        s.set_sample_queries(sample_queries)
    if policy == "recovered_neighbor":
        bus.log_setup_sample_vectors(
            num_shards=num_shards,
            num_sample_queries=len(sample_queries),
        )

    gt = GroundTruthIndex()
    init_shard_assignments = router.assign_shards(initial_vectors, initial_ids)
    gt.add(
        vectors=initial_vectors,
        doc_ids=initial_ids,
        shard_ids=init_shard_assignments,
        epoch=-1,
    )

    coordinator = Coordinator(
        shards=shards,
        bus=bus,
        m=m_routed,
        k=TOP_K,
        exact_local_search=exact_local_search,
    )
    controller = PolicyController(
        policy=policy,
        budget_bytes=budget_bytes,
        num_shards=num_shards,
        m=m_routed,
        num_epochs=num_epochs,
        charge_setup_to_budget=charge_setup_to_budget,
    )

    schedule = build_epoch_schedule(
        stream_vectors=stream_vectors,
        stream_ids=stream_ids,
        test_queries=test_queries,
        test_query_ids=test_query_ids,
        sample_queries=sample_queries,
        stream_mode=stream_mode,
        num_epochs=num_epochs,
        inserts_per_epoch=inserts_per_epoch,
        queries_per_epoch=queries_per_epoch,
        seed=seed,
        bursts=bursts,
    )

    epoch_rows: List[Dict[str, Any]] = []
    all_q_recall: List[np.ndarray] = []
    all_q_recent: List[np.ndarray] = []
    all_q_missed: List[np.ndarray] = []

    for batch in schedule:
        epoch = batch.epoch
        timer = CPUTimer()

        # Step A: Insert epoch's new vectors into shards and exact ground-truth index
        with timer.measure("insert"):
            if len(batch.insert_vectors) > 0:
                new_shard_ids = router.assign_shards(batch.insert_vectors, batch.insert_ids)
                for s_id in range(num_shards):
                    mask = new_shard_ids == s_id
                    if np.any(mask):
                        shards[s_id].add_vectors(
                            batch.insert_vectors[mask],
                            batch.insert_ids[mask],
                        )
                gt.add(
                    vectors=batch.insert_vectors,
                    doc_ids=batch.insert_ids,
                    shard_ids=new_shard_ids,
                    epoch=epoch,
                )
                controller.record_new_arrivals(
                    vectors=batch.insert_vectors,
                    doc_ids=batch.insert_ids,
                    shard_ids=new_shard_ids,
                )

        # Compute exact ground truth for the epoch's test queries (not timed as simulator CPU)
        gt_ids, _, gt_epochs, gt_shards = gt.search(batch.test_queries, k=TOP_K)

        # Step B: Update candidate summaries on shards (timed under summary component)
        with timer.measure("summary"):
            if policy not in ("all_shards", "extra_search", "push_vectors"):
                for shard in shards:
                    shard.update_candidate_summary(epoch)

        # Compute true recovered neighbors per shard BEFORE summaries are refreshed in this epoch
        true_rec_per_shard: Optional[np.ndarray] = None
        if policy not in ("all_shards", "extra_search", "push_vectors"):
            true_rec_per_shard = compute_true_recovered_neighbors_per_shard(
                shards=shards,
                coordinator=coordinator,
                test_queries=batch.test_queries,
                gt_ids=gt_ids,
                gt_shards=gt_shards,
            )

        # Step C: Execute policy refresh or adaptation under budget B
        with timer.measure("policy"):
            decision = controller.step_epoch(
                epoch=epoch,
                shards=shards,
                coordinator=coordinator,
                bus=bus,
                sample_queries=sample_queries,
                epoch_test_queries=batch.test_queries,
            )

        spearman_rho = compute_spearman_correlation(
            decision.estimated_recovered_neighbors,
            true_rec_per_shard,
        )

        # Step D: Route and execute epoch's test queries
        with timer.measure("query"):
            ret_ids, _, searched_mask = coordinator.execute_queries(
                epoch=epoch,
                queries=batch.test_queries,
                shards=shards,
                m=m_routed,
                extra_shards_per_query=decision.extra_shards_per_query,
                record_popularity=True,
            )

        # Evaluate accuracy and byte accounting
        pushed_set = coordinator._pushed_id_set if policy == "push_vectors" else None
        eval_res = evaluate_epoch_queries(
            epoch=epoch,
            returned_ids_list=ret_ids,
            searched_shards_mask=searched_mask,
            gt_ids=gt_ids,
            gt_epochs=gt_epochs,
            gt_shards=gt_shards,
            recent_window_w=recent_window_w,
            k=TOP_K,
            pushed_id_set=pushed_set,
            spearman_rho=spearman_rho,
        )

        all_q_recall.append(eval_res.per_query_recall)
        all_q_recent.append(eval_res.per_query_recent_recall)
        all_q_missed.append(eval_res.per_query_missed_neighbors)

        cat_totals = bus.epoch_category_totals(epoch)
        budget_spent = bus.epoch_budget_bytes(epoch)
        workload_spent = bus.epoch_workload_bytes(epoch)
        total_spent = bus.epoch_total_bytes(epoch)

        # Enforce strict budget compliance on all budgeted policies
        if policy not in BOUND_POLICIES and budget_spent > budget_bytes:
            raise AssertionError(
                f"Policy '{policy}' exceeded budget in epoch {epoch}: spent {budget_spent} > B={budget_bytes}"
            )

        row = {
            "run_id": run_id,
            "epoch": epoch,
            "policy": policy,
            "seed": seed,
            "stream_mode": stream_mode,
            "bursts": bursts,
            "sharding_mode": sharding_mode,
            "num_shards": num_shards,
            "m": m,
            "m_routed": m_routed,
            "budget_summaries": budget_summaries,
            "budget_bytes": budget_bytes,
            "queries_per_epoch": len(batch.test_queries),
            "inserts_this_epoch": len(batch.insert_vectors),
            "total_indexed_vectors": gt.size,
            "recall_at_10": eval_res.recall_at_10,
            "recent_recall_at_10": eval_res.recent_recall_at_10,
            "num_queries_with_recent_neighbors": eval_res.num_queries_with_recent_neighbors,
            "num_queries_without_recent_neighbors": eval_res.num_queries_without_recent_neighbors,
            "missed_neighbors_mean": eval_res.missed_neighbors_mean,
            "missed_neighbors_total": eval_res.missed_neighbors_total,
            "spearman_rho": eval_res.spearman_rho,
            "budget_bytes_spent": budget_spent,
            "budget_bytes_with_amortized_setup": bus.epoch_budget_bytes_with_amortized_setup(epoch),
            "workload_bytes": workload_spent,
            "total_epoch_bytes": total_spent,
            "bytes_summary_refresh": cat_totals["summary_refresh"],
            "bytes_rn_sample_search_reply": cat_totals["rn_sample_search_reply"],
            "bytes_rn_cutoffs": cat_totals["rn_cutoffs"],
            "bytes_rn_shard_score": cat_totals["rn_shard_score"],
            "bytes_extra_search_query": cat_totals["extra_search_query"],
            "bytes_extra_search_result": cat_totals["extra_search_result"],
            "bytes_push_vector": cat_totals["push_vector"],
            "bytes_setup_sample_vectors_amortized": cat_totals["setup_sample_vectors_amortized"],
            "bytes_query_out": cat_totals["query_out"],
            "bytes_query_result": cat_totals["query_result"],
            "num_summaries_refreshed": len(decision.refreshed_shards),
            "num_extra_searches": decision.num_extra_searches,
            "num_pushed_vectors": decision.num_pushed_vectors,
            "total_lift_in_flips": (
                int(np.sum(decision.lift_in_flips_per_shard))
                if decision.lift_in_flips_per_shard is not None
                else 0
            ),
            "total_dropout_flips": (
                int(np.sum(decision.dropout_flips_per_shard))
                if decision.dropout_flips_per_shard is not None
                else 0
            ),
            "cpu_insert_s": timer.times["insert"],
            "cpu_summary_s": timer.times["summary"],
            "cpu_policy_s": timer.times["policy"],
            "cpu_query_s": timer.times["query"],
            "cpu_total_s": timer.total,
        }
        epoch_rows.append(row)

    epochs_df = pd.DataFrame(epoch_rows)
    concat_recall = np.concatenate(all_q_recall) if all_q_recall else np.empty(0, dtype=np.float64)
    concat_recent = np.concatenate(all_q_recent) if all_q_recent else np.empty(0, dtype=np.float64)
    concat_missed = np.concatenate(all_q_missed) if all_q_missed else np.empty(0, dtype=np.int64)

    valid_recent = concat_recent[np.isfinite(concat_recent)]
    summary = {
        "run_id": run_id,
        "policy": policy,
        "seed": seed,
        "stream_mode": stream_mode,
        "bursts": bursts,
        "sharding_mode": sharding_mode,
        "num_shards": num_shards,
        "c": c,
        "m": m,
        "budget_summaries": budget_summaries,
        "budget_bytes": budget_bytes,
        "num_epochs": num_epochs,
        "inserts_per_epoch": inserts_per_epoch,
        "queries_per_epoch": queries_per_epoch,
        "recent_window_w": recent_window_w,
        "exact_local_search": exact_local_search,
        "charge_setup_to_budget": charge_setup_to_budget,
        "overall_recall_at_10_mean": float(np.mean(concat_recall)) if len(concat_recall) > 0 else 0.0,
        "epoch_recall_at_10_mean": float(epochs_df["recall_at_10"].mean()),
        "recent_recall_at_10_query_mean": (
            float(np.mean(valid_recent)) if len(valid_recent) > 0 else float("nan")
        ),
        "recent_recall_at_10_epoch_mean": float(epochs_df["recent_recall_at_10"].dropna().mean()),
        "total_queries_evaluated": int(len(concat_recall)),
        "total_queries_with_recent_neighbors": int(epochs_df["num_queries_with_recent_neighbors"].sum()),
        "total_queries_without_recent_neighbors": int(
            epochs_df["num_queries_without_recent_neighbors"].sum()
        ),
        "missed_neighbors_query_mean": (
            float(np.mean(concat_missed)) if len(concat_missed) > 0 else 0.0
        ),
        "missed_neighbors_total": int(epochs_df["missed_neighbors_total"].sum()),
        "spearman_rho_mean": float(epochs_df["spearman_rho"].mean()),
        "setup_sample_vectors_bytes": int(bus.setup_sample_vectors_bytes),
        "setup_sample_vectors_per_shard_bytes": (
            SETUP_SAMPLE_VECTORS_PER_SHARD_BYTES if policy == "recovered_neighbor" else 0
        ),
        "amortized_setup_sample_bytes_per_epoch": float(bus.amortized_setup_sample_bytes_per_epoch()),
        "total_budget_bytes_spent": int(epochs_df["budget_bytes_spent"].sum()),
        "total_budget_bytes_with_amortized_setup": float(
            epochs_df["budget_bytes_with_amortized_setup"].sum()
        ),
        "max_epoch_budget_bytes_spent": int(epochs_df["budget_bytes_spent"].max()),
        "mean_epoch_budget_bytes_spent": float(epochs_df["budget_bytes_spent"].mean()),
        "total_workload_bytes": int(epochs_df["workload_bytes"].sum()),
        "total_bytes_all_categories": int(epochs_df["total_epoch_bytes"].sum()),
        "total_summaries_refreshed": int(epochs_df["num_summaries_refreshed"].sum()),
        "total_extra_searches": int(epochs_df["num_extra_searches"].sum()),
        "total_pushed_vectors": int(epochs_df["num_pushed_vectors"].sum()),
        "total_lift_in_flips": int(epochs_df["total_lift_in_flips"].sum()),
        "total_dropout_flips": int(epochs_df["total_dropout_flips"].sum()),
        "cpu_insert_s_total": float(epochs_df["cpu_insert_s"].sum()),
        "cpu_summary_s_total": float(epochs_df["cpu_summary_s"].sum()),
        "cpu_policy_s_total": float(epochs_df["cpu_policy_s"].sum()),
        "cpu_query_s_total": float(epochs_df["cpu_query_s"].sum()),
        "cpu_total_s": float(epochs_df["cpu_total_s"].sum()),
    }

    cfg_dict = {
        "policy": policy,
        "num_shards": num_shards,
        "c": c,
        "m": m,
        "budget_summaries": budget_summaries,
        "budget_bytes": budget_bytes,
        "num_epochs": num_epochs,
        "inserts_per_epoch": inserts_per_epoch,
        "queries_per_epoch": queries_per_epoch,
        "recent_window_w": recent_window_w,
        "stream_mode": stream_mode,
        "bursts": bursts,
        "sharding_mode": sharding_mode,
        "seed": seed,
        "exact_local_search": exact_local_search,
        "charge_setup_to_budget": charge_setup_to_budget,
    }
    return SimulationRunResult(
        run_id=run_id,
        config=cfg_dict,
        epochs_df=epochs_df,
        summary=summary,
        bus=bus,
        all_query_recall=concat_recall,
        all_query_recent_recall=concat_recent,
        all_query_missed_neighbors=concat_missed,
    )


def save_run_outputs(
    result: SimulationRunResult,
    out_dir: Path,
    repo_root: Path,
    config_raw_bytes: bytes,
    data_provenance_path: Optional[Path] = None,
) -> None:
    """Write `epochs.parquet`, `summary.json`, and `provenance.json` to `out_dir`."""
    out_dir.mkdir(parents=True, exist_ok=True)
    epochs_path = out_dir / "epochs.parquet"
    summary_path = out_dir / "summary.json"
    prov_path = out_dir / "provenance.json"

    result.epochs_df.to_parquet(epochs_path, index=False)
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(result.summary, f, indent=2)

    data_prov_sha = None
    if data_provenance_path is not None and data_provenance_path.exists():
        data_prov_sha = hashlib.sha256(data_provenance_path.read_bytes()).hexdigest()

    prov = {
        "run_id": result.run_id,
        "git_commit": get_git_commit(repo_root),
        "config_sha256": hashlib.sha256(config_raw_bytes).hexdigest(),
        "data_provenance_sha256": data_prov_sha,
        "package_versions": get_package_versions(),
        "cpu_info": get_cpu_info(),
        "config": result.config,
    }
    with prov_path.open("w", encoding="utf-8") as f:
        json.dump(prov, f, indent=2)


def _as_list(val: Any) -> List[Any]:
    if isinstance(val, list):
        return val
    return [val]


def run_from_config(
    config_path: Path,
    repo_root: Optional[Path] = None,
    data_dir: Optional[Path] = None,
    results_root: Optional[Path] = None,
) -> List[SimulationRunResult]:
    """Execute all runs defined in a YAML experiment config file."""
    if repo_root is None:
        repo_root = Path(__file__).resolve().parent.parent
    if data_dir is None:
        data_dir = repo_root / "data"
    if results_root is None:
        results_root = repo_root / "results"

    raw_bytes = config_path.read_bytes()
    cfg = yaml.safe_load(raw_bytes.decode("utf-8"))

    exp_name = str(cfg.get("experiment", config_path.stem))
    exp_out_dir = results_root / exp_name
    exp_out_dir.mkdir(parents=True, exist_ok=True)

    (
        init_vecs,
        init_ids,
        stream_vecs,
        stream_ids,
        test_q,
        test_qids,
        sample_q,
        sample_qids,
    ) = load_dataset(data_dir)

    # Optional dataset subsetting (e.g. for smoke runs)
    n_init = int(cfg.get("initial_vectors_limit", len(init_vecs)))
    n_stream = int(cfg.get("stream_vectors_limit", len(stream_vecs)))
    init_vecs = init_vecs[:n_init]
    init_ids = init_ids[:n_init]
    stream_vecs = stream_vecs[:n_stream]
    stream_ids = stream_ids[:n_stream]

    policies = _as_list(cfg.get("policies", ["recovered_neighbor"]))
    shards_list = _as_list(cfg.get("num_shards", [32]))
    m_list = _as_list(cfg.get("m", [2]))
    budgets_list = _as_list(cfg.get("budget_summaries", [4]))
    q_list = _as_list(cfg.get("queries_per_epoch", [200]))
    streams_list = _as_list(cfg.get("stream_modes", [cfg.get("stream_mode", "steady")]))
    seeds_list = _as_list(cfg.get("seeds", [cfg.get("seed", 42)]))

    num_epochs = int(cfg.get("num_epochs", 50))
    inserts_per_epoch = int(cfg.get("inserts_per_epoch", 1000))
    recent_window_w = int(cfg.get("recent_window_w", 2))
    c = int(cfg.get("c", SUMMARY_CENTERS))
    bursts = bool(cfg.get("bursts", False))
    sharding_mode = str(cfg.get("sharding_mode", "kmeans"))
    exact_local_search = bool(cfg.get("exact_local_search", False))
    charge_setup_to_budget = bool(cfg.get("charge_setup_to_budget", False))

    results: List[SimulationRunResult] = []
    for stream_mode in streams_list:
        for s_count in shards_list:
            for m_val in m_list:
                for b_sum in budgets_list:
                    for q_val in q_list:
                        for seed in seeds_list:
                            for pol in policies:
                                run_id = (
                                    f"{pol}_S{s_count}_m{m_val}_B{b_sum}_Q{q_val}"
                                    f"_{stream_mode}_seed{seed}"
                                )
                                res = run_single_simulation(
                                    initial_vectors=init_vecs,
                                    initial_ids=init_ids,
                                    stream_vectors=stream_vecs,
                                    stream_ids=stream_ids,
                                    test_queries=test_q,
                                    test_query_ids=test_qids,
                                    sample_queries=sample_q,
                                    sample_query_ids=sample_qids,
                                    policy=str(pol),
                                    num_shards=int(s_count),
                                    c=c,
                                    m=int(m_val),
                                    budget_summaries=int(b_sum),
                                    num_epochs=num_epochs,
                                    inserts_per_epoch=inserts_per_epoch,
                                    queries_per_epoch=int(q_val),
                                    recent_window_w=recent_window_w,
                                    stream_mode=str(stream_mode),
                                    bursts=bursts,
                                    sharding_mode=sharding_mode,
                                    seed=int(seed),
                                    exact_local_search=exact_local_search,
                                    charge_setup_to_budget=charge_setup_to_budget,
                                    run_id=run_id,
                                )
                                run_dir = exp_out_dir / run_id
                                save_run_outputs(
                                    result=res,
                                    out_dir=run_dir,
                                    repo_root=repo_root,
                                    config_raw_bytes=raw_bytes,
                                    data_provenance_path=data_dir / "PROVENANCE.json",
                                )
                                results.append(res)

    # Also write an experiment-level summary table and paired bootstrap comparisons if recovered_neighbor is present
    if results:
        all_summaries = [r.summary for r in results]
        pd.DataFrame(all_summaries).to_parquet(exp_out_dir / "experiment_summary.parquet", index=False)

        bootstrap_comparisons: List[Dict[str, Any]] = []
        rn_runs = {
            (
                r.config["num_shards"],
                r.config["m"],
                r.config["budget_summaries"],
                r.config["queries_per_epoch"],
                r.config["stream_mode"],
            ): []
            for r in results
            if r.config["policy"] == "recovered_neighbor"
        }
        for r in results:
            key = (
                r.config["num_shards"],
                r.config["m"],
                r.config["budget_summaries"],
                r.config["queries_per_epoch"],
                r.config["stream_mode"],
            )
            if r.config["policy"] == "recovered_neighbor" and key in rn_runs:
                rn_runs[key].append(r)

        for key, rn_list in rn_runs.items():
            rn_list_sorted = sorted(rn_list, key=lambda x: x.config["seed"])
            rn_recent = np.concatenate([x.all_query_recent_recall for x in rn_list_sorted])
            rn_overall = np.concatenate([x.all_query_recall for x in rn_list_sorted])
            other_policies = sorted(
                {
                    r.config["policy"]
                    for r in results
                    if (
                        r.config["num_shards"],
                        r.config["m"],
                        r.config["budget_summaries"],
                        r.config["queries_per_epoch"],
                        r.config["stream_mode"],
                    )
                    == key
                    and r.config["policy"] != "recovered_neighbor"
                }
            )
            for op in other_policies:
                op_list = sorted(
                    [
                        r
                        for r in results
                        if (
                            r.config["num_shards"],
                            r.config["m"],
                            r.config["budget_summaries"],
                            r.config["queries_per_epoch"],
                            r.config["stream_mode"],
                        )
                        == key
                        and r.config["policy"] == op
                    ],
                    key=lambda x: x.config["seed"],
                )
                if len(op_list) == len(rn_list_sorted):
                    op_recent = np.concatenate([x.all_query_recent_recall for x in op_list])
                    op_overall = np.concatenate([x.all_query_recall for x in op_list])
                    ci_rec = paired_bootstrap_ci(rn_recent, op_recent)
                    ci_ov = paired_bootstrap_ci(rn_overall, op_overall)
                    bootstrap_comparisons.append(
                        {
                            "num_shards": key[0],
                            "m": key[1],
                            "budget_summaries": key[2],
                            "queries_per_epoch": key[3],
                            "stream_mode": key[4],
                            "baseline_policy": op,
                            "recent_recall_diff_mean": ci_rec["mean_diff"],
                            "recent_recall_ci95_low": ci_rec["ci_low"],
                            "recent_recall_ci95_high": ci_rec["ci_high"],
                            "overall_recall_diff_mean": ci_ov["mean_diff"],
                            "overall_recall_ci95_low": ci_ov["ci_low"],
                            "overall_recall_ci95_high": ci_ov["ci_high"],
                            "n_paired_queries": ci_rec["n"],
                        }
                    )

        with (exp_out_dir / "experiment_summary.json").open("w", encoding="utf-8") as f:
            json.dump(
                {
                    "experiment": exp_name,
                    "num_runs": len(results),
                    "runs": all_summaries,
                    "paired_bootstrap_vs_recovered_neighbor": bootstrap_comparisons,
                },
                f,
                indent=2,
            )

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Run DVS byte-counted shard simulation experiments.")
    parser.add_argument("--config", type=Path, required=True, help="Path to YAML experiment config")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "data",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "results",
    )
    args = parser.parse_args()
    run_from_config(
        config_path=args.config,
        data_dir=args.data_dir,
        results_root=args.results_dir,
    )


if __name__ == "__main__":
    main()
