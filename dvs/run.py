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
import multiprocessing as mp
import os
import platform
import subprocess
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import faiss
import numpy as np
import pandas as pd
import yaml

from dvs.bus import (
    ByteBus,
    DIM,
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
from dvs.policies import (
    REFERENCE_POLICIES,
    REFRESH_POLICIES,
    STARVATION_CAP_EPOCHS,
    PolicyController,
)
from dvs.shards import ShardRouterInitializer, build_initial_shards
from dvs.streams import (
    build_epoch_schedule,
    compute_new_topic_query_stats,
    partition_new_topic_stream,
)


def set_single_thread_env() -> None:
    """Pin FAISS, OpenMP, MKL, and OpenBLAS to 1 thread per worker process."""
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ["MKL_NUM_THREADS"] = "1"
    os.environ["OPENBLAS_NUM_THREADS"] = "1"
    os.environ["NUMEXPR_NUM_THREADS"] = "1"
    faiss.omp_set_num_threads(1)
    try:
        import threadpoolctl

        threadpoolctl.threadpool_limits(limits=1)
    except Exception:
        pass


@dataclass
class CachedGroundTruth:
    """Precomputed exact ground truth per epoch for a (stream, placement, seed, Q schedule)."""

    cache_key: str
    epoch_gt_ids: List[np.ndarray]
    epoch_gt_scores: List[np.ndarray]
    epoch_gt_epochs: List[np.ndarray]
    epoch_gt_shards: List[np.ndarray]
    epoch_total_indexed: List[int]
    epoch_cpu_gt_s: List[float]
    init_centroids: Optional[np.ndarray] = None
    new_topic_stats: Optional[Dict[str, Any]] = None


def make_gt_cache_key(
    stream_mode: str,
    placement_mode: str,
    sharding_mode: str,
    num_shards: int,
    num_epochs: int,
    inserts_per_epoch: int,
    queries_per_epoch: int,
    bursts: bool,
    seed: int,
    n_init: int,
    n_stream: int,
) -> str:
    """Return a deterministic string key identifying the ground-truth trajectory."""
    return (
        f"gt_{stream_mode}_{placement_mode}_{sharding_mode}"
        f"_S{int(num_shards)}_E{int(num_epochs)}_I{int(inserts_per_epoch)}"
        f"_Q{int(queries_per_epoch)}_bursts{int(bool(bursts))}"
        f"_init{int(n_init)}_stream{int(n_stream)}_seed{int(seed)}"
    )


def build_ground_truth_cache(
    initial_vectors: np.ndarray,
    initial_ids: np.ndarray,
    stream_vectors: np.ndarray,
    stream_ids: np.ndarray,
    test_queries: np.ndarray,
    test_query_ids: np.ndarray,
    sample_queries: np.ndarray,
    num_shards: int = 32,
    num_epochs: int = 50,
    inserts_per_epoch: int = 1000,
    queries_per_epoch: int = 200,
    stream_mode: str = "steady",
    bursts: bool = False,
    sharding_mode: str = "kmeans",
    placement_mode: str = "nearest_initial",
    seed: int = 42,
    kmeans_niter: int = 20,
    cache_key: Optional[str] = None,
) -> CachedGroundTruth:
    """Compute exact ground truth across all epochs for a (stream, placement, seed, Q schedule)."""
    if cache_key is None:
        cache_key = make_gt_cache_key(
            stream_mode=stream_mode,
            placement_mode=placement_mode,
            sharding_mode=sharding_mode,
            num_shards=num_shards,
            num_epochs=num_epochs,
            inserts_per_epoch=inserts_per_epoch,
            queries_per_epoch=queries_per_epoch,
            bursts=bursts,
            seed=seed,
            n_init=len(initial_vectors),
            n_stream=len(stream_vectors),
        )

    new_topic_stats: Optional[Dict[str, Any]] = None
    if stream_mode == "new_topic":
        initial_vectors, initial_ids, stream_vectors, stream_ids = partition_new_topic_stream(
            initial_vectors=initial_vectors,
            initial_ids=initial_ids,
            stream_vectors=stream_vectors,
            stream_ids=stream_ids,
            seed=seed,
        )
        new_topic_stats = compute_new_topic_query_stats(
            initial_vectors=initial_vectors,
            stream_vectors=stream_vectors,
            test_queries=test_queries,
            k=TOP_K,
        )

    vecs = np.ascontiguousarray(initial_vectors, dtype=np.float32)
    ids = np.ascontiguousarray(initial_ids, dtype=np.int64)

    km = faiss.Kmeans(
        d=DIM,
        k=int(num_shards),
        niter=int(kmeans_niter),
        seed=int(seed),
        spherical=True,
        verbose=False,
        min_points_per_centroid=1,
        max_points_per_centroid=max(len(vecs), 100000),
    )
    km.train(vecs)
    init_centroids = np.ascontiguousarray(km.centroids, dtype=np.float32)

    router = ShardRouterInitializer(
        num_shards=num_shards,
        initial_centroids=init_centroids,
        sharding_mode=sharding_mode,
        placement_mode=placement_mode,
    )
    init_shard_assignments = router.assign_shards(vecs, ids, epoch=-1)

    gt = GroundTruthIndex()
    gt.add(
        vectors=vecs,
        doc_ids=ids,
        shard_ids=init_shard_assignments,
        epoch=-1,
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

    epoch_gt_ids: List[np.ndarray] = []
    epoch_gt_scores: List[np.ndarray] = []
    epoch_gt_epochs: List[np.ndarray] = []
    epoch_gt_shards: List[np.ndarray] = []
    epoch_total_indexed: List[int] = []
    epoch_cpu_gt_s: List[float] = []

    for batch in schedule:
        epoch = batch.epoch
        new_shard_ids: Optional[np.ndarray] = None
        if len(batch.insert_vectors) > 0:
            new_shard_ids = router.assign_shards(
                batch.insert_vectors,
                batch.insert_ids,
                epoch=epoch,
            )
        t0 = time.process_time()
        if len(batch.insert_vectors) > 0 and new_shard_ids is not None:
            gt.add(
                vectors=batch.insert_vectors,
                doc_ids=batch.insert_ids,
                shard_ids=new_shard_ids,
                epoch=epoch,
            )
        gt_ids, gt_scores, gt_epochs, gt_shards = gt.search(batch.test_queries, k=TOP_K)
        t1 = time.process_time()

        epoch_gt_ids.append(gt_ids)
        epoch_gt_scores.append(gt_scores)
        epoch_gt_epochs.append(gt_epochs)
        epoch_gt_shards.append(gt_shards)
        epoch_total_indexed.append(int(gt.size))
        epoch_cpu_gt_s.append(max(0.0, t1 - t0))

    return CachedGroundTruth(
        cache_key=cache_key,
        epoch_gt_ids=epoch_gt_ids,
        epoch_gt_scores=epoch_gt_scores,
        epoch_gt_epochs=epoch_gt_epochs,
        epoch_gt_shards=epoch_gt_shards,
        epoch_total_indexed=epoch_total_indexed,
        epoch_cpu_gt_s=epoch_cpu_gt_s,
        init_centroids=init_centroids,
        new_topic_stats=new_topic_stats,
    )


def save_ground_truth_cache(gt_cache: CachedGroundTruth, path: Path) -> None:
    """Serialize `CachedGroundTruth` to an `.npz` file on disk."""
    path.parent.mkdir(parents=True, exist_ok=True)
    nt_json = json.dumps(gt_cache.new_topic_stats) if gt_cache.new_topic_stats is not None else ""
    init_c = (
        gt_cache.init_centroids
        if gt_cache.init_centroids is not None
        else np.empty((0, DIM), dtype=np.float32)
    )
    np.savez_compressed(
        path,
        cache_key=np.array(gt_cache.cache_key),
        epoch_gt_ids=np.stack(gt_cache.epoch_gt_ids, axis=0),
        epoch_gt_scores=np.stack(gt_cache.epoch_gt_scores, axis=0),
        epoch_gt_epochs=np.stack(gt_cache.epoch_gt_epochs, axis=0),
        epoch_gt_shards=np.stack(gt_cache.epoch_gt_shards, axis=0),
        epoch_total_indexed=np.asarray(gt_cache.epoch_total_indexed, dtype=np.int64),
        epoch_cpu_gt_s=np.asarray(gt_cache.epoch_cpu_gt_s, dtype=np.float64),
        init_centroids=np.ascontiguousarray(init_c, dtype=np.float32),
        new_topic_stats_json=np.array(nt_json),
    )


def load_ground_truth_cache(path: Path) -> CachedGroundTruth:
    """Deserialize `CachedGroundTruth` from an `.npz` file on disk."""
    with np.load(path, allow_pickle=False) as data:
        cache_key = str(data["cache_key"].item())
        ids_arr = data["epoch_gt_ids"]
        scores_arr = data["epoch_gt_scores"]
        epochs_arr = data["epoch_gt_epochs"]
        shards_arr = data["epoch_gt_shards"]
        total_idx = data["epoch_total_indexed"].astype(np.int64).tolist()
        cpu_gt = data["epoch_cpu_gt_s"].astype(np.float64).tolist()
        init_c_raw = data["init_centroids"] if "init_centroids" in data else np.empty((0, DIM), dtype=np.float32)
        init_c = init_c_raw.copy() if len(init_c_raw) > 0 else None
        nt_json = str(data["new_topic_stats_json"].item()) if "new_topic_stats_json" in data else ""
        nt_stats = json.loads(nt_json) if nt_json else None
        num_ep = ids_arr.shape[0]
        return CachedGroundTruth(
            cache_key=cache_key,
            epoch_gt_ids=[ids_arr[e].copy() for e in range(num_ep)],
            epoch_gt_scores=[scores_arr[e].copy() for e in range(num_ep)],
            epoch_gt_epochs=[epochs_arr[e].copy() for e in range(num_ep)],
            epoch_gt_shards=[shards_arr[e].copy() for e in range(num_ep)],
            epoch_total_indexed=[int(x) for x in total_idx],
            epoch_cpu_gt_s=[float(x) for x in cpu_gt],
            init_centroids=init_c,
            new_topic_stats=nt_stats,
        )


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
    placement_mode: str = "nearest_initial",
    seed: int = 42,
    exact_local_search: bool = False,
    charge_setup_to_budget: bool = False,
    aging_mode: str = "starvation_cap",
    starvation_cap_epochs: Optional[int] = None,
    m_hnsw: int = 16,
    ef_construction: int = 200,
    ef_search: int = 100,
    run_id: str = "run",
    cached_gt: Optional[CachedGroundTruth] = None,
    precomputed_new_topic_partition: Optional[Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]] = None,
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

    new_topic_stats: Optional[Dict[str, Any]] = None
    if stream_mode == "new_topic":
        if precomputed_new_topic_partition is not None:
            initial_vectors, initial_ids, stream_vectors, stream_ids = precomputed_new_topic_partition
        else:
            initial_vectors, initial_ids, stream_vectors, stream_ids = partition_new_topic_stream(
                initial_vectors=initial_vectors,
                initial_ids=initial_ids,
                stream_vectors=stream_vectors,
                stream_ids=stream_ids,
                seed=seed,
            )
        if cached_gt is not None and cached_gt.new_topic_stats is not None:
            new_topic_stats = cached_gt.new_topic_stats
        else:
            new_topic_stats = compute_new_topic_query_stats(
                initial_vectors=initial_vectors,
                stream_vectors=stream_vectors,
                test_queries=test_queries,
                k=TOP_K,
            )

    budget_bytes = (
        int(budget_bytes_override)
        if budget_bytes_override is not None
        else int(budget_summaries) * SUMMARY_BYTES
    )
    m_routed = int(num_shards) if policy == "all_shards" else int(m)

    bus = ByteBus(num_shards=num_shards, num_epochs=num_epochs)

    # 1. Build initial shards and ground-truth index at setup (epoch -1)
    max_elems = len(initial_vectors) + max(num_epochs * inserts_per_epoch, len(stream_vectors)) + 1024
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
        placement_mode=placement_mode,
        max_elements_per_shard=max_elems,
        precomputed_centroids=cached_gt.init_centroids if cached_gt is not None else None,
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

    gt: Optional[GroundTruthIndex] = None
    if cached_gt is None:
        gt = GroundTruthIndex()
        init_shard_assignments = router.assign_shards(initial_vectors, initial_ids, epoch=-1)
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
        aging_mode=aging_mode,
        starvation_cap_epochs=starvation_cap_epochs,
    )

    effective_cap = controller.effective_starvation_cap(num_sample_queries=len(sample_queries))
    starvation_cap_mode = "arithmetic" if starvation_cap_epochs is None else "fixed"

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

        # Step A: Insert epoch's new vectors into shards (method insert CPU)
        new_shard_ids: Optional[np.ndarray] = None
        with timer.measure("insert"):
            if len(batch.insert_vectors) > 0:
                new_shard_ids = router.assign_shards(
                    batch.insert_vectors,
                    batch.insert_ids,
                    epoch=epoch,
                )
                for s_id in range(num_shards):
                    mask = new_shard_ids == s_id
                    if np.any(mask):
                        shards[s_id].add_vectors(
                            batch.insert_vectors[mask],
                            batch.insert_ids[mask],
                        )
                controller.record_new_arrivals(
                    vectors=batch.insert_vectors,
                    doc_ids=batch.insert_ids,
                    shard_ids=new_shard_ids,
                )

        # Ground-truth index update and exact search (timed separately, excluded from method CPU)
        if cached_gt is not None:
            gt_ids = cached_gt.epoch_gt_ids[epoch]
            gt_epochs = cached_gt.epoch_gt_epochs[epoch]
            gt_shards = cached_gt.epoch_gt_shards[epoch]
            total_indexed_vectors = int(cached_gt.epoch_total_indexed[epoch])
            timer.times["ground_truth"] += float(cached_gt.epoch_cpu_gt_s[epoch])
        else:
            assert gt is not None
            with timer.measure("ground_truth"):
                if len(batch.insert_vectors) > 0 and new_shard_ids is not None:
                    gt.add(
                        vectors=batch.insert_vectors,
                        doc_ids=batch.insert_ids,
                        shard_ids=new_shard_ids,
                        epoch=epoch,
                    )
                gt_ids, _, gt_epochs, gt_shards = gt.search(batch.test_queries, k=TOP_K)
            total_indexed_vectors = gt.size

        # Step B: Update candidate summaries on shards (timed under summary component)
        with timer.measure("summary"):
            if policy in REFRESH_POLICIES or policy == "always_fresh":
                for shard in shards:
                    shard.update_candidate_summary(epoch)

        # Compute true recovered neighbors per shard BEFORE summaries are refreshed in this epoch
        true_rec_per_shard: Optional[np.ndarray] = None
        new_vecs_per_shard: Optional[np.ndarray] = None
        if policy in REFRESH_POLICIES:
            new_vecs_per_shard = np.array([s.num_new_vectors for s in shards], dtype=np.float64)
            with timer.measure("ground_truth"):
                true_rec_per_shard = compute_true_recovered_neighbors_per_shard(
                    shards=shards,
                    coordinator=coordinator,
                    test_queries=batch.test_queries,
                    gt_ids=gt_ids,
                    gt_shards=gt_shards,
                )

        # For non-RN refresh rules, compute RN estimates outside the policy timer for Spearman logging
        # so that RN's sample-query probing CPU time is charged only to recovered_neighbor.
        rn_estimates_outside_timer = None
        if policy in REFRESH_POLICIES and policy != "recovered_neighbor":
            rn_estimates_outside_timer = controller.compute_recovered_neighbor_raw_scores(
                shards=shards,
                coordinator=coordinator,
                sample_queries=sample_queries,
            )

        # Step C: Execute policy refresh or adaptation under budget B
        with timer.measure("policy"):
            decision = controller.step_epoch(
                epoch=epoch,
                shards=shards,
                coordinator=coordinator,
                bus=bus,
                sample_queries=sample_queries,
                epoch_test_queries=batch.test_queries if policy == "extra_search" else None,
                rn_estimates=rn_estimates_outside_timer,
            )

        spearman_rho = compute_spearman_correlation(
            decision.estimated_recovered_neighbors,
            true_rec_per_shard,
        )
        spearman_rho_new_vecs = compute_spearman_correlation(
            new_vecs_per_shard,
            true_rec_per_shard,
        )

        # For oracle_routing reference, determine ground-truth top-m shards outside method query timer
        oracle_top_m: Optional[np.ndarray] = None
        if policy == "oracle_routing":
            with timer.measure("ground_truth"):
                oracle_top_m = coordinator.route_queries_oracle(gt_shards, m=m_routed)

        # Step D: Route and execute epoch's test queries
        with timer.measure("query"):
            ret_ids, _, searched_mask = coordinator.execute_queries(
                epoch=epoch,
                queries=batch.test_queries,
                shards=shards,
                m=m_routed,
                extra_shards_per_query=decision.extra_shards_per_query,
                record_popularity=True,
                override_top_m_shards=oracle_top_m,
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
        if policy not in REFERENCE_POLICIES and budget_spent > budget_bytes:
            raise AssertionError(
                f"Policy '{policy}' exceeded budget in epoch {epoch}: spent {budget_spent} > B={budget_bytes}"
            )

        row = {
            "run_id": run_id,
            "epoch": epoch,
            "policy": policy,
            "seed": seed,
            "stream_mode": stream_mode,
            "placement_mode": placement_mode,
            "bursts": bursts,
            "sharding_mode": sharding_mode,
            "aging_mode": aging_mode,
            "starvation_cap_mode": starvation_cap_mode,
            "starvation_cap_epochs": int(effective_cap),
            "num_shards": num_shards,
            "m": m,
            "m_routed": m_routed,
            "budget_summaries": budget_summaries,
            "budget_bytes": budget_bytes,
            "queries_per_epoch": len(batch.test_queries),
            "inserts_this_epoch": len(batch.insert_vectors),
            "total_indexed_vectors": total_indexed_vectors,
            "recall_at_10": eval_res.recall_at_10,
            "recent_recall_at_10": eval_res.recent_recall_at_10,
            "num_queries_with_recent_neighbors": eval_res.num_queries_with_recent_neighbors,
            "num_queries_without_recent_neighbors": eval_res.num_queries_without_recent_neighbors,
            "missed_neighbors_mean": eval_res.missed_neighbors_mean,
            "missed_neighbors_total": eval_res.missed_neighbors_total,
            "spearman_rho": eval_res.spearman_rho,
            "spearman_rho_new_vecs": spearman_rho_new_vecs,
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
            "num_forced_by_cap": int(decision.num_forced_by_cap),
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
            "cpu_ground_truth_s": timer.times["ground_truth"],
        }
        epoch_rows.append(row)

    epochs_df = pd.DataFrame(epoch_rows)
    concat_recall = np.concatenate(all_q_recall) if all_q_recall else np.empty(0, dtype=np.float64)
    concat_recent = np.concatenate(all_q_recent) if all_q_recent else np.empty(0, dtype=np.float64)
    concat_missed = np.concatenate(all_q_missed) if all_q_missed else np.empty(0, dtype=np.int64)

    valid_recent = concat_recent[np.isfinite(concat_recent)]
    spearman_n_defined = int(epochs_df["spearman_rho"].notna().sum())
    spearman_rho_mean = (
        float(epochs_df["spearman_rho"].mean(skipna=True))
        if spearman_n_defined > 0
        else float("nan")
    )
    spearman_new_vecs_n_defined = int(epochs_df["spearman_rho_new_vecs"].notna().sum())
    spearman_new_vecs_rho_mean = (
        float(epochs_df["spearman_rho_new_vecs"].mean(skipna=True))
        if spearman_new_vecs_n_defined > 0
        else float("nan")
    )
    summary = {
        "run_id": run_id,
        "policy": policy,
        "seed": seed,
        "stream_mode": stream_mode,
        "placement_mode": placement_mode,
        "bursts": bursts,
        "sharding_mode": sharding_mode,
        "aging_mode": aging_mode,
        "starvation_cap_mode": starvation_cap_mode,
        "starvation_cap_epochs": int(effective_cap),
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
        "initial_set_size": int(len(initial_vectors)),
        "stream_set_size": int(len(stream_vectors)),
        "queries_with_most_neighbors_in_stream": (
            int(new_topic_stats["queries_with_most_neighbors_in_stream"])
            if new_topic_stats is not None
            else None
        ),
        "queries_with_at_least_half_neighbors_in_stream": (
            int(new_topic_stats["queries_with_at_least_half_neighbors_in_stream"])
            if new_topic_stats is not None
            else None
        ),
        "mean_stream_neighbors_in_top_10": (
            float(new_topic_stats["mean_stream_neighbors_in_top_k"])
            if new_topic_stats is not None
            else None
        ),
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
        "spearman_rho_mean": spearman_rho_mean,
        "spearman_n_defined": spearman_n_defined,
        "spearman_new_vecs_rho_mean": spearman_new_vecs_rho_mean,
        "spearman_new_vecs_n_defined": spearman_new_vecs_n_defined,
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
        "total_forced_by_cap": int(epochs_df["num_forced_by_cap"].sum()),
        "total_extra_searches": int(epochs_df["num_extra_searches"].sum()),
        "total_pushed_vectors": int(epochs_df["num_pushed_vectors"].sum()),
        "total_lift_in_flips": int(epochs_df["total_lift_in_flips"].sum()),
        "total_dropout_flips": int(epochs_df["total_dropout_flips"].sum()),
        "cpu_insert_s_total": float(epochs_df["cpu_insert_s"].sum()),
        "cpu_summary_s_total": float(epochs_df["cpu_summary_s"].sum()),
        "cpu_policy_s_total": float(epochs_df["cpu_policy_s"].sum()),
        "cpu_query_s_total": float(epochs_df["cpu_query_s"].sum()),
        "cpu_total_s": float(epochs_df["cpu_total_s"].sum()),
        "cpu_ground_truth_s_total": float(epochs_df["cpu_ground_truth_s"].sum()),
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
        "placement_mode": placement_mode,
        "bursts": bursts,
        "sharding_mode": sharding_mode,
        "aging_mode": aging_mode,
        "starvation_cap_mode": starvation_cap_mode,
        "starvation_cap_epochs": int(effective_cap),
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
    queries_path = out_dir / "queries.npz"

    result.epochs_df.to_parquet(epochs_path, index=False)
    np.savez_compressed(
        queries_path,
        recall_at_10=result.all_query_recall.astype(np.float32),
        recent_recall_at_10=result.all_query_recent_recall.astype(np.float32),
        missed_neighbors=result.all_query_missed_neighbors.astype(np.int16),
    )
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


_WORKER_STATE: Dict[str, Any] = {}


def _init_pool_worker(
    data_dir_str: str,
    n_init: int,
    n_stream: int,
    repo_root_str: str,
    config_sha256: str,
) -> None:
    """Initialize a worker process: pin threads to 1 and load dataset into process memory."""
    set_single_thread_env()
    data_dir = Path(data_dir_str)
    repo_root = Path(repo_root_str)
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
    data_prov_path = data_dir / "PROVENANCE.json"
    data_prov_sha = (
        hashlib.sha256(data_prov_path.read_bytes()).hexdigest()
        if data_prov_path.exists()
        else None
    )
    _WORKER_STATE.clear()
    _WORKER_STATE["init_vecs"] = init_vecs[:n_init]
    _WORKER_STATE["init_ids"] = init_ids[:n_init]
    _WORKER_STATE["stream_vecs"] = stream_vecs[:n_stream]
    _WORKER_STATE["stream_ids"] = stream_ids[:n_stream]
    _WORKER_STATE["test_q"] = test_q
    _WORKER_STATE["test_qids"] = test_qids
    _WORKER_STATE["sample_q"] = sample_q
    _WORKER_STATE["sample_qids"] = sample_qids
    _WORKER_STATE["git_commit"] = get_git_commit(repo_root)
    _WORKER_STATE["package_versions"] = get_package_versions()
    _WORKER_STATE["cpu_info"] = get_cpu_info()
    _WORKER_STATE["data_prov_sha"] = data_prov_sha
    _WORKER_STATE["config_sha256"] = config_sha256
    _WORKER_STATE["gt_mem_cache"] = {}
    _WORKER_STATE["nt_partition_cache"] = {}


def _worker_build_gt_task(task: Dict[str, Any]) -> Dict[str, Any]:
    """Worker entrypoint to build and persist one `CachedGroundTruth` file."""
    set_single_thread_env()
    gt_path = Path(task["gt_cache_path"])
    if not gt_path.exists():
        gt_cache = build_ground_truth_cache(
            initial_vectors=_WORKER_STATE["init_vecs"],
            initial_ids=_WORKER_STATE["init_ids"],
            stream_vectors=_WORKER_STATE["stream_vecs"],
            stream_ids=_WORKER_STATE["stream_ids"],
            test_queries=_WORKER_STATE["test_q"],
            test_query_ids=_WORKER_STATE["test_qids"],
            sample_queries=_WORKER_STATE["sample_q"],
            num_shards=int(task["num_shards"]),
            num_epochs=int(task["num_epochs"]),
            inserts_per_epoch=int(task["inserts_per_epoch"]),
            queries_per_epoch=int(task["queries_per_epoch"]),
            stream_mode=str(task["stream_mode"]),
            bursts=bool(task["bursts"]),
            sharding_mode=str(task["sharding_mode"]),
            placement_mode=str(task["placement_mode"]),
            seed=int(task["seed"]),
            cache_key=str(task["cache_key"]),
        )
        tmp_path = gt_path.with_suffix(f".tmp.{os.getpid()}.npz")
        save_ground_truth_cache(gt_cache, tmp_path)
        tmp_path.replace(gt_path)
    return {"cache_key": task["cache_key"], "gt_cache_path": str(gt_path)}


def _worker_run_sim_task(task: Dict[str, Any]) -> Dict[str, Any]:
    """Worker entrypoint to execute a single simulation run and write its output artifacts."""
    set_single_thread_env()
    run_id = str(task["run_id"])
    run_dir = Path(task["run_dir"])
    try:
        cached_gt: Optional[CachedGroundTruth] = None
        gt_cache_path_str = task.get("gt_cache_path")
        if gt_cache_path_str is not None:
            mem_cache: Dict[str, CachedGroundTruth] = _WORKER_STATE["gt_mem_cache"]
            if gt_cache_path_str not in mem_cache:
                # Keep at most 2 ground-truth objects in worker RAM to bound memory
                if len(mem_cache) >= 2:
                    mem_cache.clear()
                mem_cache[gt_cache_path_str] = load_ground_truth_cache(Path(gt_cache_path_str))
            cached_gt = mem_cache[gt_cache_path_str]

        nt_part = None
        if str(task["stream_mode"]) == "new_topic":
            seed_int = int(task["seed"])
            nt_cache: Dict[int, Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]] = _WORKER_STATE[
                "nt_partition_cache"
            ]
            if seed_int not in nt_cache:
                if len(nt_cache) >= 2:
                    nt_cache.clear()
                nt_cache[seed_int] = partition_new_topic_stream(
                    initial_vectors=_WORKER_STATE["init_vecs"],
                    initial_ids=_WORKER_STATE["init_ids"],
                    stream_vectors=_WORKER_STATE["stream_vecs"],
                    stream_ids=_WORKER_STATE["stream_ids"],
                    seed=seed_int,
                )
            nt_part = nt_cache[seed_int]

        res = run_single_simulation(
            initial_vectors=_WORKER_STATE["init_vecs"],
            initial_ids=_WORKER_STATE["init_ids"],
            stream_vectors=_WORKER_STATE["stream_vecs"],
            stream_ids=_WORKER_STATE["stream_ids"],
            test_queries=_WORKER_STATE["test_q"],
            test_query_ids=_WORKER_STATE["test_qids"],
            sample_queries=_WORKER_STATE["sample_q"],
            sample_query_ids=_WORKER_STATE["sample_qids"],
            policy=str(task["policy"]),
            num_shards=int(task["num_shards"]),
            c=int(task["c"]),
            m=int(task["m"]),
            budget_summaries=int(task["budget_summaries"]),
            num_epochs=int(task["num_epochs"]),
            inserts_per_epoch=int(task["inserts_per_epoch"]),
            queries_per_epoch=int(task["queries_per_epoch"]),
            recent_window_w=int(task["recent_window_w"]),
            stream_mode=str(task["stream_mode"]),
            bursts=bool(task["bursts"]),
            sharding_mode=str(task["sharding_mode"]),
            placement_mode=str(task["placement_mode"]),
            seed=int(task["seed"]),
            exact_local_search=bool(task["exact_local_search"]),
            charge_setup_to_budget=bool(task["charge_setup_to_budget"]),
            aging_mode=str(task["aging_mode"]),
            starvation_cap_epochs=task["starvation_cap_epochs"],
            run_id=run_id,
            cached_gt=cached_gt,
            precomputed_new_topic_partition=nt_part,
        )

        run_dir.mkdir(parents=True, exist_ok=True)
        res.epochs_df.to_parquet(run_dir / "epochs.parquet", index=False)
        np.savez_compressed(
            run_dir / "queries.npz",
            recall_at_10=res.all_query_recall.astype(np.float32),
            recent_recall_at_10=res.all_query_recent_recall.astype(np.float32),
            missed_neighbors=res.all_query_missed_neighbors.astype(np.int16),
        )
        with (run_dir / "summary.json").open("w", encoding="utf-8") as f:
            json.dump(res.summary, f, indent=2)

        prov = {
            "run_id": res.run_id,
            "git_commit": _WORKER_STATE["git_commit"],
            "config_sha256": _WORKER_STATE["config_sha256"],
            "data_provenance_sha256": _WORKER_STATE["data_prov_sha"],
            "package_versions": _WORKER_STATE["package_versions"],
            "cpu_info": _WORKER_STATE["cpu_info"],
            "config": res.config,
        }
        with (run_dir / "provenance.json").open("w", encoding="utf-8") as f:
            json.dump(prov, f, indent=2)

        return {
            "status": "ok",
            "run_id": run_id,
            "run_dir": str(run_dir),
            "config": res.config,
            "summary": res.summary,
        }
    except Exception:
        return {
            "status": "error",
            "run_id": run_id,
            "run_dir": str(run_dir),
            "error": traceback.format_exc(),
            "task": task,
        }


def _load_saved_run_result(run_dir: Path, config: Dict[str, Any], summary: Dict[str, Any]) -> SimulationRunResult:
    """Reconstruct `SimulationRunResult` from saved artifacts on disk."""
    epochs_df = pd.read_parquet(run_dir / "epochs.parquet")
    with np.load(run_dir / "queries.npz", allow_pickle=False) as qz:
        q_recall = qz["recall_at_10"].astype(np.float64)
        q_recent = qz["recent_recall_at_10"].astype(np.float64)
        q_missed = qz["missed_neighbors"].astype(np.int64)
    return SimulationRunResult(
        run_id=str(summary["run_id"]),
        config=config,
        epochs_df=epochs_df,
        summary=summary,
        bus=ByteBus(num_shards=int(config["num_shards"]), num_epochs=int(config["num_epochs"])),
        all_query_recall=q_recall,
        all_query_recent_recall=q_recent,
        all_query_missed_neighbors=q_missed,
    )


def run_from_config(
    config_path: Path,
    repo_root: Optional[Path] = None,
    data_dir: Optional[Path] = None,
    results_root: Optional[Path] = None,
    num_workers: int = 8,
    use_gt_cache: bool = True,
) -> List[SimulationRunResult]:
    """Execute all runs defined in a YAML experiment config file."""
    set_single_thread_env()
    if repo_root is None:
        repo_root = Path(__file__).resolve().parent.parent
    if data_dir is None:
        data_dir = repo_root / "data"
    if results_root is None:
        results_root = repo_root / "results"

    raw_bytes = config_path.read_bytes()
    config_sha256 = hashlib.sha256(raw_bytes).hexdigest()
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
    c = int(cfg.get("c", SUMMARY_CENTERS))
    exact_local_search = bool(cfg.get("exact_local_search", False))
    raw_cap = cfg.get("starvation_cap_epochs", None)
    if raw_cap is None or str(raw_cap).lower() in ("arithmetic", "auto", "none", "null"):
        default_cap_epochs: Optional[int] = None
    else:
        default_cap_epochs = int(raw_cap)

    # Support either explicit single-factor condition dicts or sweep lists over condition flags
    raw_conditions = cfg.get("conditions")
    if isinstance(raw_conditions, list) and len(raw_conditions) > 0:
        condition_specs: List[Dict[str, Any]] = []
        for cond in raw_conditions:
            cond_cap = cond.get("starvation_cap_epochs", default_cap_epochs)
            if cond_cap is None or str(cond_cap).lower() in ("arithmetic", "auto", "none", "null"):
                parsed_cap: Optional[int] = None
            else:
                parsed_cap = int(cond_cap)
            condition_specs.append(
                {
                    "recent_window_w": int(cond.get("recent_window_w", cfg.get("recent_window_w", 2))),
                    "bursts": bool(cond.get("bursts", cfg.get("bursts", False))),
                    "sharding_mode": str(cond.get("sharding_mode", cfg.get("sharding_mode", "kmeans"))),
                    "placement_mode": str(
                        cond.get("placement_mode", cfg.get("placement_mode", "nearest_initial"))
                    ),
                    "charge_setup_to_budget": bool(
                        cond.get("charge_setup_to_budget", cfg.get("charge_setup_to_budget", False))
                    ),
                    "aging_mode": str(cond.get("aging_mode", cfg.get("aging_mode", "starvation_cap"))),
                    "starvation_cap_epochs": parsed_cap,
                }
            )
    else:
        w_list = [int(x) for x in _as_list(cfg.get("recent_window_w", 2))]
        bursts_list = [bool(x) for x in _as_list(cfg.get("bursts", False))]
        sharding_list = [str(x) for x in _as_list(cfg.get("sharding_mode", "kmeans"))]
        placement_list = [str(x) for x in _as_list(cfg.get("placement_mode", "nearest_initial"))]
        charge_setup_list = [bool(x) for x in _as_list(cfg.get("charge_setup_to_budget", False))]
        aging_list = [str(x) for x in _as_list(cfg.get("aging_mode", "starvation_cap"))]
        condition_specs = [
            {
                "recent_window_w": w_val,
                "bursts": b_val,
                "sharding_mode": sh_val,
                "placement_mode": pl_val,
                "charge_setup_to_budget": cs_val,
                "aging_mode": ag_val,
                "starvation_cap_epochs": default_cap_epochs,
            }
            for w_val in w_list
            for b_val in bursts_list
            for sh_val in sharding_list
            for pl_val in placement_list
            for cs_val in charge_setup_list
            for ag_val in aging_list
        ]

    gt_cache_dir = results_root / "_gt_cache"
    if use_gt_cache:
        gt_cache_dir.mkdir(parents=True, exist_ok=True)

    gt_tasks_by_key: Dict[str, Dict[str, Any]] = {}
    sim_tasks: List[Dict[str, Any]] = []

    for cond in condition_specs:
        recent_window_w = cond["recent_window_w"]
        bursts = cond["bursts"]
        sharding_mode = cond["sharding_mode"]
        placement_mode = cond["placement_mode"]
        charge_setup_to_budget = cond["charge_setup_to_budget"]
        aging_mode = cond["aging_mode"]
        starvation_cap_epochs = cond["starvation_cap_epochs"]
        cap_tag = "arith" if starvation_cap_epochs is None else f"A{starvation_cap_epochs}"
        for stream_mode in streams_list:
            for s_count in shards_list:
                for m_val in m_list:
                    for b_sum in budgets_list:
                        for q_val in q_list:
                            for seed in seeds_list:
                                gt_key = make_gt_cache_key(
                                    stream_mode=str(stream_mode),
                                    placement_mode=placement_mode,
                                    sharding_mode=sharding_mode,
                                    num_shards=int(s_count),
                                    num_epochs=num_epochs,
                                    inserts_per_epoch=inserts_per_epoch,
                                    queries_per_epoch=int(q_val),
                                    bursts=bursts,
                                    seed=int(seed),
                                    n_init=n_init,
                                    n_stream=n_stream,
                                )
                                gt_cache_path = gt_cache_dir / f"{gt_key}.npz" if use_gt_cache else None
                                if use_gt_cache and gt_key not in gt_tasks_by_key:
                                    gt_tasks_by_key[gt_key] = {
                                        "cache_key": gt_key,
                                        "gt_cache_path": str(gt_cache_path),
                                        "num_shards": int(s_count),
                                        "num_epochs": num_epochs,
                                        "inserts_per_epoch": inserts_per_epoch,
                                        "queries_per_epoch": int(q_val),
                                        "stream_mode": str(stream_mode),
                                        "bursts": bursts,
                                        "sharding_mode": sharding_mode,
                                        "placement_mode": placement_mode,
                                        "seed": int(seed),
                                    }

                                for pol in policies:
                                    # When sweeping charge_setup_to_budget across [false, true],
                                    # only recovered_neighbor has setup sample vectors to charge.
                                    if (
                                        charge_setup_to_budget
                                        and len(condition_specs) > 1
                                        and str(pol) != "recovered_neighbor"
                                        and not bool(cfg.get("run_all_policies_when_setup_charged", False))
                                    ):
                                        continue
                                    run_id = (
                                        f"{pol}_S{s_count}_m{m_val}_B{b_sum}_Q{q_val}"
                                        f"_{stream_mode}_{placement_mode}_W{recent_window_w}_{sharding_mode}"
                                        f"_bursts{int(bursts)}_setup{int(charge_setup_to_budget)}"
                                        f"_{aging_mode}_{cap_tag}_seed{seed}"
                                    )
                                    run_dir = exp_out_dir / run_id
                                    sim_tasks.append(
                                        {
                                            "run_id": run_id,
                                            "run_dir": str(run_dir),
                                            "policy": str(pol),
                                            "num_shards": int(s_count),
                                            "c": c,
                                            "m": int(m_val),
                                            "budget_summaries": int(b_sum),
                                            "num_epochs": num_epochs,
                                            "inserts_per_epoch": inserts_per_epoch,
                                            "queries_per_epoch": int(q_val),
                                            "recent_window_w": recent_window_w,
                                            "stream_mode": str(stream_mode),
                                            "bursts": bursts,
                                            "sharding_mode": sharding_mode,
                                            "placement_mode": placement_mode,
                                            "seed": int(seed),
                                            "exact_local_search": exact_local_search,
                                            "charge_setup_to_budget": charge_setup_to_budget,
                                            "aging_mode": aging_mode,
                                            "starvation_cap_epochs": starvation_cap_epochs,
                                            "gt_cache_path": (
                                                str(gt_cache_path) if gt_cache_path is not None else None
                                            ),
                                        }
                                    )

    effective_workers = int(cfg.get("num_workers", num_workers))
    failed_runs_log: List[Dict[str, Any]] = []
    results: List[SimulationRunResult] = []

    if effective_workers <= 1 or len(sim_tasks) <= 1:
        _init_pool_worker(str(data_dir), n_init, n_stream, str(repo_root), config_sha256)
        if use_gt_cache:
            for gt_task in gt_tasks_by_key.values():
                _worker_build_gt_task(gt_task)
        for task in sim_tasks:
            out = _worker_run_sim_task(task)
            if out["status"] != "ok":
                failed_runs_log.append(
                    {"run_id": out["run_id"], "error": out["error"], "attempt": 1}
                )
                out = _worker_run_sim_task(task)
                if out["status"] != "ok":
                    failed_runs_log.append(
                        {"run_id": out["run_id"], "error": out["error"], "attempt": 2}
                    )
                    raise RuntimeError(f"Run {out['run_id']} failed after retry:\n{out['error']}")
            results.append(
                _load_saved_run_result(Path(out["run_dir"]), out["config"], out["summary"])
            )
    else:
        n_pool = min(effective_workers, max(1, len(sim_tasks)))
        with mp.Pool(
            processes=n_pool,
            initializer=_init_pool_worker,
            initargs=(str(data_dir), n_init, n_stream, str(repo_root), config_sha256),
        ) as pool:
            if use_gt_cache:
                missing_gt_tasks = [
                    t for t in gt_tasks_by_key.values() if not Path(t["gt_cache_path"]).exists()
                ]
                if missing_gt_tasks:
                    list(pool.imap_unordered(_worker_build_gt_task, missing_gt_tasks))

            raw_outcomes = list(pool.imap(_worker_run_sim_task, sim_tasks))

            for idx, out in enumerate(raw_outcomes):
                if out["status"] != "ok":
                    failed_runs_log.append(
                        {"run_id": out["run_id"], "error": out["error"], "attempt": 1}
                    )
                    retry_out = pool.apply(_worker_run_sim_task, (sim_tasks[idx],))
                    if retry_out["status"] != "ok":
                        failed_runs_log.append(
                            {"run_id": retry_out["run_id"], "error": retry_out["error"], "attempt": 2}
                        )
                        with (exp_out_dir / "failed_runs.json").open("w", encoding="utf-8") as f:
                            json.dump(failed_runs_log, f, indent=2)
                        raise RuntimeError(
                            f"Run {retry_out['run_id']} failed after retry:\n{retry_out['error']}"
                        )
                    out = retry_out
                results.append(
                    _load_saved_run_result(Path(out["run_dir"]), out["config"], out["summary"])
                )

    with (exp_out_dir / "failed_runs.json").open("w", encoding="utf-8") as f:
        json.dump(failed_runs_log, f, indent=2)

    # Also write an experiment-level summary table and paired bootstrap comparisons if recovered_neighbor is present
    if results:
        all_summaries = [r.summary for r in results]
        pd.DataFrame(all_summaries).to_parquet(exp_out_dir / "experiment_summary.parquet", index=False)

        def _group_key(r: SimulationRunResult) -> Tuple[Any, ...]:
            return (
                r.config["num_shards"],
                r.config["m"],
                r.config["budget_summaries"],
                r.config["queries_per_epoch"],
                r.config["stream_mode"],
                r.config["placement_mode"],
                r.config["sharding_mode"],
                r.config["bursts"],
                r.config["charge_setup_to_budget"],
                r.config["recent_window_w"],
                r.config["aging_mode"],
                r.config["starvation_cap_mode"],
            )

        bootstrap_comparisons: List[Dict[str, Any]] = []
        rn_runs: Dict[Tuple[Any, ...], List[SimulationRunResult]] = {
            _group_key(r): []
            for r in results
            if r.config["policy"] == "recovered_neighbor"
        }
        for r in results:
            key = _group_key(r)
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
                    if _group_key(r) == key and r.config["policy"] != "recovered_neighbor"
                }
            )
            for op in other_policies:
                op_list = sorted(
                    [
                        r
                        for r in results
                        if _group_key(r) == key and r.config["policy"] == op
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
                            "placement_mode": key[5],
                            "sharding_mode": key[6],
                            "bursts": key[7],
                            "charge_setup_to_budget": key[8],
                            "recent_window_w": key[9],
                            "aging_mode": key[10],
                            "starvation_cap_mode": key[11],
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
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of parallel worker processes (default: 8)",
    )
    parser.add_argument(
        "--no-gt-cache",
        action="store_true",
        help="Disable ground-truth caching across methods",
    )
    args = parser.parse_args()
    run_from_config(
        config_path=args.config,
        data_dir=args.data_dir,
        results_root=args.results_dir,
        num_workers=args.workers,
        use_gt_cache=not args.no_gt_cache,
    )


if __name__ == "__main__":
    main()
