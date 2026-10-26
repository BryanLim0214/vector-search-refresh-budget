#!/usr/bin/env python3
"""Generate paper/generated/numbers.tex from analysis_root/out_addendum2/*.csv,
analysis_root/out_addendum3/*.csv, raw run outputs (queries.npz, epochs.parquet,
summary.json, failed_runs.json, RUN_ADD3.log) under analysis_root/main/ and
repo-prereg2/results/addendum3/, pilot outputs under repo/results/pilot/, and
active-shard constants from audit/M3_DESIGN_CHECK.md.

Every paired gap in percentage points (pp) is formatted with a sign and its 95%
bootstrap confidence interval in brackets: $+X.XX\\text{ pp } [+Y.YY, +Z.ZZ]$.
"""

from __future__ import annotations

import csv
import json
import math
import re
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "analysis_root" / "out_addendum2"
ADD3_OUT_DIR = ROOT / "analysis_root" / "out_addendum3"
ADD3_RESULTS_DIR = ROOT / "repo-prereg2" / "results" / "addendum3"
MAIN_DIR = ROOT / "analysis_root" / "main"
PILOT_DIR = ROOT / "repo" / "results" / "pilot"
PAPER_DIR = ROOT / "paper"
NUMBERS_TEX = PAPER_DIR / "generated" / "numbers.tex"
PLAIN_NUMBERS_TEX = PAPER_DIR.parent / "paper_plain" / "generated" / "numbers.tex"


def read_csv(name: str, base_dir: Path = OUT_DIR) -> list[dict[str, str]]:
    path = base_dir / name
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def fmt_pp_ci(mean_val: float, low_val: float, high_val: float) -> str:
    """Format a fraction difference as signed pp with 95% CI in brackets."""
    m = mean_val * 100.0
    lo = low_val * 100.0
    hi = high_val * 100.0
    if abs(m) < 0.005:
        m = 0.0
    if abs(lo) < 0.005:
        lo = 0.0
    if abs(hi) < 0.005:
        hi = 0.0
    return f"${m:+.2f}$~pp $[{lo:+.2f},$ ${hi:+.2f}]$"


def fmt_pp_ci_nopp(mean_val: float, low_val: float, high_val: float) -> str:
    """Format a fraction difference as signed pp without 'pp' suffix for compact table cells."""
    m = mean_val * 100.0
    lo = low_val * 100.0
    hi = high_val * 100.0
    if abs(m) < 0.005:
        m = 0.0
    if abs(lo) < 0.005:
        lo = 0.0
    if abs(hi) < 0.005:
        hi = 0.0
    return f"${m:+.2f}$ $[{lo:+.2f},$ ${hi:+.2f}]$"


def m1_seed_diffs_pp(
    stream: str, q_val: int, pol_a: str, pol_b: str, seeds: list[int] | None = None
) -> tuple[list[float], float, list[np.ndarray]]:
    """Compute per-seed recent_recall_at_10 differences (in pp), pooled query-mean diff, and raw diff arrays for M1."""
    if seeds is None:
        seeds = [42, 43, 44, 45, 46]
    m1_dir = MAIN_DIR / "M1"
    diffs: list[float] = []
    all_diffs: list[np.ndarray] = []
    for seed in seeds:
        dir_a = (
            m1_dir
            / f"{pol_a}_S32_m2_B4_Q{q_val}_{stream}_nearest_initial_W2_kmeans_bursts0_setup0_starvation_cap_arith_seed{seed}"
        )
        dir_b = (
            m1_dir
            / f"{pol_b}_S32_m2_B4_Q{q_val}_{stream}_nearest_initial_W2_kmeans_bursts0_setup0_starvation_cap_arith_seed{seed}"
        )
        qa = np.load(dir_a / "queries.npz")["recent_recall_at_10"]
        qb = np.load(dir_b / "queries.npz")["recent_recall_at_10"]
        mask = ~np.isnan(qa) & ~np.isnan(qb)
        d = qa[mask] - qb[mask]
        diffs.append(100.0 * float(np.mean(d)))
        all_diffs.append(d)
    pooled = 100.0 * float(np.mean(np.concatenate(all_diffs)))
    return diffs, pooled, all_diffs


def m3_seed_diffs_pp(
    stream: str, b_a: int, pol_a: str, b_b: int, pol_b: str, key: str = "recent_recall_at_10"
) -> tuple[list[float], float, list[np.ndarray]]:
    """Compute per-seed recall differences (in pp), pooled query-mean diff, and raw diff arrays."""
    m3_dir = MAIN_DIR / "M3"
    diffs: list[float] = []
    all_diffs: list[np.ndarray] = []
    for seed in [42, 43, 44, 45, 46]:
        dir_a = (
            m3_dir
            / f"{pol_a}_S32_m2_B{b_a}_Q200_{stream}_time_segment_g_W2_kmeans_bursts0_setup0_starvation_cap_arith_seed{seed}"
        )
        dir_b = (
            m3_dir
            / f"{pol_b}_S32_m2_B{b_b}_Q200_{stream}_time_segment_g_W2_kmeans_bursts0_setup0_starvation_cap_arith_seed{seed}"
        )
        qa = np.load(dir_a / "queries.npz")[key]
        qb = np.load(dir_b / "queries.npz")[key]
        mask = ~np.isnan(qa) & ~np.isnan(qb)
        d = qa[mask] - qb[mask]
        diffs.append(100.0 * float(np.mean(d)))
        all_diffs.append(d)
    pooled = 100.0 * float(np.mean(np.concatenate(all_diffs)))
    return diffs, pooled, all_diffs


def two_stage_boot_ci_95(
    diff_arrays: list[np.ndarray], n_boot: int = 2000, seed: int = 42
) -> tuple[float, float, float]:
    """Two-stage paired hierarchical bootstrap 95% CI matching repo-prereg2/analysis/analyze.py."""
    k_seeds = len(diff_arrays)
    pooled_mean = float(np.mean(np.concatenate(diff_arrays)))
    counts = np.array([len(a) for a in diff_arrays], dtype=np.float64)
    rng = np.random.default_rng(int(seed))
    within_slot_sums = np.empty((k_seeds, n_boot, k_seeds), dtype=np.float64)
    for s_idx, a in enumerate(diff_arrays):
        n_s = len(a)
        for slot in range(k_seeds):
            q_idx = rng.integers(0, n_s, size=(n_boot, n_s))
            within_slot_sums[s_idx, :, slot] = np.sum(a[q_idx], axis=1)
    seed_choices = rng.integers(0, k_seeds, size=(n_boot, k_seeds))
    rep_indices = np.arange(n_boot)[:, None]
    slot_indices = np.arange(k_seeds)[None, :]
    chosen_sums = within_slot_sums[seed_choices, rep_indices, slot_indices]
    chosen_counts = counts[seed_choices]
    hier_means = np.sum(chosen_sums, axis=1) / np.sum(chosen_counts, axis=1)
    ci_low, ci_high = np.quantile(hier_means, [0.025, 0.975])
    return pooled_mean, float(ci_low), float(ci_high)


def t_interval_95(vals: list[float]) -> tuple[float, float, float]:
    """Compute 5-seed Student-t 95% confidence interval (df=4)."""
    arr = np.asarray(vals, dtype=float)
    mean = float(np.mean(arr))
    se = float(np.std(arr, ddof=1) / math.sqrt(len(arr)))
    t_crit = 2.7764451051977987  # Student t_{0.975, df=4}
    return mean, mean - t_crit * se, mean + t_crit * se


def fmt_frac_ci(mean_val: float, low_val: float, high_val: float) -> str:
    """Format a recall@10 level on [0, 1] as a plain 4-decimal mean (intervals reserved for pp paired gaps per Guardian/C-6)."""
    _ = (low_val, high_val)
    return f"${mean_val:.4f}$"


def fmt_frac(mean_val: float) -> str:
    """Format a recall@10 level or correlation on [0, 1] to 4 decimals."""
    return f"${mean_val:.4f}$"


def fmt_rho(val: float) -> str:
    """Format a Spearman correlation to 2 decimals."""
    return f"${val:.2f}$"


def fmt_int(val: int | float) -> str:
    """Format an integer with commas in math mode."""
    return f"${int(round(float(val))):,d}$"


def fmt_dec1(val: float) -> str:
    return f"${val:.1f}$"


def fmt_pct1(val: float) -> str:
    return f"${val:.1f}\\%$"


def fmt_pct2(val: float) -> str:
    return f"${val:.2f}\\%$"


def find_row(rows: list[dict[str, str]], **kwargs: str) -> dict[str, str]:
    for r in rows:
        if all(str(r.get(k)) == str(v) for k, v in kwargs.items()):
            return r
    raise KeyError(f"Row not found for {kwargs}")


def build_all_macros() -> dict[str, str]:
    m1 = read_csv("m1_conditions.csv")
    m1o = read_csv("m1_online_conditions.csv")
    m2 = read_csv("m2_conditions.csv")
    m3 = read_csv("m3_conditions.csv")
    sens = read_csv("sens_conditions.csv")
    paired = read_csv("paired_comparisons.csv")
    lookahead = read_csv("lookahead_comparison.csv")
    spearman = read_csv("spearman_comparison.csv")
    bq = read_csv("breakeven_q.csv")
    bqo = read_csv("breakeven_q_online.csv")

    macros: dict[str, str] = {}

    # -------------------------------------------------------------------------
    # Constants from audit/M3_DESIGN_CHECK.md (lines 14-21)
    # -------------------------------------------------------------------------
    macros["RNumMThreeActiveSteadyMean"] = "$8.00$"
    macros["RNumMThreeActiveSteadyMin"] = "$8$"
    macros["RNumMThreeActiveSteadyBelowEight"] = "$0$"
    macros["RNumMThreeActiveSteadyAllEight"] = "$250$"
    macros["RNumMThreeActiveNewTopicMean"] = "$7.96$"
    macros["RNumMThreeActiveNewTopicMin"] = "$5$"
    macros["RNumMThreeActiveNewTopicBelowEight"] = "$6$"
    macros["RNumMThreeActiveNewTopicFiveShards"] = "$2$"
    macros["RNumMThreeActiveNewTopicSevenShards"] = "$4$"
    macros["RNumMThreeActiveNewTopicAllEight"] = "$244$"
    macros["RNumMThreeActiveTotalAllEight"] = "$494$"
    macros["RNumMThreeActiveTotalEpochs"] = "$500$"
    macros["RNumMThreeActiveEpochsPerStream"] = "$250$"

    # -------------------------------------------------------------------------
    # Query counts with recent ground-truth neighbors (from m1_conditions.csv)
    # -------------------------------------------------------------------------
    for q_val, q_tag in [("50", "QFifty"), ("200", "QTwoHundred"), ("800", "QEightHundred")]:
        for stream, s_tag in [("steady", "Steady"), ("new_topic", "NewTopic")]:
            r = find_row(m1, stream_mode=stream, queries_per_epoch=q_val, policy="never_refresh")
            n_with = int(r["n_queries_with_recent"])
            n_without = int(r["n_queries_without_recent"])
            n_tot = n_with + n_without
            macros[f"RNumQueriesWithRecent{s_tag}{q_tag}"] = fmt_int(n_with)
            macros[f"RNumQueriesTotal{q_tag}"] = fmt_int(n_tot)
            macros[f"RNumQueriesWithRecentFrac{s_tag}{q_tag}"] = fmt_pct2(100.0 * n_with / n_tot)

    # -------------------------------------------------------------------------
    # H1 (M1, B=4, Q=200, always_fresh - never_refresh)
    # -------------------------------------------------------------------------
    for stream, s_tag in [("steady", "Steady"), ("new_topic", "NewTopic")]:
        r = find_row(paired, experiment="M1", stream_mode=stream, comparison_type="H1_gate_M1")
        macros[f"RNumHOneGap{s_tag}"] = fmt_pp_ci(
            float(r["recent_diff_mean"]), float(r["recent_diff_ci95_low"]), float(r["recent_diff_ci95_high"])
        )
        macros[f"RNumHOneOverallGap{s_tag}"] = fmt_pp_ci(
            float(r["overall_diff_mean"]), float(r["overall_diff_ci95_low"]), float(r["overall_diff_ci95_high"])
        )

        r_nr = find_row(m1, stream_mode=stream, queries_per_epoch="200", policy="never_refresh")
        r_af = find_row(m1, stream_mode=stream, queries_per_epoch="200", policy="always_fresh")
        r_or = find_row(m1, stream_mode=stream, queries_per_epoch="200", policy="oracle_routing")
        r_all = find_row(m1, stream_mode=stream, queries_per_epoch="200", policy="all_shards")

        nr_rec = float(r_nr["recent_recall_at_10_mean"])
        af_rec = float(r_af["recent_recall_at_10_mean"])
        or_rec = float(r_or["recent_recall_at_10_mean"])

        macros[f"RNumMOneNRRecent{s_tag}"] = fmt_frac_ci(
            nr_rec, float(r_nr["recent_recall_at_10_ci95_low"]), float(r_nr["recent_recall_at_10_ci95_high"])
        )
        macros[f"RNumMOneAFRecent{s_tag}"] = fmt_frac_ci(
            af_rec, float(r_af["recent_recall_at_10_ci95_low"]), float(r_af["recent_recall_at_10_ci95_high"])
        )
        macros[f"RNumMOneOracleRecent{s_tag}"] = fmt_frac_ci(
            or_rec, float(r_or["recent_recall_at_10_ci95_low"]), float(r_or["recent_recall_at_10_ci95_high"])
        )
        macros[f"RNumMOneAllShardsRecent{s_tag}"] = fmt_frac_ci(
            float(r_all["recent_recall_at_10_mean"]),
            float(r_all["recent_recall_at_10_ci95_low"]),
            float(r_all["recent_recall_at_10_ci95_high"]),
        )
        share_closed = 100.0 * (af_rec - nr_rec) / (or_rec - nr_rec)
        macros[f"RNumMOneAFShareOfOracle{s_tag}"] = fmt_pct1(share_closed)

        for pol, p_tag in [
            ("round_robin", "RR"),
            ("data_change", "DC"),
            ("recovered_neighbor", "RN"),
            ("ql", "QL"),
            ("ss", "SS"),
            ("extra_search", "ES"),
            ("push_vectors", "PV"),
        ]:
            rp = find_row(m1, stream_mode=stream, queries_per_epoch="200", policy=pol)
            macros[f"RNumMOne{p_tag}Recent{s_tag}"] = fmt_frac_ci(
                float(rp["recent_recall_at_10_mean"]),
                float(rp["recent_recall_at_10_ci95_low"]),
                float(rp["recent_recall_at_10_ci95_high"]),
            )
            macros[f"RNumMOne{p_tag}RecentMean{s_tag}"] = fmt_frac(float(rp["recent_recall_at_10_mean"]))
        r_eso = find_row(m1o, stream_mode=stream, queries_per_epoch="200", policy="extra_search_online")
        macros[f"RNumMOneESORecent{s_tag}"] = fmt_frac_ci(
            float(r_eso["recent_recall_at_10_mean"]),
            float(r_eso["recent_recall_at_10_ci95_low"]),
            float(r_eso["recent_recall_at_10_ci95_high"]),
        )
        macros[f"RNumMOneESOBytes{s_tag}"] = fmt_int(float(r_eso["budget_bytes_spent_mean"]))

    # M1 run budget bytes and vector-push sampling check on steady
    r_rr_m1 = find_row(m1, stream_mode="steady", queries_per_epoch="200", policy="round_robin")
    r_rn_m1 = find_row(m1, stream_mode="steady", queries_per_epoch="200", policy="recovered_neighbor")
    r_es_m1 = find_row(m1, stream_mode="steady", queries_per_epoch="200", policy="extra_search")
    r_pv_m1 = find_row(m1, stream_mode="steady", queries_per_epoch="200", policy="push_vectors")
    r_nr_m1_s = find_row(m1, stream_mode="steady", queries_per_epoch="200", policy="never_refresh")

    macros["RNumMOneFourRulesBytes"] = fmt_int(float(r_rr_m1["budget_bytes_spent_mean"]))
    macros["RNumMOneRNBytes"] = fmt_int(float(r_rn_m1["budget_bytes_spent_mean"]))
    macros["RNumMOneRNAmortBytes"] = fmt_int(float(r_rn_m1["budget_bytes_with_amortized_setup_mean"]))
    macros["RNumMOneESBytes"] = fmt_int(float(r_es_m1["budget_bytes_spent_mean"]))
    macros["RNumMOnePVBytes"] = fmt_int(float(r_pv_m1["budget_bytes_spent_mean"]))
    macros["RNumMOnePVPushedVecs"] = fmt_int(float(r_pv_m1["pushed_vectors_mean"]))

    pv_share_b4 = float(r_pv_m1["pushed_vectors_mean"]) / 50000.0
    macros["RNumMOnePVShareSteady"] = fmt_pct2(100.0 * pv_share_b4)
    macros["RNumMOnePVShareFracSteady"] = fmt_frac(pv_share_b4)
    nr_m1_s_rec = float(r_nr_m1_s["recent_recall_at_10_mean"])
    pv_pred_m1_s = nr_m1_s_rec + pv_share_b4 * (1.0 - nr_m1_s_rec)
    macros["RNumMOnePVPredRecentSteady"] = fmt_frac(pv_pred_m1_s)

    # -------------------------------------------------------------------------
    # H2 and H2b (M1 and M1_online at Q=200, B=4)
    # -------------------------------------------------------------------------
    for stream, s_tag, best_pol in [
        ("steady", "Steady", "recovered_neighbor"),
        ("new_topic", "NewTopic", "data_change"),
    ]:
        r_es = find_row(
            paired,
            experiment="M1",
            stream_mode=stream,
            queries_per_epoch="200",
            policy_a="extra_search",
            policy_b=best_pol,
        )
        r_pv = find_row(
            paired,
            experiment="M1",
            stream_mode=stream,
            queries_per_epoch="200",
            policy_a="push_vectors",
            policy_b=best_pol,
        )
        r_eso = find_row(
            paired,
            experiment="M1+M1_online",
            stream_mode=stream,
            queries_per_epoch="200",
            policy_a="extra_search_online",
            policy_b=best_pol,
        )
        macros[f"RNumHTwoESvsRefresh{s_tag}"] = fmt_pp_ci(
            float(r_es["recent_diff_mean"]), float(r_es["recent_diff_ci95_low"]), float(r_es["recent_diff_ci95_high"])
        )
        macros[f"RNumHTwoESvsRefreshOverall{s_tag}"] = fmt_pp_ci(
            float(r_es["overall_diff_mean"]),
            float(r_es["overall_diff_ci95_low"]),
            float(r_es["overall_diff_ci95_high"]),
        )
        macros[f"RNumHTwoPVvsRefresh{s_tag}"] = fmt_pp_ci(
            float(r_pv["recent_diff_mean"]), float(r_pv["recent_diff_ci95_low"]), float(r_pv["recent_diff_ci95_high"])
        )
        macros[f"RNumHTwoPVvsRefreshOverall{s_tag}"] = fmt_pp_ci(
            float(r_pv["overall_diff_mean"]),
            float(r_pv["overall_diff_ci95_low"]),
            float(r_pv["overall_diff_ci95_high"]),
        )
        macros[f"RNumHTwoBESOvsRefresh{s_tag}"] = fmt_pp_ci(
            float(r_eso["recent_diff_mean"]),
            float(r_eso["recent_diff_ci95_low"]),
            float(r_eso["recent_diff_ci95_high"]),
        )
        macros[f"RNumHTwoBESOvsRefreshOverall{s_tag}"] = fmt_pp_ci(
            float(r_eso["overall_diff_mean"]),
            float(r_eso["overall_diff_ci95_low"]),
            float(r_eso["overall_diff_ci95_high"]),
        )

    # -------------------------------------------------------------------------
    # Query-rate sweep (Q in {50, 200, 800} at B=4) from breakeven_q*.csv
    # -------------------------------------------------------------------------
    for q_val, q_tag in [("50", "QFifty"), ("200", "QTwoHundred"), ("800", "QEightHundred")]:
        for stream, s_tag in [("steady", "Steady"), ("new_topic", "NewTopic")]:
            rb = find_row(bq, stream_mode=stream, queries_per_epoch=q_val)
            rbo = find_row(bqo, stream_mode=stream, queries_per_epoch=q_val)
            macros[f"RNumQSweepRefMinusES{q_tag}{s_tag}"] = fmt_pp_ci(
                float(rb["refresh_minus_extra_search_mean"]),
                float(rb["refresh_minus_extra_search_ci95_low"]),
                float(rb["refresh_minus_extra_search_ci95_high"]),
            )
            macros[f"RNumQSweepRefMinusES{q_tag}{s_tag}Cell"] = fmt_pp_ci_nopp(
                float(rb["refresh_minus_extra_search_mean"]),
                float(rb["refresh_minus_extra_search_ci95_low"]),
                float(rb["refresh_minus_extra_search_ci95_high"]),
            )
            macros[f"RNumQSweepRefMinusESO{q_tag}{s_tag}"] = fmt_pp_ci(
                float(rbo["refresh_minus_extra_search_online_mean"]),
                float(rbo["refresh_minus_extra_search_online_ci95_low"]),
                float(rbo["refresh_minus_extra_search_online_ci95_high"]),
            )
            macros[f"RNumQSweepRefMinusESO{q_tag}{s_tag}Cell"] = fmt_pp_ci_nopp(
                float(rbo["refresh_minus_extra_search_online_mean"]),
                float(rbo["refresh_minus_extra_search_online_ci95_low"]),
                float(rbo["refresh_minus_extra_search_online_ci95_high"]),
            )
            macros[f"RNumQSweepRefMinusPV{q_tag}{s_tag}"] = fmt_pp_ci(
                float(rb["refresh_minus_push_vectors_mean"]),
                float(rb["refresh_minus_push_vectors_ci95_low"]),
                float(rb["refresh_minus_push_vectors_ci95_high"]),
            )
            macros[f"RNumQSweepRefMinusPV{q_tag}{s_tag}Cell"] = fmt_pp_ci_nopp(
                float(rb["refresh_minus_push_vectors_mean"]),
                float(rb["refresh_minus_push_vectors_ci95_low"]),
                float(rb["refresh_minus_push_vectors_ci95_high"]),
            )
            macros[f"RNumQSweepRefRecent{q_tag}{s_tag}"] = fmt_frac(float(rb["best_refresh_recent_recall"]))
            macros[f"RNumQSweepESRecent{q_tag}{s_tag}"] = fmt_frac(float(rb["extra_search_recent_recall"]))
            macros[f"RNumQSweepESORecent{q_tag}{s_tag}"] = fmt_frac(float(rbo["extra_search_online_recent_recall"]))
            macros[f"RNumQSweepPVRecent{q_tag}{s_tag}"] = fmt_frac(float(rb["push_vectors_recent_recall"]))

    # Extra-search shard query load % across Q (59 extra searches over 2*Q base shard searches)
    macros["RNumESLoadPctQFifty"] = fmt_pct1(100.0 * 59.0 / 100.0)
    macros["RNumESLoadPctQTwoHundred"] = fmt_pct1(100.0 * 59.0 / 400.0)
    macros["RNumESLoadPctQEightHundred"] = fmt_pct1(100.0 * 59.0 / 1600.0)

    # Extra-search shard query load % across B at Q=200 (29, 59, 118 over 400 base searches)
    macros["RNumESLoadPctBTwo"] = fmt_pct1(100.0 * 29.0 / 400.0)
    macros["RNumESLoadPctBFour"] = fmt_pct1(100.0 * 59.0 / 400.0)
    macros["RNumESLoadPctBEight"] = fmt_pct1(100.0 * 118.0 / 400.0)

    # -------------------------------------------------------------------------
    # LOOKAHEAD (extra_search - extra_search_online) from lookahead_comparison.csv
    # -------------------------------------------------------------------------
    lk_m1_q200_vals = []
    for q_val, q_tag in [("50", "QFifty"), ("200", "QTwoHundred"), ("800", "QEightHundred")]:
        for stream, s_tag in [("steady", "Steady"), ("new_topic", "NewTopic")]:
            rl = find_row(lookahead, experiment="M1+M1_online", stream_mode=stream, queries_per_epoch=q_val)
            macros[f"RNumLookaheadMOne{q_tag}{s_tag}"] = fmt_pp_ci(
                float(rl["recent_diff_mean"]),
                float(rl["recent_diff_ci95_low"]),
                float(rl["recent_diff_ci95_high"]),
            )
            macros[f"RNumLookaheadMOne{q_tag}{s_tag}Cell"] = fmt_pp_ci_nopp(
                float(rl["recent_diff_mean"]),
                float(rl["recent_diff_ci95_low"]),
                float(rl["recent_diff_ci95_high"]),
            )
            if q_val == "200":
                lk_m1_q200_vals.append(100.0 * float(rl["recent_diff_mean"]))
            share_bytes = 100.0 * float(rl["budget_bytes_b_mean"]) / float(rl["budget_bytes_a_mean"])
            macros[f"RNumMOneESOShareOfES{q_tag}{s_tag}"] = fmt_pct1(share_bytes)
    macros["RNumLookaheadMOneMaxQTwoHundred"] = f"${max(lk_m1_q200_vals):.2f}$~pp"

    lk_m3_b4_vals = []
    for b_val, b_tag in [("2", "BTwo"), ("4", "BFour"), ("8", "BEight")]:
        for stream, s_tag in [("steady", "Steady"), ("new_topic", "NewTopic")]:
            rl = find_row(lookahead, experiment="M3", stream_mode=stream, budget_summaries=b_val)
            macros[f"RNumLookaheadMThree{b_tag}{s_tag}"] = fmt_pp_ci(
                float(rl["recent_diff_mean"]),
                float(rl["recent_diff_ci95_low"]),
                float(rl["recent_diff_ci95_high"]),
            )
            if b_val == "4":
                lk_m3_b4_vals.append(100.0 * float(rl["recent_diff_mean"]))
            share_bytes = 100.0 * float(rl["budget_bytes_b_mean"]) / float(rl["budget_bytes_a_mean"])
            macros[f"RNumMThreeESOShareOfES{b_tag}{s_tag}"] = fmt_pct1(share_bytes)
    macros["RNumLookaheadMThreeMaxBFour"] = f"${max(lk_m3_b4_vals):.2f}$~pp"

    # -------------------------------------------------------------------------
    # M2 (time_segment): PRED_M2_TIES, GATE_M2, H3, H4 (computed over all matching rows per L-2)
    # -------------------------------------------------------------------------
    m2_tie_rows = [
        r for r in paired if r.get("experiment") == "M2" and r.get("comparison_type", "").startswith("PRED_M2_TIES")
    ]
    assert len(m2_tie_rows) == 30
    m2_tie_max_diff = max(abs(float(r["recent_diff_mean"])) for r in m2_tie_rows)
    m2_tie_max_lo = max(abs(float(r["recent_diff_ci95_low"])) for r in m2_tie_rows)
    m2_tie_max_hi = max(abs(float(r["recent_diff_ci95_high"])) for r in m2_tie_rows)
    m2_tie_max_abs_q = max(float(r["max_abs_query_diff"]) for r in m2_tie_rows)
    macros["RNumPredMTwoTiesGap"] = fmt_pp_ci(m2_tie_max_diff, -m2_tie_max_lo, m2_tie_max_hi)
    macros["RNumPredMTwoTiesMaxAbs"] = fmt_frac(m2_tie_max_abs_q)

    h3_rows = [
        r for r in paired if r.get("experiment") == "M2" and r.get("comparison_type") == "H3_rn_vs_refresh_rules_B4"
    ]
    assert len(h3_rows) == 8
    h3_max_diff = max(abs(float(r["recent_diff_mean"])) for r in h3_rows)
    h3_max_lo = max(abs(float(r["recent_diff_ci95_low"])) for r in h3_rows)
    h3_max_hi = max(abs(float(r["recent_diff_ci95_high"])) for r in h3_rows)
    macros["RNumHThreeGapZero"] = fmt_pp_ci(h3_max_diff, -h3_max_lo, h3_max_hi)

    for stream, s_tag in [("steady", "Steady"), ("new_topic", "NewTopic")]:
        rg = find_row(paired, experiment="M2", stream_mode=stream, comparison_type="GATE_M2")
        macros[f"RNumGateMTwo{s_tag}"] = fmt_pp_ci(
            float(rg["recent_diff_mean"]), float(rg["recent_diff_ci95_low"]), float(rg["recent_diff_ci95_high"])
        )
        macros[f"RNumGateMTwoOverall{s_tag}"] = fmt_pp_ci(
            float(rg["overall_diff_mean"]), float(rg["overall_diff_ci95_low"]), float(rg["overall_diff_ci95_high"])
        )

        r_nr = find_row(m2, stream_mode=stream, budget_summaries="4", policy="never_refresh")
        r_af = find_row(m2, stream_mode=stream, budget_summaries="4", policy="always_fresh")
        r_or = find_row(m2, stream_mode=stream, budget_summaries="4", policy="oracle_routing")
        r_all = find_row(m2, stream_mode=stream, budget_summaries="4", policy="all_shards")

        macros[f"RNumMTwoNRRecent{s_tag}"] = fmt_frac_ci(
            float(r_nr["recent_recall_at_10_mean"]),
            float(r_nr["recent_recall_at_10_ci95_low"]),
            float(r_nr["recent_recall_at_10_ci95_high"]),
        )
        macros[f"RNumMTwoAFRecent{s_tag}"] = fmt_frac_ci(
            float(r_af["recent_recall_at_10_mean"]),
            float(r_af["recent_recall_at_10_ci95_low"]),
            float(r_af["recent_recall_at_10_ci95_high"]),
        )
        macros[f"RNumMTwoNROverall{s_tag}"] = fmt_frac_ci(
            float(r_nr["recall_at_10_mean"]),
            float(r_nr["recall_at_10_ci95_low"]),
            float(r_nr["recall_at_10_ci95_high"]),
        )
        macros[f"RNumMTwoAFOverall{s_tag}"] = fmt_frac_ci(
            float(r_af["recall_at_10_mean"]),
            float(r_af["recall_at_10_ci95_low"]),
            float(r_af["recall_at_10_ci95_high"]),
        )
        macros[f"RNumMTwoOracleRecent{s_tag}"] = fmt_frac_ci(
            float(r_or["recent_recall_at_10_mean"]),
            float(r_or["recent_recall_at_10_ci95_low"]),
            float(r_or["recent_recall_at_10_ci95_high"]),
        )
        macros[f"RNumMTwoAllShardsRecent{s_tag}"] = fmt_frac_ci(
            float(r_all["recent_recall_at_10_mean"]),
            float(r_all["recent_recall_at_10_ci95_low"]),
            float(r_all["recent_recall_at_10_ci95_high"]),
        )

        for b_val, b_tag in [("2", "BTwo"), ("4", "BFour"), ("8", "BEight")]:
            r_ref_es = find_row(
                paired,
                experiment="M2",
                stream_mode=stream,
                budget_summaries=b_val,
                comparison_type=f"H4_best_refresh_vs_extra_search_B{b_val}",
            )
            r_ref_pv = find_row(
                paired,
                experiment="M2",
                stream_mode=stream,
                budget_summaries=b_val,
                comparison_type=f"H4_best_refresh_vs_push_vectors_B{b_val}",
            )
            r_es_pv = find_row(
                paired,
                experiment="M2",
                stream_mode=stream,
                budget_summaries=b_val,
                comparison_type=f"H4_extra_search_vs_push_vectors_B{b_val}",
            )
            macros[f"RNumHFourRefMinusES{b_tag}{s_tag}"] = fmt_pp_ci(
                float(r_ref_es["recent_diff_mean"]),
                float(r_ref_es["recent_diff_ci95_low"]),
                float(r_ref_es["recent_diff_ci95_high"]),
            )
            macros[f"RNumHFourRefMinusES{b_tag}{s_tag}Cell"] = fmt_pp_ci_nopp(
                float(r_ref_es["recent_diff_mean"]),
                float(r_ref_es["recent_diff_ci95_low"]),
                float(r_ref_es["recent_diff_ci95_high"]),
            )
            macros[f"RNumHFourRefMinusPV{b_tag}{s_tag}"] = fmt_pp_ci(
                float(r_ref_pv["recent_diff_mean"]),
                float(r_ref_pv["recent_diff_ci95_low"]),
                float(r_ref_pv["recent_diff_ci95_high"]),
            )
            macros[f"RNumHFourRefMinusPV{b_tag}{s_tag}Cell"] = fmt_pp_ci_nopp(
                float(r_ref_pv["recent_diff_mean"]),
                float(r_ref_pv["recent_diff_ci95_low"]),
                float(r_ref_pv["recent_diff_ci95_high"]),
            )
            macros[f"RNumHFourRefMinusPVOverall{b_tag}{s_tag}"] = fmt_pp_ci(
                float(r_ref_pv["overall_diff_mean"]),
                float(r_ref_pv["overall_diff_ci95_low"]),
                float(r_ref_pv["overall_diff_ci95_high"]),
            )
            macros[f"RNumHFourESMinusPV{b_tag}{s_tag}"] = fmt_pp_ci(
                float(r_es_pv["recent_diff_mean"]),
                float(r_es_pv["recent_diff_ci95_low"]),
                float(r_es_pv["recent_diff_ci95_high"]),
            )
            macros[f"RNumHFourESMinusPV{b_tag}{s_tag}Cell"] = fmt_pp_ci_nopp(
                float(r_es_pv["recent_diff_mean"]),
                float(r_es_pv["recent_diff_ci95_low"]),
                float(r_es_pv["recent_diff_ci95_high"]),
            )

            # Condition recent recall for PV and sampling prediction on M2
            rpv = find_row(m2, stream_mode=stream, budget_summaries=b_val, policy="push_vectors")
            macros[f"RNumMTwoPVRecent{b_tag}{s_tag}"] = fmt_frac_ci(
                float(rpv["recent_recall_at_10_mean"]),
                float(rpv["recent_recall_at_10_ci95_low"]),
                float(rpv["recent_recall_at_10_ci95_high"]),
            )
            if stream == "steady":
                pv_share = float(rpv["pushed_vectors_mean"]) / 50000.0
                macros[f"RNumMTwoPVShare{b_tag}"] = fmt_pct2(100.0 * pv_share)
                nr_rec_m2 = float(r_nr["recent_recall_at_10_mean"])
                pred_rec = nr_rec_m2 + pv_share * (1.0 - nr_rec_m2)
                macros[f"RNumMTwoPVPredRecent{b_tag}Steady"] = fmt_frac(pred_rec)

    # -------------------------------------------------------------------------
    # M3 (time_segment_g, G=8): GATE_M3, H3b, PRED_M3_B8_TIES, H4b (L-2 fixed)
    # -------------------------------------------------------------------------
    m3_tie4_rows = [
        r
        for r in paired
        if r.get("experiment") == "M3"
        and r.get("comparison_type", "").startswith("PRED_M3_B8_TIES")
        and r.get("policy_a") != "recovered_neighbor"
    ]
    assert len(m3_tie4_rows) == 8
    m3_tie4_max_diff = max(abs(float(r["recent_diff_mean"])) for r in m3_tie4_rows)
    m3_tie4_max_lo = max(abs(float(r["recent_diff_ci95_low"])) for r in m3_tie4_rows)
    m3_tie4_max_hi = max(abs(float(r["recent_diff_ci95_high"])) for r in m3_tie4_rows)
    m3_tie4_max_abs_q = max(float(r["max_abs_query_diff"]) for r in m3_tie4_rows)
    macros["RNumPredMThreeBEightFourRulesGap"] = fmt_pp_ci(m3_tie4_max_diff, -m3_tie4_max_lo, m3_tie4_max_hi)
    macros["RNumPredMThreeBEightFourRulesMaxAbs"] = fmt_frac(m3_tie4_max_abs_q)

    rn_b8_max_abs_list = []
    pv_nt_excess_list = []
    for stream, s_tag in [("steady", "Steady"), ("new_topic", "NewTopic")]:
        r_rn_b8 = find_row(
            paired,
            experiment="M3",
            stream_mode=stream,
            budget_summaries="8",
            policy_a="recovered_neighbor",
            policy_b="always_fresh",
        )
        macros[f"RNumPredMThreeBEightRNGap{s_tag}"] = fmt_pp_ci(
            float(r_rn_b8["recent_diff_mean"]),
            float(r_rn_b8["recent_diff_ci95_low"]),
            float(r_rn_b8["recent_diff_ci95_high"]),
        )
        rn_b8_max_abs_list.append(float(r_rn_b8["max_abs_query_diff"]))

        rg3 = find_row(paired, experiment="M3", stream_mode=stream, comparison_type="GATE_M3")
        macros[f"RNumGateMThree{s_tag}"] = fmt_pp_ci(
            float(rg3["recent_diff_mean"]), float(rg3["recent_diff_ci95_low"]), float(rg3["recent_diff_ci95_high"])
        )
        macros[f"RNumGateMThreeOverall{s_tag}"] = fmt_pp_ci(
            float(rg3["overall_diff_mean"]),
            float(rg3["overall_diff_ci95_low"]),
            float(rg3["overall_diff_ci95_high"]),
        )

        r_nr3 = find_row(m3, stream_mode=stream, budget_summaries="4", policy="never_refresh")
        r_af3 = find_row(m3, stream_mode=stream, budget_summaries="4", policy="always_fresh")
        r_or3 = find_row(m3, stream_mode=stream, budget_summaries="4", policy="oracle_routing")
        r_all3 = find_row(m3, stream_mode=stream, budget_summaries="4", policy="all_shards")

        macros[f"RNumMThreeNRRecent{s_tag}"] = fmt_frac_ci(
            float(r_nr3["recent_recall_at_10_mean"]),
            float(r_nr3["recent_recall_at_10_ci95_low"]),
            float(r_nr3["recent_recall_at_10_ci95_high"]),
        )
        macros[f"RNumMThreeAFRecent{s_tag}"] = fmt_frac_ci(
            float(r_af3["recent_recall_at_10_mean"]),
            float(r_af3["recent_recall_at_10_ci95_low"]),
            float(r_af3["recent_recall_at_10_ci95_high"]),
        )
        macros[f"RNumMThreeOracleRecent{s_tag}"] = fmt_frac_ci(
            float(r_or3["recent_recall_at_10_mean"]),
            float(r_or3["recent_recall_at_10_ci95_low"]),
            float(r_or3["recent_recall_at_10_ci95_high"]),
        )
        macros[f"RNumMThreeAllShardsRecent{s_tag}"] = fmt_frac_ci(
            float(r_all3["recent_recall_at_10_mean"]),
            float(r_all3["recent_recall_at_10_ci95_low"]),
            float(r_all3["recent_recall_at_10_ci95_high"]),
        )

        # H3b pairwise at B=4
        for base_pol, p_tag in [
            ("round_robin", "RR"),
            ("data_change", "DC"),
            ("ql", "QL"),
            ("ss", "SS"),
        ]:
            rh3b = find_row(
                paired,
                experiment="M3",
                stream_mode=stream,
                budget_summaries="4",
                policy_a="recovered_neighbor",
                policy_b=base_pol,
                comparison_type="H3b_rn_vs_refresh_rules_B4",
            )
            macros[f"RNumHThreeBRNMinus{p_tag}{s_tag}"] = fmt_pp_ci(
                float(rh3b["recent_diff_mean"]),
                float(rh3b["recent_diff_ci95_low"]),
                float(rh3b["recent_diff_ci95_high"]),
            )
            macros[f"RNumHThreeBRNMinus{p_tag}Overall{s_tag}"] = fmt_pp_ci(
                float(rh3b["overall_diff_mean"]),
                float(rh3b["overall_diff_ci95_low"]),
                float(rh3b["overall_diff_ci95_high"]),
            )

        # Condition recalls across B in {2, 4, 8} for M3
        for b_val, b_tag in [("2", "BTwo"), ("4", "BFour"), ("8", "BEight")]:
            for pol, p_tag in [
                ("round_robin", "RR"),
                ("data_change", "DC"),
                ("recovered_neighbor", "RN"),
                ("ql", "QL"),
                ("ss", "SS"),
                ("extra_search", "ES"),
                ("extra_search_online", "ESO"),
                ("push_vectors", "PV"),
            ]:
                rc = find_row(m3, stream_mode=stream, budget_summaries=b_val, policy=pol)
                macros[f"RNumMThree{p_tag}Recent{b_tag}{s_tag}"] = fmt_frac_ci(
                    float(rc["recent_recall_at_10_mean"]),
                    float(rc["recent_recall_at_10_ci95_low"]),
                    float(rc["recent_recall_at_10_ci95_high"]),
                )

            # H4b comparisons
            r_ref_eso = find_row(
                paired,
                experiment="M3",
                stream_mode=stream,
                budget_summaries=b_val,
                comparison_type=f"H4b_best_refresh_vs_extra_search_online_B{b_val}",
            )
            r_ref_pv = find_row(
                paired,
                experiment="M3",
                stream_mode=stream,
                budget_summaries=b_val,
                comparison_type=f"H4b_best_refresh_vs_push_vectors_B{b_val}",
            )
            r_eso_pv = find_row(
                paired,
                experiment="M3",
                stream_mode=stream,
                budget_summaries=b_val,
                comparison_type=f"H4b_extra_search_online_vs_push_vectors_B{b_val}",
            )
            macros[f"RNumHFourBRefMinusESO{b_tag}{s_tag}"] = fmt_pp_ci(
                float(r_ref_eso["recent_diff_mean"]),
                float(r_ref_eso["recent_diff_ci95_low"]),
                float(r_ref_eso["recent_diff_ci95_high"]),
            )
            macros[f"RNumHFourBRefMinusESO{b_tag}{s_tag}Cell"] = fmt_pp_ci_nopp(
                float(r_ref_eso["recent_diff_mean"]),
                float(r_ref_eso["recent_diff_ci95_low"]),
                float(r_ref_eso["recent_diff_ci95_high"]),
            )
            macros[f"RNumHFourBRefMinusPV{b_tag}{s_tag}"] = fmt_pp_ci(
                float(r_ref_pv["recent_diff_mean"]),
                float(r_ref_pv["recent_diff_ci95_low"]),
                float(r_ref_pv["recent_diff_ci95_high"]),
            )
            macros[f"RNumHFourBRefMinusPV{b_tag}{s_tag}Cell"] = fmt_pp_ci_nopp(
                float(r_ref_pv["recent_diff_mean"]),
                float(r_ref_pv["recent_diff_ci95_low"]),
                float(r_ref_pv["recent_diff_ci95_high"]),
            )
            if b_val == "2" and stream == "new_topic":
                macros["RNumHFourBRefMinusPVBTwoNewTopicLow"] = (
                    f"${100.0 * float(r_ref_pv['recent_diff_ci95_low']):+.2f}$~pp"
                )
            macros[f"RNumHFourBRefMinusPVOverall{b_tag}{s_tag}"] = fmt_pp_ci(
                float(r_ref_pv["overall_diff_mean"]),
                float(r_ref_pv["overall_diff_ci95_low"]),
                float(r_ref_pv["overall_diff_ci95_high"]),
            )
            macros[f"RNumHFourBESOMinusPV{b_tag}{s_tag}"] = fmt_pp_ci(
                float(r_eso_pv["recent_diff_mean"]),
                float(r_eso_pv["recent_diff_ci95_low"]),
                float(r_eso_pv["recent_diff_ci95_high"]),
            )
            macros[f"RNumHFourBESOMinusPV{b_tag}{s_tag}Cell"] = fmt_pp_ci_nopp(
                float(r_eso_pv["recent_diff_mean"]),
                float(r_eso_pv["recent_diff_ci95_low"]),
                float(r_eso_pv["recent_diff_ci95_high"]),
            )

            # Sampling prediction for PV on M3 steady and excess on M3 new_topic (M-13, L-R5)
            if stream == "steady":
                rpv3 = find_row(m3, stream_mode="steady", budget_summaries=b_val, policy="push_vectors")
                pv_share3 = float(rpv3["pushed_vectors_mean"]) / 50000.0
                nr3_rec = float(r_nr3["recent_recall_at_10_mean"])
                pred3 = nr3_rec + pv_share3 * (1.0 - nr3_rec)
                macros[f"RNumMThreePVPredRecent{b_tag}Steady"] = fmt_frac(pred3)
            else:
                rpv3_nt = find_row(m3, stream_mode="new_topic", budget_summaries=b_val, policy="push_vectors")
                # Compute 5-seed mean stream_set_size across seeds 42..46 (50,880.6; Guardian L-R2-2, Reviewer B-5)
                seed_stream_sizes: list[float] = []
                for s_id in [42, 43, 44, 45, 46]:
                    s_json = json.loads(
                        (
                            MAIN_DIR
                            / "M3"
                            / f"push_vectors_S32_m2_B{b_val}_Q200_new_topic_time_segment_g_W2_kmeans_bursts0_setup0_starvation_cap_arith_seed{s_id}"
                            / "summary.json"
                        ).read_text(encoding="utf-8")
                    )
                    seed_stream_sizes.append(float(s_json["stream_set_size"]))
                pv_share3_nt = float(rpv3_nt["pushed_vectors_mean"]) / float(np.mean(seed_stream_sizes))
                nr3_nt_rec = float(r_nr3["recent_recall_at_10_mean"])
                pred_gain_nt = 100.0 * pv_share3_nt * (1.0 - nr3_nt_rec)
                obs_gain_nt = 100.0 * (float(rpv3_nt["recent_recall_at_10_mean"]) - nr3_nt_rec)
                pv_nt_excess_list.append(obs_gain_nt - pred_gain_nt)

    macros["RNumPredMThreeBEightRNMaxAbs"] = fmt_frac(max(rn_b8_max_abs_list))
    macros["RNumPredMThreeBEightRNMaxAbsOne"] = fmt_dec1(max(rn_b8_max_abs_list))
    macros["RNumPredMThreeBEightRNMaxAbsPP"] = f"${int(round(100.0 * max(rn_b8_max_abs_list)))}$~pp"
    macros["RNumMThreePVNewTopicExcessMin"] = fmt_dec1(min(pv_nt_excess_list))
    macros["RNumMThreePVNewTopicExcessMax"] = f"${max(pv_nt_excess_list):.1f}$~pp"

    # Steady refresh-rule span at Q=800 in M1 (A2 / B-F1), Q=50 RN vs DC on steady (B-3), and SS vs DC at B=2 in M3 steady (B-F6)
    r800_steady_recalls = [
        float(find_row(m1, stream_mode="steady", queries_per_epoch="800", policy=p)["recent_recall_at_10_mean"])
        for p in ["round_robin", "data_change", "recovered_neighbor", "ql", "ss"]
    ]
    macros["RNumMOneQSweepSteadyRuleSpanQEightHundred"] = (
        f"${100.0 * (max(r800_steady_recalls) - min(r800_steady_recalls)):.2f}$~pp"
    )
    r800_rn_s = float(
        find_row(m1, stream_mode="steady", queries_per_epoch="800", policy="recovered_neighbor")["recent_recall_at_10_mean"]
    )
    r800_dc_s = float(
        find_row(m1, stream_mode="steady", queries_per_epoch="800", policy="data_change")["recent_recall_at_10_mean"]
    )
    macros["RNumMOneRNRecentMeanQEightHundredSteady"] = fmt_frac(r800_rn_s)
    macros["RNumMOneDCRecentMeanQEightHundredSteady"] = fmt_frac(r800_dc_s)
    r50_rn_s = float(find_row(m1, stream_mode="steady", queries_per_epoch="50", policy="recovered_neighbor")["recent_recall_at_10_mean"])
    r50_dc_s = float(find_row(m1, stream_mode="steady", queries_per_epoch="50", policy="data_change")["recent_recall_at_10_mean"])
    macros["RNumMOneRNRecentMeanQFiftySteady"] = fmt_frac(r50_rn_s)
    macros["RNumMOneDCRecentMeanQFiftySteady"] = fmt_frac(r50_dc_s)
    _, _, d_dc_rn_50_s = m1_seed_diffs_pp("steady", 50, "data_change", "recovered_neighbor")
    dc_rn_50_m, dc_rn_50_lo, dc_rn_50_hi = two_stage_boot_ci_95(d_dc_rn_50_s, n_boot=2000, seed=42)
    macros["RNumMOneDCMinusRNQFiftySteady"] = fmt_pp_ci(dc_rn_50_m, dc_rn_50_lo, dc_rn_50_hi)
    _, _, d_dc_pv_50_s = m1_seed_diffs_pp("steady", 50, "data_change", "push_vectors")
    dc_pv_50_m, dc_pv_50_lo, dc_pv_50_hi = two_stage_boot_ci_95(d_dc_pv_50_s, n_boot=2000, seed=42)
    macros["RNumMOneDCMinusPVQFiftySteady"] = fmt_pp_ci(dc_pv_50_m, dc_pv_50_lo, dc_pv_50_hi)
    _, _, d_ss_dc_b2_s = m3_seed_diffs_pp("steady", 2, "ss", 2, "data_change")
    ss_dc_b2_m, ss_dc_b2_lo, ss_dc_b2_hi = two_stage_boot_ci_95(d_ss_dc_b2_s, n_boot=2000, seed=42)
    macros["RNumMThreeSSMinusDCBTwoSteady"] = fmt_pp_ci(ss_dc_b2_m, ss_dc_b2_lo, ss_dc_b2_hi)

    # Cap-forced refresh shares at Q=200, B=4 for ql, ss, data_change in M1 and M3 (A3)
    for stream, s_tag in [("steady", "Steady"), ("new_topic", "NewTopic")]:
        for pol, p_tag in [("ql", "QL"), ("ss", "SS"), ("data_change", "DC")]:
            rm1_p = find_row(m1, stream_mode=stream, queries_per_epoch="200", policy=pol)
            macros[f"RNumForcedShare{p_tag}{s_tag}MOne"] = fmt_pct1(
                100.0 * float(rm1_p["forced_by_cap_mean"]) / float(rm1_p["summaries_refreshed_mean"])
            )
            rm3_p = find_row(m3, stream_mode=stream, budget_summaries="4", policy=pol)
            macros[f"RNumForcedShare{p_tag}{s_tag}MThree"] = fmt_pct1(
                100.0 * float(rm3_p["forced_by_cap_mean"]) / float(rm3_p["summaries_refreshed_mean"])
            )

    # RN overhead share of B at B=2, 4, 8 (21,632 / 49,152, 98,304, 196,608)
    macros["RNumRNOverheadShareBTwo"] = fmt_pct1(100.0 * 21632.0 / 49152.0)
    macros["RNumRNOverheadShareBFour"] = fmt_pct1(100.0 * 21632.0 / 98304.0)
    macros["RNumRNOverheadShareBEight"] = fmt_pct1(100.0 * 21632.0 / 196608.0)

    # ESO byte share of refresh rules at B=2 in M3
    r_eso_m3_b2_s = find_row(m3, stream_mode="steady", budget_summaries="2", policy="extra_search_online")
    r_eso_m3_b2_n = find_row(m3, stream_mode="new_topic", budget_summaries="2", policy="extra_search_online")
    macros["RNumMThreeESOShareOfRefreshBTwoSteady"] = fmt_pct1(
        100.0 * float(r_eso_m3_b2_s["budget_bytes_spent_mean"]) / 2457600.0
    )
    macros["RNumMThreeESOShareOfRefreshBTwoNewTopic"] = fmt_pct1(
        100.0 * float(r_eso_m3_b2_n["budget_bytes_spent_mean"]) / 2457600.0
    )

    # ESO underspend valuation from ES slope across B in M3 (in pp, per Guardian M-4)
    eso_shares = []
    underspend_vals_pp = []
    for stream in ["steady", "new_topic"]:
        es_b2 = find_row(m3, stream_mode=stream, budget_summaries="2", policy="extra_search")
        es_b4 = find_row(m3, stream_mode=stream, budget_summaries="4", policy="extra_search")
        es_b8 = find_row(m3, stream_mode=stream, budget_summaries="8", policy="extra_search")
        slope_24 = (float(es_b4["recent_recall_at_10_mean"]) - float(es_b2["recent_recall_at_10_mean"])) / (
            float(es_b4["extra_searches_mean"]) - float(es_b2["extra_searches_mean"])
        )
        slope_48 = (float(es_b8["recent_recall_at_10_mean"]) - float(es_b4["recent_recall_at_10_mean"])) / (
            float(es_b8["extra_searches_mean"]) - float(es_b4["extra_searches_mean"])
        )
        for b_val, slope in [("2", slope_24), ("4", slope_24), ("8", slope_48)]:
            es_r = find_row(m3, stream_mode=stream, budget_summaries=b_val, policy="extra_search")
            eso_r = find_row(m3, stream_mode=stream, budget_summaries=b_val, policy="extra_search_online")
            n_es = float(es_r["extra_searches_mean"])
            n_eso = float(eso_r["extra_searches_mean"])
            eso_shares.append(100.0 * n_eso / n_es)
            underspend_vals_pp.append(100.0 * (n_es - n_eso) * slope)

    macros["RNumMThreeESOShareOfESMin"] = fmt_pct1(min(eso_shares))
    macros["RNumMThreeESOShareOfESMax"] = fmt_pct1(max(eso_shares))
    macros["RNumMThreeESOUnderspendValMin"] = f"${min(underspend_vals_pp):.2f}$"
    macros["RNumMThreeESOUnderspendValMax"] = f"${max(underspend_vals_pp):.2f}$~pp"

    # PV max buffered vectors at B=4 and B=8
    macros["RNumPVMaxVecsBFour"] = fmt_int(3150)
    macros["RNumPVMaxVecsBEight"] = fmt_int(6350)

    # -------------------------------------------------------------------------
    # Spearman correlations and forced-by-cap shares from spearman_comparison.csv
    # -------------------------------------------------------------------------
    for stream, s_tag in [("steady", "Steady"), ("new_topic", "NewTopic")]:
        # M1 across Q in {50, 200, 800} for recovered_neighbor
        for q_val, q_tag in [("50", "QFifty"), ("200", "QTwoHundred"), ("800", "QEightHundred")]:
            rs = find_row(
                spearman,
                experiment="M1",
                stream_mode=stream,
                queries_per_epoch=q_val,
                policy="recovered_neighbor",
            )
            n_paired = int(rs["spearman_paired_n"])
            n_all_nv = int(rs["spearman_new_vectors_all_epochs_n"])
            n_zero = n_all_nv - n_paired
            n_zero_true = 250 - n_all_nv
            forced = float(rs["forced_by_cap_mean"])
            refreshed = float(rs["summaries_refreshed_mean"])
            macros[f"RNumSpearmanRN{s_tag}MOne{q_tag}"] = fmt_rho(float(rs["spearman_rn_estimator_mean"]))
            macros[f"RNumSpearmanNewVecs{s_tag}MOne{q_tag}"] = fmt_rho(
                float(rs["spearman_new_vectors_estimator_mean"])
            )
            macros[f"RNumSpearmanPairedN{s_tag}MOne{q_tag}"] = fmt_int(n_paired)
            macros[f"RNumSpearmanZeroEpochs{s_tag}MOne{q_tag}"] = fmt_int(n_zero)
            macros[f"RNumSpearmanZeroTrue{s_tag}MOne{q_tag}"] = fmt_int(n_zero_true)
            macros[f"RNumSpearmanZeroPct{s_tag}MOne{q_tag}"] = fmt_pct1(100.0 * n_zero / 250.0)
            macros[f"RNumForcedShareRN{s_tag}MOne{q_tag}"] = fmt_pct1(100.0 * forced / refreshed)
            if q_val == "200":
                macros[f"RNumSpearmanRN{s_tag}MOne"] = fmt_rho(float(rs["spearman_rn_estimator_mean"]))
                macros[f"RNumSpearmanNewVecs{s_tag}MOne"] = fmt_rho(
                    float(rs["spearman_new_vectors_estimator_mean"])
                )
                macros[f"RNumSpearmanNewVecsAll{s_tag}MOne"] = fmt_rho(
                    float(rs["spearman_new_vectors_all_epochs_mean"])
                )
                macros[f"RNumSpearmanPairedN{s_tag}MOne"] = fmt_int(n_paired)
                macros[f"RNumSpearmanZeroEpochs{s_tag}MOne"] = fmt_int(n_zero)
                macros[f"RNumSpearmanZeroPct{s_tag}MOne"] = fmt_pct1(100.0 * n_zero / 250.0)
                macros[f"RNumForcedShareRN{s_tag}MOne"] = fmt_pct1(100.0 * forced / refreshed)
                macros["RNumSpearmanAllEpochsNewVecsMOne"] = fmt_int(n_all_nv)

        # M3 across B in {2, 4, 8} for recovered_neighbor
        for b_val, b_tag in [("2", "BTwo"), ("4", "BFour"), ("8", "BEight")]:
            rs3 = find_row(
                spearman,
                experiment="M3",
                stream_mode=stream,
                budget_summaries=b_val,
                policy="recovered_neighbor",
            )
            n_paired3 = int(rs3["spearman_paired_n"])
            n_all_nv3 = int(rs3["spearman_new_vectors_all_epochs_n"])
            n_zero3 = n_all_nv3 - n_paired3
            n_zero_true3 = 250 - n_all_nv3
            forced3 = float(rs3["forced_by_cap_mean"])
            refreshed3 = float(rs3["summaries_refreshed_mean"])
            macros[f"RNumSpearmanRN{s_tag}MThree{b_tag}"] = fmt_rho(float(rs3["spearman_rn_estimator_mean"]))
            macros[f"RNumSpearmanNewVecs{s_tag}MThree{b_tag}"] = fmt_rho(
                float(rs3["spearman_new_vectors_estimator_mean"])
            )
            macros[f"RNumSpearmanPairedN{s_tag}MThree{b_tag}"] = fmt_int(n_paired3)
            macros[f"RNumSpearmanZeroEpochs{s_tag}MThree{b_tag}"] = fmt_int(n_zero3)
            macros[f"RNumSpearmanZeroTrue{s_tag}MThree{b_tag}"] = fmt_int(n_zero_true3)
            macros[f"RNumSpearmanZeroPct{s_tag}MThree{b_tag}"] = fmt_pct1(100.0 * n_zero3 / 250.0)
            macros[f"RNumForcedShareRN{s_tag}MThree{b_tag}"] = fmt_pct1(100.0 * forced3 / refreshed3)
            if b_val == "4":
                macros[f"RNumSpearmanRN{s_tag}MThree"] = fmt_rho(float(rs3["spearman_rn_estimator_mean"]))
                macros[f"RNumSpearmanNewVecs{s_tag}MThree"] = fmt_rho(
                    float(rs3["spearman_new_vectors_estimator_mean"])
                )
                macros[f"RNumSpearmanNewVecsAll{s_tag}MThree"] = fmt_rho(
                    float(rs3["spearman_new_vectors_all_epochs_mean"])
                )
                macros[f"RNumSpearmanPairedN{s_tag}MThree"] = fmt_int(n_paired3)
                macros[f"RNumSpearmanZeroEpochs{s_tag}MThree"] = fmt_int(n_zero3)
                macros[f"RNumSpearmanZeroPct{s_tag}MThree"] = fmt_pct1(100.0 * n_zero3 / 250.0)
                macros[f"RNumForcedShareRN{s_tag}MThree"] = fmt_pct1(100.0 * forced3 / refreshed3)

        # M3 B=8 4-rule identical Spearman values (renamed per Guardian M-2)
        rs_rr8 = find_row(
            spearman, experiment="M3", stream_mode=stream, budget_summaries="8", policy="round_robin"
        )
        macros[f"RNumSpearmanRNEstFourRulesMThreeBEight{s_tag}"] = fmt_rho(
            float(rs_rr8["spearman_rn_estimator_mean"])
        )

        # Counterfactual Spearman on data_change runs at Q=200, B=4 (Reviewer B-4)
        rs_dc_m1 = find_row(
            spearman, experiment="M1", stream_mode=stream, queries_per_epoch="200", budget_summaries="4", policy="data_change"
        )
        rs_dc_m3 = find_row(
            spearman, experiment="M3", stream_mode=stream, queries_per_epoch="200", budget_summaries="4", policy="data_change"
        )
        macros[f"RNumSpearmanRNOnDC{s_tag}MOne"] = fmt_rho(float(rs_dc_m1["spearman_rn_estimator_mean"]))
        macros[f"RNumSpearmanNewVecsOnDC{s_tag}MOne"] = fmt_rho(float(rs_dc_m1["spearman_new_vectors_estimator_mean"]))
        macros[f"RNumSpearmanRNOnDC{s_tag}MThree"] = fmt_rho(float(rs_dc_m3["spearman_rn_estimator_mean"]))
        macros[f"RNumSpearmanNewVecsOnDC{s_tag}MThree"] = fmt_rho(float(rs_dc_m3["spearman_new_vectors_estimator_mean"]))

    # Degenerate M2 Spearman = 1.00
    rs_m2 = find_row(
        spearman, experiment="M2", stream_mode="steady", budget_summaries="4", policy="recovered_neighbor"
    )
    macros["RNumSpearmanDegenerateMTwo"] = fmt_rho(float(rs_m2["spearman_rn_estimator_mean"]))

    # Sensitivity condition (A=8 vs arithmetic on M2 at B=4, Q=200)
    r_m2_rn = find_row(m2, stream_mode="steady", budget_summaries="4", policy="recovered_neighbor")
    r_m2_dc = find_row(m2, stream_mode="steady", budget_summaries="4", policy="data_change")
    r_sens_rn = find_row(sens, stream_mode="steady", budget_summaries="4", policy="recovered_neighbor")
    r_sens_dc = find_row(sens, stream_mode="steady", budget_summaries="4", policy="data_change")
    r_sens_rn_nt = find_row(sens, stream_mode="new_topic", budget_summaries="4", policy="recovered_neighbor")

    macros["RNumMTwoForcedShareRNBFour"] = fmt_pct1(
        100.0 * float(r_m2_rn["forced_by_cap_mean"]) / float(r_m2_rn["summaries_refreshed_mean"])
    )
    macros["RNumMTwoForcedShareDCBFour"] = fmt_pct1(
        100.0 * float(r_m2_dc["forced_by_cap_mean"]) / float(r_m2_dc["summaries_refreshed_mean"])
    )
    macros["RNumSensForcedShareRN"] = fmt_pct1(
        100.0 * float(r_sens_rn["forced_by_cap_mean"]) / float(r_sens_rn["summaries_refreshed_mean"])
    )
    macros["RNumSensForcedShareDC"] = fmt_pct1(
        100.0 * float(r_sens_dc["forced_by_cap_mean"]) / float(r_sens_dc["summaries_refreshed_mean"])
    )
    macros["RNumSensRecallMeanSteady"] = fmt_frac_ci(
        float(r_sens_rn["recent_recall_at_10_mean"]),
        float(r_sens_rn["recent_recall_at_10_ci95_low"]),
        float(r_sens_rn["recent_recall_at_10_ci95_high"]),
    )
    macros["RNumSensRecallMeanNewTopic"] = fmt_frac_ci(
        float(r_sens_rn_nt["recent_recall_at_10_mean"]),
        float(r_sens_rn_nt["recent_recall_at_10_ci95_low"]),
        float(r_sens_rn_nt["recent_recall_at_10_ci95_high"]),
    )

    # -------------------------------------------------------------------------
    # Pilot numbers from repo/results/pilot/*/summary.json and PILOT_REPORT.md (Guardian H-1)
    # -------------------------------------------------------------------------
    p_af_s = json.loads(
        (
            PILOT_DIR
            / "always_fresh_S32_m2_B4_Q200_steady_W2_kmeans_bursts0_setup0_starvation_cap_seed42"
            / "summary.json"
        ).read_text(encoding="utf-8")
    )["recent_recall_at_10_query_mean"]
    p_nr_s = json.loads(
        (
            PILOT_DIR
            / "never_refresh_S32_m2_B4_Q200_steady_W2_kmeans_bursts0_setup0_starvation_cap_seed42"
            / "summary.json"
        ).read_text(encoding="utf-8")
    )["recent_recall_at_10_query_mean"]
    p_af_nt = json.loads(
        (
            PILOT_DIR
            / "always_fresh_S32_m2_B4_Q200_new_topic_W2_kmeans_bursts0_setup0_starvation_cap_seed42"
            / "summary.json"
        ).read_text(encoding="utf-8")
    )["recent_recall_at_10_query_mean"]
    p_nr_nt = json.loads(
        (
            PILOT_DIR
            / "never_refresh_S32_m2_B4_Q200_new_topic_W2_kmeans_bursts0_setup0_starvation_cap_seed42"
            / "summary.json"
        ).read_text(encoding="utf-8")
    )["recent_recall_at_10_query_mean"]
    macros["RNumPilotGateSteady"] = f"${100.0 * (p_af_s - p_nr_s):+.2f}$~pp"
    macros["RNumPilotGateNewTopic"] = f"${100.0 * (p_af_nt - p_nr_nt):+.2f}$~pp"

    pilot_md = (PILOT_DIR / "PILOT_REPORT.md").read_text(encoding="utf-8")
    m_own = re.search(
        r"route\s+([\d.]+)%\s+of streamed vectors into their own shard's top 2\s+\(([\d.]+)%\s+top 1\)",
        pilot_md,
    )
    assert m_own is not None
    macros["RNumPilotOwnShardTopTwo"] = fmt_pct1(float(m_own.group(1)))
    macros["RNumPilotOwnShardTopOne"] = fmt_pct1(float(m_own.group(2)))

    m_cap = re.search(
        r"forced\s+\$(\d+)\\%\$\s+\(\$126\s*/\s*150\$\)\s+of\s+`recovered_neighbor`\s+choices\s+and\s+\$(\d+)\\%\$\s+\(\$127\s*/\s*200\$\)",
        pilot_md,
    )
    assert m_cap is not None
    macros["RNumPilotCapShareRN"] = f"${int(m_cap.group(1))}\\%$"
    macros["RNumPilotCapShareDC"] = f"${int(m_cap.group(2))}\\%$"

    # -------------------------------------------------------------------------
    # Per-seed M3 calculations, 5-seed t-intervals, and bootstrap width ratio (Guardian H-2, H-3, L-1, L-R4, B-F5)
    # -------------------------------------------------------------------------
    gate_nt_seeds, _, gate_nt_diff_arrays = m3_seed_diffs_pp("new_topic", 4, "always_fresh", 4, "never_refresh")
    min_gate_idx = int(np.argmin(gate_nt_seeds))
    gate_t_mean, gate_t_lo, gate_t_hi = t_interval_95(gate_nt_seeds)
    macros["RNumGateMThreeNewTopicMinSeed"] = f"${42 + min_gate_idx}$"
    macros["RNumGateMThreeNewTopicMinSeedGap"] = f"${gate_nt_seeds[min_gate_idx]:+.2f}$~pp"
    macros["RNumGateMThreeNewTopicTInt"] = f"$[{gate_t_lo:+.2f},$ ${gate_t_hi:+.2f}]$~pp"
    macros["RNumGateMThreeNewTopicUnweightedMean"] = f"${gate_t_mean:+.2f}$~pp"
    nt_seed_valid_q_counts = [len(a) for a in gate_nt_diff_arrays]
    macros["RNumNewTopicValidQueriesSeedMinQTwoHundred"] = fmt_int(min(nt_seed_valid_q_counts))
    macros["RNumNewTopicValidQueriesSeedMaxQTwoHundred"] = fmt_int(max(nt_seed_valid_q_counts))

    h4b_b2_seeds, _, _ = m3_seed_diffs_pp("new_topic", 2, "data_change", 2, "push_vectors")
    min_h4b_idx = int(np.argmin(h4b_b2_seeds))
    _, h4b_t_lo, h4b_t_hi = t_interval_95(h4b_b2_seeds)
    macros["RNumHFourBDCMinusPVBTwoNewTopicNegSeed"] = f"${42 + min_h4b_idx}$"
    macros["RNumHFourBDCMinusPVBTwoNewTopicNegSeedGap"] = f"${h4b_b2_seeds[min_h4b_idx]:+.2f}$~pp"
    macros["RNumHFourBDCMinusPVBTwoNewTopicTInt"] = f"$[{h4b_t_lo:+.2f},$ ${h4b_t_hi:+.2f}]$~pp"

    # M3 H3b RN - DC overall recall_at_10 5-seed t-interval (Reviewer B-1, Guardian Re-check 5b B-1)
    rn_dc_ov_nt_seeds, _, _ = m3_seed_diffs_pp(
        "new_topic", 4, "recovered_neighbor", 4, "data_change", key="recall_at_10"
    )
    _, rn_dc_ov_t_lo, rn_dc_ov_t_hi = t_interval_95(rn_dc_ov_nt_seeds)
    macros["RNumHThreeBRNMinusDCOverallNewTopicTInt"] = f"$[{rn_dc_ov_t_lo:+.2f},$ ${rn_dc_ov_t_hi:+.2f}]$~pp"

    # M3 H3b RN - QL new_topic 5-seed t-interval (Reviewer B-1, Guardian R4-H2, R4-L7)
    rn_ql_nt_seeds, _, _ = m3_seed_diffs_pp("new_topic", 4, "recovered_neighbor", 4, "ql")
    _, rn_ql_t_lo, rn_ql_t_hi = t_interval_95(rn_ql_nt_seeds)
    macros["RNumHThreeBMThreeRNMinusQLNewTopicTInt"] = f"$[{rn_ql_t_lo:+.2f},$ ${rn_ql_t_hi:+.2f}]$~pp"
    macros["RNumFiveSeeds"] = fmt_int(sum(1 for v in rn_ql_nt_seeds if v > 0))
    row_rn_ql_nt = find_row(
        paired,
        experiment="M3",
        stream_mode="new_topic",
        budget_summaries="4",
        policy_a="recovered_neighbor",
        policy_b="ql",
        comparison_type="H3b_rn_vs_refresh_rules_B4",
    )
    boot_w_rn_ql = 100.0 * (float(row_rn_ql_nt["recent_diff_ci95_high"]) - float(row_rn_ql_nt["recent_diff_ci95_low"]))
    t_w_rn_ql = rn_ql_t_hi - rn_ql_t_lo
    macros["RNumBootWiderMThreeRNMinusQLNewTopic"] = f"${int(round(100.0 * (boot_w_rn_ql / t_w_rn_ql - 1.0)))}\\%$"

    # M1 LK (extra_search - extra_search_online) steady Q=200 5-seed t-interval (Reviewer B-1, Guardian R4-L7)
    m1_dir_lk = MAIN_DIR / "M1"
    m1o_dir_lk = MAIN_DIR / "M1_online"
    lk_s_q200_seeds: list[float] = []
    for s_id in [42, 43, 44, 45, 46]:
        qa_lk = np.load(
            m1_dir_lk
            / f"extra_search_S32_m2_B4_Q200_steady_nearest_initial_W2_kmeans_bursts0_setup0_starvation_cap_arith_seed{s_id}"
            / "queries.npz"
        )["recent_recall_at_10"]
        qb_lk = np.load(
            m1o_dir_lk
            / f"extra_search_online_S32_m2_B4_Q200_steady_nearest_initial_W2_kmeans_bursts0_setup0_starvation_cap_arith_seed{s_id}"
            / "queries.npz"
        )["recent_recall_at_10"]
        m_lk = ~np.isnan(qa_lk) & ~np.isnan(qb_lk)
        lk_s_q200_seeds.append(100.0 * float(np.mean(qa_lk[m_lk] - qb_lk[m_lk])))
    min_lk_idx = int(np.argmin(lk_s_q200_seeds))
    _, lk_t_lo, lk_t_hi = t_interval_95(lk_s_q200_seeds)
    macros["RNumLookaheadMOneQTwoHundredSteadyNegSeed"] = f"${42 + min_lk_idx}$"
    macros["RNumLookaheadMOneQTwoHundredSteadyNegSeedGap"] = f"${lk_s_q200_seeds[min_lk_idx]:+.2f}$~pp"
    macros["RNumLookaheadMOneQTwoHundredSteadyTInt"] = f"$[{lk_t_lo:+.2f},$ ${lk_t_hi:+.2f}]$~pp"

    # Smallest steady margin across all rules and Q: data_change (M1) minus extra_search_online (M1_online) at Q=800 steady (Guardian Re-check 6b B-1)
    dc_eso_q800_diffs: list[np.ndarray] = []
    for s_id in [42, 43, 44, 45, 46]:
        qa_dc = np.load(
            m1_dir_lk
            / f"data_change_S32_m2_B4_Q800_steady_nearest_initial_W2_kmeans_bursts0_setup0_starvation_cap_arith_seed{s_id}"
            / "queries.npz"
        )["recent_recall_at_10"]
        qb_eso = np.load(
            m1o_dir_lk
            / f"extra_search_online_S32_m2_B4_Q800_steady_nearest_initial_W2_kmeans_bursts0_setup0_starvation_cap_arith_seed{s_id}"
            / "queries.npz"
        )["recent_recall_at_10"]
        m_de = ~np.isnan(qa_dc) & ~np.isnan(qb_eso)
        dc_eso_q800_diffs.append(qa_dc[m_de] - qb_eso[m_de])
    de_mean, de_lo, de_hi = two_stage_boot_ci_95(dc_eso_q800_diffs, n_boot=2000, seed=42)
    macros["RNumMOneDCMinusESOQEightHundredSteady"] = fmt_pp_ci(de_mean, de_lo, de_hi)

    # Exploratory cross-budget check: RN (B=8, 7 refreshes) minus DC (B=4, 4 refreshes) and RN (B=4, 3) minus DC (B=2, 2) on M3 new_topic (B-F5, B-6)
    _, _, rn8_dc4_diffs = m3_seed_diffs_pp("new_topic", 8, "recovered_neighbor", 4, "data_change")
    rn8_dc4_mean, rn8_dc4_lo, rn8_dc4_hi = two_stage_boot_ci_95(rn8_dc4_diffs, n_boot=2000, seed=42)
    macros["RNumMThreeRNBEightMinusDCBFourNewTopic"] = fmt_pp_ci(rn8_dc4_mean, rn8_dc4_lo, rn8_dc4_hi)
    macros["RNumMThreeRNBEightMinusDCBFourNewTopicMean"] = f"${100.0 * rn8_dc4_mean:+.2f}$~pp"

    _, _, rn4_dc2_diffs = m3_seed_diffs_pp("new_topic", 4, "recovered_neighbor", 2, "data_change")
    rn4_dc2_mean, rn4_dc2_lo, rn4_dc2_hi = two_stage_boot_ci_95(rn4_dc2_diffs, n_boot=2000, seed=42)
    macros["RNumMThreeRNBFourMinusDCBTwoNewTopic"] = fmt_pp_ci(rn4_dc2_mean, rn4_dc2_lo, rn4_dc2_hi)

    # Round-robin minus never-refresh on M3 new_topic at B=2 and B=4 (Guardian H-R2-1)
    _, _, rr2_nr_diffs = m3_seed_diffs_pp("new_topic", 2, "round_robin", 2, "never_refresh")
    rr2_nr_mean, rr2_nr_lo, rr2_nr_hi = two_stage_boot_ci_95(rr2_nr_diffs, n_boot=2000, seed=42)
    macros["RNumMThreeRRMinusNRBTwoNewTopic"] = fmt_pp_ci(rr2_nr_mean, rr2_nr_lo, rr2_nr_hi)

    _, _, rr4_nr_diffs = m3_seed_diffs_pp("new_topic", 4, "round_robin", 4, "never_refresh")
    rr4_nr_mean, rr4_nr_lo, rr4_nr_hi = two_stage_boot_ci_95(rr4_nr_diffs, n_boot=2000, seed=42)
    macros["RNumMThreeRRMinusNRBFourNewTopic"] = fmt_pp_ci(rr4_nr_mean, rr4_nr_lo, rr4_nr_hi)

    # Symmetric 5-seed t-intervals for borderline comparisons (Reviewer B-2)
    q50_dc_pv_seeds, _, _ = m1_seed_diffs_pp("new_topic", 50, "data_change", "push_vectors")
    _, q50_t_lo, q50_t_hi = t_interval_95(q50_dc_pv_seeds)
    macros["RNumQSweepRefMinusPVQFiftyNewTopicTInt"] = f"$[{q50_t_lo:+.2f},$ ${q50_t_hi:+.2f}]$~pp"

    h4b_b8_eso_seeds, _, _ = m3_seed_diffs_pp("new_topic", 8, "round_robin", 8, "extra_search_online")
    _, b8_eso_t_lo, b8_eso_t_hi = t_interval_95(h4b_b8_eso_seeds)
    macros["RNumHFourBRefMinusESOBEightNewTopicTInt"] = f"$[{b8_eso_t_lo:+.2f},$ ${b8_eso_t_hi:+.2f}]$~pp"

    # 4-seed out-of-sample H1 mean and seed range excluding pilot seed 42 (Reviewer B-4, B-2)
    for st_h1, st_tag in [("steady", "Steady"), ("new_topic", "NewTopic")]:
        h1_oos_seeds, h1_oos_pooled, _ = m1_seed_diffs_pp(
            st_h1, 200, "always_fresh", "never_refresh", seeds=[43, 44, 45, 46]
        )
        macros[f"RNumHOneOutOfSampleMean{st_tag}"] = f"${h1_oos_pooled:+.2f}$~pp"
        macros[f"RNumHOneOutOfSampleUnweightedMean{st_tag}"] = f"${float(np.mean(h1_oos_seeds)):+.2f}$~pp"
        macros[f"RNumHOneOutOfSampleMin{st_tag}"] = f"${min(h1_oos_seeds):+.2f}$"
        macros[f"RNumHOneOutOfSampleMax{st_tag}"] = f"${max(h1_oos_seeds):+.2f}$~pp"

    # R10-3 (B-7): verify all 16 out-of-sample (seeds 43..46) H2 ES and PV gains over best refresh are positive
    h2_oos_all_16: list[float] = []
    for st_h2, ref_h2 in [("steady", "recovered_neighbor"), ("new_topic", "data_change")]:
        for alt_h2 in ["extra_search", "push_vectors"]:
            h2_oos_seeds, _, _ = m1_seed_diffs_pp(
                st_h2, 200, alt_h2, ref_h2, seeds=[43, 44, 45, 46]
            )
            assert len(h2_oos_seeds) == 4 and all(v > 0.0 for v in h2_oos_seeds), (
                f"R10-3 check failed for {st_h2} {alt_h2} vs {ref_h2}: {h2_oos_seeds}"
            )
            h2_oos_all_16.extend(h2_oos_seeds)
    assert len(h2_oos_all_16) == 16 and all(v > 0.0 for v in h2_oos_all_16), (
        f"R10-3 check failed across 16 out-of-sample H2 gaps: {h2_oos_all_16}"
    )

    macros["RNumQueryRepeatsQFifty"] = fmt_dec1(50.0 * 50.0 / 1000.0)
    macros["RNumQueryRepeatsQTwoHundred"] = fmt_int(50 * 200 // 1000)
    macros["RNumQueryRepeatsQEightHundred"] = fmt_int(50 * 800 // 1000)

    # Seed-42 and 5-seed new_topic stream sizes from summary.json (Guardian Re-check 2 hand-typed table, L-R2-2)
    nt_init_sizes: list[int] = []
    nt_stream_sizes: list[int] = []
    for s_id in [42, 43, 44, 45, 46]:
        sj_nt = json.loads(
            (
                MAIN_DIR
                / "M3"
                / f"never_refresh_S32_m2_B4_Q200_new_topic_time_segment_g_W2_kmeans_bursts0_setup0_starvation_cap_arith_seed{s_id}"
                / "summary.json"
            ).read_text(encoding="utf-8")
        )
        nt_init_sizes.append(int(sj_nt["initial_set_size"]))
        nt_stream_sizes.append(int(sj_nt["stream_set_size"]))
    s42_stream = nt_stream_sizes[0]
    macros["RNumNewTopicInitVecsSeedFortyTwo"] = fmt_int(nt_init_sizes[0])
    macros["RNumNewTopicStreamVecsSeedFortyTwo"] = fmt_int(s42_stream)
    macros["RNumNewTopicStreamVecsMean"] = f"${float(np.mean(nt_stream_sizes)):,.1f}$"
    macros["RNumNewTopicEpochVecsHighSeedFortyTwo"] = fmt_int(math.ceil(s42_stream / 50.0))
    macros["RNumNewTopicEpochVecsLowSeedFortyTwo"] = fmt_int(s42_stream // 50)
    macros["RNumNewTopicHighLastEpochSeedFortyTwo"] = fmt_int((s42_stream % 50) - 1)
    macros["RNumNewTopicLowFirstEpochSeedFortyTwo"] = fmt_int(s42_stream % 50)
    macros["RNumChanceRecallMTwo"] = fmt_frac(2.0 / 32.0)

    # Bootstrap interval narrowing vs 5-seed t-interval across the 5 M3 headline comparisons (SKEPTIC_M3 Sec 8, L-R4, B-1)
    narrowing_pcts: list[float] = []
    for st_m, b_a, p_a, b_b, p_b, comp_type in [
        ("steady", 4, "always_fresh", 4, "never_refresh", "GATE_M3"),
        ("new_topic", 4, "always_fresh", 4, "never_refresh", "GATE_M3"),
        ("new_topic", 4, "recovered_neighbor", 4, "data_change", "H3b_rn_vs_refresh_rules_B4"),
        ("new_topic", 2, "data_change", 2, "push_vectors", "H4b_best_refresh_vs_push_vectors_B2"),
        ("new_topic", 4, "data_change", 4, "push_vectors", "H4b_best_refresh_vs_push_vectors_B4"),
    ]:
        s_vals, _, _ = m3_seed_diffs_pp(st_m, b_a, p_a, b_b, p_b)
        _, t_lo, t_hi = t_interval_95(s_vals)
        t_w = t_hi - t_lo
        row_p = find_row(
            paired,
            experiment="M3",
            stream_mode=st_m,
            budget_summaries=str(b_a),
            policy_a=p_a,
            policy_b=p_b,
            comparison_type=comp_type,
        )
        boot_w = 100.0 * (float(row_p["recent_diff_ci95_high"]) - float(row_p["recent_diff_ci95_low"]))
        narrowing_pcts.append(100.0 * (1.0 - boot_w / t_w))
    macros["RNumBootNarrowerMin"] = f"${int(round(min(narrowing_pcts)))}\\%$"
    macros["RNumBootNarrowerMax"] = f"${int(round(max(narrowing_pcts)))}\\%$"

    # Unmodified summary shares in M2 for the 4 zero-overhead rules at B in {2, 4, 8} (Guardian Re-check 4b A-3)
    macros["RNumMTwoUnmodShareBTwo"] = f"${int(round(100.0 * 1.0 / 2.0))}\\%$"
    macros["RNumMTwoUnmodShareBFour"] = f"${int(round(100.0 * 3.0 / 4.0))}\\%$"
    macros["RNumMTwoUnmodShareBEight"] = fmt_pct1(100.0 * 7.0 / 8.0)

    # Run count and missing runs from CSVs
    missing_rows = read_csv("missing_runs.csv")
    macros["RNumMissingRuns"] = fmt_int(len(missing_rows))
    total_runs = sum(
        int(r["n_seeds"]) for tbl in [m1, m1o, m2, m3, sens] for r in tbl
    )
    macros["RNumTotalMainRuns"] = fmt_int(total_runs)
    macros["RNumSensTotalRuns"] = fmt_int(sum(int(r["n_seeds"]) for r in sens))

    # -------------------------------------------------------------------------
    # Audit and design constants (with source comments per GUARDIAN_C7_full.md)
    # -------------------------------------------------------------------------
    # Source: audit/GUARDIAN_C1_technical.md A & D, repo-prereg2/docs/DEVIATIONS.md, prereg_addendum2.yaml (L-R7)
    macros["RNumPilotSeed"] = "$42$"
    macros["RNumDataSeed"] = "$42$"
    macros["RNumBootSeed"] = "$42$"
    macros["RNumSeedMin"] = "$42$"
    macros["RNumOutOfSampleSeedMin"] = "$43$"
    macros["RNumSeedMax"] = "$46$"
    macros["RNumSensSeedMax"] = "$44$"
    macros["RNumPassOneFailedRuns"] = "$113$"
    macros["RNumPassOneRetriedOk"] = "$112$"
    macros["RNumPassOneStoppedAt"] = "$149/330$"
    macros["RNumPassTwoRemainingRuns"] = "$181$"
    macros["RNumPassTwoRetriedOk"] = "$47$"
    macros["RNumPartialMOneTestedRuns"] = "$117$"
    macros["RNumAddendumTwoMTwoDone"] = "$140/330$"

    # System & protocol design constants derived from S=32, m=2, c=16, E=50, Q_s=64, B_4=98,304 (L-R8, A1, A4)
    macros["RNumNumShards"] = fmt_int(32)
    macros["RNumTopM"] = fmt_int(2)
    macros["RNumThreeShards"] = fmt_int(3)
    macros["RNumCentersPerSummary"] = fmt_int(16)
    macros["RNumNumEpochs"] = fmt_int(50)
    macros["RNumEpochsPerCondition"] = fmt_int(50 * 5)
    macros["RNumSampleQueries"] = fmt_int(64)
    macros["RNumCIPct"] = "$95\\%$"
    macros["RNumGateThresholdPP"] = "$5$~pp"
    macros["RNumAlwaysFreshEpochBytes"] = fmt_int(32 * 24576)
    macros["RNumBudgetBFourEpochBytes"] = fmt_int(4 * 24576)
    macros["RNumSummaryBytes"] = fmt_int(24576)
    macros["RNumRNOverheadEpochBytes"] = fmt_int(5120 + 16384 + 128)
    macros["RNumRNStrandedEpochBytesBFour"] = fmt_int(4 * 24576 - 21632 - 3 * 24576)
    macros["RNumDCScoreEpochBytes"] = fmt_int(32 * 4)
    macros["RNumScoreBytesPerShard"] = fmt_int(4)
    macros["RNumRNSetupBroadcastBytes"] = fmt_int(64 * 32 * 1536)
    macros["RNumRNSetupAmortEpochBytes"] = fmt_int(round(64 * 32 * 1536 / 50.0))
    macros["RNumRNRemAfterAmortBFourBytes"] = fmt_int(98304 - 21632 - round(64 * 32 * 1536 / 50.0))
    macros["RNumRNSampleSearchesPerEpoch"] = fmt_int(64 * 2)
    macros["RNumKMeansIters"] = fmt_int(15)
    macros["RNumRoutingDotProds"] = fmt_int(32 * 16)
    macros["RNumPVDotProdRatioBFour"] = f"${3150.0 / 512.0:.1f}\\times$"
    macros["RNumPVDotProdRatioBEight"] = f"${6350.0 / 512.0:.1f}\\times$"
    macros["RNumPVFifoVecsBFour"] = fmt_int(2 * 63)
    macros["RNumESOUnspentMinBytesQFifty"] = fmt_int(98304 - 50 * 1656)
    macros["RNumESOUnderspendVsESQFiftyBytes"] = fmt_int(59 * 1656 - 50 * 1656)
    macros["RNumExtraSearchesBFour"] = fmt_int(98304 // 1656)
    macros["RNumESOQFiftyMaxSearches"] = fmt_int(50)
    macros["RNumESQFiftySecondShardSearches"] = fmt_int(59 - 50)
    macros["RNumPushedVecsBFourPerEpoch"] = fmt_int(98304 // 1544)
    macros["RNumSteadyStreamTotalVecs"] = fmt_int(50000)
    macros["RNumSteadyEpochInsertVecs"] = fmt_int(1000)
    macros["RNumCorpusTotalPassages"] = fmt_int(100000)
    macros["RNumVectorDimFloats"] = fmt_int(384)
    macros["RNumMTwoSeedStreamBudgetConds"] = fmt_int(5 * 2 * 3)
    macros["RNumFourRulesSummariesBFour"] = fmt_int(4)
    macros["RNumRNSummariesBFour"] = fmt_int(3)
    macros["RNumRNSummariesBEight"] = fmt_int(7)
    macros["RNumFourRulesSummariesBTwo"] = fmt_int(2)
    macros["RNumOneSummary"] = fmt_int(1)
    macros["RNumZeroBytes"] = fmt_int(0)
    macros["RNumQueriesPerSeedQTwoHundred"] = fmt_int(50 * 200)

    # Mean-only macros for Q=800 and H3b used in conclusion/results alongside Addendum 3
    rb_800_nt = find_row(bq, stream_mode="new_topic", queries_per_epoch="800")
    macros["RNumQSweepRefMinusESQEightHundredNewTopicMean"] = (
        f"${100.0 * float(rb_800_nt['refresh_minus_extra_search_mean']):+.2f}$~pp"
    )
    macros["RNumQSweepRefMinusESQEightHundredNewTopicAbsMean"] = (
        f"${abs(100.0 * float(rb_800_nt['refresh_minus_extra_search_mean'])):.2f}$~pp"
    )
    macros["RNumQSweepRefMinusPVQEightHundredNewTopicMean"] = (
        f"${100.0 * float(rb_800_nt['refresh_minus_push_vectors_mean']):+.2f}$~pp"
    )
    rh3b_dc_nt = find_row(
        paired,
        experiment="M3",
        stream_mode="new_topic",
        budget_summaries="4",
        policy_a="recovered_neighbor",
        policy_b="data_change",
        comparison_type="H3b_rn_vs_refresh_rules_B4",
    )
    macros["RNumHThreeBRNMinusDCNewTopicAbsMean"] = (
        f"${abs(100.0 * float(rh3b_dc_nt['recent_diff_mean'])):.2f}$~pp"
    )

    # -------------------------------------------------------------------------
    # Exploratory Addendum 3 (from analysis_root/out_addendum3/*.csv and repo-prereg2/results/addendum3/)
    # -------------------------------------------------------------------------
    add3_paired = read_csv("add3_paired_comparisons.csv", ADD3_OUT_DIR)
    add3_conds = read_csv("add3_conditions.csv", ADD3_OUT_DIR)
    add3_epochs = read_csv("add3_dc_charged_epochs.csv", ADD3_OUT_DIR)
    add3_rep = read_csv("replication_check.csv", ADD3_OUT_DIR)

    # Run counts, commit check, and retries from RUN_ADD3.log, failed_runs.json, and run folders
    run_add3_log = (ADD3_RESULTS_DIR / "RUN_ADD3.log").read_text(encoding="utf-8").splitlines()
    commit_short = run_add3_log[1].strip()[:7]
    prov_files = list(ADD3_RESULTS_DIR.glob("*/*/provenance.json"))
    assert len(prov_files) == 42
    for pf in prov_files:
        pj = json.loads(pf.read_text(encoding="utf-8"))
        assert str(pj["git_commit"]).startswith(commit_short)

    n_add3_m3 = sum(1 for r in add3_epochs if r["experiment"] == "addendum3/M3_dc_charged")
    n_add3_m1 = sum(1 for r in add3_epochs if r["experiment"] == "addendum3/M1_dc_charged")
    n_add3_rep = sum(1 for r in add3_rep if r["status"] == "PASS")
    assert n_add3_m3 == 30 and n_add3_m1 == 10 and n_add3_rep == 2
    macros["RNumAddThreeMThreeRuns"] = fmt_int(n_add3_m3)
    macros["RNumAddThreeMOneRuns"] = fmt_int(n_add3_m1)
    macros["RNumAddThreeRepRuns"] = fmt_int(n_add3_rep)
    macros["RNumAddThreeTotalRuns"] = fmt_int(n_add3_m3 + n_add3_m1 + n_add3_rep)

    failed_add3 = json.loads(
        (ADD3_RESULTS_DIR / "M3_dc_charged" / "failed_runs.json").read_text(encoding="utf-8")
    )
    n_failed_add3 = sum(1 for entry in failed_add3 if int(entry.get("attempt", 0)) == 1)
    macros["RNumAddThreeMThreeFailedAttempts"] = fmt_int(n_failed_add3)

    macros["RNumAddThreeCompRows"] = fmt_int(len(add3_paired))
    macros["RNumAddThreeTotalIntervals"] = fmt_int(2 * len(add3_paired))

    # P1 and P2 thresholds from prediction_note in add3_paired_comparisons.csv
    r_p1 = find_row(add3_paired, prediction_id="P1")
    r_p2 = find_row(add3_paired, prediction_id="P2")
    m_p1 = re.search(r"lower bound > ([+-]?[\d.]+) pp", r_p1["prediction_note"])
    m_p2 = re.search(r"lower bound <= ([+-]?[\d.]+) pp", r_p2["prediction_note"])
    assert m_p1 is not None and m_p2 is not None
    macros["RNumAddThreePOneBound"] = f"${float(m_p1.group(1)):+.2f}$~pp"
    macros["RNumAddThreePTwoBound"] = f"${float(m_p2.group(1)):.2f}$~pp"

    # Unspent bytes per epoch under zero-slack floor for charged data_change (24,576 * B - epoch_budget_bytes_spent = 24,448)
    unspent_dc_set = {
        int(r["budget_summaries"]) * 24576 - int(r["epoch_budget_bytes_spent"]) for r in add3_epochs
    }
    assert len(unspent_dc_set) == 1
    macros["RNumAddThreeDCUnspentEpochBytes"] = fmt_int(next(iter(unspent_dc_set)))

    # Extra bytes/epoch spent by recovered_neighbor vs charged data_change in X1 (21,504 B/epoch)
    x1_rows = [r for r in add3_paired if r["comparison_group"] == "X1"]
    rn_extra_epoch_set = {
        int(round((float(r["budget_bytes_b_mean"]) - float(r["budget_bytes_a_mean"])) / 50.0)) for r in x1_rows
    }
    assert len(rn_extra_epoch_set) == 1
    macros["RNumAddThreeRNExtraEpochBytesVsChargedDC"] = fmt_int(next(iter(rn_extra_epoch_set)))

    # Charged data-change condition levels from add3_conditions.csv
    for stream, s_tag in [("steady", "Steady"), ("new_topic", "NewTopic")]:
        for b_val, b_tag in [("2", "BTwo"), ("4", "BFour"), ("8", "BEight")]:
            rc_m3 = find_row(
                add3_conds,
                experiment="addendum3/M3_dc_charged",
                stream_mode=stream,
                budget_summaries=b_val,
                policy="data_change",
            )
            macros[f"RNumAddThreeMThreeChargedDCRecent{b_tag}{s_tag}"] = fmt_frac(
                float(rc_m3["recent_recall_at_10_mean"])
            )
        rc_m1 = find_row(
            add3_conds,
            experiment="addendum3/M1_dc_charged",
            stream_mode=stream,
            budget_summaries="4",
            queries_per_epoch="800",
            policy="data_change",
        )
        macros[f"RNumAddThreeMOneChargedDCRecent{s_tag}"] = fmt_frac(
            float(rc_m1["recent_recall_at_10_mean"])
        )

    # All 32 rows of add3_paired_comparisons.csv (X1, X2, X3, X4)
    for stream, s_tag in [("steady", "Steady"), ("new_topic", "NewTopic")]:
        for b_val, b_tag in [("2", "BTwo"), ("4", "BFour"), ("8", "BEight")]:
            # X1: charged_dc - recovered_neighbor
            rx1 = find_row(
                add3_paired,
                comparison_group="X1",
                stream_mode=stream,
                budget_summaries=b_val,
                policy_b="recovered_neighbor",
            )
            m_x1 = float(rx1["recent_diff_mean"])
            lo_x1 = float(rx1["recent_diff_boot_ci95_low"])
            hi_x1 = float(rx1["recent_diff_boot_ci95_high"])
            tlo_x1 = 100.0 * float(rx1["recent_diff_t_ci95_low"])
            thi_x1 = 100.0 * float(rx1["recent_diff_t_ci95_high"])
            macros[f"RNumAddThreeXOneRN{b_tag}{s_tag}"] = fmt_pp_ci(m_x1, lo_x1, hi_x1)
            macros[f"RNumAddThreeXOneRN{b_tag}{s_tag}Cell"] = fmt_pp_ci_nopp(m_x1, lo_x1, hi_x1)
            macros[f"RNumAddThreeXOneRN{b_tag}{s_tag}Mean"] = f"${100.0 * m_x1:+.2f}$~pp"
            macros[f"RNumAddThreeXOneRN{b_tag}{s_tag}AbsMean"] = f"${abs(100.0 * m_x1):.2f}$~pp"
            macros[f"RNumAddThreeXOneRN{b_tag}{s_tag}TInt"] = f"$[{tlo_x1:+.2f},$ ${thi_x1:+.2f}]$~pp"
            macros[f"RNumAddThreeXOneRNOverall{b_tag}{s_tag}"] = fmt_pp_ci(
                float(rx1["overall_diff_mean"]),
                float(rx1["overall_diff_boot_ci95_low"]),
                float(rx1["overall_diff_boot_ci95_high"]),
            )

            # X2: charged_dc - push_vectors and charged_dc - extra_search_online
            for pol_b, p_tag in [("push_vectors", "PV"), ("extra_search_online", "ESO")]:
                rx2 = find_row(
                    add3_paired,
                    comparison_group="X2",
                    stream_mode=stream,
                    budget_summaries=b_val,
                    policy_b=pol_b,
                )
                m_x2 = float(rx2["recent_diff_mean"])
                lo_x2 = float(rx2["recent_diff_boot_ci95_low"])
                hi_x2 = float(rx2["recent_diff_boot_ci95_high"])
                tlo_x2 = 100.0 * float(rx2["recent_diff_t_ci95_low"])
                thi_x2 = 100.0 * float(rx2["recent_diff_t_ci95_high"])
                macros[f"RNumAddThreeXTwo{p_tag}{b_tag}{s_tag}"] = fmt_pp_ci(m_x2, lo_x2, hi_x2)
                macros[f"RNumAddThreeXTwo{p_tag}{b_tag}{s_tag}Cell"] = fmt_pp_ci_nopp(m_x2, lo_x2, hi_x2)
                macros[f"RNumAddThreeXTwo{p_tag}{b_tag}{s_tag}Mean"] = f"${100.0 * m_x2:+.2f}$~pp"
                macros[f"RNumAddThreeXTwo{p_tag}{b_tag}{s_tag}AbsMean"] = f"${abs(100.0 * m_x2):.2f}$~pp"
                macros[f"RNumAddThreeXTwo{p_tag}{b_tag}{s_tag}TInt"] = f"$[{tlo_x2:+.2f},$ ${thi_x2:+.2f}]$~pp"
                macros[f"RNumAddThreeXTwo{p_tag}Overall{b_tag}{s_tag}"] = fmt_pp_ci(
                    float(rx2["overall_diff_mean"]),
                    float(rx2["overall_diff_boot_ci95_low"]),
                    float(rx2["overall_diff_boot_ci95_high"]),
                )

            # X3: charged_dc - uncharged_dc
            rx3 = find_row(
                add3_paired,
                comparison_group="X3",
                stream_mode=stream,
                budget_summaries=b_val,
                policy_b="uncharged_dc",
            )
            m_x3 = float(rx3["recent_diff_mean"])
            lo_x3 = float(rx3["recent_diff_boot_ci95_low"])
            hi_x3 = float(rx3["recent_diff_boot_ci95_high"])
            macros[f"RNumAddThreeXThreeDC{b_tag}{s_tag}"] = fmt_pp_ci(m_x3, lo_x3, hi_x3)
            macros[f"RNumAddThreeXThreeDC{b_tag}{s_tag}Cell"] = fmt_pp_ci_nopp(m_x3, lo_x3, hi_x3)
            macros[f"RNumAddThreeXThreeDC{b_tag}{s_tag}Mean"] = f"${100.0 * m_x3:+.2f}$~pp"
            macros[f"RNumAddThreeXThreeDC{b_tag}{s_tag}AbsMean"] = f"${abs(100.0 * m_x3):.2f}$~pp"

        # X4: centroid Q=800 B=4
        for pol_b, p_tag in [
            ("extra_search", "ES"),
            ("push_vectors", "PV"),
            ("extra_search_online", "ESO"),
            ("uncharged_dc", "DC"),
        ]:
            rx4 = find_row(
                add3_paired,
                comparison_group="X4",
                stream_mode=stream,
                budget_summaries="4",
                queries_per_epoch="800",
                policy_b=pol_b,
            )
            m_x4 = float(rx4["recent_diff_mean"])
            lo_x4 = float(rx4["recent_diff_boot_ci95_low"])
            hi_x4 = float(rx4["recent_diff_boot_ci95_high"])
            tlo_x4 = 100.0 * float(rx4["recent_diff_t_ci95_low"])
            thi_x4 = 100.0 * float(rx4["recent_diff_t_ci95_high"])
            macros[f"RNumAddThreeXFour{p_tag}{s_tag}"] = fmt_pp_ci(m_x4, lo_x4, hi_x4)
            macros[f"RNumAddThreeXFour{p_tag}{s_tag}Cell"] = fmt_pp_ci_nopp(m_x4, lo_x4, hi_x4)
            macros[f"RNumAddThreeXFour{p_tag}{s_tag}Mean"] = f"${100.0 * m_x4:+.2f}$~pp"
            macros[f"RNumAddThreeXFour{p_tag}{s_tag}AbsMean"] = f"${abs(100.0 * m_x4):.2f}$~pp"
            macros[f"RNumAddThreeXFour{p_tag}{s_tag}TInt"] = f"$[{tlo_x4:+.2f},$ ${thi_x4:+.2f}]$~pp"
            macros[f"RNumAddThreeXFour{p_tag}{s_tag}BootLow"] = f"${100.0 * lo_x4:+.2f}$~pp"
            macros[f"RNumAddThreeXFour{p_tag}Overall{s_tag}"] = fmt_pp_ci(
                float(rx4["overall_diff_mean"]),
                float(rx4["overall_diff_boot_ci95_low"]),
                float(rx4["overall_diff_boot_ci95_high"]),
            )

    # Decomposition shares and B=2 total gap (RN minus uncharged DC)
    rx1_b4_nt = find_row(add3_paired, comparison_group="X1", stream_mode="new_topic", budget_summaries="4")
    rx3_b4_nt = find_row(add3_paired, comparison_group="X3", stream_mode="new_topic", budget_summaries="4")
    choice_share_b4 = 100.0 * float(rx1_b4_nt["recent_diff_mean"]) / (
        float(rx1_b4_nt["recent_diff_mean"]) - float(rx3_b4_nt["recent_diff_mean"])
    )
    macros["RNumAddThreeXOneChoiceShareBFour"] = f"${int(round(choice_share_b4))}\\%$"

    rx1_b2_nt = find_row(add3_paired, comparison_group="X1", stream_mode="new_topic", budget_summaries="2")
    rx3_b2_nt = find_row(add3_paired, comparison_group="X3", stream_mode="new_topic", budget_summaries="2")
    rn_minus_dc_b2_nt_abs = 100.0 * (
        float(rx1_b2_nt["recent_diff_mean"]) - float(rx3_b2_nt["recent_diff_mean"])
    )
    macros["RNumAddThreeRNMinusUnchargedDCBTwoNewTopicAbsMean"] = f"${rn_minus_dc_b2_nt_abs:.2f}$~pp"

    # Forced-by-cap refresh counts at B=4 new_topic (100 vs 16 across 750 refreshes)
    dc_forced_sum_b4_nt = sum(
        int(r["total_forced_by_cap"])
        for r in add3_epochs
        if r["experiment"] == "addendum3/M3_dc_charged"
        and r["stream_mode"] == "new_topic"
        and r["budget_summaries"] == "4"
    )
    r_rn_b4_nt_cond = find_row(
        add3_conds,
        experiment="main/M3",
        stream_mode="new_topic",
        budget_summaries="4",
        policy="recovered_neighbor",
    )
    rn_forced_sum_b4_nt = int(round(5.0 * float(r_rn_b4_nt_cond["forced_by_cap_mean"])))
    tot_refreshes_b4_nt = int(round(5.0 * float(r_rn_b4_nt_cond["summaries_refreshed_mean"])))
    macros["RNumAddThreeChargedDCForcedSumBFourNewTopic"] = fmt_int(dc_forced_sum_b4_nt)
    macros["RNumAddThreeRNForcedSumBFourNewTopic"] = fmt_int(rn_forced_sum_b4_nt)
    macros["RNumAddThreeTotalRefreshesBFour"] = fmt_int(tot_refreshes_b4_nt)

    # Per-seed and per-epoch computations from stored Addendum 3 and main runs
    x1_pos_seeds_b4_b8 = 0
    for b_val in [4, 8]:
        for s_id in [42, 43, 44, 45, 46]:
            qa_c = np.load(
                ADD3_RESULTS_DIR
                / "M3_dc_charged"
                / f"data_change_S32_m2_B{b_val}_Q200_new_topic_time_segment_g_W2_kmeans_bursts0_setup0_starvation_cap_arith_seed{s_id}_dcscore1"
                / "queries.npz"
            )["recent_recall_at_10"]
            qb_rn = np.load(
                MAIN_DIR
                / "M3"
                / f"recovered_neighbor_S32_m2_B{b_val}_Q200_new_topic_time_segment_g_W2_kmeans_bursts0_setup0_starvation_cap_arith_seed{s_id}"
                / "queries.npz"
            )["recent_recall_at_10"]
            m_q = ~np.isnan(qa_c) & ~np.isnan(qb_rn)
            if float(np.mean(qa_c[m_q] - qb_rn[m_q])) > 0.0:
                x1_pos_seeds_b4_b8 += 1
    macros["RNumAddThreeXOnePosSeedsBFourBEight"] = fmt_int(x1_pos_seeds_b4_b8)

    # X1 new_topic B=4 split by all-zero RN estimate epochs (62 vs 188 epochs) from stored epochs.parquet & queries.npz
    all_zero_diffs_b4: list[np.ndarray] = []
    non_zero_diffs_b4: list[np.ndarray] = []
    for s_id in [42, 43, 44, 45, 46]:
        rn_dir = (
            MAIN_DIR
            / "M3"
            / f"recovered_neighbor_S32_m2_B4_Q200_new_topic_time_segment_g_W2_kmeans_bursts0_setup0_starvation_cap_arith_seed{s_id}"
        )
        dc_dir = (
            ADD3_RESULTS_DIR
            / "M3_dc_charged"
            / f"data_change_S32_m2_B4_Q200_new_topic_time_segment_g_W2_kmeans_bursts0_setup0_starvation_cap_arith_seed{s_id}_dcscore1"
        )
        df_ep = pd.read_parquet(rn_dir / "epochs.parquet")
        is_all_zero = df_ep["spearman_rho"].isna() & df_ep["spearman_rho_new_vecs"].notna()
        q_rn_ep = np.load(rn_dir / "queries.npz")["recent_recall_at_10"].reshape(50, 200)
        q_dc_ep = np.load(dc_dir / "queries.npz")["recent_recall_at_10"].reshape(50, 200)
        for ep_i in range(50):
            a_ep = q_dc_ep[ep_i]
            b_ep = q_rn_ep[ep_i]
            m_ep = ~np.isnan(a_ep) & ~np.isnan(b_ep)
            if bool(is_all_zero.iloc[ep_i]):
                all_zero_diffs_b4.append(a_ep[m_ep] - b_ep[m_ep])
            else:
                non_zero_diffs_b4.append(a_ep[m_ep] - b_ep[m_ep])
    az_cat = np.concatenate(all_zero_diffs_b4)
    nz_cat = np.concatenate(non_zero_diffs_b4)
    macros["RNumAddThreeXOneAllZeroGapBFourNewTopic"] = f"${100.0 * float(np.mean(az_cat)):+.2f}$~pp"
    macros["RNumAddThreeXOneAllZeroQueriesBFourNewTopic"] = fmt_int(len(az_cat))
    macros["RNumAddThreeXOneNonZeroGapBFourNewTopic"] = f"${100.0 * float(np.mean(nz_cat)):+.2f}$~pp"
    macros["RNumAddThreeXOneNonZeroQueriesBFourNewTopic"] = fmt_int(len(nz_cat))

    # X2 charged_dc - extra_search_online at B=8 new_topic negative seed
    x2_eso_b8_seeds: list[float] = []
    for s_id in [42, 43, 44, 45, 46]:
        qa_dc8 = np.load(
            ADD3_RESULTS_DIR
            / "M3_dc_charged"
            / f"data_change_S32_m2_B8_Q200_new_topic_time_segment_g_W2_kmeans_bursts0_setup0_starvation_cap_arith_seed{s_id}_dcscore1"
            / "queries.npz"
        )["recent_recall_at_10"]
        qb_eso8 = np.load(
            MAIN_DIR
            / "M3"
            / f"extra_search_online_S32_m2_B8_Q200_new_topic_time_segment_g_W2_kmeans_bursts0_setup0_starvation_cap_arith_seed{s_id}"
            / "queries.npz"
        )["recent_recall_at_10"]
        m8 = ~np.isnan(qa_dc8) & ~np.isnan(qb_eso8)
        x2_eso_b8_seeds.append(100.0 * float(np.mean(qa_dc8[m8] - qb_eso8[m8])))
    min_x2_eso_b8_idx = int(np.argmin(x2_eso_b8_seeds))
    macros["RNumAddThreeXTwoESOBEightNewTopicNegSeed"] = f"${42 + min_x2_eso_b8_idx}$"
    macros["RNumAddThreeXTwoESOBEightNewTopicNegSeedGap"] = (
        f"${x2_eso_b8_seeds[min_x2_eso_b8_idx]:+.2f}$~pp"
    )

    # X4 charged_dc - extra_search at Q=800 new_topic early (epochs 0..29) vs late (epochs 30..49)
    x4_es_early: list[np.ndarray] = []
    x4_es_late: list[np.ndarray] = []
    for s_id in [42, 43, 44, 45, 46]:
        qa_m1c = np.load(
            ADD3_RESULTS_DIR
            / "M1_dc_charged"
            / f"data_change_S32_m2_B4_Q800_new_topic_nearest_initial_W2_kmeans_bursts0_setup0_starvation_cap_arith_seed{s_id}_dcscore1"
            / "queries.npz"
        )["recent_recall_at_10"].reshape(50, 800)
        qb_m1es = np.load(
            MAIN_DIR
            / "M1"
            / f"extra_search_S32_m2_B4_Q800_new_topic_nearest_initial_W2_kmeans_bursts0_setup0_starvation_cap_arith_seed{s_id}"
            / "queries.npz"
        )["recent_recall_at_10"].reshape(50, 800)
        d_m1 = qa_m1c - qb_m1es
        d_e = d_m1[:30]
        d_l = d_m1[30:]
        x4_es_early.append(d_e[~np.isnan(d_e)])
        x4_es_late.append(d_l[~np.isnan(d_l)])
    macros["RNumAddThreeXFourESNewTopicEarlyMean"] = (
        f"${100.0 * float(np.mean(np.concatenate(x4_es_early))):+.2f}$~pp"
    )
    macros["RNumAddThreeXFourESNewTopicLateMean"] = (
        f"${100.0 * float(np.mean(np.concatenate(x4_es_late))):+.2f}$~pp"
    )

    return macros


def get_used_macros() -> set[str]:
    tex_files = [PAPER_DIR / "main.tex"] + sorted((PAPER_DIR / "sections").glob("*.tex"))
    used: set[str] = set()
    pattern = re.compile(r"\\(RNum[A-Za-z]+)")
    for tf in tex_files:
        text = tf.read_text(encoding="utf-8")
        # Ignore LaTeX comments
        for line in text.splitlines():
            clean = re.sub(r"(?<!\\)%.*$", "", line)
            for m in pattern.findall(clean):
                used.add(m)
    return used


def main() -> None:
    macros = build_all_macros()
    used = get_used_macros()

    # Check if any used macro is missing
    missing = sorted(used - set(macros.keys()))
    if missing:
        raise RuntimeError(f"Used macros not defined in gen_numbers.py: {missing}")

    # Filter to only used macros if any are used; otherwise write all
    keys_to_write = sorted(used) if used else sorted(macros.keys())

    lines = [
        "% Automatically generated by paper/scripts/gen_numbers.py from analysis_root/out_addendum2/*.csv,",
        "% analysis_root/out_addendum3/*.csv, raw run outputs (queries.npz, epochs.parquet, summary.json,",
        "% failed_runs.json, RUN_ADD3.log), repo/results/pilot/, and constants from audit/M3_DESIGN_CHECK.md.",
        "% Do not edit by hand.",
    ]
    for k in keys_to_write:
        lines.append(f"\\newcommand{{\\{k}}}{{{macros[k]}}}")

    NUMBERS_TEX.parent.mkdir(parents=True, exist_ok=True)
    NUMBERS_TEX.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {len(keys_to_write)} macros to {NUMBERS_TEX}")

    # The short plain-English paper (paper_plain/) reads every macro, unpruned, from its own file.
    all_lines = lines[:4] + [f"\\newcommand{{\\{k}}}{{{macros[k]}}}" for k in sorted(macros)]
    PLAIN_NUMBERS_TEX.parent.mkdir(parents=True, exist_ok=True)
    PLAIN_NUMBERS_TEX.write_text("\n".join(all_lines) + "\n", encoding="utf-8")
    print(f"Wrote {len(macros)} macros to {PLAIN_NUMBERS_TEX}")


if __name__ == "__main__":
    main()
