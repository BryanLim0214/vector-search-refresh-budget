#!/usr/bin/env python3
"""Generate paper/figures/fig_m2_m3_budget.pdf from analysis_root/out_addendum2/*.csv and out_addendum3/*.csv."""

from __future__ import annotations

import csv
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "analysis_root" / "out_addendum2"
ADD3_OUT_DIR = ROOT / "analysis_root" / "out_addendum3"
FIG_DIR = ROOT / "paper" / "figures"


def read_csv(name: str, base_dir: Path = OUT_DIR) -> list[dict[str, str]]:
    path = base_dir / name
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def find_row(rows: list[dict[str, str]], **kwargs: str) -> dict[str, str]:
    for r in rows:
        if all(str(r.get(k)) == str(v) for k, v in kwargs.items()):
            return r
    raise KeyError(f"Row not found for {kwargs}")


def extract_series(rows: list[dict[str, str]], stream: str, policy: str, budgets: list[int]):
    means, lows, highs = [], [], []
    for b in budgets:
        r = find_row(rows, stream_mode=stream, budget_summaries=str(b), policy=policy)
        m = float(r["recent_recall_at_10_mean"])
        lo = float(r["recent_recall_at_10_ci95_low"])
        hi = float(r["recent_recall_at_10_ci95_high"])
        means.append(m)
        lows.append(m - lo)
        highs.append(hi - m)
    return means, [lows, highs]


def main() -> None:
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    m2 = read_csv("m2_conditions.csv")
    m3 = read_csv("m3_conditions.csv")
    add3_m3 = [
        r for r in read_csv("add3_conditions.csv", ADD3_OUT_DIR)
        if r.get("experiment") == "addendum3/M3_dc_charged"
    ]

    plt.rcParams.update({
        "font.family": "serif",
        "font.size": 7.5,
        "axes.titlesize": 8.0,
        "axes.labelsize": 7.5,
        "xtick.labelsize": 7.0,
        "ytick.labelsize": 7.0,
        "legend.fontsize": 6.6,
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
    })

    budgets = [2, 4, 8]
    fig, axes = plt.subplots(2, 2, figsize=(3.48, 2.30), sharex=True)

    # Colors and styles (colorblind-safe palette)
    c_dc = "#0072B2"    # blue: data-change / M2 refresh rules
    c_rn = "#56B4E9"    # sky blue: recovered-neighbor refresh
    c_es = "#009E73"    # bluish green: extra shard search
    c_pv = "#D55E00"    # vermilion: vector push
    c_af = "#333333"    # dark gray: always fresh
    c_nr = "#888888"    # medium gray: never refresh

    panels = [
        (axes[0, 0], m2, "steady", "(a) M2 (1 active): Steady", False),
        (axes[0, 1], m2, "new_topic", "(b) M2 (1 active): New-topic", False),
        (axes[1, 0], m3, "steady", "(c) M3 (8 active): Steady", True),
        (axes[1, 1], m3, "new_topic", "(d) M3 (8 active): New-topic", True),
    ]

    for ax, rows, stream, title, is_m3 in panels:
        nr_row = find_row(rows, stream_mode=stream, budget_summaries="4", policy="never_refresh")
        af_row = find_row(rows, stream_mode=stream, budget_summaries="4", policy="always_fresh")
        nr_val = float(nr_row["recent_recall_at_10_mean"])
        af_val = float(af_row["recent_recall_at_10_mean"])

        ax.axhline(nr_val, color=c_nr, linestyle="--", linewidth=0.9, label="Never refresh")
        ax.axhline(af_val, color=c_af, linestyle=":", linewidth=1.1, zorder=4, label="Always fresh")

        # Data-change (or all 5 refresh rules in M2, which equal always_fresh)
        dc_m, dc_err = extract_series(rows, stream, "data_change", budgets)
        ax.errorbar(
            budgets, dc_m, yerr=dc_err, color=c_dc, marker="o", markersize=3.8,
            linewidth=1.2, capsize=2.0, label="Data-change refresh (M2: all 5 rules)"
        )

        if is_m3:
            dcc_m, dcc_err = extract_series(add3_m3, stream, "data_change", budgets)
            ax.errorbar(
                budgets, dcc_m, yerr=dcc_err, color=c_dc, linestyle="--",
                marker="o", markerfacecolor="white", markersize=3.8,
                linewidth=1.2, capsize=2.0, label="Data-change, scores charged (Add. 3)"
            )
            rn_m, rn_err = extract_series(rows, stream, "recovered_neighbor", budgets)
            ax.errorbar(
                budgets, rn_m, yerr=rn_err, color=c_rn, marker="s", markersize=3.5,
                linestyle="-.", linewidth=1.1, capsize=2.0, label="Recovered-neighbor refresh"
            )
            es_pol = "extra_search_online"
        else:
            es_pol = "extra_search"

        es_m, es_err = extract_series(rows, stream, es_pol, budgets)
        ax.errorbar(
            budgets, es_m, yerr=es_err, color=c_es, marker="^", markersize=3.8,
            linewidth=1.2, capsize=2.0, label="Extra shard search (M2: batch; M3: online)"
        )

        pv_m, pv_err = extract_series(rows, stream, "push_vectors", budgets)
        ax.errorbar(
            budgets, pv_m, yerr=pv_err, color=c_pv, marker="D", markersize=3.5,
            linewidth=1.2, capsize=2.0, label="Vector push"
        )

        ax.set_title(title, pad=3.5)
        ax.set_xticks(budgets)
        ax.set_xticklabels(["2", "4", "8"])
        ax.grid(True, linestyle=":", linewidth=0.5, alpha=0.6)

    axes[0, 0].set_yticks([0.06, 0.12, 0.18])

    fig.supylabel("Recent recall@10", fontsize=7.5, x=0.01)
    fig.supxlabel("Budget $B$ (summary equivalents)", fontsize=7.5, y=0.01)

    # Collect unique legend handles from M3 panel (which has all 7 series)
    handles, labels = axes[1, 0].get_legend_handles_labels()
    fig.legend(
        handles, labels, loc="upper center", bbox_to_anchor=(0.5, 1.02),
        ncol=2, frameon=False, columnspacing=0.9, handletextpad=0.4
    )

    fig.tight_layout(rect=[0.0, 0.03, 1.0, 0.76], pad=0.3, w_pad=0.55, h_pad=0.45)
    out_path = FIG_DIR / "fig_m2_m3_budget.pdf"
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
