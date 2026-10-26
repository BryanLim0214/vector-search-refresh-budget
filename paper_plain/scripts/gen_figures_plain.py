#!/usr/bin/env python3
"""Generate paper_plain/figures/fig_budget.pdf for the short plain-English paper.

Reads only analysis_root/out_addendum2/m2_conditions.csv and m3_conditions.csv, and the charged most-changed rows
(experiment addendum3/M3_dc_charged) of analysis_root/out_addendum3/add3_conditions.csv. No value is typed: every point
and error bar comes from these CSV rows. Recall is shown in percent. Every panel shows never refresh, most-changed refresh
(in one-shard placement every rule gives the same result), one version of extra search, and push. One-shard panels show
planned extra search (the only version run there); eight-shard panels show live extra search, the version in Table II,
and paying most-changed from the exploratory follow-up (most-changed with its score bytes charged).
Each legend entry has one meaning in every panel where it appears.
"""

from __future__ import annotations

import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "analysis_root" / "out_addendum2"
ADD3_DIR = ROOT / "analysis_root" / "out_addendum3"
FIG_DIR = ROOT / "paper_plain" / "figures"


def read_csv(name: str, base: Path = OUT_DIR) -> list[dict[str, str]]:
    with (base / name).open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def find_row(rows: list[dict[str, str]], **kwargs: str) -> dict[str, str]:
    for r in rows:
        if all(str(r.get(k)) == str(v) for k, v in kwargs.items()):
            return r
    raise KeyError(f"Row not found for {kwargs}")


def series(rows, stream: str, policy: str, budgets: list[int]):
    """Mean recent recall (percent) and unpaired 95% error bars per budget."""
    means, lows, highs = [], [], []
    for b in budgets:
        r = find_row(rows, stream_mode=stream, budget_summaries=str(b), policy=policy)
        m = 100.0 * float(r["recent_recall_at_10_mean"])
        means.append(m)
        lows.append(m - 100.0 * float(r["recent_recall_at_10_ci95_low"]))
        highs.append(100.0 * float(r["recent_recall_at_10_ci95_high"]) - m)
    return means, [lows, highs]


def main() -> None:
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    m2 = read_csv("m2_conditions.csv")
    m3 = read_csv("m3_conditions.csv")
    paying = [r for r in read_csv("add3_conditions.csv", ADD3_DIR) if r.get("experiment") == "addendum3/M3_dc_charged"]

    plt.rcParams.update({
        "font.family": "serif",
        "font.serif": ["Times New Roman", "Times", "DejaVu Serif"],
        "font.size": 8.5,
        "axes.titlesize": 8.5,
        "axes.labelsize": 8.5,
        "xtick.labelsize": 8.0,
        "ytick.labelsize": 8.0,
        "legend.fontsize": 8.0,
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
    })

    budgets = [2, 4, 8]
    fig, axes = plt.subplots(2, 2, figsize=(3.48, 2.25), sharex=True)

    # Okabe-Ito colorblind-safe colors, each method also with its own marker and line style.
    never = ("never_refresh", "Never refresh", "#7F7F7F", "x", ":")
    changed = ("data_change", "Most-changed refresh", "#0072B2", "o", "-")
    planned = ("extra_search", "Planned extra search", "#009E73", "^", "--")
    live = ("extra_search_online", "Live extra search", "#CC79A7", "s", (0, (4, 1, 1, 1, 1, 1)))
    push = ("push_vectors", "Push", "#D55E00", "D", "-.")
    paid = ("data_change", "Paying most-changed", "#000000", "v", (0, (3, 2)))
    one_shard = [(never, None), (changed, None), (planned, None), (push, None)]
    eight_shard = [(never, None), (changed, None), (live, None), (paid, paying), (push, None)]
    panels = [
        (axes[0, 0], m2, "steady", "(a) One shard, mixed topics", one_shard),
        (axes[0, 1], m2, "new_topic", "(b) One shard, new topics", one_shard),
        (axes[1, 0], m3, "steady", "(c) Eight shards, mixed topics", eight_shard),
        (axes[1, 1], m3, "new_topic", "(d) Eight shards, new topics", eight_shard),
    ]
    for ax, rows, stream, title, methods in panels:
        for (pol, label, color, marker, ls), src in methods:
            m, e = series(src if src is not None else rows, stream, pol, budgets)
            ax.errorbar(budgets, m, yerr=e, color=color, marker=marker, linestyle=ls, markersize=4.0,
                        linewidth=1.4, capsize=2.2, markerfacecolor="white" if marker == "o" else color,
                        markeredgewidth=1.2, label=label)
        ax.set_title(title, pad=3.0)
        ax.set_xticks(budgets)
        ax.set_xticklabels(["2", "4", "8"])
        ax.grid(True, linestyle=":", linewidth=0.5, alpha=0.6)

    fig.supylabel("Recent recall (%)", fontsize=8.5, x=0.01)
    fig.supxlabel("Budget (summaries per time step)", fontsize=8.5, y=0.01)
    by_label = {}
    for ax in (axes[0, 0], axes[1, 0]):
        for h, lab in zip(*ax.get_legend_handles_labels()):
            by_label.setdefault(lab, h)
    order = ["Never refresh", "Planned extra search", "Most-changed refresh", "Live extra search",
             "Paying most-changed", "Push"]
    fig.legend([by_label[k] for k in order], order, loc="upper center", bbox_to_anchor=(0.5, 1.0), ncol=3,
               frameon=False, columnspacing=0.8, handlelength=1.8, handletextpad=0.3, labelspacing=0.3)
    fig.tight_layout(rect=[0.0, 0.03, 1.0, 0.85], pad=0.3, w_pad=0.6, h_pad=0.6)
    out = FIG_DIR / "fig_budget.pdf"
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
