# Skeptic review of Addendum 3 (data-change with its score bytes charged)

Reviewer: Claude skeptic. Date: 2026-10-25. Scope: the 42 Addendum 3 runs in `repo-prereg2\results\addendum3\`
(M3_dc_charged 30, M1_dc_charged 10, replication 1, M1_replication 1), the stored comparison runs in
`repo-prereg2\results\main\M3`, `repo\results\main\M1` (via `analysis_root\main\M1`) and `repo-prereg2\results\main\M1_online`,
the analysis output in `results\addendum3\out\`, `configs\prereg_addendum3.yaml`, `docs\DEVIATIONS.md` D-17,
`audit\GUARDIAN_D1_addendum3_code.md`, `audit\SKEPTIC_M3.md`, and the paper (`paper\main.tex`, `paper\sections\*.tex`,
`paper\generated\numbers.tex`).

I changed no file in `repo`, `repo-prereg2`, `analysis_root` or `paper`. This report is the only file I wrote outside my
scratchpad. My checks read the raw `summary.json`, `provenance.json`, `queries.npz` and `epochs.parquet` files with my own
code and do not import `analyze_add3.py`. For the registered interval I call `paired_hierarchical_bootstrap_ci` from
`analysis\analyze.py` on paired arrays I build myself; I also run my own two-stage bootstrap (4,000 replicates, RNG seed
777), a five-seed t-interval, an epoch-block bootstrap, leave-one-seed-out means and time splits. For item 2 I reran
10 simulations in memory only (recovered-neighbor and charged data-change, new-topic, B = 4, seeds 42 to 46) with a
logging wrapper around `step_epoch`; nothing was written to `results\`.

Scripts and outputs, all in
`<reviewer scratch folder, not published>\add3\`:
`verify_add3.py` (all 32 rows plus extras, output `verify_out.txt`, `my_comparisons.csv`), `check_epochs.py` (per-epoch
k, bytes, cap, forced counts), `robust.py` (`robust.csv`), `split_allzero.py`, `x4_blocks.py`, `instr_choices.py` and
`analyze_instr.py` (instrumented reruns, logs in `instr\`).

All numbers are recent recall@10 differences in percentage points (pp), pooled over queries and seeds 42 to 46, unless
stated otherwise. "Seeds" lists per-seed gaps in seed order 42, 43, 44, 45, 46. "Charged DC" is data-change with
`charge_dc_scores_to_budget: true`; "DC" alone is the stored, uncharged rule.

## Verdict table

| # | Claim | Verdict | My numbers |
|---|---|---|---|
| 1 | All 32 X1 to X4 rows in `add3_paired_comparisons.csv` | **HOLDS** | All 64 means, bootstrap CIs and t-intervals (recent and overall) match the CSV within 0.005 pp. Pairing is exact. |
| 2 | X1 new-topic B = 4: charged DC minus RN = +2.66 [+1.46, +4.00] (P1) | **HOLDS** | +2.66 [+1.46, +4.00]; own bootstrap [+1.48, +4.08]; t [+0.91, +4.57]; epoch-block [+1.29, +4.10]; seeds +0.93 +2.15 +3.30 +2.43 +4.89. |
| 3 | X1 new-topic B = 8: +1.76 [+1.04, +2.59] | **HOLDS** | t [+0.73, +2.79]; seeds +1.97 +1.29 +3.02 +1.71 +0.82. |
| 4 | X1 new-topic B = 2 and all steady rows: no reliable difference | **HOLDS** | B = 2 +0.48 [-0.48, +1.66]; steady +0.30, +0.19, +0.19, all intervals include 0. |
| 5 | X1 is a clean equal-k comparison | **HOLDS, with two stated differences** | Same k (1, 3, 7 every epoch), same cap (64, 22, 10), same selection code; only the score differs. RN still spends 21,504 B/epoch more and its setup broadcast is uncharged (both favour RN). |
| 6 | Why RN loses at equal k | **Explained (exploratory)** | RN's estimate is zero for about 23 of 25 changed shards per epoch; 53% of its picks are made with a zero estimate; 100 vs 16 forced-by-cap refreshes; it captures 34% vs 57% of the best possible lift-in neighbours. |
| 7 | X2 new-topic B = 2: charged DC minus PV = -0.16 [-1.89, +1.49] (P2) | **HOLDS** (as "no lead", not "PV wins") | t [-2.56, +2.18]; seeds -0.81 +2.07 -2.69 -0.86 +1.35. |
| 8 | X2 PV rows at B = 4, 8 and on steady | **HOLDS** | new-topic +0.10 [-1.40, +1.52], -3.42 [-5.15, -1.75]; steady -1.58, -3.87, -7.16, all seeds negative. |
| 9 | X2 charged DC minus online extra search, new-topic | B = 4 **HOLDS**; B = 8 **FRAGILE**; B = 2 not significant | B = 4 +2.92 [+1.39, +4.26]; B = 8 +1.63 [+0.27, +2.95], t [+0.10, +3.17], seed 42 negative, and online extra search wins overall recall@10 by 3.43 pp; B = 2 +1.12 [-0.39, +2.62]. |
| 10 | X3 cost of charging: -1.63, -0.94, +0.16 at B = 2, 4, 8 (new-topic) | **HOLDS, but mislabelled if read as "the cost of 128 bytes"** | It is the cost of losing one whole summary per epoch under the zero-slack floor. Charged DC leaves 24,448 B unspent every epoch. |
| 11 | X4 new-topic Q = 800: charged DC still overtakes batch extra search, +0.38 [+0.01, +0.74] | **FRAGILE** | Seeds all positive (+0.29 +0.21 +0.71 +0.26 +0.47), t [+0.13, +0.64]; but the bootstrap lower bound is -0.01 to +0.03 across 20 RNG seeds, the lead is absent in epochs 30 to 49, overall recall@10 reverses (-0.20 [-0.38, +0.02]), and a Holm correction over the 8 X4 rows would not keep it. |
| 12 | X4 other rows | **HOLDS** | new-topic vs online extra search +0.43 [+0.07, +0.79] (same caveats as row 11, slightly stronger); vs PV -2.39 [-2.84, -1.99]; steady vs batch extra search -0.49 [-0.78, -0.16]. |
| 13 | Run integrity (commit, configs, replications, folders) | **HOLDS, one disclosure** | All 42 runs at 7c4201d with the committed configs. 22 of the 30 M3 charged runs failed once with memory errors and were rerun; my 10 reruns reproduce stored per-query results exactly. |

---

## 1. Independent recomputation

**Verdict: HOLDS.** My means and both interval types match the CSV for all 32 rows on both metrics (largest difference
0.005 pp, which is rounding). My own bootstrap with a different RNG seed differs from the registered one by at most 0.12 pp
at any bound.

Recent recall@10, all rows (registered bootstrap, own bootstrap, five-seed t-interval):

| Row | Mean | Bootstrap | Own bootstrap | t-interval | Seeds | Seeds > 0 |
|---|---|---|---|---|---|---|
| X1 RN steady B2 | +0.30 | [-0.29, +0.88] | [-0.30, +0.90] | [-0.44, +1.05] | +0.57 -0.37 -0.19 +1.11 +0.41 | 3/5 |
| X1 RN steady B4 | +0.19 | [-0.29, +0.63] | [-0.29, +0.65] | [-0.26, +0.64] | +0.30 +0.23 -0.38 +0.17 +0.64 | 4/5 |
| X1 RN steady B8 | +0.19 | [-0.29, +0.64] | [-0.29, +0.65] | [-0.46, +0.83] | +0.68 +0.58 +0.17 -0.64 +0.17 | 4/5 |
| X1 RN new_topic B2 | +0.48 | [-0.48, +1.66] | [-0.52, +1.67] | [-0.88, +1.95] | -0.55 +0.55 +0.48 -0.20 +2.39 | 3/5 |
| **X1 RN new_topic B4** | **+2.66** | **[+1.46, +4.00]** | [+1.48, +4.08] | [+0.91, +4.57] | +0.93 +2.15 +3.30 +2.43 +4.89 | 5/5 |
| X1 RN new_topic B8 | +1.76 | [+1.04, +2.59] | [+1.02, +2.55] | [+0.73, +2.79] | +1.97 +1.29 +3.02 +1.71 +0.82 | 5/5 |
| X2 PV steady B2 | -1.58 | [-2.11, -1.06] | [-2.15, -1.07] | [-1.97, -1.19] | -1.38 -2.09 -1.29 -1.48 -1.65 | 0/5 |
| X2 PV steady B4 | -3.87 | [-4.72, -3.11] | [-4.69, -3.12] | [-4.76, -3.00] | -5.03 -3.34 -3.51 -4.09 -3.41 | 0/5 |
| X2 PV steady B8 | -7.16 | [-7.98, -6.33] | [-7.99, -6.31] | [-7.99, -6.36] | -7.57 -6.28 -7.22 -7.97 -6.83 | 0/5 |
| **X2 PV new_topic B2** | **-0.16** | **[-1.89, +1.49]** | [-1.82, +1.49] | [-2.56, +2.18] | -0.81 +2.07 -2.69 -0.86 +1.35 | 2/5 |
| X2 PV new_topic B4 | +0.10 | [-1.40, +1.52] | [-1.32, +1.64] | [-1.70, +1.96] | -1.48 +0.92 -1.19 +0.34 +2.05 | 3/5 |
| X2 PV new_topic B8 | -3.42 | [-5.15, -1.75] | [-5.11, -1.65] | [-5.56, -1.28] | -4.71 -1.59 -5.24 -3.95 -1.61 | 0/5 |
| X2 ESO steady B2 | -0.87 | [-1.41, -0.36] | [-1.41, -0.34] | [-1.40, -0.33] | -0.71 -1.49 -1.03 -0.31 -0.79 | 0/5 |
| X2 ESO steady B4 | -2.29 | [-2.90, -1.68] | [-2.87, -1.69] | [-2.85, -1.74] | -2.67 -2.23 -2.69 -2.30 -1.58 | 0/5 |
| X2 ESO steady B8 | -4.28 | [-5.17, -3.42] | [-5.13, -3.42] | [-5.31, -3.25] | -3.20 -3.85 -4.69 -5.39 -4.29 | 0/5 |
| X2 ESO new_topic B2 | +1.12 | [-0.39, +2.62] | [-0.33, +2.59] | [-0.90, +3.15] | -0.86 +3.10 +0.02 +0.99 +2.38 | 4/5 |
| X2 ESO new_topic B4 | +2.92 | [+1.39, +4.26] | [+1.36, +4.32] | [+1.11, +4.81] | +0.42 +3.90 +2.96 +3.43 +4.10 | 5/5 |
| X2 ESO new_topic B8 | +1.63 | [+0.27, +2.95] | [+0.28, +3.01] | [+0.10, +3.17] | -0.17 +3.27 +2.00 +1.41 +1.68 | 4/5 |
| X3 DCu steady B2 | +0.33 | [-0.39, +1.08] | [-0.41, +1.08] | [-0.64, +1.32] | +1.35 -0.55 +0.23 +0.90 -0.25 | 3/5 |
| X3 DCu steady B4 | -0.20 | [-0.71, +0.30] | [-0.68, +0.31] | [-0.82, +0.42] | -0.64 -0.32 +0.03 +0.56 -0.62 | 2/5 |
| X3 DCu steady B8 | +0.00 | [-0.17, +0.19] | [-0.17, +0.19] | [-0.15, +0.16] | +0.00 -0.02 +0.21 -0.04 -0.13 | 1/5 |
| X3 DCu new_topic B2 | -1.63 | [-2.90, -0.48] | [-2.88, -0.45] | [-3.33, +0.05] | -0.52 -1.30 -3.08 -3.05 -0.24 | 0/5 |
| X3 DCu new_topic B4 | -0.94 | [-1.55, -0.32] | [-1.57, -0.31] | [-1.56, -0.30] | -0.57 -1.65 -1.27 -0.71 -0.45 | 0/5 |
| X3 DCu new_topic B8 | +0.16 | [-0.10, +0.41] | [-0.09, +0.41] | [+0.02, +0.31] | +0.24 +0.13 +0.25 +0.22 -0.03 | 4/5 |
| X4 ES steady | -0.49 | [-0.78, -0.16] | [-0.78, -0.17] | [-0.90, -0.08] | -0.50 -0.64 -0.49 +0.04 -0.85 | 1/5 |
| **X4 ES new_topic** | **+0.38** | **[+0.01, +0.74]** | [+0.02, +0.76] | [+0.13, +0.64] | +0.29 +0.21 +0.71 +0.26 +0.47 | 5/5 |
| X4 ESO steady | -0.45 | [-0.75, -0.12] | [-0.74, -0.14] | [-0.86, -0.05] | -0.49 -0.60 -0.43 +0.07 -0.82 | 1/5 |
| X4 ESO new_topic | +0.43 | [+0.07, +0.79] | [+0.08, +0.80] | [+0.19, +0.68] | +0.33 +0.30 +0.76 +0.30 +0.50 | 5/5 |
| X4 PV steady | -1.54 | [-1.96, -1.12] | [-1.96, -1.12] | [-2.12, -0.96] | -2.07 -1.47 -1.27 -0.95 -1.94 | 0/5 |
| X4 PV new_topic | -2.39 | [-2.84, -1.99] | [-2.81, -1.98] | [-2.73, -2.08] | -2.22 -2.18 -2.44 -2.34 -2.84 | 0/5 |
| X4 DCu steady | +0.17 | [-0.01, +0.35] | [-0.01, +0.35] | [-0.06, +0.41] | +0.19 +0.33 +0.09 +0.36 -0.11 | 4/5 |
| X4 DCu new_topic | -0.63 | [-0.89, -0.38] | [-0.90, -0.38] | [-0.85, -0.42] | -0.39 -0.76 -0.75 -0.52 -0.77 | 0/5 |

Two extra rows I computed that the plan did not list (charged DC and RN both refresh 3 summaries per epoch at M1,
Q = 800, B = 4): new-topic +0.75 [+0.42, +1.10], seeds +0.91 +0.76 +1.08 +0.42 +0.59; steady +0.24 [+0.08, +0.39],
seeds +0.19 +0.28 +0.24 +0.37 +0.11. These agree with X1 but are unplanned; label them as such if used.

Overall recall@10 rows also match. The ones the paper needs: X1 new-topic B = 4 +0.42 [+0.11, +0.69] (t [-0.02, +0.86]);
X2 PV new-topic B = 2 +0.62 [+0.35, +0.90]; X2 ESO new-topic B = 4 -0.79 [-1.26, -0.25] and B = 8 -3.43 [-3.77, -3.02];
X4 ES new-topic -0.20 [-0.38, +0.02] (seeds -0.36 -0.33 +0.19 -0.39 -0.09).

**Pairing evidence (every seed of every comparison, 250 run pairs including the always-fresh, never-refresh and
extra RN pairs, no exception):**
- Provenance `config` of a charged run and its stored partner differ only in `charge_dc_scores_to_budget` and the derived
  `starvation_cap_epochs` (22 vs 16 at B = 4); `data_provenance_sha256` and all package versions are identical.
- Equal `total_queries_evaluated`, `total_queries_with_recent_neighbors`, `initial_set_size`, `stream_set_size`,
  `queries_with_most_neighbors_in_stream` and `mean_stream_neighbors_in_top_10`.
- Identical NaN masks of per-query recent recall, identical per-epoch counts of queries with recent neighbours and
  identical per-epoch insert counts. So the query schedule and ground truth are the same.
- Per-epoch means rebuilt from `queries.npz` equal `epochs.parquet` in every run checked (200 or 800 queries per epoch,
  in epoch order).
- Comparison runs come from three commits: M3 and M1-online at 67f049d, M1 at 625f307, charged runs at 7c4201d. Both
  replication runs (flag off) reproduce the stored runs exactly (`replication_check.csv`, status PASS), and SKEPTIC_M3
  already showed the M1 and M1-online ground-truth caches are identical.

## 2. Did the charged runs do what they claim, and is X1 clean?

**Verdict: HOLDS.** Every charged run refreshed exactly k summaries in every one of its 50 epochs, paid exactly 128 B on
top of them, used the planned cap, and spent no other budgeted bytes:

| Placement, B | k every epoch (min = max) | Extra budget bytes/epoch | Cap | Budget spent/epoch | Unspent/epoch |
|---|---|---|---|---|---|
| M3, B = 2 | 1 | 128 | 64 | 24,704 | 24,448 (50%) |
| M3, B = 4 | 3 | 128 | 22 | 73,856 | 24,448 (25%) |
| M3, B = 8 | 7 | 128 | 10 | 172,160 | 24,448 (12%) |
| M1, B = 4, Q = 800 | 3 | 128 | 22 | 73,856 | 24,448 (25%) |

Forced-by-cap refreshes, summed over 5 seeds (refreshes in brackets):

| Stream, B | Charged DC | RN (same k, same cap) | Uncharged DC | RN all-zero-estimate epochs (of 250) |
|---|---|---|---|---|
| M3 new-topic, 2 | 0 (250) | 0 (250) | 6 (500) | 8 |
| M3 new-topic, 4 | **16 (750)** | **100 (750)** | 24 (1,000) | 62 |
| M3 new-topic, 8 | 11 (1,750) | 93 (1,750) | 0 (2,000) | 81 |
| M3 steady, 2 | 0 | 0 | 0 | 50 |
| M3 steady, 4 | 0 | 129 | 0 | 121 |
| M3 steady, 8 | 0 | 87 | 0 | 185 |
| M1 new-topic, 4, Q = 800 | 36 (750) | 163 (750) | 69 (1,000) | 90 |
| M1 steady, 4, Q = 800 | 0 | 204 | 0 | 167 |

At B = 2 the cap (64 epochs) never fires in a 50-epoch run, for either rule.

**Is X1 clean?** Yes, as an equal-k comparison:
- k and the cap are identical in every epoch (table above).
- Both rules go through the same selection code (`dvs\policies.py`, `np.lexsort((shard_ids, -priorities, -is_starved,
  -has_new))`): shards with new vectors first, then shards past the cap, then the rule's scaled score, then lowest shard ID.
  The only input that differs is the raw score (data-change score vs RN estimate).
- RN's probing code also runs in data-change runs (for Spearman logging, outside the timer), so any side effect would
  be the same in both. None was found by SKEPTIC_M3.
- Two differences remain, and both favour RN: RN spends 21,504 B/epoch more than charged DC (46,208 vs 24,704 at B = 2;
  95,360 vs 73,856 at B = 4; 193,664 vs 172,160 at B = 8), and its 3,145,728-byte setup broadcast is still uncharged.
  So X1 is equal k and equal budget B, not equal bytes spent.

**Tie-breaking when RN's estimate is all zero.** This is not an extra difference between the rules; it is how RN
behaves. Data-change scores are continuous and never tie among changed shards, so data-change never reaches the shard-ID
tie-break. RN reaches it often. Splitting X1 new-topic by epochs where RN's estimate is zero for every changed shard
(Spearman proxy from SKEPTIC_M3; my instrumented reruns count the same 62 epochs directly at B = 4):

| B | All-zero epochs | Gap in those epochs | Gap in other epochs |
|---|---|---|---|
| 2 | 8/250 (199 queries) | +2.90 | +0.41 (6,176 queries) |
| 4 | 62/250 (1,428 queries) | +5.42, seeds +2.58 +6.15 +5.13 +7.42 +5.22 | +1.86 (4,947 queries), seeds +0.62 +0.74 +2.69 +1.27 +4.76 |
| 8 | 81/250 (1,849 queries) | +1.97 | +1.67 (4,526 queries) |

At B = 4 the all-zero epochs hold 22% of the queries but give 46% of the gap; RN still loses in the other epochs in all
five seeds. The trajectories diverge after the first epoch, so this split is descriptive, not causal.

**What explains RN's loss at equal k (exploratory, from my 10 instrumented reruns, new-topic, B = 4).** All 10 reruns
reproduce the stored per-query recall arrays exactly (recent and overall). Means over seeds:

| Quantity | RN | Charged DC |
|---|---|---|
| Shards holding unrefreshed new vectors, per epoch | 25.0 | 23.3 |
| ...of which the rule's own score is zero | 23.0 | 0 |
| Refreshes picked with a zero score (by cap or lowest shard ID) | 397 of 750 (53%) | 0 of 750 |
| Forced by cap | 100 | 16 |
| Share of the best possible lift-in neighbour count captured by the k = 3 picks | 0.34 (seeds 0.43, 0.39, 0.29, 0.35, 0.25) | 0.57 (0.57, 0.59, 0.50, 0.63, 0.53) |
| Spearman, own score vs lift-in neighbour counts | 0.26 | 0.51 |
| Spearman, new-vector count vs lift-in neighbour counts (same run) | 0.63 | 0.52 |

Plain reading: the 64 sample queries rarely hit a changed shard, so RN's estimate is non-zero for only about 2 shards per
epoch, fewer than the 3 it must refresh. The rest of its refreshes go to the lowest-numbered changed shard or to shards
that waited past the cap, not to the shards with the most recent change. Data-change scores every changed shard and so
ranks all of them. The data-change Spearman (0.51) is a new scratch measurement; the paper currently says "rho for
data-change was not measured" and should keep saying so unless the committed analysis computes it.

**Decomposition of the registered H3b loss.** Because all runs share the same queries, paired gaps add exactly:
RN minus uncharged DC = (RN minus charged DC) + (charged DC minus uncharged DC).
- B = 4: -3.60 = -2.66 (shard choice at equal k) + -0.94 (one fewer refresh). Choice is 74% of the loss.
- B = 2: -2.11 = -0.48 (not significant) + -1.63. The refresh count is 77% of the loss.
- B = 8: -1.59 (the paper's RN minus always-fresh) = -1.76 + 0.16, up to rounding. All of it is choice.

## 3. X4: does charged data-change still overtake batch extra search at Q = 800?

**Verdict: FRAGILE.** The direction is consistent, the size is tiny and the evidence is thin.

| Check | Charged DC minus batch ES | Charged DC minus online ES |
|---|---|---|
| Pooled mean | +0.38 | +0.43 |
| Registered bootstrap (seed 42) | [+0.01, +0.74] | [+0.07, +0.79] |
| Registered bootstrap, lower bound over 20 RNG seeds (1000 to 1019) | -0.01 to +0.03 | +0.05 to +0.09 |
| Own bootstrap | [+0.02, +0.76] | [+0.08, +0.80] |
| Epoch-block bootstrap (whole epochs within seed) | [+0.03, +0.74] | [+0.09, +0.79] |
| Five-seed t-interval | [+0.13, +0.64], p = 0.013 | [+0.19, +0.68], p = 0.008 |
| Seeds | +0.29 +0.21 +0.71 +0.26 +0.47 | +0.33 +0.30 +0.76 +0.30 +0.50 |
| Leave one seed out | +0.30 to +0.42 | +0.35 to +0.46 |
| Epochs 0-9 / 10-19 / 20-29 / 30-39 / 40-49 | +0.70 / +0.79 / +0.55 / -0.08 / -0.35 | +0.77 / +0.83 / +0.62 / -0.05 / -0.32 |
| Epochs 0-24 / 25-49 | +0.69 / -0.02 | +0.75 / +0.02 |
| Overall recall@10 | -0.20 [-0.38, +0.02], 4/5 seeds negative | -0.17 [-0.35, +0.04] |

For comparison, uncharged DC minus batch ES by the same time blocks is +1.47 / +1.18 / +1.27 / +0.71 / +0.15, so the
uncharged lead also fades late in the run; charging removes it entirely after epoch 30.

Why FRAGILE rather than HOLDS:
1. Size. +0.38 pp on a recent recall@10 near 0.48 is under 1% relative, and charging cut the registered +1.01 pp lead by
   0.63 pp (X4 charged minus uncharged: -0.63 [-0.89, -0.38]).
2. The registered lower bound sits on zero: it is -0.01 under some bootstrap RNG seeds.
3. The lead comes from the first 30 epochs; in epochs 30 to 49 charged DC is at or below batch extra search.
4. On overall recall@10 batch extra search is ahead (-0.20 pp, interval reaching +0.02).
5. Multiplicity. With seed-level t-tests and a Holm correction over the 8 X4 rows, this row (p = 0.013 against a
   threshold of 0.0125) is not kept; the online row (p = 0.008 against 0.010) is. Over all 32 recent rows no
   correction is applied anywhere in the addendum.

Why not FAILS: all five seeds are positive, the t-interval and the epoch-block bootstrap exclude zero, and leaving out
any seed keeps the mean between +0.30 and +0.42. Safe wording: "still slightly ahead of extra search on new-topic
(+0.38 pp, CI +0.01 to +0.74), well short of the 1.01 pp measured with scores uncharged". Do not write "overtakes" without
the number.

## 4. What the paper now says that is outdated or wrong

Macro values below are from `paper\generated\numbers.tex`.

| # | Where | Current text (abridged) | Problem | Should say |
|---|---|---|---|---|
| P-1 | `results.tex:180`, Threats to validity | "charging 128 bytes/epoch ... drops data-change to n_B in {1, 3, 7}, so none of its results was tested with charged scores. This removes its B = 8 PR3 tie and leaves untested its fragile B = 2 H4b lead and its centroid Q = 800 lead over extra search, since we ran neither data-change at n_B in {1, 3, 7} nor a variant that carries unspent bytes into the next epoch." | **Wrong.** Addendum 3 ran exactly these. | "An exploratory follow-up charged these bytes (n_B in {1, 3, 7}). Data-change then lost its B = 2 H4b lead (-0.16 pp vs vector push), kept a thin Q = 800 lead over batch extra search (+0.38 pp, CI +0.01 to +0.74), and still beat recovered-neighbor at equal n_B. We did not run a carryover variant." |
| P-2 | `results.tex:180` | "Even so, recovered-neighbor with 7 refreshes (B = 8) trails data-change with 4 (B = 4) by -1.32 pp" | Superseded by the direct equal-k test. | Replace with "At equal n_B = 3, charged data-change still led recovered-neighbor by 2.66 pp (CI 1.46 to 4.00) on new-topic." |
| P-3 | `results.tex:144`, H3b paragraph | "One reason recovered-neighbor loses to data-change on new-topic is probe cost ... Charging data-change's uncharged 128 bytes ... would also cut it to n_B in {1, 3, 7}. A second reason is that the probe picks worse shards: ..." | Outdated: "would" is now "did", and the order of the two causes is wrong at B = 4. | State the measured split: of the 3.60 pp loss at B = 4, 2.66 pp remains at equal n_B (shard choice) and 0.94 pp is the extra refresh (X3). Present shard choice first. |
| P-4 | `results.tex:146`, H4b paragraph | "The B = 2 data-change lead over vector push (+1.47 pp) is fragile: ... Charging 128 bytes of scores also cuts data-change from 2 to 1 summary at B = 2." | Incomplete: the consequence is now known. | Add: "With scores charged, data-change minus vector push at B = 2 is -0.16 pp (CI -1.89 to +1.49; exploratory)." |
| P-5 | `results.tex:146` | "On new-topic, data-change (0.2889) beats online extra search (0.2613) and leads vector push ... at B = 2" | True for uncharged runs only. | Add that charged, data-change minus online extra search at B = 2 is +1.12 pp (CI -0.39 to +2.62), not significant. |
| P-6 | `results.tex:107`, Q-sweep | "On new-topic at Q = 800, data-change refresh (0.4872) overtakes batch (0.4771, by +1.01 pp) and online extra search (0.4766, by +1.06 pp)." | Uncharged only; the charged result is much weaker. | Add: "With its score bytes charged (3 refreshes), the lead falls to +0.38 pp (CI +0.01 to +0.74) over batch and +0.43 pp (CI +0.07 to +0.79) over online, and batch extra search leads on overall recall@10 (-0.20 pp, CI -0.38 to +0.02)." |
| P-7 | `main.tex:30`, abstract | "with eight, it leads vector push fragilely at B=2, has no clear winner at B=4, and loses at B=8." | The B = 2 lead depends on uncharged bytes and disappears when they are charged. | "with eight, it no longer leads vector push at B=2 once its score bytes are charged, has no clear winner at B=4, and loses at B=8." |
| P-8 | `main.tex:30`, abstract | "Probing spends 22% of B (data-change's 128 score bytes/epoch are uncharged), and it ranks shards worse than new-vector counts." | The parenthesis now suggests an accounting artifact that the follow-up ruled out. | "Probing spends 22% of B, but even when data-change pays for its scores and refreshes as few summaries, it leads by 2.7 pp." |
| P-9 | `intro.tex:10` | "That B = 2 lead is fragile: its t-interval over five seeds crosses zero, and it relies on 128 uncharged score bytes per epoch." | Now tested. | Add: "Charging those bytes in an exploratory rerun removes it (-0.16 pp, CI -1.89 to +1.49)." |
| P-10 | `intro.tex:10` | "Two causes explain the loss: probing consumes 22% of the budget, and the probe picks worse shards (with 7 refreshes at B = 8 it still trails data-change with 4 at B = 4, an exploratory check)." | Order and evidence outdated. | "Mostly the probe picks worse shards: with data-change charged so both refresh 3 summaries, data-change still leads by 2.66 pp (CI 1.46 to 4.00); the extra refresh is worth 0.94 pp." |
| P-11 | `intro.tex:12` | "Section~ref{sec:setup} describes a pre-registered plan and two addenda." | Now three addenda. | "...a pre-registered plan, two addenda, and one exploratory addendum." |
| P-12 | `setup.tex:40`, "Analysis plan and addenda" | Ends with Addendum 2. | Missing Addendum 3. | Add: written after all main results; committed at 7c4201d before any charged run; 42 runs (30 M3, 10 M1 at Q = 800, 2 flag-off replications that matched stored runs exactly); comparisons X1 to X4 and two predictions; no gates. |
| P-13 | `conclusion.tex:4` | "data-change refresh overtakes extra search only on new-topic at Q = 800 while trailing vector push." | Needs the charged size. | "...overtakes extra search only on new-topic at Q = 800, by 1.01 pp uncharged and 0.38 pp with its score bytes charged, while trailing vector push." |
| P-14 | `conclusion.tex:4` | "refresh leads vector push only narrowly and fragilely at B = 2" | Same as P-7. | "...leads vector push narrowly at B = 2 only when data-change's score bytes are free..." |
| P-15 | `conclusion.tex:4` | "The 64-query probe consumes 22% of the budget, leaving 3 rather than 4 refreshes, while data-change's 128 bytes/epoch of scores were uncharged. The probe also selects worse shards. In the exploratory check, 7 probe refreshes still trail 4 data-change refreshes." | Superseded. | "Charging data-change's 128 score bytes gives both rules 3 refreshes, and data-change still leads by 2.66 pp, so the probe mainly selects worse shards." |
| P-16 | `conclusion.tex:6`, rule (1) | "where data-change refresh (with its score bytes uncharged) overtook extra search at Q = 800 but still trailed vector push (-1.76 pp)." | Charged numbers exist. | "...overtook extra search at Q = 800 by 1.01 pp, or 0.38 pp with its score bytes charged, but trailed vector push (-1.76 pp; -2.39 pp charged)." |
| P-17 | `conclusion.tex:6`, rule (3) | "rank shards by the data-change score (whose 128 bytes/epoch of shard scores were uncharged) rather than paying 21,632 bytes/epoch ..." | The caveat is now answered. | "(which still beat the probe at equal refresh count when its 128 bytes/epoch were charged)". |
| P-18 | `results.tex:38` | "Across all five suites (1116 runs, 0 missing) ..." | Not wrong, but Addendum 3 runs must be counted separately. | Add one clause: 42 exploratory runs, all complete; 22 first attempts failed with memory errors and were rerun. |
| P-19 | `system.tex:65`, item (3) | "charging 128 bytes cuts data-change from n_B = 2, 4, 8 to 1, 3, 7 summaries." | Still true; add a pointer. | Append "(tested in an exploratory addendum, Section ...)". |

Statements that stay correct: "At B = 8 (PR3), the four n_B = 8 rules match always-fresh on every query" (uncharged runs);
"no clear winner at B = 4" (charged +0.10 pp); "loses at B = 8" (charged -3.42 pp); vector push wins on steady at every B
(charged -1.58, -3.87, -7.16 pp); "it ranks shards worse than new-vector counts" (Spearman, unchanged).

## 5. Other checks and suspicious points

- **Commit and configs.** All 42 runs record `git_commit` 7c4201d (committed 15:06:10, first run started 15:06:12);
  `git status` is clean. Each run's `config_sha256` equals the SHA-256 of the committed YAML (add3_M3_dc_charged
  5e8e1727..., add3_M1_dc_charged 48360aef..., add3_replication f229334a..., add3_M1_replication b04b935b...). The flag is
  True in all 40 charged runs and False in both replications. No other config field differs from the stored partner
  runs.
- **Memory-error retries.** `M3_dc_charged\failed_runs.json` lists 22 first-attempt failures (`std::bad_alloc`, "Not
  enough memory"), all attempt 1; every run then completed (30 folders, 50 epochs each, all checks above pass). Three of
  those 22 (new-topic, B = 4, seeds 43, 45, 46) are among my 10 reruns, which reproduce the stored per-query arrays
  exactly. The runner was launched with 3 workers. Disclose as for M1 pass 1.
- **Folders.** M1 baselines are read from `repo\results\main\M1` (625f307), not a copy; M1-online from
  `repo-prereg2\results\main\M1_online`; M3 from `repo-prereg2\results\main\M3`. Correct.
- **Analysis log.** `analyze_add3.log` and `.err` are empty because the script prints nothing; the four CSVs were written
  at 15:55:53. Not a problem, but there is no run log of the analysis.
- **Exploratory and post hoc.** The plan says so in its title, status and scope, and the paper must too. The addendum
  produces 32 rows times 2 metrics = 64 intervals with no adjustment. The borderline ones are X4 ES new-topic (lower
  bound +0.01), X2 ESO new-topic B = 8 (t lower bound +0.10) and X3 new-topic B = 8 (t lower bound +0.02, bootstrap
  includes 0). Even X1 at B = 4 and B = 8 has seed-level t-test p-values of 0.014 and 0.009, which sit at the edge of a
  Holm correction over the six X1 rows; their bootstrap and epoch-block intervals are well clear of zero and all ten
  seed gaps are positive, so I keep them as HOLDS.
- **P1 is lenient.** It only required the lower bound to exceed -1 pp. The paper should report the stronger fact that the
  whole interval is above zero, not merely that "P1 held".
- **P2 is an absence of a lead.** -0.16 pp (CI -1.89 to +1.49) means no difference was detected, not that vector push
  wins. Three of five seeds are negative, and the gap is +0.75 in epochs 0 to 24 and -1.34 in epochs 25 to 49.
- **The charging rule is a worst case for data-change.** Under the zero-slack floor, 128 B costs a whole summary every
  epoch and leaves 24,448 B unspent every epoch (half the budget at B = 2). With carryover, the same 128 B/epoch would cost
  at most one summary over 50 epochs. So X3 measures the cost of integer packing, and X2 and X4 compare charged data-change
  spending 24,704 B/epoch (B = 2) against vector push spending 47,864 B/epoch. Recovered-neighbor also strands
  2,944 B/epoch, so X1 is not affected.
- **Odd but small: X3 new-topic B = 8.** Charged data-change (7 refreshes) is +0.16 pp above always-fresh (CI -0.10 to
  +0.41; t [+0.02, +0.31]; 4/5 seeds positive). Refreshing every changed shard is not always best, but the bootstrap
  interval includes zero; do not claim it. It does mean charged data-change no longer ties always-fresh at B = 8.
- **Overall recall@10 can disagree.** Charged data-change loses to online extra search on overall recall@10 at
  new-topic B = 4 (-0.79 pp) and B = 8 (-3.43 pp) while winning on recent recall@10, and loses to batch extra search on
  overall recall@10 at Q = 800 (-0.20 pp). Report both metrics where the paper reports a refresh win.
- **Uncharged costs that remain:** RN's setup broadcast and local compute, the has-new-vectors bit used by every refresh
  rule, extra search's shard load and fan-out, vector push's coordinator memory and scans. Unchanged from SKEPTIC_M3.

---

## Must disclose

1. Addendum 3 is exploratory and post hoc: written after all M1, M2, Sens, M1-online and M3 results were read, committed
   at 7c4201d before any charged run, 42 runs, no pass/fail gates, and 64 intervals with no multiplicity adjustment.
2. Charging uses the zero-slack integer floor. The 128 B/epoch removes a whole summary per epoch (n_B = 1, 3, 7) and
   leaves 24,448 B unspent each epoch, so the charged results are a pessimistic case for data-change; a carryover
   variant (cost at most one summary over 50 epochs) was not run.
3. X1 is equal refresh count and equal cap, not equal bytes spent: recovered-neighbor spends 21,504 B/epoch more, and its
   3,145,728-byte setup broadcast is still uncharged. Both favour recovered-neighbor.
4. Equal-k result: on new-topic, charged data-change beat recovered-neighbor by 2.66 pp at B = 4 (CI 1.46 to 4.00) and
   1.76 pp at B = 8 (CI 1.04 to 2.59), all ten seed gaps positive; no reliable difference at B = 2 (+0.48) or on steady.
5. The registered 3.60 pp H3b loss splits exactly into 2.66 pp of shard choice and 0.94 pp from the extra refresh. At
   B = 2 the split is 0.48 (not significant) and 1.63. The paper must stop leading with probe cost.
6. Mechanism (exploratory): recovered-neighbor's estimate was zero for about 23 of 25 changed shards per epoch, so 53% of
   its refreshes were chosen by cap or lowest shard ID; it had 100 forced-by-cap refreshes versus 16. Its estimate was
   zero for every changed shard in 62 of 250 epochs, where the gap was 5.42 pp versus 1.86 pp elsewhere.
7. The fragile H4b B = 2 lead of data-change over vector push disappears when scores are charged: -0.16 pp
   (CI -1.89 to +1.49; t-interval -2.56 to +2.18). This is no lead, not a vector-push win.
8. Charged data-change still edges batch extra search at centroid Q = 800 on new-topic, but only by 0.38 pp
   (CI 0.01 to 0.74), down from 1.01 pp. The lead is absent after epoch 30, reverses on overall recall@10 (-0.20 pp),
   and its bootstrap lower bound touches zero under other RNG seeds.
9. Charged data-change minus online extra search on new-topic: +1.12 pp at B = 2 (not significant), +2.92 pp at B = 4,
   +1.63 pp at B = 8 (t lower bound +0.10, one seed negative). Online extra search wins overall recall@10 at B = 4 and 8.
10. Vector push still beats charged data-change at every B on steady (1.58, 3.87, 7.16 pp), at B = 8 on new-topic
    (3.42 pp) and at Q = 800 on new-topic (2.39 pp).
11. 22 of the 30 M3 charged runs failed once with memory errors and were rerun to completion; reruns are deterministic
    (10 of my reruns, including 3 of these, reproduce stored per-query results exactly).
12. Charged data-change at B = 8 no longer ties always-fresh (+0.16 pp, CI -0.10 to +0.41), so PR3's tie applies to the
    uncharged rule only.
13. Replace the Threats sentence that says charged runs were not done (P-1), and update P-2 to P-19 in Section 4.

## Safe sentences

Numbers are from `add3_paired_comparisons.csv` (registered two-stage bootstrap, 95% CI) unless marked. Each is at most
30 words, with no em dashes.

1. "After reading the main results, we ran an exploratory follow-up in which data-change paid for its 128 bytes of
   scores per epoch."
2. "Paying for its scores left data-change room for 1, 3 and 7 summaries per epoch at B = 2, 4 and 8, the same as
   recovered-neighbor."
3. "Both replication checks passed: with the charge turned off, reruns matched the stored runs exactly."
4. "With both rules refreshing 3 summaries per epoch on new-topic, data-change still beat recovered-neighbor by 2.66
   points (95% CI 1.46 to 4.00)."
5. "With 7 refreshes each, data-change led by 1.76 points (CI 1.04 to 2.59); with 1 refresh each, the gap was 0.48
   points (CI -0.48 to 1.66)."
6. "On the steady stream, the two rules did not differ reliably at any budget; every interval included zero."
7. "So of recovered-neighbor's 3.60-point loss at B = 4, 2.66 points came from its shard choices and 0.94 points from
   refreshing one summary fewer."
8. "At equal refresh counts, recovered-neighbor was forced to refresh long-waiting shards 100 times over five seeds,
   against 16 times for data-change." (stored epochs.parquet)
9. "In 62 of 250 new-topic epochs at B = 4, its estimate was zero for every shard with new vectors, so ties were broken
   by shard number." (Spearman proxy; confirmed by skeptic reruns)
10. "Charging the scores cost data-change 1.63 points at B = 2 and 0.94 points at B = 4 on new-topic, because a whole
    summary no longer fit."
11. "With its scores charged, data-change no longer led vector push at B = 2 on new-topic: the gap was -0.16 points
    (CI -1.89 to 1.49)."
12. "At B = 4 the two stayed level (0.10 points, CI -1.40 to 1.52), and at B = 8 vector push led by 3.42 points
    (CI 1.75 to 5.15)."
13. "On steady, vector push beat charged data-change by 1.58, 3.87 and 7.16 points at B = 2, 4 and 8."
14. "Charged data-change beat online extra search on new-topic at B = 4 by 2.92 points (CI 1.39 to 4.26), but not
    reliably at B = 2 (CI -0.39 to 2.62)."
15. "Under centroid placement at 800 queries per epoch, charged data-change led batch extra search on new-topic by only
    0.38 points (CI 0.01 to 0.74)."
16. "That is well below the 1.01-point lead with scores uncharged, and batch extra search led on overall recall@10 by
    0.20 points (CI -0.02 to 0.38)."
17. "Vector push still led charged data-change at 800 queries per epoch on new-topic, by 2.39 points (CI 1.99 to 2.84)."
18. "Because unused bytes do not carry over between epochs, charging 128 bytes removes a whole summary; with carryover it
    would cost at most one summary in 50 epochs."
19. "These follow-up comparisons were chosen after the main results were known, so they are exploratory and their
    intervals are not adjusted for multiple comparisons."

Usable only after the committed analysis reproduces them (skeptic reruns, new-topic, B = 4):

20. "Recovered-neighbor's estimate was zero for about 23 of the 25 shards holding new vectors, so 53% of its refreshes
    were picked without a score."
21. "Its three picks per epoch captured 34% of the best possible lift-in neighbor count, against 57% for data-change."
