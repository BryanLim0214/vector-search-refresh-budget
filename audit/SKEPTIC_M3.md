# Skeptic review of the Addendum 2 results (M3, M1_online, H2b, H4b, LOOKAHEAD)

Reviewer: Claude skeptic. Date: 2026-10-24. Scope: `analysis_root/out_addendum2/` (SUMMARY.md, hypothesis_tests.csv,
m3_conditions.csv, paired_comparisons.csv, lookahead_comparison.csv, m1_online_conditions.csv, breakeven_q_online.csv),
`repo-prereg2/configs/prereg_addendum2.yaml`, `repo-prereg2/docs/DEVIATIONS.md` (D-14 to D-16), `repo-prereg2/dvs/` at
commit `67f049d` (working tree clean, equal to the commit in every M3 and M1_online `provenance.json`),
`audit/SKEPTIC_MAIN.md`, `audit/M3_DESIGN_CHECK.md`, the raw `queries.npz`, `epochs.parquet`, `summary.json` and
`provenance.json` files under `analysis_root/main/{M1,M1_online,M3}`, and the ground-truth caches in
`repo/results/_gt_cache` (M1) and `repo-prereg2/results/_gt_cache` (M1_online, M3).

I changed no file in `repo`, `repo-prereg2` or `analysis_root`, and I ran no simulation. My script reads the raw files
directly, does not import `analyze.py` or `dvs`, and uses its own two-stage bootstrap (resample the 5 seeds, then queries
within each drawn seed; 4,000 replicates; RNG seed 20271, different from the analysis script). Script and full output:
`<reviewer scratch folder, not published>\m3\verify_m3.py`
and `out_*.txt` in the same folder.

All numbers below are recent recall@10 in percentage points (pp), pooled over queries and 5 seeds (42 to 46), Q = 200,
unless stated otherwise. "Seeds" lists the per-seed gaps in seed order 42, 43, 44, 45, 46.

## Verdict table

| # | Claim | Verdict |
|---|---|---|
| 1 | GATE_M3 and H3b numbers and pairing | **HOLDS.** Every mean matches the CSV to the printed digit; intervals differ by at most 0.07 pp (RNG). Pairing is exact. |
| 2 | H3b fails on new_topic; recovered_neighbor is 3.60 pp below data_change | **HOLDS** (real, not a bug). Caused by fewer refreshes (pre-registered k = 3 vs 4) **and** worse choices; worse choices are the larger part. data_change's k = 4 depends on its uncharged 128 B/epoch. |
| 3 | PRED_M3_B8_TIES: recovered_neighbor max per-query difference 100 pp | **HOLDS, but the 100 pp figure is a reporting ARTIFACT.** It comes from queries with one recent neighbour. Mean difference is -0.19 pp (steady) and -1.59 pp (new_topic). The four predicted ties are exact. |
| 4 | H4b winners | steady, PV wins at every B: **HOLDS.** new_topic B = 2, data_change wins: **HOLDS WITH CAVEAT** (fragile). new_topic B = 4, no clear winner: **HOLDS.** new_topic B = 8, PV wins: **HOLDS** (it beats fresh summaries, not a rule). ESO underspend is worth about 0.13 to 0.18 pp and changes no winner. |
| 5 | GATE_M3 steady fails (+1.16 pp); is it a floor or ceiling? | **HOLDS.** No floor or ceiling: never_refresh 0.326, always_fresh 0.337, oracle 0.505, all_shards 0.998. New: fresh summaries lower overall recall on M3 steady by 0.91 pp. GATE_M3 new_topic pass: **HOLDS WITH CAVEAT** (seed 42 is +4.10 pp, below 5). |
| 6 | LOOKAHEAD barely matters at equal bytes | **HOLDS WITH CAVEAT.** At Q = 200 and Q = 800 the gap is at most 0.30 pp, the same size as the estimated value of ESO's unspent bytes, so look-ahead cannot be separated from underspend. Q = 50 is not a look-ahead measurement. |
| 7 | H2b pairing across commits 625f307 (M1) and 67f049d (M1_online) | **HOLDS.** Ground-truth arrays are identical in both caches for all 30 (stream, Q, seed) cells; NaN masks are identical; H2b numbers reproduce. |
| 8 | Anything too clean | No bug found. Two cautions: the pre-registered bootstrap intervals are 20 to 27% narrower than t-intervals over the 5 seed means, which matters for two borderline results; and round_robin is no better than never refreshing on M3 new_topic, so "RN beats round_robin" means little. |

---

## 1. Independent recomputation of GATE_M3 and H3b

**Verdict: HOLDS.**

| Quantity | CSV | Mine | Seeds |
|---|---|---|---|
| GATE_M3 steady, AF - NR | +1.16 [+0.38, +1.91] | +1.16 [+0.39, +1.90] | +1.57, +1.61, +0.73, +0.02, +1.84 |
| GATE_M3 new_topic, AF - NR | +6.74 [+5.16, +8.16] | +6.74 [+5.17, +8.18] | +4.10, +8.10, +6.66, +7.13, +7.86 |
| H3b new_topic, RN - round_robin | +2.38 [+1.27, +3.34] | +2.38 [+1.28, +3.34] | +2.69, +3.01, +1.69, +3.44, +0.77 |
| H3b new_topic, RN - data_change | -3.60 [-4.93, -2.26] | -3.60 [-4.91, -2.29] | -1.49, -3.79, -4.57, -3.14, -5.34 |
| H3b new_topic, RN - ql | +0.50 [-0.22, +1.14] | +0.50 [-0.21, +1.19] | +0.97, +0.69, +0.26, +0.40, +0.05 |
| H3b new_topic, RN - ss | -0.48 [-1.79, +0.62] | -0.48 [-1.82, +0.68] | +0.46, +0.30, -2.24, +0.55, -1.84 |
| (not tested) steady, RN - data_change | -0.39 [-1.06, +0.32] | -0.39 [-1.09, +0.33] | |

Pairing evidence:
- Every M3 run has 10,000 query slots (50 epochs x 200). For each seed and stream, the NaN mask of recent recall is
  identical across all 12 methods at all 3 budgets (36 runs per seed).
- I rebuilt the set of queries with recent neighbours from the ground-truth cache alone (true top-10 neighbour inserted
  in epoch e or e - 1), without the metrics code. It equals the non-NaN set in every run: 11,917 queries on steady
  (2,368 / 2,462 / 2,400 / 2,331 / 2,356 per seed) and 6,375 on new_topic (1,357 / 1,370 / 1,190 / 1,303 / 1,155).
- Per-epoch recall in `epochs.parquet` and run means in `summary.json` agree with `queries.npz` for all 40 runs checked.
- H3b fails on the decision rule by a wide margin: even the best gap (RN - round_robin, +2.38 pp) is below 5 pp, and
  RN is reliably below data_change in all 5 seeds.

## 2. Why recovered_neighbor loses to data_change on new_topic

**Verdict: HOLDS (real result, no implementation bug found).** The loss has two parts: RN refreshes fewer summaries
(by the pre-registered byte accounting) and, more importantly, it picks worse shards. The part due to choices is
exploratory (not pre-registered).

### 2a. Refreshes and bytes per epoch (all 250 epochs, every epoch identical)

| B | Rule | Summaries/epoch | Summary bytes | RN probing bytes | Budget spent/epoch | Forced by cap (share of refreshes, new_topic / steady) | Cap A |
|---|---|---|---|---|---|---|---|
| 2 | RR, DC, QL, SS | 2 | 49,152 | 0 | 49,152 | RR 0 / 0; DC 1.2% / 0; QL 24% / 24%; SS 21% / 23% | 32 |
| 2 | RN | **1** | 24,576 | 21,632 (44% of B) | 46,208 | 0 / 0 | 64 |
| 4 | RR, DC, QL, SS | 4 | 98,304 | 0 | 98,304 | RR 0 / 0; DC 2.4% / 0; QL 17% / 18%; SS 18% / 17% | 16 |
| 4 | RN | **3** | 73,728 | 21,632 (22% of B) | 95,360 | 13% / 17% | 22 |
| 8 | RR, DC, QL, SS | 8 | 196,608 | 0 | 196,608 | 0 / 0 | 8 |
| 8 | RN | **7** | 172,032 | 21,632 (11% of B) | 193,664 | 5% / 5% | 10 |

The RN overhead (64 x 32 x 8 = 16,384 B cutoffs, 32 x 4 = 128 B scores, 64 x 2 x 40 = 5,120 B replies) is computed as
pre-registered (D-10 states k = 3 at B = 4; PRED_M3_B8_TIES states k = 7). k = 1 at B = 2 follows from the same
arithmetic. RN's 3,145,728 B setup broadcast is still not charged, which favours RN.

**Accounting point that cuts the other way:** data_change's 4 B per shard score message (128 B/epoch) is not charged
(D-16). If it were, data_change would have k = 1 at B = 2, k = 3 at B = 4 and k = 7 at B = 8, the same as RN
(floor((49,152 - 128) / 24,576) = 1, and so on). So at B = 4 the "3 vs 4 refreshes" difference exists only because RN is
charged for its per-epoch messages and DC is not.

### 2b. Fewer refreshes or worse choices? (EXPLORATORY, not pre-registered)

The ground truth and query schedule do not depend on B (masks identical across B), so runs at different B pair exactly.

| Comparison (new_topic) | Refreshes/epoch | Gap | Seeds |
|---|---|---|---|
| RN (B = 8) - DC (B = 4) | 7 vs 4 | -1.32 [-2.15, -0.60] | -1.27, -0.88, -2.68, -1.21, -0.61 |
| RN (B = 4) - DC (B = 2) | 3 vs 2 | -1.13 [-2.24, +0.08] | +0.89, -1.16, -2.20, -1.38, -2.09 |
| RN (B = 2) - DC (B = 2) | 1 vs 2 | -2.11 [-3.36, -0.78] | |
| RN (B = 2) - RR (B = 2) | 1 vs 2 | +2.26 [+1.37, +3.14] | |
| RN (B = 8) - SS (B = 4) | 7 vs 4 | +1.80 [+0.43, +3.10] | |
| RN (B = 8) - QL (B = 4) | 7 vs 4 | +2.78 [+1.60, +4.07] | |

- With 7 refreshes per epoch RN does worse than data_change with 4, in all 5 seeds. Fewer refreshes cannot explain this;
  data_change simply picks better shards.
- Linear interpolation of data_change between k = 2 (0.2889) and k = 4 (0.3135) gives 0.3012 at k = 3; RN at k = 3 is
  0.2775, 2.36 pp lower. On this rough split, about 1.2 pp of the 3.60 pp gap is the missing refresh and about 2.4 pp is
  choice quality.
- Per refresh, RN chooses better than round_robin, ql and (roughly) ss, but worse than data_change.
- On steady the same comparisons are all within about 0.6 pp and mostly not significant.

### 2c. Is RN choosing badly because its scores are empty?

In RN's own runs (5 seeds x 50 epochs):

| Stream, B | Epochs with all-zero RN estimate | Epochs with no lift-in flip at all | Mean lift-in flips/epoch (all shards) | Mean Spearman, RN estimate vs truth | Mean Spearman, new-vector count vs truth |
|---|---|---|---|---|---|
| new_topic, 2 | 8 / 250 (3%) | 0 | 17.1 | 0.25 | 0.65 |
| new_topic, 4 | 62 / 250 (25%) | 0 | 7.9 | 0.26 | 0.63 |
| new_topic, 8 | 81 / 250 (32%) | 10 | 4.5 | 0.42 | 0.62 |
| steady, 2 | 50 / 250 (20%) | 0 | 14.6 | 0.07 | 0.40 |
| steady, 4 | 121 / 250 (48%) | 0 | 8.8 | 0.10 | 0.45 |
| steady, 8 | 185 / 250 (74%) | 17 | 3.2 | 0.14 | 0.47 |

"All-zero" is counted as epochs where the RN Spearman is undefined while the new-vector Spearman is defined, so the truth
is not constant and the RN estimate must be (estimates are zero for unflipped shards). Per-shard counts of non-zero scores
are not logged, so I cannot report partial ties without rerunning; I did not rerun.

- When all scores are zero, `scale_scores_unit_interval` returns zeros (`policies.py:88-89`) and the order falls back to
  "has new vectors", then "starved past the cap", then **lowest shard ID** (`policies.py:639`). This is the same fallback
  SKEPTIC_MAIN found on M1.
- The sample queries do reach the shards (lift-in flips never drop to zero at B = 2 or 4), but few of the 64 queries have
  new neighbours that beat their 10th-best result, so the scores are sparse.
- On new_topic B = 4, the RN - DC gap is -6.15 pp in the 62 all-zero epochs (1,428 queries) and -2.86 pp in the other
  188 epochs (4,947 queries). The fallback hurts, but RN still loses when its scores are defined. The gap does not shrink
  over time (-3.47, -3.72, -3.57 pp in epochs 0-9, 10-24, 25-49).
- The free "new vectors since last refresh" count tracks the true recovered neighbours much better than the RN estimate
  (0.63 vs 0.26 on new_topic B = 4), the same finding as on M1.

Code check: the RN estimator and the selection code are byte-identical between `625f307` and `67f049d` (the diff only adds
lines, none in these functions). Scoring calls in `coordinator.py` have no side effects (no byte logging, no popularity
update). I found no defect specific to `time_segment_g`.

**Conclusion:** the H3b failure is a fair consequence of the pre-registered design, with one disclosure: the equal-byte
comparison charges RN for its per-epoch messages but not DC for its 128 B. Even setting bytes aside, RN chose worse
shards than data_change (RN with 7 refreshes < DC with 4).

## 3. PRED_M3_B8_TIES and the 100 pp maximum

**Verdict: the four pre-registered ties HOLD exactly; the RN "100 pp" is a reporting artifact, not a defect.**

- round_robin, data_change, ql and ss equal always_fresh on every query at B = 8, for recent and overall recall, on both
  streams (0 of 11,917 and 0 of 6,375 queries differ).
- RN (k = 7) vs always_fresh at B = 8:

| Stream | Queries that differ | Queries with a 100 pp difference | Recent neighbours of those queries | Mean difference | Mean absolute difference | Paired gap |
|---|---|---|---|---|---|---|
| steady | 215 / 11,917 | 192 | 184 have 1, 8 have 2 | -0.19 pp | 1.71 pp | -0.19 [-0.63, +0.28] |
| new_topic | 390 / 6,375 | 119 | 88 have 1, 18 have 2, 9 have 3, 1 has 4, 2 have 5, 1 has 6 | -1.59 pp | 3.47 pp | -1.59 [-2.33, -0.96] |

A query whose only recent neighbour sits on a shard RN did not refresh flips from 1 to 0 (or back). The maximum is the
wrong statistic to quote; report the mean. On new_topic, missing one of 8 refreshes costs a real 1.59 pp.

## 4. H4b: winners and actual bytes

**Verdicts:** steady (PV wins at every B): **HOLDS.** new_topic B = 2 (data_change wins): **HOLDS WITH CAVEAT, fragile.**
new_topic B = 4 (no clear winner): **HOLDS.** new_topic B = 8 (PV wins): **HOLDS**, read as PV against fresh summaries.

| Stream, B | Best rule (2nd) | Best - ESO | Best - PV | ESO - PV | Bytes per run: best / ESO / PV |
|---|---|---|---|---|---|
| steady, 2 | ss (ql, +0.01) | -1.08 [-1.52, -0.63] | -1.79 [-2.37, -1.20] | -0.71 [-1.12, -0.29] | 2,457,600 / 2,200,162 / 2,393,200 |
| steady, 4 | data_change (ql, +0.26) | -2.09 [-2.82, -1.33] | -3.67 [-4.54, -2.80] | -1.58 [-2.25, -0.92] | 4,915,200 / 4,649,386 / 4,863,600 |
| steady, 8 | round_robin (= AF) | -4.28 [-5.16, -3.43] | -7.17 [-8.04, -6.29] | -2.89 [-3.83, -2.06] | 9,830,400 / 9,432,576 / 9,804,400 |
| new_topic, 2 | data_change (RN, +2.11) | +2.75 [+0.88, +4.34] | **+1.47 [+0.06, +2.90]** | -1.28 [-2.18, -0.39] | 2,457,600 / 2,180,952 / 2,393,200 |
| new_topic, 4 | data_change (ss, +3.12) | +3.86 [+2.16, +5.35] | +1.04 [-0.46, +2.53] | -2.82 [-3.75, -1.92] | 4,915,200 / 4,604,342 / 4,863,600 |
| new_topic, 8 | round_robin (= AF) | +1.47 [+0.09, +2.83] | -3.59 [-5.27, -1.84] | -5.05 [-6.45, -3.77] | 9,830,400 / 9,461,059 / 9,804,400 |

All means match the CSV; CSV intervals: new_topic B = 2 DC - PV is [+0.03, +2.90].

Steady: every gap has the same sign in all 5 seeds. Picking the best rule on the same data favours refresh, so PV's wins
are conservative. PV also wins on overall recall at every B (refresh minus PV: -0.99, -2.18, -3.69 pp).

new_topic B = 2 is fragile for four reasons:
1. The lower bound is +0.03 pp (mine +0.06); seed 42 is negative (-0.29, -0.34 for DC - ESO).
2. A t-interval over the 5 seed means is [-0.35, +3.25], which includes zero (see Section 8).
3. data_change's 128 B/epoch are uncharged; charging them would halve it to k = 1 at B = 2. RN at k = 1 scored 0.2677,
   below PV's 0.2742. How DC would do at k = 1 is unknown.
4. PV's bytes are lower too (31 vectors x 1,544 = 47,864 B vs 49,152 B per epoch, integer rounding).
Overall recall agrees with the recent-recall direction at B = 2 (DC - PV +1.03 [+0.64, +1.44]).

**ESO underspend.** ESO spends 90.8 to 96.8% of the extra searches ES spends. The threshold comes from the previous
epoch's gaps, so in about half the epochs ESO hits the cap (125 to 133 of 250) and in the rest it falls short (minimum
12 to 13 of 29 at B = 2). To value the shortfall I used the slope of ES recent recall per extra search between adjacent
budgets (an upper-side estimate, since the unspent searches would be the least useful ones):

| Stream, B | ES searches/run | ESO searches/run | Shortfall | Estimated value |
|---|---|---|---|---|
| steady 2 / 4 / 8 | 1,450 / 2,950 / 5,900 | 1,328.6 / 2,807.6 / 5,696.0 | 121 / 142 / 204 | 0.13 / 0.15 / 0.18 pp |
| new_topic 2 / 4 / 8 | 1,450 / 2,950 / 5,900 | 1,317.0 / 2,780.4 / 5,713.2 | 133 / 170 / 187 | 0.13 / 0.16 / 0.17 pp |

The smallest H4b gap involving ESO is 0.71 pp (steady B = 2, ESO - PV), so the underspend changes no winner. It is the
same size as the LOOKAHEAD gap, though (Section 6).

**Uncharged costs from SKEPTIC_MAIN still apply on M3** (same code paths at `67f049d`):
- PV: the coordinator keeps every pushed vector (up to 6,350 vectors, 9.8 MB at B = 8) and scans all of them for every
  query at no byte cost.
- ES/ESO: extra shard work not in B: 29, 59 and 118 extra searches per epoch on 400 base shard searches (+7%, +15%, +30%
  shard query load), plus an extra round trip.
- DC: 128 B/epoch score messages (changes DC's k at every B in M3, including the B = 8 tie).
- RN: 3,145,728 B setup broadcast (158,275 B/epoch with amortized setup at B = 4, vs 95,360 B charged).
- All rules: the "has new vectors" signal.
- PV's gain is again mostly a sample of new vectors searched exactly: predicted (share pushed x share missed by routing)
  vs observed PV - NR: steady +2.09/+2.13, +4.25/+4.42, +8.56/+8.33; new_topic +2.21/+2.53, +4.49/+5.42, +9.05/+10.32.
  On new_topic the observed gain is 0.3 to 1.3 pp above the prediction. A likely reason (not verified) is that PV pushes
  the newest arrivals and new_topic arrives cluster by cluster, so pushed vectors fall on the newest topic, which stale
  routing misses most.

## 5. GATE_M3 levels: floor or ceiling?

**Verdict: HOLDS. The steady failure is not a floor or ceiling effect.**

| Method (B-independent) | steady recent | steady overall | new_topic recent | new_topic overall |
|---|---|---|---|---|
| never_refresh | 0.3258 | 0.5869 | 0.2489 | 0.5759 |
| always_fresh | 0.3374 | 0.5779 | 0.3163 | 0.5932 |
| oracle_routing | 0.5046 | 0.6996 | 0.4863 | 0.7229 |
| all_shards | 0.9982 | 0.9989 | 0.9967 | 0.9986 |

- Reference runs are identical per query across B = 2, 4, 8, as they should be.
- The vectors are findable (all_shards 0.998) and routing has 17.9 pp of headroom on steady (oracle - never_refresh);
  fresh summaries recover 1.16 pp of it (6.5%). Recent recall is well above the 2/32 chance level that sank M2 steady.
- **New finding:** on M3 steady, fresh summaries lower overall recall by 0.91 pp [-1.03, -0.79], negative in all
  5 seeds (-0.92, -0.92, -0.81, -1.03, -0.85), as on M2 steady. On new_topic they raise it by 1.73 pp [+1.13, +2.33].
- GATE_M3 new_topic passes on the mean (+6.74 pp, decision rule is mean >= 5 pp), but seed 42 is +4.10 pp and a
  t-interval over seed means is [+4.79, +8.75]. The pass is correct under the rule; it is not far above the threshold.

## 6. LOOKAHEAD

**Verdict: HOLDS WITH CAVEAT.** "Look-ahead barely matters at equal bytes" is safe at Q = 200 and 800, with the wording
that the small gap also contains ESO's unspent bytes.

| Condition | ES - ESO, mine | CSV | ESO bytes as % of ES |
|---|---|---|---|
| M1 steady Q = 50 | +1.33 [+0.78, +1.99] | +1.33 [+0.77, +2.00] | 83.1% |
| M1 steady Q = 200 | +0.21 [+0.03, +0.37] | +0.21 [+0.03, +0.37] | 95.2% |
| M1 steady Q = 800 | +0.03 [+0.01, +0.06] | +0.03 [+0.00, +0.06] | 96.8% |
| M1 new_topic Q = 50 | +1.99 [+1.17, +2.92] | +1.99 [+1.12, +3.01] | 83.0% |
| M1 new_topic Q = 200 | +0.13 [-0.07, +0.34] | +0.13 [-0.07, +0.35] | 94.3% |
| M1 new_topic Q = 800 | +0.05 [+0.01, +0.09] | +0.05 [+0.01, +0.09] | 96.9% |
| M3 steady B = 2 / 4 / 8 | -0.01 / +0.16 / +0.18 | same | 91.6 / 95.2 / 96.5% |
| M3 new_topic B = 2 / 4 / 8 | +0.24 / +0.30 / +0.28 | same | 90.8 / 94.3 / 96.8% |

- At Q >= 200 the gap is at most 0.30 pp. The estimated value of ESO's unspent searches is 0.13 to 0.18 pp in M3
  (Section 4), so look-ahead and underspend together explain these gaps and cannot be told apart.
- At Q = 50, ESO spends only 83% of the bytes and ES puts 9 of its 59 searches on a second extra shard; the +1.3 to
  +2.0 pp gap is not a look-ahead measurement (the pre-registration says so).

## 7. H2b pairing across commits

**Verdict: HOLDS. No mismatch.**

- For every stream, Q in {50, 200, 800} and seed (30 cells), the M1 cache (`repo/results/_gt_cache`, built at
  `625f307`) and the M1_online cache (`repo-prereg2/results/_gt_cache`, `67f049d`) hold identical `epoch_gt_ids`,
  `epoch_gt_scores`, `epoch_gt_epochs`, `epoch_gt_shards`, `epoch_total_indexed` and `init_centroids`.
- The NaN mask of every ESO run equals the mask of the matching M1 run; array lengths match; data provenance hashes match.
- H2b reproduces: steady ESO - RN +2.55 [+2.17, +2.96] (CSV +2.55 [+2.17, +2.97]), PV - RN +1.88 [+1.42, +2.35];
  new_topic ESO - DC +1.84 [+0.63, +2.99] (CSV +1.84 [+0.58, +3.01]), PV - DC +2.09 [+1.04, +3.28].

## 8. Other checks

- **No identical runs across seeds:** per-query recall differs across the 5 seeds in all 186 M3 and M1_online run groups.
- **No unexpected exact ties:** at B = 2 and 4, no two refresh rules (or a rule and AF or NR) are identical on all
  queries. The only exact zeros are the predicted B = 8 ties and the B-independent reference runs.
- **Budget never exceeded** in any M3 epoch for any budgeted method.
- **Bootstrap width.** The pre-registered two-stage bootstrap gives intervals 20 to 27% narrower than a t-interval over
  the 5 seed means (df = 4): GATE steady 1.52 vs 1.89 pp wide; GATE new_topic 3.01 vs 3.97; RN - DC 2.62 vs 3.65;
  DC - PV B = 2 2.88 vs 3.60; DC - PV B = 4 2.96 vs 3.76. A percentile bootstrap over only 5 seeds understates
  between-seed spread. This is the registered method and changes no registered outcome, but two results sit on the
  edge: H4b new_topic B = 2 (t-interval [-0.35, +3.25]) and GATE_M3 new_topic (t lower bound +4.79).
- **round_robin is a weak baseline on M3.** Its cursor advances by k (2 or 4) shards per epoch while the active group
  advances by 8, so it rarely refreshes the shards that just got data: round_robin - never_refresh on new_topic is
  -0.37 [-1.12, +0.34] at B = 2 and +0.48 [-0.42, +1.42] at B = 4. RN's +2.38 pp over round_robin is a gain over a rule
  that does no better than not refreshing.
- **Spearman values at B = 8** are identical for RR, DC, QL and SS (0.5382 new_topic, 0.1773 steady) because their
  trajectories are identical; they are not four independent measurements.

---

## Must disclose in the paper

1. H3b fails on new_topic, and RN is 3.60 pp below data_change at B = 4. Give both causes: RN refreshes 1, 3, 7
   summaries per epoch where the other rules refresh 2, 4, 8 (its sample-query messages take 21,632 B per epoch, 44%,
   22% and 11% of B), and it picks worse shards (with 7 refreshes it is 1.32 pp below data_change with 4; exploratory).
2. data_change's 128 B per epoch score messages are not charged. Charging them would give it k = 1, 3, 7 at B = 2, 4, 8,
   the same as RN, so the B = 4 refresh-count difference in H3b and the B = 8 data_change tie both depend on this.
   RN's 3,145,728 B setup broadcast is also uncharged and favours RN.
3. RN's estimate is all zero in 25% (new_topic) and 48% (steady) of epochs at B = 4; in those epochs it refreshes by
   lowest shard ID among shards with new vectors. Its Spearman with the true recovered neighbours (0.26 new_topic) is far
   below the free new-vector count (0.63).
4. Report the RN B = 8 difference from always_fresh as a mean (-0.19 pp steady, -1.59 pp new_topic), not as the 100 pp
   maximum, which comes from queries with a single recent neighbour.
5. H4b new_topic B = 2 ("data_change wins") is fragile: lower bound +0.03 pp, one seed negative, a seed-level t-interval
   includes zero, and it relies on DC's uncharged score bytes. Do not present it as a clear refresh win.
6. On M3 steady, fresh summaries lower overall recall by 0.91 pp (all seeds negative); report this next to GATE_M3.
7. ESO spends 91 to 97% of the extra searches ES spends (89.5% of the refresh rules' bytes at B = 2). The shortfall is
   worth about 0.13 to 0.18 pp, which changes no H4b winner but is as large as the look-ahead gap at Q >= 200.
8. Uncharged costs from SKEPTIC_MAIN still apply in M3: PV's coordinator storage and scans (up to 9.8 MB), the extra
   shard load of ES/ESO (+7%, +15%, +30%), RN setup bytes, DC score bytes, and the has-new-vectors signal.
9. PV's gain on M3 follows the share of new vectors it pushes (as in M1/M2), slightly exceeded on new_topic.
10. round_robin does no better than never refreshing on M3 new_topic at B = 2 and 4, so comparisons against it are weak.
11. GATE_M3 new_topic passes on the mean (+6.74 pp), but one seed is below 5 pp (+4.10).
12. The bootstrap intervals over 5 seeds are narrower than seed-level t-intervals; say how they were computed.

## Safe sentences

- "When each epoch's arrivals go to 8 of 32 shards, refreshing all summaries every epoch raised recent recall@10 by
  6.7 points (95% CI 5.2 to 8.2) on the new_topic stream but only 1.2 points (0.4 to 1.9) on the steady stream, where
  it also lowered overall recall@10 by 0.9 points."
- "On the steady stream this small gain is not a floor effect: without refresh recent recall@10 was 0.33, routing by
  true neighbour counts reached 0.50, and searching all shards reached 1.00."
- "The recovered-neighbor rule did not beat the other rules: at 4 summaries' worth of bytes on new_topic it was
  3.6 points below data_change (95% CI -4.9 to -2.3) and within 0.5 points of ql and ss."
- "Its sample-query messages cost 21,632 bytes per epoch, so it refreshed 1, 3 and 7 summaries per epoch where the other
  rules refreshed 2, 4 and 8; data_change's 128-byte score messages were not charged."
- "Fewer refreshes do not explain the gap: with 7 refreshes per epoch, recovered-neighbor still scored 1.3 points below
  data_change with 4 (exploratory comparison)."
- "Its estimate was zero for every shard in a quarter of new_topic epochs at B = 4, and it tracked the true recovered
  neighbours less well than simply counting each shard's new vectors (mean Spearman 0.26 vs 0.63)."
- "As predicted, at 8 summaries per epoch round-robin, data_change, ql and ss refreshed every shard that received data
  and matched always-fresh summaries on every query."
- "On the steady stream, pushing the newest vectors to the coordinator gave the highest recent recall@10 at every budget,
  ahead of the best refresh rule by 1.8, 3.7 and 7.2 points at B = 2, 4 and 8."
- "On new_topic, data_change led pushing vectors by 1.5 points at B = 2 (95% CI 0.0 to 2.9) and by 1.0 point at B = 4
  (CI -0.5 to 2.5), while pushing vectors led fresh summaries by 3.6 points at B = 8."
- "A causal extra-search rule that sees each query only on arrival came within 0.3 points of the batch version at
  200 or more queries per epoch, while spending 3 to 6% fewer bytes."
- "The causal and batch versions of extra search were compared on the same queries and the same ground truth: the
  ground-truth arrays used by the two code versions are identical."
