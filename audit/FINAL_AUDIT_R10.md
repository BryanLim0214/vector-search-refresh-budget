# Final Consistency Audit, Round 10

Independent read. No GUARDIAN_*, HOSTILE_* or FIX_* file was opened. No file in `paper/` was edited.

## State audited

The paper changed while this audit ran (R10b to R10c rebuild at 19:10). Every finding below was re-checked against the current state, which is identical to `audit/snapshots/paper_R10c`:

| File | md5 |
|---|---|
| main.tex | cd50f01d7c0eeeb6f4295d6c9dc409bc |
| sections/intro.tex | fed72878e06f7cb4e48379cd3016b228 |
| sections/related.tex | 045f47ca1cfaea25c4049dd3a8d7fd36 |
| sections/system.tex | bf7ad7b0916408eedd1a3f5df10f66a3 |
| sections/setup.tex | 26e2c8372312103081c5d358af07c108 |
| sections/results.tex | 37d85e0bd7d6f984aa9c39dcf7e35e35 |
| sections/conclusion.tex | 85d9ecaf20353b8826cdc9a8b3ec55f5 |
| generated/numbers.tex | 821b0b249fdd1b35490f40ce5ee661cd |
| main.pdf | built 2026-10-25 19:10:34, 10 pages |

If the paper is edited again, line numbers may shift; the quoted current text is the anchor.

## Counts

- High: 2
- Medium: 11
- Low: 15
- Macro spot-checks: 22 of 22 match the CSVs; 17 raw recomputations from `queries.npz` also match.

Word counts below are for the full rendered sentence after the fix, with macros expanded.

---

## High

### H-1. Conclusion gives batch extra-search load caps as the load of causal online extra search

- File: `sections/conclusion.tex:6`
- Current: `(1) Under centroid placement at $Q \le 200$, spend bytes on causal online extra search if shards can absorb \RNumESLoadPctQTwoHundred{} to \RNumESLoadPctQFifty{} more HNSW searches ($Q = 200$ to $50$) and \RNumThreeShards{}-shard fan-out (or an extra round trip).`
- Renders as "absorb 14.8% to 59.0% more HNSW searches".
- Problem: both macros are batch ES caps (`gen_numbers.py`: 59/(2Q)). Online ES makes at most 1 extra search per query, so at most 50 per epoch at Q = 50 (results.tex:107 says so). That is a 50% cap. Measured ESO load (`m1_online_conditions.csv`, `extra_searches_mean`/50): 49.1 searches/epoch (49.1%) at Q = 50 and 56.2 (14.0%) at Q = 200. "59.0%" cannot happen for the method being recommended.
- Replacement (37 words): `(1) Under centroid placement at $Q \le 200$, spend bytes on causal online extra search if shards can absorb up to \RNumExtraSearchesBFour{} more HNSW searches/epoch (\RNumESOQFiftyMaxSearches{} at $Q = 50$) and \RNumThreeShards{}-shard fan-out (or an extra round trip).`

### H-2. Conclusion rule (3) says charged data-change beat the probe, without the conditions where it did

- File: `sections/conclusion.tex:6`
- Current: `(3) When scheduling refreshes across active shards, rank shards by the data-change score (which still beat the probe at equal refreshes with its \RNumDCScoreEpochBytes{} bytes/epoch charged) rather than paying \RNumRNOverheadEpochBytes{} bytes/epoch (plus setup broadcast) for \RNumSampleQueries{}-query cutoff probing.`
- Problem: X1 (Table VIII) supports this only on new-topic at B = 4 (+2.66 [+1.46, +4.00]) and B = 8 (+1.76 [+1.04, +2.59]). The intervals include zero at new-topic B = 2 (+0.48 [-0.48, +1.66]) and at all three steady budgets (+0.30 [-0.29, +0.88], +0.19 [-0.29, +0.63], +0.19 [-0.29, +0.64]). results.tex:178 already says "but not reliably at B = 2 or on steady". Adding the qualifier inside the parenthesis pushes the sentence to 45 words, so it is split in two.
- Replacement (25 + 19 words): `(3) When scheduling refreshes across active shards, rank shards by the data-change score rather than paying \RNumRNOverheadEpochBytes{} bytes/epoch (plus setup broadcast) for \RNumSampleQueries{}-query cutoff probing. With its \RNumDCScoreEpochBytes{} bytes/epoch charged, data-change still beat the probe at equal refreshes on new-topic at $B \in \{4, 8\}$.`

---

## Medium

### M-1. Intro says Section V gives CIs on every gap except epoch splits

- File: `sections/intro.tex:10`
- Current: `Section~\ref{sec:results} reports gaps in percentage points (pp), with \RNumCIPct{} CIs except for descriptive epoch splits.`
- Problem: Section V also reports many pp values without CIs that are not epoch splits. Examples: per-seed gaps (seed 42 +4.10 pp, seed 45 -0.04 pp), out-of-sample means (+0.52, +2.37 pp), the 0.06 pp rule span, the 2.66 / 0.94 pp decomposition, the 0.2 to 1.0 pp PV excess, and the 0.13 to 0.18 pp ESO underspend value.
- Replacement (24 words): `Section~\ref{sec:results} reports paired gaps in percentage points (pp), with \RNumCIPct{} CIs on pooled five-seed gaps; per-seed, subset, decomposed, and epoch-split values are descriptive.`

### M-2. PR2 cites Table VII, which has no M2 rows

- File: `sections/results.tex:176`
- Current: `(PR2: \RNumPredMTwoTiesGap{}, max diff \RNumPredMTwoTiesMaxAbs{}; Table~\ref{tab:refresh_rules})`
- Problem: Table VII (tab:refresh_rules) lists only M1 and M3 rows. PR2 is in Table IV.
- Replacement: `(PR2: \RNumPredMTwoTiesGap{}, max diff \RNumPredMTwoTiesMaxAbs{}; Table~\ref{tab:hypotheses})`

### M-3. "Its estimate" can be read as data-change's estimate, which was never measured

- File: `sections/conclusion.tex:4`
- Current: `Its estimate correlates less strongly with lift-in neighbor counts than $N_{\text{new}}(s)$ on average in every centroid and eight-segment condition.`
- Problem: the previous sentence's subject is data-change. The paper states that rho for DC was not measured (results.tex:212, setup.tex:38).
- Replacement (20 words): `The probe's estimate correlates less strongly with lift-in neighbor counts than $N_{\text{new}}(s)$ on average in every centroid and eight-segment condition.`

### M-4. "Better than the probe in every condition" is false for M2

- File: `sections/conclusion.tex:6`
- Current: `The new-vector count $N_{\text{new}}(s)$ tracked lift-in neighbor counts better than the probe in every condition, though it was not tested as a refresh rule.`
- Problem: in M2, rho(RN) = rho(N_new) = 1.00 in all 30 rows of `spearman_comparison.csv`, and results.tex:212 reports rho = 1.00 in M2. Intro and conclusion paragraph 1 correctly say "every centroid and eight-segment condition".
- Replacement (27 words): `The new-vector count $N_{\text{new}}(s)$ tracked lift-in neighbor counts better than the probe in every centroid and eight-segment condition, though it was not tested as a refresh rule.`

### M-5. M2 result states a cause as fact; Threats calls the mechanism untested

- File: `sections/results.tex:176`
- Current: `Because \RNumCentersPerSummary{} centers cannot represent a uniform topic mix, always-fresh reaches only \RNumMTwoAFRecentSteady{} (...)`
- Problem: Threats (results.tex:214) says the same effect is "plausibly shifting centers toward random arrivals (not tested; per-segment summaries not modeled)".
- Replacement for the opening clause (full sentence 38 words): `Plausibly because \RNumCentersPerSummary{} centers cannot represent a uniform topic mix (not tested), always-fresh reaches only \RNumMTwoAFRecentSteady{} (...`, with the rest unchanged.

### M-6. Threats says Addendum 3 "charged these bytes" after a list of all uncharged costs

- File: `sections/results.tex:214`
- Current: `Exploratory Addendum 3 charged these bytes ($n_B \in \{1, 3, 7\}$), removing ...`
- Problem: the preceding parenthesis lists RN's setup broadcast, local compute, ES HNSW load, coordinator scans, and DC's score bytes. Addendum 3 charged only DC's 128 score bytes/epoch.
- Replacement (48 words): `Exploratory Addendum 3 charged DC's score bytes ($n_B \in \{1, 3, 7\}$), removing ...`, with the rest unchanged.

### M-7. Garbled subject: "recovered-neighbor ... spans" the five-rule range

- File: `sections/results.tex:107`
- Current: `On steady, recovered-neighbor (\RNumMOneRNRecentMeanQFiftySteady{}) trails data-change (\RNumMOneDCRecentMeanQFiftySteady{}) by \RNumMOneDCMinusRNQFiftySteady{} at $Q{=}50$ (DC minus PV: \RNumMOneDCMinusPVQFiftySteady{}) and spans \RNumMOneRNRecentMeanQEightHundredSteady{} to \RNumMOneDCRecentMeanQEightHundredSteady{} (\RNumMOneQSweepSteadyRuleSpanQEightHundred{} across five rules) at $Q{=}800$.`
- Problem: a single rule cannot "span" a range. At Q = 800 steady the five rules run from RN 0.6996 to DC 0.7003 (`m1_conditions.csv`: RN 0.6996, QL 0.6999, RR 0.7000, SS 0.7000, DC 0.7003). The rounded endpoints also differ by 0.07, while the macro gives the unrounded 0.06 pp.
- Replacement (21 + 17 words): `On steady, recovered-neighbor (\RNumMOneRNRecentMeanQFiftySteady{}) trails data-change (\RNumMOneDCRecentMeanQFiftySteady{}) by \RNumMOneDCMinusRNQFiftySteady{} at $Q{=}50$ (DC minus PV: \RNumMOneDCMinusPVQFiftySteady{}). At $Q{=}800$, the five rules span \RNumMOneRNRecentMeanQEightHundredSteady{} (RN) to \RNumMOneDCRecentMeanQEightHundredSteady{} (DC), an unrounded span of \RNumMOneQSweepSteadyRuleSpanQEightHundred{}.`

### M-8. "Vector push leads on new-topic at every Q" is false against extra search at Q = 50

- File: `sections/results.tex:107`
- Current: `Vector push (\RNumQSweepPVRecentQEightHundredNewTopic{} at $Q{=}800$) leads on new-topic at every $Q$: uncharged refresh minus vector push is ...`
- Problem: at Q = 50 new-topic, ES (0.5621) and ESO (0.5422) beat PV (0.4980) (`m1_conditions.csv`, `m1_online_conditions.csv`). The data supports only "beats refresh", which is what the clause after the colon reports.
- Replacement (38 words): `Vector push (\RNumQSweepPVRecentQEightHundredNewTopic{} at $Q{=}800$) beats refresh on new-topic at every $Q$: uncharged refresh minus vector push is ...`, with the rest unchanged.

### M-9. The paper never says that Q = 800 was added by Addendum 1

- File: `sections/setup.tex:40`
- Current: the plan "fixed ... sweeps over $S, m, B \le 16, Q \le 400$", and `Before main runs, Addendum 1 dropped unused sweeps, added single-segment placement, set $A = \lceil 2S / n_B \rceil$, corrected pushed vectors from $1{,}536$ to $1{,}544$ B ($8$-B ID), cached ground truth, and replaced the initial hypotheses with H1, H2, GT2, H3, and H4.`
- Problem: M1 and the X4 headline use Q = 800, which is outside the plan's Q <= 400. The source shows `repo/configs/prereg.yaml:61` `queries_per_epoch: [50, 100, 200, 400]` and `prereg_addendum1.yaml:136` `[50, 200, 800]` (DEVIATIONS D-9, D-12). As written, readers may think Q = 800 was post hoc.
- Replacement (48 words): `Before main runs, Addendum 1 dropped unused sweeps, set $Q \in \{50, 200, 800\}$, added single-segment placement, set $A = \lceil 2S / n_B \rceil$, corrected pushed vectors from $1{,}536$ to $1{,}544$ B ($8$-B ID), cached ground truth, and replaced the initial hypotheses with H1, H2, GT2, H3, and H4.`

### M-10. Sensitivity-run recalls appear next to "match M2" but differ from the M2 numbers in the same paragraph

- File: `sections/results.tex:176`
- Current: `... and the $A = 8$ sensitivity runs (seeds \RNumSeedMin{} to \RNumSensSeedMax{}) match M2 on every query (\RNumSensRecallMeanSteady{} steady, \RNumSensRecallMeanNewTopic{} new-topic).`
- Problem: 0.0810 and 0.2799 are 3-seed means. Two sentences later, M2 always-fresh is 0.0793 and 0.2861 (5 seeds). Without a label this reads as a mismatch.
- Replacement (39 words): `... and the $A = 8$ sensitivity runs (seeds \RNumSeedMin{} to \RNumSensSeedMax{}) match M2 on every query (means over those seeds: \RNumSensRecallMeanSteady{} steady, \RNumSensRecallMeanNewTopic{} new-topic).`

### M-11. Conclusion says fresh summaries and vector push are "close" with G = 8, but on steady vector push wins clearly

- File: `sections/conclusion.tex:6`
- Current: `With $G = 8$ active segments, fresh summaries and vector push are close at $B \le 4$ and vector push leads at $B = 8$.`
- Problem: this is true only on new-topic. On steady, M3 refresh minus PV is -1.79 [-2.38, -1.20] at B = 2 and -3.67 [-4.57, -2.85] at B = 4. The abstract, intro and results all say vector push wins at every B on steady.
- Replacement (26 words): `On new-topic with $G = 8$ active segments, fresh summaries and vector push are close at $B \le 4$ and vector push leads at $B = 8$.`

---

## Low

### L-1. Abstract uses "data-change" before saying it is a refresh rule
- `main.tex:30`. Current: `With eight, it no longer leads vector push at $B{=}2$ once data-change's score messages are charged, has no clear winner at $B{=}4$, and loses at $B{=}8$.`
- Replacement (27 words): `With eight, data-change refresh no longer leads vector push at $B{=}2$ once its score messages are charged, has no clear winner at $B{=}4$, and loses at $B{=}8$.`

### L-2. "CI" is never spelled out anywhere in the paper
- `main.tex:30`. Current: `gaps are percentage points (pp) with \RNumCIPct{} CIs.`
- Replacement (sentence 24 words): `gaps are percentage points (pp) with \RNumCIPct{} confidence intervals (CIs).`

### L-3. "Nearest center" in the abstract and intro does not match the definition in Section III
- Section III places arrivals by the nearest initial setup centroid (32-cluster k-means). The intro's first paragraph uses "center" to mean one of the 16 summary centers.
- `main.tex:30`: `When arrivals join the nearest-center shard,` becomes `When arrivals join the nearest-centroid shard,` (28 words).
- `intro.tex:10` (two places): `the shard with the nearest initial center` becomes `the shard with the nearest initial setup centroid`, and `the active shard with the nearest initial center` becomes `the active shard with the nearest initial setup centroid`. The longer sentence becomes 38 words.

### L-4. "Reverses" on overall recall@10 claims more than the interval shows
- The interval is -0.20 [-0.38, +0.02] and includes zero.
- `results.tex:107`: `and it reverses on overall recall@10 (\RNumAddThreeXFourESOverallNewTopic{})` becomes `and it is not ahead on overall recall@10 (\RNumAddThreeXFourESOverallNewTopic{})`.
- `conclusion.tex:4`: `(fragile, reversing on overall recall@10)` becomes `(fragile, and not ahead on overall recall@10)` (35 words).

### L-5. The asterisk footnote ("lead or tie weakens or disappears when charged") is applied unevenly
- Two leads weaken under charging but are not marked:
  - H3b: the DC lead over RN falls from 3.60 to 2.66.
  - M3 B = 4 new-topic Ref - ESO: falls from +3.86 to +2.92.
- Meanwhile Q = 800 Ref - ESO (+1.06 falling to +0.43 [+0.07, +0.79], still significant) is marked.
- `results.tex:25`: `Gated / Fail (data-change wins) \\` becomes `Gated / Fail (data-change wins)$^{\ast}$ \\`.
- `results.tex:97`: `\RNumHFourBRefMinusESOBFourNewTopicCell{} \\` becomes `\RNumHFourBRefMinusESOBFourNewTopicCell{}$^{\ast}$ \\`.

### L-6. Table VII caption mentions M2, but the table has no M2 rows
- `results.tex:118`. Current: `M2: $5$ tie AF;`
- Replacement: `M2 omitted (all $5$ tie AF);`

### L-7. Stream sentence cites a table that does not list streams
- `setup.tex:36`. Current: `We test two $50$-epoch streams (Table~\ref{tab:exp_matrix}).`
- Replacement: `We test two $50$-epoch streams in every suite.`

### L-8. Sentence fragment
- `setup.tex:36`. Current: `\textbf{(2) New-topic stream.} Modeled after the Big-ANN NeurIPS'23 streaming runbook,`
- Replacement: `\textbf{(2) New-topic stream.} It is modeled after the Big-ANN NeurIPS'23 streaming runbook,`

### L-9. Dangling opening phrase
- `conclusion.tex:6`. Current: `From our single-process simulator on \RNumCorpusTotalPassages{} MS MARCO MiniLM vectors, future work should test adaptive summaries, schedule-aware routing, higher ingest rates, and tail latency.`
- Replacement (27 words): `Our results come from a single-process simulator on \RNumCorpusTotalPassages{} MS MARCO MiniLM vectors; future work should test adaptive summaries, schedule-aware routing, higher ingest rates, and tail latency.`

### L-10. "The NR gap" is unclear
- The 5.6% and 13.5% are shares of the gap from never-refresh to oracle routing.
- `results.tex:43`: `of the NR gap.` becomes `of the never-refresh-to-oracle gap.` (33 words).

### L-11. "Dashed" in the Fig. 1 caption is ambiguous
- The never-refresh line is also dashed, in every panel.
- `results.tex:112`: `Dashed (c, d): data-change with` becomes `Dashed blue (c, d): data-change with`.

### L-12. "Action-space" is now unexplained (regression from the R10 edits)
- R10 deleted the sentence saying that at Q = 50 batch spends 9 of its 59 searches on a fourth shard. "Action-space" now has no referent.
- `results.tex:107`: `mixes look-ahead with action-space and byte caps.` becomes `mixes look-ahead with byte caps and with batch's option to search two extra shards for some queries.` (30 words).

### L-13. Table numbers do not follow the order of first citation
- First citations run I, II, III, V (setup.tex:42), IV (results.tex:38), VI, VIII (results.tex:107), VII (results.tex:178 after M-2).
- Fix 1, `setup.tex:42`: `Table~\ref{tab:q_sweep} fixes the $Q{=}200$ winner across $Q$` becomes `the query-rate sweep (Section~\ref{sec:results_m1}) fixes the $Q{=}200$ winner across $Q$`.
- Fix 2: in results.tex, move the `tab:add3` float block above the `tab:refresh_rules` block, so Addendum 3 becomes Table VII.

### L-14. Table headers and symbols that are never defined
- Tables VII and IX use the header "Str." without defining it. Table VII also uses "Cond." Add `Cond.: condition; Str.: stream;` to the Table VII caption and `Str.: stream;` to the Table IX caption (results.tex:118, 187).
- Table I's caption defines "B: bytes", while its header "Epoch Total ($B{=}4$)" uses B for the budget. Table I also uses $Q_s$ before Section III defines it.
- `system.tex:8`: `($S{=}32, m{=}2, Q{=}200, Q_s{=}64$ queries; RN: recovered-neighbor; B: bytes)` becomes `($S{=}32, m{=}2, Q{=}200, Q_s{=}64$ sample queries, budget $B{=}4$; RN: recovered-neighbor; B: bytes)`.

### L-15. "[-0.00, +0.42]" t-interval looks like a typesetting artifact
- At `results.tex:107`, `\RNumLookaheadMOneQTwoHundredSteadyTInt` renders as "[−0.00, +0.42]". The real lower bound is -0.004 (recomputed), so the text is honest but reads oddly.
- Fix in `scripts/gen_numbers.py`: format this one macro's lower bound with 3 decimals, `f"$[{lk_t_lo:+.3f},$ ${lk_t_hi:+.2f}]$~pp"`, which renders "[-0.004, +0.42]". No new macro is needed.

---

## Macro spot-checks (recomputed from CSVs with the same formatting)

All 22 match `generated/numbers.tex` exactly.

| Macro | Source (CSV, row / column) | Value |
|---|---|---|
| RNumHOneGapSteady | paired_comparisons, M1 steady H1_gate_M1, recent_diff | +0.55 [-0.07, +1.12] |
| RNumHOneGapNewTopic | same, new_topic | +2.29 [+1.47, +3.17] |
| RNumHTwoPVvsRefreshNewTopic | paired, M1 NT Q200 push_vectors - data_change | +2.09 [+1.00, +3.27] |
| RNumMOneAFRecentNewTopic | m1_conditions, NT Q200 always_fresh | 0.4872 |
| RNumGateMTwoNewTopic | paired, M2 NT GATE_M2 | +22.61 [+20.83, +24.40] |
| RNumHFourBRefMinusPVBTwoNewTopic | paired, M3 NT B2 H4b vs PV (policy_a = data_change) | +1.47 [+0.03, +2.90] |
| RNumHThreeBRNMinusDCNewTopic | paired, M3 NT B4 RN - DC | -3.60 [-4.93, -2.26] |
| RNumAddThreeXOneRNBFourNewTopic | add3_paired, X1 NT B4 (boot) | +2.66 [+1.46, +4.00] |
| RNumAddThreeXTwoPVBTwoNewTopic (+ TInt) | add3_paired, X2 NT B2 PV | -0.16 [-1.89, +1.49]; t [-2.56, +2.18] |
| RNumAddThreeXFourESNewTopic (+ Overall) | add3_paired, X4 NT ES | +0.38 [+0.01, +0.74]; overall -0.20 [-0.38, +0.02] |
| RNumQSweepRefMinusESQEightHundredNewTopic | breakeven_q, NT Q800 (best = data_change) | +1.01 [+0.60, +1.42] |
| RNumSpearmanRNNewTopicMThree / NewVecs | spearman_comparison, M3 NT B4 RN | 0.26 / 0.63 |
| RNumLookaheadMOneQTwoHundredSteady | lookahead_comparison, M1+M1_online steady Q200 | +0.21 [+0.03, +0.37] |
| RNumMThreePVRecentBEightNewTopic | m3_conditions, NT B8 PV | 0.3521 |
| RNumMOneESOBytesSteady | m1_online_conditions, steady Q200 | 4,649,386 |
| RNumQueriesWithRecentSteadyQTwoHundred | m1_conditions, n_queries_with_recent | 11,917 |
| RNumGateMTwoOverallSteady | paired, M2 steady GATE_M2 overall | -1.59 [-1.74, -1.45] |
| RNumAddThreeXThreeDCBFourNewTopicCell | add3_paired, X3 NT B4 | -0.94 [-1.55, -0.32] |
| RNumGateMThreeSteady | paired, M3 steady GATE_M3 | +1.16 [+0.38, +1.91] |
| RNumMThreeSSRecentBFourNewTopic | m3_conditions, NT B4 SS | 0.2823 |

Raw recomputations from `queries.npz`, using gen_numbers' own bootstrap and t functions; all match the paper:

- M3 NT B2 DC - PV: t [-0.35, +3.25]; seed 42 = -0.29.
- M3 NT B4 RN - DC: boot [-4.93, -2.26].
- GT3 NT: t [+4.79, +8.75]; seed 42 = +4.10.
- GT3 steady: overall negative in all 5 seeds.
- M3 NT B8 RR - ESO: t [-0.12, +3.07].
- M3 NT B4 DC - PV: +1.04 [-0.50, +2.53].
- Bootstrap narrowing on the five M3 headline tests: 19% to 27%.
- RN - QL: 51% wider.
- M1 steady RN - NR [-0.20, +0.55] and DC - NR [-0.25, +0.52], supporting "none beats NR reliably".
- M1 ES - AF and PV - AF exclude zero on both streams, supporting "beat always-fresh".
- Charged DC at B = 8 differs from AF on 92 steady and 116 new-topic queries, supporting "no longer ties".
- M2 rho(RN) = rho(N_new) = 1.00 (used in M-4).

Arithmetic hand-checked:
- Tables I and II byte totals.
- 1,116 total runs.
- Unspent and stranded bytes (24,448 and 2,944).
- Amortized RN budget (13,757).
- Starvation caps A.
- Seed-42 per-epoch insert split (27 × 1,055 + 23 × 1,054 = 52,727).
- X1 epoch split (weighted mean 2.66).
- 3.60 = 2.66 + 0.94.
- Oracle-gap shares (5.6% and 13.5%).
- PV sampling predictions.

## Checks with no problem found

- **Numbers across sections:** every macro used in the abstract, intro, results, tables and conclusion resolves to the same value everywhere, with consistent sign and order of subtraction. Prose levels agree with Table VII (for example 0.2775 vs 0.3135, 0.4813, 0.7042), and prose gaps agree with Tables IV, V, VI and VIII.
- **Hypothesis outcomes:** H1, H2, H2b, PR2, GT2, H3, H4, PR3, GT3, H3b, H4b and LK are described the same way in the abstract, intro, Table IV, results and conclusion. The B = 2 H4b lead is called fragile everywhere and is removed when scores are charged.
- **References:** no undefined references or citations in main.log.
- **Text hygiene:**
  - No en or em dashes in prose. The only dashes are the em dash IEEEtran places after "Abstract", the bibliography's repeated-author dash, and page ranges.
  - No LaTeX artifacts.
  - No doubled words.
  - Brackets balance in every paragraph.
- **Double-blind:**
  - The author block reads "Anonymous Authors".
  - pdfinfo shows only Creator "TeX" and Producer "pdfTeX-1.40.27 (TeX Live 2025)", with no Title or Author entry.
  - Decompressed PDF streams contain none of the following: names, institution, e-mail, local paths, GitHub links, or the repo commit hashes 625f307, 67f049d and 7c4201d.
  - The figure carries only the Matplotlib creator string.
  - The new Acknowledgment section names AI tools only, so it reveals no identity. Some double-blind venues ask authors to drop acknowledgments at review time; check the CCGrid 2027 instructions.
- **Sentence-length gate (scripts/sentence_stats.py on the current PDF):**
  - Abstract: max 28 words, none over 30.
  - Introduction: max 38 words.
  - Conclusion: max 39 words.
  - Section III has two math-heavy definition sentences (RR and batch ES) that count over 50 tokens because the symbols are counted as words. This is noted only; it was not changed in this round.

## R10 deletions reviewed for side effects

Sentences deleted between postFixR9 and R10c:
- the Q = 50 batch s(m+2) allocation;
- M2 "batch barely beats never-refresh";
- RN steady pairwise gaps at B = 4;
- "rho(N_new) defined in all epochs";
- M3 B = 8 counterfactual rho;
- M3 QL/SS cap-forcing shares;
- the Sens cap-refresh sentence.

None of these deletions leaves a broken reference or a dangling number. The only side effect is L-12 ("action-space"). The macros for the deleted text are no longer written to numbers.tex, which is harmless.
