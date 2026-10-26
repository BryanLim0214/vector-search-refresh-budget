# Skeptic review of the main results (M1, M2, sens, M1_online)

Reviewer: Claude skeptic. Date: 2026-10-24. Scope: `analysis_root/out_addendum1/SUMMARY.md` and CSVs,
`repo-prereg2/configs/prereg_addendum1.yaml` and `prereg_addendum2.yaml`, `repo/dvs/` at commit `625f307`
(M1, M2, sens), the `extra_search_online` code at `67f049d` (M1_online), `audit/SKEPTIC_PILOT.md`,
`audit/GUARDIAN_C1_technical.md`, and the raw `queries.npz`, `epochs.parquet`, `summary.json` and
`provenance.json` files under `analysis_root/main/{M1,M2,sens,M1_online}`. M3 is ignored (still running).

I changed no file in `repo`, `repo-prereg2` or `analysis_root/main`. My scripts and their full output are in
`<reviewer scratch folder, not published>\`
(`verify.py`, `verify.out`, `verify2.py`, `verify2.out`). They read the per-query arrays directly and use their
own two-stage bootstrap (resample seeds, then queries within each drawn seed; 4,000 replicates; different RNG
from the analysis script).

## Bottom line

- **No number in SUMMARY.md is wrong.** Every value I recomputed (H1, H2, H2b at Q = 200, GATE_M2, H4 at all B,
  PRED_M2_TIES, sens, bytes, Spearman) matches to the printed digits. Bootstrap intervals differ only in the second
  decimal, as expected from a different RNG.
- **No bug found** in the code paths of `extra_search`, `extra_search_online`, `push_vectors`, the recent-recall
  metric, or query pairing. `push_vectors` does not see future queries or the ground truth.
- **Claim 3, as phrased in the brief, is false.** On M2, `push_vectors` wins at every B only on `steady`. On
  `new_topic`, the refresh rules (identical to `always_fresh`) beat `push_vectors` at every B by +19.40, +16.04 and
  +9.64 pp. SUMMARY.md has this right (line 11); the paper must not say "at every B" without the stream.
- **Three results follow from the design rather than from the methods,** and the paper must say so:
  (a) `push_vectors`' gain in recent recall equals the share of new vectors it pushes times the share routing
  misses, to within 0.1 to 1.2 pp in all eight conditions; (b) M2 `steady` recent recall sits at the chance level
  2/32 = 0.0625 because a 16-center summary cannot describe a random sample of the whole corpus; (c) the M2
  Spearman values of 1.0 are degenerate.
- **New finding not in SUMMARY.md:** on M2 `steady`, fresh summaries *lower* overall recall@10 by 1.59 pp
  [-1.74, -1.44], negative in all 5 seeds. The +1.66 pp recent gain is bought with an overall loss.

## Independent recomputation (pooled over queries, 5 seeds unless noted)

| Quantity | SUMMARY.md | Mine | Per-seed gaps (42..46) |
|---|---|---|---|
| H1 steady, AF - NR recent | +0.55 [-0.07, +1.12] | +0.55 [-0.10, +1.14] | +0.67, +0.69, +0.54, +1.31, -0.47 |
| H1 new_topic, AF - NR recent | +2.29 [+1.47, +3.17] | +2.29 [+1.48, +3.17] | +1.98, +1.66, +2.77, +2.05, +3.18 |
| H2 steady, ES - RN | +2.76 [+2.33, +3.22] | +2.76 [+2.30, +3.20] | +2.44, +3.22, +2.86, +2.35, +2.89 |
| H2 steady, PV - RN | +1.88 [+1.44, +2.36] | +1.88 [+1.43, +2.35] | +2.44, +1.72, +1.66, +1.52, +2.06 |
| H2 new_topic, ES - DC | +1.97 [+0.74, +3.15] | +1.97 [+0.76, +3.11] | +2.63, +1.84, +0.46, +3.60, +1.04 |
| H2 new_topic, PV - DC | +2.09 [+1.00, +3.27] | +2.09 [+1.02, +3.28] | +2.01, +1.75, +1.62, +3.86, +1.10 |
| H2b steady, ESO - RN | +2.55 [+2.17, +2.97] | +2.55 [+2.17, +2.94] | +2.31, +2.98, +2.47, +2.40, +2.57 |
| H2b new_topic, ESO - DC | +1.84 [+0.58, +3.01] | +1.84 [+0.62, +3.01] | +2.50, +1.82, +0.08, +3.43, +1.10 |
| GATE_M2 steady, AF - NR recent | +1.66 [+1.17, +2.24] | +1.66 [+1.17, +2.24] | +1.60, +2.53, +1.26, +1.39, +1.49 |
| GATE_M2 new_topic | +22.61 [+20.83, +24.40] | +22.61 [+20.83, +24.32] | +20.41 .. +24.81 |
| H4 steady B=4, RR - PV | -4.31 [-5.09, -3.47] | -4.31 [-5.06, -3.50] | -5.05, -3.05, -4.53, -4.56, -4.41 |
| H4 new_topic B=4, RR - PV | +16.04 [+14.40, +17.73] | +16.04 [+14.34, +17.68] | +14.84 .. +17.76 |
| H4 steady B=4, RR - ES | +0.87 [+0.33, +1.46] | +0.87 [+0.34, +1.45] | +0.80, +1.77, +0.35, +0.58, +0.81 |
| PRED_M2_TIES max abs per-query diff | 0.0 | 0.0 (recent and overall, all B, both streams) | |

Mismatches with SUMMARY.md: **none.**

---

## Claim 1. H1 PASS: under centroid placement, fresh summaries add little (+0.55 pp steady, +2.29 pp new_topic)

**Verdict: HOLDS WITH CAVEAT.**

Evidence:
- Numbers reproduce (table above). Both upper 95% bounds (+1.12 and +3.17 pp) are below 5 pp, so the result holds
  on the interval, not only on the point estimate (the gap GUARDIAN_C1 H-6 item 5 asked to close).
- Same queries in every method: 11,917 (steady) and 6,375 (new_topic) queries with recent neighbours for all
  11 methods; NaN masks identical per seed. The recent-recall filter does not differ by method
  (`metrics.py:216-224` depends only on ground truth).
- On overall recall, AF - NR is +0.00 pp [-0.25, +0.23] (steady) and +0.58 pp [+0.34, +0.80] (new_topic).
- "Little" is relative to large headroom: `oracle_routing` - NR is +9.70 pp (steady) and +16.92 pp (new_topic),
  so fresh summaries close about 6% and 14% of the routing gap.
- Steady is not significant at all (seed 46 is negative, -0.47 pp).
- Cause is the design, as the pilot skeptic showed (`SKEPTIC_PILOT.md`, bottom line, 85.5% of new vectors land on a
  shard already in their top 2): new vectors go to the shard of their nearest *initial* center
  (`shards.py:294-300`), so stale summaries already point at the right shard.
- H1 restates a pilot observation and reuses seed 42 (GUARDIAN_C1 H-6 item 2). It is a confirmation, not an
  independent test.

Safe sentence: "When new vectors go to the shard with the nearest initial center, refreshing all 32 summaries every
epoch raised recent recall@10 by 0.55 points (95% CI -0.07 to 1.12) on the steady stream and 2.29 points (1.47 to
3.17) on new_topic, while routing by true neighbour counts would gain 9.7 and 16.9 points; this confirms the pilot
observation on five seeds, one of which (42) the pilot also used."

## Claim 2. H2 and H2b PASS: at equal bytes, extra_search and push_vectors beat the best refresh rule

**Verdict: HOLDS WITH CAVEAT** (the result is real and robust; the framing of each method needs care).

Evidence for robustness:
- All 5 seeds positive for ES, ESO and PV against the best rule on both streams. ES and PV also beat *every* refresh
  rule in every seed (`verify.out`, "vs round_robin" ... lines), and both beat unbudgeted `always_fresh`
  (steady: ES +2.36 [+1.78, +2.94], PV +1.48 [+0.87, +2.09]; new_topic: ES +1.38 [+0.43, +2.27], PV +1.50
  [+0.47, +2.49]) while spending one eighth of its bytes.
- The gap is not an early-epoch effect: ES - RN steady is +2.84 pp in epochs 0-24 and +2.65 pp in epochs 25-49;
  PV +1.98 / +1.73.
- Byte accounting favours the refresh rules, not the alternatives. Per run: RR/DC/QL/SS 4,915,200 B; RN 4,768,000 B
  charged, 7,913,728 B with its setup broadcast spread over 50 epochs; ES 4,885,200; PV 4,863,600; ESO 4,649,386
  (steady) and 4,604,342 (new_topic). Not charged and all favouring refresh: RN setup (3,145,728 B,
  `run.py:470-474`, `charge_setup_to_budget: false`), the 4-byte per-shard `data_change` score (128 B/epoch,
  `policies.py:457-458`, would cut DC from k = 4 to k = 3), and the "has new vectors" bit every rule sorts on
  (`policies.py:517-523`). ES gets 117,200 B more than RN over a run (about 1.4 extra searches per epoch out of 59);
  irrelevant, since ES also beats DC, which spends more bytes than ES.
- The best rule is picked on the same data from five near-identical means (winner's curse), which inflates the
  baseline and makes H2 conservative.
- `extra_search_online` is causal: its threshold uses only the previous epoch's query gaps, or the 64 sample queries
  at epoch 0 (`repo-prereg2/dvs/policies.py`, `start_online_epoch` and `decide_extra_search_online_query`, commit
  `67f049d`). Non-ESO code paths did not change between `625f307` and `67f049d` (diff touches only
  `time_segment_g` placement and the online branch), and NaN masks match, so pairing ESO (commit `67f049d`) against
  M1 refresh runs (commit `625f307`) is valid.
- `push_vectors` has no look-ahead: it pushes the newest arrivals from a buffer filled at insert time
  (`policies.py:176-188, 370-386`); inserts, pushes and refreshes all happen before the epoch's queries for every
  method (`run.py:531-640`). It never reads queries or ground truth.

Caveats the paper must carry:
1. **`push_vectors` is a sampling effect, not a routing improvement.** Its recent-recall gain over `never_refresh`
   equals (share of new vectors pushed) x (share of recent neighbours routing misses):

   | Condition | Share pushed | Predicted PV - NR | Observed PV - NR (recent) | Observed (overall) |
   |---|---|---|---|---|
   | M1 steady B=4 | 6.30% | +1.87 | +2.03 | +0.60 |
   | M1 new_topic B=4 | 6.19% | +3.32 | +3.79 | +0.93 |
   | M2 steady B=2 / 4 / 8 | 3.10 / 6.30 / 12.70% | +2.91 / +5.91 / +11.90 | +3.06 / +5.98 / +11.82 | +0.94 / +1.86 / +3.79 |
   | M2 new_topic B=2 / 4 / 8 | 3.05 / 6.19 / 12.48% | +2.86 / +5.82 / +11.73 | +3.21 / +6.57 / +12.97 | +0.90 / +1.83 / +3.74 |

   The coordinator keeps every pushed vector for the whole run (`coordinator.py:61-90`) and scans all of them for
   every query by exact search at no byte cost (`coordinator.py:285-295`). On overall recall PV gains only
   +0.54 pp [+0.37, +0.75] (steady) and +0.32 pp [+0.12, +0.54] (new_topic) over the best rule, because
   pushed vectors are a small share of the corpus. Overall recall is not traded away, but the large recent gain is
   specific to the recent-recall metric.
2. **`extra_search` is a generic routing improvement, not a freshness method.** Its overall-recall gain over the
   best rule (+3.03 pp steady, +2.25 pp new_topic) is as large as its recent gain. It helps old and new vectors
   alike.
3. **Equal bytes is not equal work.** ES adds 59 shard searches per epoch on top of 2Q base searches (+15% shard
   query load at Q = 200, +59% at Q = 50, +3.7% at Q = 800) and an extra round trip for those queries. PV moves
   storage (up to 3,150 vectors, 4.9 MB) and per-query scans to the coordinator. Refresh rules spend shard CPU on
   k-means. None of this is in B.
4. ES (batch) sees the whole epoch's queries before choosing (`run.py:611`, `policies.py:310-340`). ESO removes
   this and loses only 0.21 pp (steady) and 0.13 pp (new_topic) at Q = 200, so look-ahead does not drive H2.
5. Break-even: on new_topic, ES and ESO fall behind `data_change` at Q = 800 (-1.01 and -1.06 pp); PV stays ahead at
   all Q. At Q = 50, ES spends 9 of its 59 searches on a second extra shard and ESO leaves at least 14,904 B/epoch
   unspent (both disclosed in Addendum 2).

Safe sentences: "At equal or smaller byte cost, searching one more shard for the 59 closest-call queries per epoch
raised recent recall@10 by 2.76 points (95% CI 2.33 to 3.22) on steady and 1.97 points (0.74 to 3.15) on new_topic
over the best refresh rule at 200 queries per epoch; a causal version that sees each query only on arrival raised it
by 2.55 and 1.84 points. The gain is as large on overall recall, so extra search improves routing in general rather
than finding new vectors in particular." and "Pushing the newest 63 vectors per epoch to the coordinator raised
recent recall@10 by 1.88 and 2.09 points over the best refresh rule, but overall recall by only 0.54 and 0.32 points;
the gain matches what exact search over a random 6% of new vectors would give."

## Claim 3. M2: all refresh rules tie always_fresh; push_vectors wins at every B and its lead grows with B

**Verdict on the ties: HOLDS (by construction).** **Verdict on "push_vectors wins at every B": ARTIFACT of
mis-stating the result; true only on steady.**

Evidence:
- Ties: the maximum absolute per-query difference between any refresh rule and `always_fresh` is exactly 0 for
  recent and overall recall, at B = 2, 4, 8, on both streams, and in `sens`. The reason is structural
  (GUARDIAN_C1 H-1): one shard receives all of an epoch's inserts (`shards.py:289-291`), every rule ranks shards
  with new vectors first (`policies.py:492-507, 517-523`), and every rule has k >= 1 at B >= 2. PRED_M2_TIES was
  stated after about 140 of 330 M2 runs had finished but before any was read (Addendum 2 header). The exact
  zeros also prove the per-query pairing is aligned: a misaligned pair could not tie on every query.
- So in H4 the "best refresh rule" is `always_fresh`. H4 compares fresh summaries with the alternatives; it says
  nothing about any rule. The +0.00 pp gap to the second-best rule in every H4 row (SUMMARY.md lines 24-29) is this
  tie, not a close race.
- Winners (recent recall, RR - PV): steady -1.40, -4.31, -10.15 pp (PV wins, lead grows); new_topic +19.40,
  +16.04, +9.64 pp (**fresh summaries win, PV loses at every B**). SUMMARY.md line 11 and lines 504-507 state this
  correctly.
- Why PV's lead on steady grows with B: B fixes how many vectors PV pushes (31, 63, 127 per epoch), and its gain
  tracks that share almost exactly (table in Claim 2). Fresh summaries gain nothing more with B because the one
  active shard is already refreshed at B = 2. The growth is arithmetic, not a property of the method.
- On new_topic, PV also loses on overall recall (-8.97, -8.04, -6.12 pp vs RR).
- `sens` (fixed cap A = 8, seeds 42-44) gives per-seed gaps identical to M2 B = 4 for the same seeds (AF - NR steady
  +1.60, +2.53, +1.26 in both). The cap cannot change recall here, so `sens` carries no information about the cap.

Safe sentence: "When all of an epoch's vectors go to one growing segment, every refresh rule refreshes that segment
and routes exactly like always_fresh (identical on every query), so this placement cannot rank the rules. On the
new_topic stream, fresh summaries beat pushing vectors at every budget (by 19.4, 16.0 and 9.6 points at B = 2, 4,
8); on the steady stream, pushing vectors wins by 1.4, 4.3 and 10.2 points, a lead that grows in proportion to the
number of vectors the budget can push."

## Claim 4. M2 steady: recent recall@10 is about 0.079 for every refresh rule and GATE_M2 fails (+1.66 pp). Bug or real?

**Verdict: HOLDS (real, follows from the design). Not a bug.** Must be disclosed with the overall-recall loss.

Evidence:
- `never_refresh` recent recall is 0.0627, which equals the chance level 2/32 = 0.0625: each query searches 2 of 32
  shards, and recent vectors sit on shards e mod 32 and (e-1) mod 32 regardless of topic. The stale summaries carry
  no information about where recent vectors are.
- The vectors are present and findable: `all_shards` recent recall is 0.9996. Ground truth, inserts and shard
  search work.
- The steady stream inserts vectors in a seeded random order over the whole stream set (`streams.py:182-183`), so
  each segment holds about 1,000 (epochs 0-31) or 2,000 (epochs 32-49) vectors spread over every topic, on top of its
  original cluster of about 1,560. Its 16-center summary is k-means over *all* the shard's vectors, warm-started from
  the old centers (`shards.py:155-179`). The full corpus needs about 32 x 16 = 512 centers to be routed by topic; one
  segment gets 16. The summary cannot represent a uniform sample of the corpus, so fresh summaries lift recent recall
  only from 0.0627 to 0.0793.
- New finding: fresh summaries hurt overall recall on M2 steady: AF - NR overall = -1.59 pp [-1.74, -1.44], negative
  in all 5 seeds (-1.44, -1.60, -1.45, -1.63, -1.81). The refreshed segment summary drifts toward the random new
  vectors and takes routing slots from the shards that hold the query's topic. The same is true for every refresh
  rule (they tie with AF), so on M2 steady the refresh rules lose 1.58 pp of overall recall to `never_refresh`.
- On new_topic the stream is ordered cluster by cluster (`streams.py:176-177`), so a segment receives one or two
  coherent topics per epoch, which 16 centers can describe; hence the +22.61 pp gate pass.
- Oracle routing reaches only 0.2346 on steady because a query's true top 10 is mostly old vectors on topic shards,
  so the two shards with the most true neighbours are rarely the time segments.

Safe sentence: "With random arrivals placed by time, each segment holds a sample of every topic, which 16 centers
cannot summarize: without refresh, recent recall@10 is 0.063, the chance level of searching 2 of 32 shards, and
fresh summaries raise it only to 0.079 while lowering overall recall@10 by 1.6 points (95% CI 1.4 to 1.7)."

## Claim 5. Refresh rules nearly tie in M1; is recovered_neighbor's estimator doing anything?

**Verdict: HOLDS WITH CAVEAT for "rules tie". ARTIFACT for any claim that the sample-query estimator works; the
M2 Spearman of 1.0 is degenerate and must not be reported as evidence.**

Evidence (M1, B = 4, Q = 200, recent recall, 95% CI):
- Steady, against `never_refresh`: RN +0.15 [-0.23, +0.56], DC +0.13 [-0.26, +0.52], RR -0.19 [-0.70, +0.29].
  RN - DC +0.02 [-0.33, +0.33]. All five rules lie within 0.34 pp of each other and of `never_refresh`
  (0.7026; rules 0.7008 to 0.7042). No rule is distinguishable from not refreshing at all on steady.
- New_topic: DC - NR +1.70 [+0.69, +2.78]; RN - NR +0.86 [-0.01, +1.74]; RN - DC -0.84 [-1.75, +0.03];
  RN - RR +0.84 [+0.14, +1.54]. DC, the only clearly useful rule, also gets its score messages free.
- The sample-query estimator ranks shards worse than the free "new vectors since last refresh" count in every
  M1 run. On the epochs where both are defined, RN-run means are 0.036 vs 0.196 (steady, NV higher in 69% of epochs)
  and 0.177 vs 0.497 (new_topic, 92%). Same pattern under all five rules (`verify.out`, Spearman block).
- The estimator is often empty. In RN's own steady runs, Spearman is defined in only 83 of 250 epochs; the
  estimator or the truth is constant (mostly all zero) in the rest (`metrics.py:309-310`). RN sees about 5 lift-in
  flips per epoch among its 64 sample queries. When all its scores are zero, the scaled priorities are all zero
  (`policies.py:80-89`) and the order falls back to "has new vectors", then "starved", then **lowest shard ID**
  (`policies.py:523`). So in most steady epochs RN behaves like a fixed shard-ID order with a starvation cap; 204 of
  its 750 refreshes (27%) were forced by the cap.
- M2 and `sens` Spearman = 1.0 (SUMMARY.md lines 557 onward): only the active segment has new vectors and a changed
  candidate summary, so both the estimate and the truth are zero on 31 shards and positive on one. Any estimator
  that is positive on that shard scores exactly 1.0. In seed 42 steady, RN's value is NaN in 35 of 50 epochs and
  1.0 in the other 15.
- Minor wording: SUMMARY.md Section 7 says "on Same Refresh Decisions"; the correlation is over all 32 shards per
  epoch before refresh (`run.py:579-622`), not over the refresh choices.

Safe sentence: "Under centroid placement the five refresh rules differed by at most 0.34 points of recent recall on
the steady stream and none differed reliably from never refreshing; on new_topic only data_change helped (+1.7
points, 95% CI 0.7 to 2.8). The recovered-neighbor estimator, built from 64 sample queries, ranked shards worse than
simply counting each shard's new vectors (mean Spearman 0.04 vs 0.20 on steady, 0.18 vs 0.50 on new_topic) and gave
no ranking at all in two thirds of steady epochs."

---

## Must fix or disclose before writing

1. **Do not write "push_vectors wins at every B" for M2.** It wins on steady only; fresh summaries win on new_topic
   at every B (Claim 3).
2. **State that M2 cannot rank refresh rules.** Every rule equals `always_fresh` on every query; H3 and H4's
   "best refresh rule" are `always_fresh`; `sens` reproduces M2 and says nothing about the cap. Do not report M2 or
   `sens` Spearman values (1.0) as estimator evidence.
3. **Report the M2 steady overall-recall loss** from fresh summaries (-1.59 pp, all seeds negative) next to the
   GATE_M2 value, and explain that recent recall sits at the 2/32 chance level by design.
4. **Explain `push_vectors` as a sample of new vectors searched exactly at the coordinator.** Give the share pushed
   (about 6% at B = 4) and the predicted-vs-observed gain, report its overall-recall gain (+0.54 / +0.32 pp) beside
   the recent gain, and list its free coordinator memory and scans.
5. **Report overall recall for `extra_search`** and say its gain is not specific to new vectors.
6. **"Equal bytes" paragraph** must list what is not charged: RN setup broadcast (3,145,728 B), DC score messages
   (128 B/epoch), the has-new-vectors signal all rules use, ES's extra shard work and round trip, PV's coordinator
   storage and scans, and ingest traffic. Say that these favour the refresh rules except the last three.
7. **Disclose that the sample-query estimator is weaker than the new-vector count** in every M1 condition and is
   undefined in most steady epochs, where RN reduces to lowest-shard-ID order plus the starvation cap.
8. **H1 framing:** say it confirms the pilot (seed 42 reused) and that the small gain follows from nearest-initial
   placement, which puts new vectors where stale summaries already point.
9. Fix the SUMMARY.md Section 7 heading ("same refresh decisions" is not what is computed).

## What I checked and found clean

- Query pairing: per-seed arrays have equal length (10,000 at Q = 200) and identical NaN masks for every pair
  compared; recent-recall denominators are identical across all 11 methods (11,917 and 6,375).
- No single seed drives any headline gap; every H2/H2b/H4 gap has the same sign in all 5 seeds.
- Byte totals in `summary.json` are identical across seeds and match the arithmetic (59 x 1,656 x 50 = 4,885,200;
  63 x 1,544 x 50 = 4,863,600; budget never exceeded, enforced at `run.py:666-670`).
- Ground-truth cache key includes stream, placement, Q and seed (`run.py:92-111`), so M1 and M2 do not share
  `gt_shards`.
- `push_vectors` dedupes by document ID at the coordinator (`coordinator.py:80-83`) and in the merge
  (`coordinator.py:201-212`); missed-neighbour counts skip pushed IDs (`metrics.py:232-233`).
