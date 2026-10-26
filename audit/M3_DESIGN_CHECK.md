# M3 design check: shards with new vectors per epoch (done before reading H3b)

Date: 2026-10-24. Required by the technical re-check ("count shards with new vectors per epoch before
H3b is read"). Done after all 360 M3 runs finished and before any M3 result was opened.

Script: `scripts/count_active_shards_m3.py`. It rebuilds each M3 trajectory with the simulator's own code
(`partition_new_topic_stream`, `build_epoch_schedule`, `ShardRouterInitializer.assign_shards`, G = 8),
using the setup centers stored in each cached ground-truth file. Cross-check: for every exact top-10
neighbour inserted in the current epoch, the rebuilt shard equals the shard stored in the ground-truth
cache. Mismatches: 0 of 14,979 neighbours checked (steady 6,782; new_topic 8,197).

| Stream | Seeds | Mean shards with new vectors per epoch | Min | Epochs below 8 |
|---|---|---|---|---|
| steady | 42-46 | 8.00 | 8 | 0 of 250 |
| new_topic | 42-46 | 7.96 | 5 | 6 of 250 (5 shards: 2; 7 shards: 4) |

Conclusion: the risk that cluster-by-cluster arrival leaves fewer active shards than the budget did not
occur. At B = 2 and B = 4 the refresh rules must choose among about 8 shards with new vectors in every
epoch, on both streams, so H3b and H4b test real choices. At B = 8 (n_B = G) the rules can refresh every
shard with new vectors in almost every epoch, as PRED_M3_B8_TIES expects. Safe sentence for the paper:
"In M3, new vectors reached all 8 active shards in 494 of 500 epochs (at least 5 in every epoch)."
