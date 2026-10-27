# Live view of the simulator

`live_sim.py` runs the simulator one time step at a time and shows it in a browser page. Six methods run side by side on the same data: never refresh, rotation, most-changed, sample-query probe, extra search and push. Nothing is replayed: each step, the simulator's own code inserts the new passages, lets each method spend its budget, sends the test questions, and checks the answers against an exact search over every passage.

The page shows:

- the 32 shards as each method's coordinator sees them: darker squares hold more new passages that the shard's summary does not show yet, a colored ring marks a fresh summary sent this step, and a red dot marks new matches missed on that shard;
- where this step's new passages landed;
- the share of new matches each method found, this step and averaged over the steps so far;
- the bytes each method spent this step.

You can pick where new passages land (most similar shard, one shard per step, eight shards per step), whether they bring new topics or mixed topics, the budget (2, 4 or 8 summaries' worth of bytes per step) and the seed. The other settings are those of the paper's main runs: 32 shards, 2 shards per question, 16-center summaries, 50 time steps of 1,000 new passages and 200 test questions, and a recent window of 2 steps.

## Running it

From the repository root, with the simulator's packages installed (`requirements.lock.txt`) and the data made by `dvs/data.py`:

```
python demo/live_sim.py --repo . --open
```

The page opens at http://127.0.0.1:8765/ and is reachable only from the same computer. It starts with eight shards per step, new topics, budget 4 and seed 42. Each step takes about 3 seconds for all six methods on one core, and the process uses about 3 to 4 GB of memory. Stop it with Ctrl+C.

To compare every step with saved runs, add `--results` with the folders that hold them, for example `--results results/main`. When a saved run has the same setting and seed, the page says at each step whether the live recall equals the saved recall exactly. With the author's saved runs, all six methods matched at all 50 steps for eight shards per step, new topics, budget 4, seed 42, and at the first five steps for most similar shard, mixed topics, budget 4, seed 42.

## A saved run

`run_eight_shards_new_topics_B4_seed42.json` is the full record of one run with the starting setting (eight shards per step, new topics, budget 4, seed 42), as the page received it: for every step and method, the shards' sizes and new passages not yet in their summaries, which summaries were sent, missed new matches per shard, recall, bytes spent, and whether the step matched the saved paper run. All six methods matched at all 50 steps. `run_eight_shards_new_topics_B4_seed42.png` is the page at the end of that run.

Averaged over the 50 steps, the share of new matches found was: push 30.3%, most-changed 29.7%, extra search 28.0%, sample-query probe 27.4%, never refresh 25.6%, rotation 24.8%. These are per-step averages for one seed, so they are not the paper's reported numbers, which pool all questions over five seeds.

## How it relates to `dvs/run.py`

`live_sim.py` drives the same classes in the same order as `run_single_simulation` in `dvs/run.py`. It leaves out two things that only feed a logged rank correlation and change no state: the true number of neighbors each refresh would recover, and the probe's estimate for the other refresh rules. Each method builds its own shards: a copied hnswlib index restarts the random numbers that place new passages in its graph, so copies drift slightly from the paper runs. Shard indexes start small and grow when full, which gives the same index as the full-size ones the paper runs used.
