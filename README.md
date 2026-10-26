# Where to Spend Maintenance Bytes in Sharded Vector Search

This repository holds the simulator, experiment settings, analysis code, result tables, paper and a
selected review record for the paper "Where to Spend Maintenance Bytes in Sharded Vector Search:
Summary Refresh, Extra Shard Searches, or Vector Push". All experiments run in a simulator on one
computer. No real network is used; the simulator counts the bytes that each network message would take.

## The question

A vector database stores pieces of text as vectors, which are lists of numbers, and answers a question
by finding the stored vectors closest to the question's vector. Here the database is split into 32
parts called shards. A central program, the coordinator, keeps a short summary of each shard: 16 center
vectors that stand for what the shard holds. For each question it searches only the 2 shards whose
summaries look closest.

New passages arrive in rounds called time steps. They come either in random topic order or grouped by
topics that were not in the database at the start ("new topics"). Shards store their new passages, but
the coordinator's summaries stay as they were, so questions about new passages can go to the wrong
shards. Each time step the system may spend a fixed number of network bytes, its budget, counted in
whole summaries. The paper tests budgets of 2, 4 and 8 summaries. The project asks which way of
spending the budget works best:

1. **Refresh:** send new summaries for a few shards.
2. **Extra search:** search one more shard for some questions.
3. **Vector push:** send copies of new vectors to the coordinator, which then checks them for every
   question.

A second question is how to pick the shards to refresh. Among other rules, the project compares
**data change**, which ranks shards by how much of each shard is new and how poorly its current summary
fits the new passages, with **recovered neighbor**, a probe that sends 64 sample questions to guess
which refresh would find the most new neighbors. Data change needs each shard to send the coordinator a
4-byte score every time step. The main runs did not charge these scores to the budget.

Success is measured by **recent recall**: take a question's 10 truly closest passages and keep those
that arrived in the last two time steps; recent recall is the share of these that the system returns.
**Overall recall** is the same share counted over all 10 closest passages, old or new. Differences are
given in percentage points: going from 30% to 33% recall is a gain of 3 percentage points.

## What it found

These points restate the paper's abstract and conclusion in plain words. Numbers are rounded; the paper
(`paper/main.pdf`) gives exact values with 95% confidence intervals.

The best choice depends on where new passages are stored.

- **Each new passage goes to the shard whose summary center is closest.** These are the centers
  computed when the data was first split, so the old summaries already send questions to the shards
  that receive new passages. Compared with never refreshing, refreshing every summary in every time
  step changed recent recall by only about 0.6 percentage points for random topic order. With new
  topics, the change was about 2.3 percentage points. With a budget of 4 summaries and 200 questions
  per time step, extra search and vector push both did better than every refresh rule. Refresh by data
  change moved slightly ahead of extra search only with new topics and 800 questions per time step.
  That lead was fragile and did not show in overall recall, and refresh still trailed vector push.
- **New passages are stored by arrival time.** All of a time step's arrivals go to one active shard, or
  are spread over a group of eight active shards.
  - With random topic order, summaries of 16 center vectors could not describe the mixed arrivals.
    Refreshing them lowered overall recall a little, and vector push won at every budget.
  - With new topics and one active shard, refreshing raised recent recall by about 23 percentage points
    over never refreshing. It beat vector push at every budget tested.
  - With new topics and eight active shards, refreshing raised recent recall by about 7 percentage
    points over never refreshing. Against vector push there was no clear winner at a budget of 4, and
    vector push won at a budget of 8. At a budget of 2, refresh led only while the data change scores
    went uncharged. Because the budget buys whole summaries, paying for those few score bytes costs one
    summary refresh, and then the lead was gone.
- **Picking the shards to refresh.** One clear case is eight active shards with new topics and a budget
  of 4. There, data change reached a recent recall of about 31%, against about 28% for the recovered
  neighbor probe. The probe used 22% of the budget. In the closest-center and eight-shard settings, a
  simple count of new passages per shard predicted better than the probe how many new neighbors a
  refresh would bring in, though that count was not tested as a refresh rule. Even when data change paid for its own scores, so that both rules
  made the same number of refreshes, data change stayed ahead by about 2.7 percentage points.
- **Limits.** The experiments use 100,000 passages from MS MARCO, a public set of web passages and
  search questions, with about 1,000 new passages per time step. They do not measure response time, do
  not test routing that knows the arrival schedule, and leave some costs uncharged (the paper lists
  them).

## Repository layout

| Path | What it holds |
|---|---|
| `dvs/` | The simulator (a Python package). |
| `configs/` | Experiment settings in YAML, and the pre-registration files (`prereg*.yaml`). |
| `analysis/` | Scripts that turn run outputs into result tables (`analyze.py`, `analyze_add3.py`). |
| `analysis_root/` | Result tables and summaries from the author's runs (see below). |
| `scripts/` | Run queues, a runner that resumes stopped experiments, and paper check tools. |
| `tests/` | Automated tests (pytest). |
| `data/` | Only `PROVENANCE.json`. The vectors are not included; `dvs/data.py` makes them. |
| `docs/` | Design notes (`DESIGN.md`), changes to the plan (`DEVIATIONS.md`), and the commit ID map (`COMMIT_MAP.md`). |
| `paper/` | The paper: LaTeX sources, the number and figure scripts, and the built `main.pdf`. |
| `audit/` | Five selected review reports; `audit/README.md` explains each one. |
| `run_addendum2.cmd` | The original Addendum 2 launcher, kept as a record. It uses the author's local paths. |
| `requirements.lock.txt` | Exact package versions. |

The simulator modules in `dvs/`:

| File | What it does |
|---|---|
| `data.py` | Downloads MS MARCO and turns passages and queries into vectors. |
| `shards.py` | Splits the starting passages into shards with k-means, places new passages, and keeps one search index per shard (HNSW, a structure for fast approximate search; `M=16`, `ef_construction=200`, `ef=100`). |
| `summary.py` | Builds each shard's 16-center summary (24,576 bytes). |
| `coordinator.py` | Holds the last summary each shard sent, picks shards for each question, and merges results into a top-10 list. |
| `bus.py` | Counts the bytes of every network message. |
| `policies.py` | The refresh rules (round robin, data change, recovered neighbor, query popularity, shard size), the equal-byte alternatives (extra search, online extra search, vector push), and reference methods (never refresh, always fresh, oracle routing, all shards). |
| `streams.py` | The order in which new passages arrive (steady, cluster order, new topic, query shift). |
| `metrics.py` | Exact top-10 answers, recall, CPU timers, rank correlation and bootstrap intervals. |
| `run.py` | Runs a config file and writes the results. |

`analysis_root/` holds what the analysis produced from the author's runs:

- `out_addendum2/`: the full analysis of the main runs, used by the paper.
- `out_addendum3/`: the analysis of the exploratory Addendum 3 runs, also used by the paper.
- `out_addendum1/`: an earlier analysis made while the eight-shard runs were still going, kept for the record.
- `main/<experiment>/`: one summary per experiment (`experiment_summary.json` and `.parquet`) and the list
  of failed attempts (`failed_runs*.json`). The 1,116 raw run folders are not included.

Some older files (docs, configs, code comments, audit reports) mention the author's local folders
(`repo/`, `repo-prereg2/`, `E:\...`) and documents that are not published here, such as the project
proposal. They also cite old commit IDs; see "Pre-registration and commit history" below.

## Install

The project ran on Windows 10 with Python 3.12. The commands below use Windows paths. On Linux or macOS,
use `.venv/bin/python` and forward slashes; the `.cmd` and `.ps1` scripts are for Windows only.

```powershell
py -3.12 -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.lock.txt
```

The scripts look for Python at `.venv\Scripts\python.exe` in the repository root. To use another Python,
set the environment variable `DVS_PYTHON` to its path.

## Prepare the data

The MS MARCO data and the vectors made from it are not included here because of the MS MARCO license
terms. `dvs/data.py` downloads `BeIR/msmarco` and `BeIR/msmarco-qrels` from Hugging Face at fixed
revisions and builds the vectors:

- 100,000 passages: 50,000 present at the start and 50,000 that arrive over time;
- 1,000 test questions from the dev split;
- 64 sample questions from the train split, kept apart from the test questions;
- each one turned into a 384-number vector of length 1 with `sentence-transformers/all-MiniLM-L6-v2`.

This step needs a second Python environment with PyTorch and sentence-transformers. The author used
torch 2.5.1 with CUDA 12.1 and sentence-transformers 5.1.2; the script also uses numpy, pandas, pyarrow
and huggingface_hub. It uses a GPU when one is available and the CPU otherwise.

```powershell
<embedding python> -m dvs.data --data-dir data --seed 42
```

It writes the `.npy` files and ID lists into `data/` and rewrites `data/PROVENANCE.json`, which records
the dataset revisions, counts and file hashes. Compare your copy with the committed one using `git diff`.

## Run the experiments

The runner reads one config file and writes one folder per run under `results/`:

```powershell
$env:OPENBLAS_NUM_THREADS = '1'
.venv\Scripts\python.exe -m dvs.run --config configs\smoke.yaml --workers 1
```

Each run folder holds `epochs.parquet`, `summary.json`, `queries.npz` and `provenance.json`.

| Config | What it runs | Runs |
|---|---|---|
| `smoke.yaml` | A tiny check: 8 shards, 5 time steps, 2,000 vectors. | small |
| `pilot.yaml` | One-seed pilot used to fix settings before the main runs. | 1 seed |
| `main_M1.yaml` | New passages go to the shard with the closest starting center. | 330 |
| `main_M2.yaml` | All of a time step's arrivals go to one shard. | 330 |
| `main_sens.yaml` | A sensitivity check of M2 with a fixed waiting limit of 8 time steps for refreshes. | 66 |
| `main_M1_online.yaml` | Extra search that decides one question at a time, without looking ahead. | 30 |
| `main_M3.yaml` | Arrivals spread over a group of eight active shards. | 360 |
| `add3_*.yaml` | Exploratory Addendum 3: data change pays for its score messages, plus two replication runs. | 42 |
| `exp1` to `exp5` | Configs from the first plan; the paper's results do not use them. | |

`scripts\run_missing.py` runs only the runs that have no complete output, so a stopped experiment can
continue where it stopped:

```powershell
.venv\Scripts\python.exe scripts\run_missing.py --config configs\main_M3.yaml --workers 2
```

It counts a run as complete only when all four files exist and the run's recorded commit equals the
current commit. The queues `scripts\run_main_all.cmd` (M1, M2 and sensitivity runs) and
`scripts\run_add3.cmd` (Addendum 3) run a full set in order and log progress under `results\`.
`run_main_all.cmd` keeps the 8 workers of the original first pass, which ran out of memory (see the
memory notes); lower the `--workers` value in it to fit your machine.

To repeat the paper's runs as closely as possible, run each set from the commit it used. The run
outputs record these commits by their old IDs: `273bc59` (old `625f307`) for M1, M2 and the sensitivity
runs; `deae588` (old `67f049d`) for the online extra search and M3; and `5024201` (old `7c4201d`) for
Addendum 3. You can check out an older commit in a separate folder with
`git worktree add ..\dvs-273bc59 273bc59`. Those commits do not contain the scripts in `scripts\`, so in
that folder run `python -m dvs.run --config <config> --data-dir <path to your data folder>`. The later
commits say they keep the default behavior unchanged, and test 12 checks this on a small run.

### Memory notes

- On the author's machine (64 GB memory limit) each worker process used about 6.5 GB. Pick the number
  of workers so that this fits in the free memory.
- The first pass of the M1 runs used 8 workers and ran out of memory in 113 runs;
  `analysis_root/main/M1/failed_runs_pass1.json` lists them. The author then reran only the missing runs
  with `scripts\run_missing.py` and fewer workers.
- The `dvs` package sets `OMP_NUM_THREADS`, `MKL_NUM_THREADS`, `OPENBLAS_NUM_THREADS` and
  `NUMEXPR_NUM_THREADS` to 1 unless they are already set. Keep them at 1, also for the tests and the
  paper scripts: when memory was low, OpenBLAS with several threads failed to allocate memory.
- `scripts\supervise_add2.ps1` watched free memory during the Addendum 2 runs. It stops the queue below
  2 GB free, starts it again with 1 to 3 workers once 14 GB is free, and restarts the Windows
  RuntimeBroker process when it holds more than 6 GB. The comments at the top of the script show how to
  use it.

## Run the analysis

After all runs finish, the analysis scripts read the run folders under `results\main\` and
`results\addendum3\`. Write the output to new folders so you can compare it with the committed tables:

```powershell
.venv\Scripts\python.exe analysis\analyze.py --results-root results --out my_analysis\out_addendum2
.venv\Scripts\python.exe analysis\analyze_add3.py --results-root results --out-dir my_analysis\out_addendum3
```

`analyze.py` checks every run against the planned grid in the configs, lists missing runs, and writes
`SUMMARY.md` and the CSV tables. `analyze_add3.py` writes the Addendum 3 tables.

## Rebuild the paper numbers and the PDF

Every result number in the paper is a LaTeX macro in `paper/generated/numbers.tex`, written by
`paper/scripts/gen_numbers.py`. The figure comes from `paper/scripts/gen_figures.py`, which needs only the
committed tables in `analysis_root/`. It also needs matplotlib, which is not in `requirements.lock.txt`
(the committed figure was made with matplotlib 3.10.6).

`gen_numbers.py` also reads the raw run folders, which are not included. It looks for them at paths
taken from the author's local layout, all relative to the repository root:

- `analysis_root/main/<experiment>/<run>/` for M1, M2, the sensitivity runs, the online extra search and
  M3. `scripts\link_results.py` fills these folders from `results\main\` (with links, or copies when given
  `--mode copy`). It deletes those folders first, including the committed summary files in them;
  `git status` will then show those files as deleted or changed.
- `repo-prereg2/results/addendum3/` for the Addendum 3 runs and their `RUN_ADD3.log`.
- `repo/results/pilot/` for the pilot runs and `PILOT_REPORT.md`, a hand-written pilot report that is not
  included.

```powershell
cd paper
..\.venv\Scripts\python.exe scripts\gen_numbers.py
..\.venv\Scripts\python.exe scripts\gen_figures.py
latexmk -pdf main.tex
```

Without the raw run folders, `gen_numbers.py` stops with a missing-file error and leaves the committed
`numbers.tex` as it is. Skip it in that case; `latexmk` then uses the committed numbers.

`gen_numbers.py` also writes `paper_plain/generated/numbers.tex` for a shorter plain-English version of
the paper, which is not in this repository yet.

The PDF builds from the committed sources alone. Use TeX Live (the author used TeX Live 2025) and make
sure its `latexmk` comes first on your PATH; a `latexmk` without Perl, as in some MiKTeX setups, fails
without a clear message. `scripts\paper_gates.ps1` runs the number script, builds the PDF and prints
checks: page count, LaTeX errors, banned words, em dashes and typed numbers. Its double-blind check
reads author and institution names from the environment variable `DVS_BLIND_NAMES` (a regular
expression such as `Name1|Name2`).

## Run the tests

```powershell
$env:OPENBLAS_NUM_THREADS = '1'
.venv\Scripts\python.exe -m pytest -q
```

There are 24 tests. Without the vectors in `data/`, pytest ends with "2 failed, 12 passed, 10 errors".
The 12 that pass are all 11 tests in `tests/test_analyze.py` and test 6 in `tests/test_simulator.py`.
The other 12, all in `tests/test_simulator.py`, need the data:

- tests 1 to 5, 7 to 11 and 13 load the vectors from `data/` and stop with a missing-file error
  without them;
- test 12 also needs the data, and it reads the code of commit `625f307` by that old ID. In this
  repository that commit is `273bc59`, so create a local tag first: `git tag 625f307 273bc59`.

## Pre-registration and commit history

The plan was written down and committed in steps, each one before the runs it covers, except for the
exploratory Addendum 3:

- `configs/prereg.yaml`: the first plan, committed with the data record (`f00b849`).
- `docs/DEVIATIONS.md`: every change to the plan with its reason, first committed before the pilot
  and the main runs (`11722f5`) and extended in later commits.
- `configs/prereg_addendum1.yaml`: Addendum 1, written after the one-seed pilot and committed before
  the main runs (`566a665`). The analysis script was committed before the main results were read
  (`9f15f8f`).
- `configs/prereg_addendum2.yaml`: Addendum 2, which adds the eight-shard and online extra search runs
  (`152b06c`, corrected in `deae588` before any of those runs). `docs/DEVIATIONS.md` notes that 140 of
  the 330 M2 runs had finished, unread, when it was committed.
- `configs/prereg_addendum3.yaml`: Addendum 3, written after all main results were read, so its
  comparisons are exploratory and have no pass or fail test (`5024201`).

Before publishing, the author email in every commit was replaced with a GitHub private address. This
changed every commit ID. Files written before that, and the provenance records inside the run outputs,
cite the old IDs. `docs/COMMIT_MAP.md` maps each old ID to its published ID.

## AI use

The paper's Acknowledgment states: Gemini (Google) drafted the simulator, analysis code, figure and
number scripts, and the text of every section. Claude (Anthropic) checked each citation against its
source, recomputed reported numbers, and wrote or edited text in every section. Gemini, Claude, and
Codex (OpenAI) reviewed drafts; Codex edited no files. Result numbers come from scripts run on stored
run outputs. The authors take responsibility for the content.

For this repository: the reports in `audit/` come from that checking. Claude also helped put the
repository together; it adapted the script paths and wrote this README and `audit/README.md`.

## License

The code is released under the MIT license; see `LICENSE` (copyright 2026 Bryan Lim). The paper text and
figures in `paper/`, including `paper/main.pdf`, are included for reference and are not covered by the
MIT license. The MS MARCO data has its own license and is not included.
