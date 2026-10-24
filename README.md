# Distributed Vector Search (DVS) Byte-Counted Shard Simulator

Simulator, data pipeline, and test suite for the paper **"Finding New Passages in Distributed Vector Search"** (`proposal/proposal.tex`).

## Environment Setup

- **Simulator environment (`repo/.venv`)**: Python 3.12 virtual environment with pinned dependencies in `requirements.lock.txt` (`numpy`, `scipy`, `pandas`, `pyarrow`, `faiss-cpu`, `chroma-hnswlib` providing `hnswlib`, `scikit-learn`, `PyYAML`, `pytest`, `tqdm`).
- **GPU embedding environment (used only once for `dvs/data.py`)**: `E:\ChronoStreamRAG-Paper\.venv\Scripts\python.exe` (`torch 2.5.1+cu121`, `sentence-transformers 5.1.2`).

## Repository Layout

- `dvs/__init__.py`: Package initialization.
- `dvs/data.py`: Downloads official `BeIR/msmarco` corpus and queries and `BeIR/msmarco-qrels` (`train.tsv`, `dev.tsv`), samples 100,000 passages (50,000 initial + 50,000 streamed), 1,000 dev test queries, and 64 train sample queries (strictly disjoint from test queries), embeds them with `sentence-transformers/all-MiniLM-L6-v2` into unit-length `float32` 384-d vectors, and writes `.npy` arrays, `.json` ID lists, and `data/PROVENANCE.json`.
- `dvs/shards.py`: Partitions the 50,000 initial vectors into $S$ shards using `faiss.Kmeans` (or hash sharding by ID as a reference check), routes new vectors to the shard with the nearest initial center without data movement, and maintains each shard's `hnswlib` inner-product index (`M=16, ef_construction=200, ef=100`, single-threaded deterministic seed) and raw vectors.
- `dvs/summary.py`: Represents each shard's $c=16$-center summary (`24,576` bytes) and builds candidate summaries via 16-center spherical `faiss.Kmeans` warm-started from the shard's published centers.
- `dvs/coordinator.py`: Holds each shard's last published summary, scores shards for a query by maximum dot product with published centers, routes queries to the top $m$ shards, executes extra shard searches or local pushed-vector searches, and merges candidates into the final top 10 unique document IDs.
- `dvs/bus.py`: Explicit byte accounting for every network message (`1,536`-byte vectors, `24,576`-byte summaries, `120`-byte result lists, `1,656`-byte shard searches, `512`-byte per-shard sample cutoffs, `4`-byte per-shard refresh scores, `64 * m * 40`-byte sample search replies, and `98,304`-byte per-shard setup sample vectors reported both as setup and amortized over epochs).
- `dvs/policies.py`: Implements all 5 refresh rules (`round_robin`, `data_change`, `recovered_neighbor`, `ql`, `ss`) with $[0, 1]$ cross-shard score scaling and $+0.5$ aging per epoch waited, 2 equal-bytes alternatives (`extra_search`, `push_vectors`), and 2 bounds (`always_fresh`, `all_shards`).
- `dvs/streams.py`: Generates passage arrival orders (`steady` seeded random order and Big-ANN `cluster_order` 64-cluster k-means arrival order), `query_shift` test query selection (drawn from the dev test queries with lowest mean dot-product similarity to the 64 sample queries), and optional `bursts`.
- `dvs/metrics.py`: Incremental exact `faiss.IndexFlatIP` ground-truth index, overall `recall@10`, `recent_recall@10` over window $W$ (excluding and counting queries with zero recent true top-10 neighbors), `missed_neighbors` on skipped shards, per-component `time.process_time()` CPU timers, Spearman rank correlation between estimated and true recovered neighbors, and paired bootstrap 95% confidence intervals.
- `dvs/run.py`: CLI and programmatic runner that executes YAML configs and writes `epochs.parquet`, `summary.json`, and `provenance.json` under `results/<exp>/<run>/`.
- `configs/`: Pre-registered specification (`prereg.yaml`), smoke test (`smoke.yaml`), and experiment configs (`exp1_steady.yaml`, `exp2_cluster_order.yaml`, `exp3_query_shift.yaml`, `exp4_sweeps.yaml`, `exp5_reference_checks.yaml`).
- `tests/test_simulator.py`: Pytest suite verifying all 7 required invariants.
- `docs/DESIGN.md`: Sentence-by-sentence traceability matrix mapping `proposal/proposal.tex` to code.

## How to Run

### 1. Generate Embeddings (already completed and recorded in `data/PROVENANCE.json`)
```powershell
& E:\ChronoStreamRAG-Paper\.venv\Scripts\python.exe -m dvs.data --data-dir data --seed 42
```

### 2. Run the Test Suite
```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

### 3. Run the Smoke Test (`S=8`, 5 epochs, 2,000 vectors subset)
```powershell
.\.venv\Scripts\python.exe -m dvs.run --config configs/smoke.yaml
```

### 4. Run Main Experiments
```powershell
.\.venv\Scripts\python.exe -m dvs.run --config configs/exp1_steady.yaml
.\.venv\Scripts\python.exe -m dvs.run --config configs/exp2_cluster_order.yaml
.\.venv\Scripts\python.exe -m dvs.run --config configs/exp3_query_shift.yaml
.\.venv\Scripts\python.exe -m dvs.run --config configs/exp4_sweeps.yaml
.\.venv\Scripts\python.exe -m dvs.run --config configs/exp5_reference_checks.yaml
```
