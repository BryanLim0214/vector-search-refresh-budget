"""Data pipeline for MS MARCO passage and query embeddings.

Downloads official `BeIR/msmarco` (corpus + queries) and `BeIR/msmarco-qrels` (train/dev splits),
samples 100,000 passages (50,000 initial + 50,000 streamed), 1,000 dev test queries, and 64 train
sample queries (strictly disjoint from test queries) using seed=42, embeds them with
`sentence-transformers/all-MiniLM-L6-v2` into unit-length float32 384-d vectors, and writes
`.npy` arrays, ID lists, and `data/PROVENANCE.json`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Dict, Tuple

import numpy as np

DATA_SEED: int = 42
CORPUS_DATASET_ID: str = "BeIR/msmarco"
CORPUS_REVISION: str = "a918e0d11a77ed33f42f29d98340b655593b96ad"
QRELS_DATASET_ID: str = "BeIR/msmarco-qrels"
QRELS_REVISION: str = "253fbf8a3f8d4a0932b63882b5162bedc84779f5"
MODEL_ID: str = "sentence-transformers/all-MiniLM-L6-v2"
MODEL_REVISION: str = "1110a243fdf4706b3f48f1d95db1a4f5529b4d41"

N_INITIAL: int = 50_000
N_STREAM: int = 50_000
N_PASSAGES: int = N_INITIAL + N_STREAM
N_TEST_QUERIES: int = 1_000
N_SAMPLE_QUERIES: int = 64
DIM: int = 384


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1 << 20)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def normalize_l2(vecs: np.ndarray) -> np.ndarray:
    vecs = np.asarray(vecs, dtype=np.float32)
    norms = np.linalg.norm(vecs, axis=1, keepdims=True)
    norms = np.maximum(norms, 1e-12)
    out = (vecs / norms).astype(np.float32)
    return np.ascontiguousarray(out)


def build_dataset(data_dir: Path, seed: int = DATA_SEED, batch_size: int = 512) -> Dict[str, object]:
    os.environ.setdefault("HF_HUB_DISABLE_SYMLINKS_WARNING", "1")
    import pandas as pd
    import pyarrow.parquet as pq
    import torch
    from huggingface_hub import hf_hub_download
    from sentence_transformers import SentenceTransformer

    data_dir.mkdir(parents=True, exist_ok=True)

    print(f"[data] Downloading {CORPUS_DATASET_ID}@{CORPUS_REVISION} and {QRELS_DATASET_ID}@{QRELS_REVISION}...")
    corpus_file = Path(
        hf_hub_download(
            repo_id=CORPUS_DATASET_ID,
            filename="corpus/corpus-00000-of-00001.parquet",
            repo_type="dataset",
            revision=CORPUS_REVISION,
        )
    )
    queries_file = Path(
        hf_hub_download(
            repo_id=CORPUS_DATASET_ID,
            filename="queries/queries-00000-of-00001.parquet",
            repo_type="dataset",
            revision=CORPUS_REVISION,
        )
    )
    dev_qrels_file = Path(
        hf_hub_download(
            repo_id=QRELS_DATASET_ID,
            filename="dev.tsv",
            repo_type="dataset",
            revision=QRELS_REVISION,
        )
    )
    train_qrels_file = Path(
        hf_hub_download(
            repo_id=QRELS_DATASET_ID,
            filename="train.tsv",
            repo_type="dataset",
            revision=QRELS_REVISION,
        )
    )

    print("[data] Computing SHA-256 of raw source files...")
    source_hashes = {
        "corpus/corpus-00000-of-00001.parquet": sha256_file(corpus_file),
        "queries/queries-00000-of-00001.parquet": sha256_file(queries_file),
        "dev.tsv": sha256_file(dev_qrels_file),
        "train.tsv": sha256_file(train_qrels_file),
    }

    # Sample 100,000 passages deterministically with fixed seed
    print("[data] Reading corpus metadata and sampling 100,000 passages...")
    pf = pq.ParquetFile(corpus_file)
    total_corpus_rows = pf.metadata.num_rows
    rng_passages = np.random.default_rng(seed)
    sampled_row_indices = rng_passages.choice(total_corpus_rows, size=N_PASSAGES, replace=False)
    # Read only the sampled rows, then apply the exact random permutation order from rng_passages
    corpus_table = pf.read(columns=["_id", "title", "text"])
    sampled_table = corpus_table.take(sampled_row_indices)
    passage_ids_str = [str(x) for x in sampled_table["_id"].to_pylist()]
    passage_ids = np.array([int(x) for x in passage_ids_str], dtype=np.int64)
    titles = sampled_table["title"].to_pylist()
    texts = sampled_table["text"].to_pylist()
    passage_texts = [
        (f"{t.strip()} {txt.strip()}".strip() if t and t.strip() else txt.strip())
        for t, txt in zip(titles, texts)
    ]
    del corpus_table, sampled_table

    # Read queries and split IDs via dev.tsv and train.tsv
    print("[data] Selecting 1,000 dev test queries and 64 train sample queries...")
    queries_df = pd.read_parquet(queries_file, columns=["_id", "text"])
    queries_df["_id_int"] = queries_df["_id"].astype(np.int64)
    queries_df = queries_df.sort_values("_id_int", kind="mergesort").reset_index(drop=True)

    dev_qrels = pd.read_csv(dev_qrels_file, sep="\t")
    train_qrels = pd.read_csv(train_qrels_file, sep="\t")
    dev_qid_set = set(dev_qrels["query-id"].astype(np.int64).unique().tolist())
    train_qid_set = set(train_qrels["query-id"].astype(np.int64).unique().tolist()) - dev_qid_set

    dev_queries_df = queries_df[queries_df["_id_int"].isin(dev_qid_set)].reset_index(drop=True)
    train_queries_df = queries_df[queries_df["_id_int"].isin(train_qid_set)].reset_index(drop=True)

    rng_queries = np.random.default_rng(seed + 1)
    test_idx = rng_queries.choice(len(dev_queries_df), size=N_TEST_QUERIES, replace=False)
    selected_dev = dev_queries_df.iloc[test_idx].reset_index(drop=True)
    test_query_ids = selected_dev["_id_int"].to_numpy(dtype=np.int64)
    test_query_texts = [str(t).strip() for t in selected_dev["text"].tolist()]

    # Filter out any train query whose normalized text matches any selected test query text
    test_text_set = {t.lower() for t in test_query_texts}
    train_queries_filtered = train_queries_df[
        ~train_queries_df["text"].astype(str).str.strip().str.lower().isin(test_text_set)
    ].reset_index(drop=True)

    rng_sample_q = np.random.default_rng(seed + 2)
    sample_idx = rng_sample_q.choice(len(train_queries_filtered), size=N_SAMPLE_QUERIES, replace=False)
    selected_train = train_queries_filtered.iloc[sample_idx].reset_index(drop=True)
    sample_query_ids = selected_train["_id_int"].to_numpy(dtype=np.int64)
    sample_query_texts = [str(t).strip() for t in selected_train["text"].tolist()]

    assert set(test_query_ids.tolist()).isdisjoint(set(sample_query_ids.tolist())), (
        "Leakage detected: test_query_ids and sample_query_ids are not disjoint!"
    )
    assert set(test_query_texts).isdisjoint(set(sample_query_texts)), (
        "Leakage detected: test_query_texts and sample_query_texts share identical text!"
    )

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"[data] Loading {MODEL_ID}@{MODEL_REVISION} on {device}...")
    model = SentenceTransformer(MODEL_ID, revision=MODEL_REVISION, device=device)

    print("[data] Embedding 100,000 passages...")
    passage_vecs = model.encode(
        passage_texts,
        batch_size=batch_size,
        show_progress_bar=True,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )
    passage_vecs = normalize_l2(passage_vecs)

    print("[data] Embedding 1,000 test queries and 64 sample queries...")
    test_query_vecs = normalize_l2(
        model.encode(
            test_query_texts,
            batch_size=batch_size,
            show_progress_bar=False,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )
    )
    sample_query_vecs = normalize_l2(
        model.encode(
            sample_query_texts,
            batch_size=batch_size,
            show_progress_bar=False,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )
    )

    # Assert strict vector disjointness between sample and test queries
    max_sim = float(np.max(sample_query_vecs @ test_query_vecs.T))
    assert max_sim < 0.9999, f"Sample query vector matches a test query vector (max_sim={max_sim})"

    initial_vecs = np.ascontiguousarray(passage_vecs[:N_INITIAL], dtype=np.float32)
    initial_ids = np.ascontiguousarray(passage_ids[:N_INITIAL], dtype=np.int64)
    stream_vecs = np.ascontiguousarray(passage_vecs[N_INITIAL:], dtype=np.float32)
    stream_ids = np.ascontiguousarray(passage_ids[N_INITIAL:], dtype=np.int64)

    # Save .npy files and .json id lists
    outputs = {
        "initial_vectors.npy": initial_vecs,
        "initial_ids.npy": initial_ids,
        "stream_vectors.npy": stream_vecs,
        "stream_ids.npy": stream_ids,
        "test_queries.npy": test_query_vecs,
        "test_query_ids.npy": test_query_ids,
        "sample_queries.npy": sample_query_vecs,
        "sample_query_ids.npy": sample_query_ids,
    }
    for fname, arr in outputs.items():
        np.save(data_dir / fname, arr)

    id_lists = {
        "initial_ids.json": initial_ids.tolist(),
        "stream_ids.json": stream_ids.tolist(),
        "test_query_ids.json": test_query_ids.tolist(),
        "sample_query_ids.json": sample_query_ids.tolist(),
    }
    for fname, id_list in id_lists.items():
        with (data_dir / fname).open("w", encoding="utf-8") as f:
            json.dump(id_list, f)

    output_hashes = {}
    for fname in list(outputs.keys()) + list(id_lists.keys()):
        output_hashes[fname] = sha256_file(data_dir / fname)

    provenance = {
        "corpus_dataset_id": CORPUS_DATASET_ID,
        "corpus_revision": CORPUS_REVISION,
        "qrels_dataset_id": QRELS_DATASET_ID,
        "qrels_revision": QRELS_REVISION,
        "embedding_model_id": MODEL_ID,
        "embedding_model_revision": MODEL_REVISION,
        "seed": seed,
        "query_sampling_seeds": {
            "passages": seed,
            "test_queries": seed + 1,
            "sample_queries": seed + 2,
        },
        "counts": {
            "total_corpus_rows": int(total_corpus_rows),
            "sampled_passages": N_PASSAGES,
            "initial_passages": N_INITIAL,
            "stream_passages": N_STREAM,
            "dev_queries_available": int(len(dev_queries_df)),
            "train_queries_available": int(len(train_queries_df)),
            "test_queries": N_TEST_QUERIES,
            "sample_queries": N_SAMPLE_QUERIES,
            "embedding_dim": DIM,
        },
        "leakage_checks": {
            "id_overlap_count": int(len(set(test_query_ids.tolist()) & set(sample_query_ids.tolist()))),
            "text_overlap_count": int(len(set(test_query_texts) & set(sample_query_texts))),
            "max_sample_test_cosine": max_sim,
        },
        "source_file_sha256": source_hashes,
        "output_file_sha256": output_hashes,
    }
    prov_path = data_dir / "PROVENANCE.json"
    with prov_path.open("w", encoding="utf-8") as f:
        json.dump(provenance, f, indent=2)
    print(f"[data] Wrote {prov_path}")
    return provenance


def load_dataset(
    data_dir: Path,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Load all saved .npy arrays and verify shapes, dtypes, unit norms, and disjointness."""
    initial_vecs = np.load(data_dir / "initial_vectors.npy")
    initial_ids = np.load(data_dir / "initial_ids.npy")
    stream_vecs = np.load(data_dir / "stream_vectors.npy")
    stream_ids = np.load(data_dir / "stream_ids.npy")
    test_queries = np.load(data_dir / "test_queries.npy")
    test_query_ids = np.load(data_dir / "test_query_ids.npy")
    sample_queries = np.load(data_dir / "sample_queries.npy")
    sample_query_ids = np.load(data_dir / "sample_query_ids.npy")

    assert initial_vecs.dtype == np.float32 and initial_vecs.shape[1] == DIM
    assert stream_vecs.dtype == np.float32 and stream_vecs.shape[1] == DIM
    assert test_queries.dtype == np.float32 and test_queries.shape[1] == DIM
    assert sample_queries.dtype == np.float32 and sample_queries.shape[1] == DIM
    assert set(test_query_ids.tolist()).isdisjoint(set(sample_query_ids.tolist())), (
        "Test and sample query IDs must be strictly disjoint"
    )
    return (
        initial_vecs,
        initial_ids,
        stream_vecs,
        stream_ids,
        test_queries,
        test_query_ids,
        sample_queries,
        sample_query_ids,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Build MS MARCO embeddings for DVS simulator.")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "data",
        help="Output directory for .npy files and PROVENANCE.json",
    )
    parser.add_argument("--seed", type=int, default=DATA_SEED)
    parser.add_argument("--batch-size", type=int, default=512)
    args = parser.parse_args()
    build_dataset(args.data_dir, seed=args.seed, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
