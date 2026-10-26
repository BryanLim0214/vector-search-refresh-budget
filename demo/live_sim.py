"""Live view of the shard simulator: runs six methods side by side, one time step at a time, and shows
them in a browser page served from this computer only (http://127.0.0.1:8765).

Usage:
  python demo/live_sim.py --repo <folder with the dvs package> [--data <data folder>]
                          [--results <folder> ...] [--port 8765] [--open]

It drives the simulator's own classes (shards, coordinator, refresh rules, byte counter, exact search)
in the same order as dvs/run.py's run_single_simulation, with the same settings as the paper's main
runs: 32 shards, 2 shards per question, 16-center summaries, 50 time steps of 1,000 new passages and
200 test questions, recent window of 2 steps. Two things are left out because they only feed a
logged rank correlation and change no state: the per-shard true recovered neighbors, and the probe
estimate for the other refresh rules. Each shard index starts small and grows when full; this gives
the same index as the full-size one used in the paper runs (checked against the saved runs).

When a saved run with the same setting and seed is found under --results, every step's recall is
compared with it, and the page shows whether they match exactly.
"""
from __future__ import annotations

import os

# One thread for the math libraries, set before they load, as in the paper runs' worker processes.
for _var in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
    os.environ[_var] = "1"

import argparse
import json
import math
import sys
import threading
import time
import traceback
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import numpy as np

ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
ap.add_argument("--repo", required=True, help="folder that holds the dvs package")
ap.add_argument("--data", help="data folder (default: <repo>/data)")
ap.add_argument("--results", nargs="*", default=[], help="folders with saved runs, e.g. <repo>/results/main")
ap.add_argument("--port", type=int, default=8765)
ap.add_argument("--open", action="store_true", help="open the page in the default browser")
ARGS = ap.parse_args()
REPO = Path(ARGS.repo).resolve()
DATA_DIR = Path(ARGS.data).resolve() if ARGS.data else REPO / "data"
sys.path.insert(0, str(REPO))

import pandas as pd  # noqa: E402

from dvs.bus import SUMMARY_BYTES, SUMMARY_CENTERS, TOP_K, ByteBus  # noqa: E402
from dvs.coordinator import Coordinator  # noqa: E402
from dvs.data import load_dataset  # noqa: E402
from dvs.metrics import GroundTruthIndex, evaluate_epoch_queries  # noqa: E402
from dvs.policies import REFRESH_POLICIES, PolicyController  # noqa: E402
from dvs.run import set_single_thread_env  # noqa: E402
from dvs.shards import build_initial_shards  # noqa: E402
from dvs.streams import build_epoch_schedule, partition_new_topic_stream  # noqa: E402

PAGE = Path(__file__).with_name("dashboard.html")

# Code name and the name used in the short paper.
METHODS = [
    ("never_refresh", "Never refresh"),
    ("round_robin", "Rotation"),
    ("data_change", "Most-changed"),
    ("recovered_neighbor", "Sample-query probe"),
    ("extra_search", "Extra search"),
    ("push_vectors", "Push"),
]
NUM_SHARDS, M, NUM_EPOCHS, INSERTS, QUERIES, WINDOW = 32, 2, 50, 1000, 200, 2
PLACEMENTS = ("nearest_initial", "time_segment", "time_segment_g")
STREAMS = ("new_topic", "steady")
BUDGETS = (2, 4, 8)
SEEDS = (42, 43, 44, 45, 46)
DEFAULT = {"placement": "time_segment_g", "stream": "new_topic", "budget": 4, "seed": 42, "delay": 0.0}

_data = None
_partitions: dict = {}


class Stopped(Exception):
    pass


def dataset():
    global _data
    if _data is None:
        _data = load_dataset(DATA_DIR)
    return _data


def num(x):
    """JSON-safe float (NaN becomes null)."""
    x = float(x)
    return None if math.isnan(x) else x


def saved_run(policy, s):
    """Per-step (recent recall, overall recall) of a saved run with the same setting, or None."""
    run_id = (f"{policy}_S{NUM_SHARDS}_m{M}_B{s['budget']}_Q{QUERIES}_{s['stream']}_{s['placement']}"
              f"_W{WINDOW}_kmeans_bursts0_setup0_starvation_cap_arith_seed{s['seed']}")
    for root in ARGS.results:
        for exp in Path(root).iterdir():
            f = exp / run_id / "epochs.parquet"
            if f.exists():
                df = pd.read_parquet(f, columns=["epoch", "recent_recall_at_10", "recall_at_10"]).sort_values("epoch")
                return df["recent_recall_at_10"].tolist(), df["recall_at_10"].tolist()
    return None


def same(a, b):
    return (math.isnan(a) and math.isnan(b)) or a == b


class Session:
    def __init__(self, settings):
        self.id = int(time.time() * 1000)
        self.settings = settings
        self.delay = float(settings["delay"])
        self.frames: list = []
        self.status, self.message = "setup", "Starting"
        self.saved_found: dict = {}
        self.stop = threading.Event()
        self.go = threading.Event()
        self.go.set()
        self.lock = threading.Lock()
        threading.Thread(target=self.run, daemon=True).start()

    def say(self, msg):
        self.message = msg
        print(msg, flush=True)

    def check(self):
        while not self.go.wait(0.2):
            if self.stop.is_set():
                raise Stopped
        if self.stop.is_set():
            raise Stopped

    def run(self):
        try:
            self.simulate()
            self.status, self.message = "done", "Finished all 50 time steps"
        except Stopped:
            self.status, self.message = "stopped", "Stopped"
        except Exception as exc:  # shown on the page
            traceback.print_exc()
            self.status, self.message = "error", f"{type(exc).__name__}: {exc}"

    def simulate(self):
        # Faiss keeps its thread count per thread, so pin it here too, as the paper runs did.
        set_single_thread_env()
        s = self.settings
        seed, placement, stream = s["seed"], s["placement"], s["stream"]
        budget_bytes = s["budget"] * SUMMARY_BYTES
        self.say("Loading 100,000 passages and the test questions")
        init_v, init_i, str_v, str_i, test_q, test_qid, samp_q, samp_qid = dataset()
        if stream == "new_topic":
            self.say("Sorting passages into old topics and new topics")
            if seed not in _partitions:
                _partitions[seed] = partition_new_topic_stream(
                    initial_vectors=init_v, initial_ids=init_i, stream_vectors=str_v, stream_ids=str_i, seed=seed)
            init_v, init_i, str_v, str_i = _partitions[seed]
        self.check()

        # Each method builds its own shards. A copied hnswlib index restarts the random numbers that place
        # new passages in its graph, so copies would drift slightly from the paper runs.
        runs, router, centers = [], None, None
        for n, (code, label) in enumerate(METHODS, 1):
            self.say(f"Grouping {len(init_v):,} starting passages into {NUM_SHARDS} shards and building their "
                     f"indexes for {label} ({n} of {len(METHODS)})")
            m_shards, router = build_initial_shards(
                initial_vectors=init_v, initial_ids=init_i, num_shards=NUM_SHARDS, c=SUMMARY_CENTERS, seed=seed,
                sharding_mode="kmeans", placement_mode=placement, active_segments=8, max_elements_per_shard=2048,
                precomputed_centroids=centers)
            centers = router.initial_centroids
            for sh in m_shards:
                sh.set_sample_queries(samp_q)
            self.check()
            bus = ByteBus(num_shards=NUM_SHARDS, num_epochs=NUM_EPOCHS)
            bus.log_setup_initial_summaries(num_shards=NUM_SHARDS, c=SUMMARY_CENTERS)
            if code == "recovered_neighbor":
                bus.log_setup_sample_vectors(num_shards=NUM_SHARDS, num_sample_queries=len(samp_q))
            runs.append({
                "code": code, "shards": m_shards, "bus": bus,
                "coord": Coordinator(shards=m_shards, bus=bus, m=M, k=TOP_K, exact_local_search=False),
                "ctrl": PolicyController(policy=code, budget_bytes=budget_bytes, num_shards=NUM_SHARDS, m=M,
                                         num_epochs=NUM_EPOCHS, aging_mode="starvation_cap"),
                "saved": saved_run(code, s),
            })
            self.saved_found[code] = runs[-1]["saved"] is not None
        gt = GroundTruthIndex()
        gt.add(vectors=init_v, doc_ids=init_i, shard_ids=router.assign_shards(init_v, init_i, epoch=-1), epoch=-1)
        schedule = build_epoch_schedule(
            stream_vectors=str_v, stream_ids=str_i, test_queries=test_q, test_query_ids=test_qid,
            sample_queries=samp_q, stream_mode=stream, num_epochs=NUM_EPOCHS, inserts_per_epoch=INSERTS,
            queries_per_epoch=QUERIES, seed=seed, bursts=False)
        self.status = "running"

        for batch in schedule:
            self.check()
            t0 = time.perf_counter()
            e = batch.epoch
            self.say(f"Time step {e + 1} of {NUM_EPOCHS}")
            new_sids = router.assign_shards(batch.insert_vectors, batch.insert_ids, epoch=e)
            gt.add(vectors=batch.insert_vectors, doc_ids=batch.insert_ids, shard_ids=new_sids, epoch=e)
            gt_ids, _, gt_epochs, gt_shards = gt.search(batch.test_queries, k=TOP_K)
            recent = (gt_epochs >= max(0, e - WINDOW + 1)) & (gt_epochs <= e)
            frame = {"epoch": e, "inserted": np.bincount(new_sids, minlength=NUM_SHARDS).tolist(),
                     "passages": int(gt.size), "questions": len(batch.test_queries), "methods": {}}

            for r in runs:
                self.check()
                code, shards_r, bus, coord, ctrl = r["code"], r["shards"], r["bus"], r["coord"], r["ctrl"]
                for sid in range(NUM_SHARDS):
                    mask = new_sids == sid
                    if np.any(mask):
                        shards_r[sid].add_vectors(batch.insert_vectors[mask], batch.insert_ids[mask])
                ctrl.record_new_arrivals(vectors=batch.insert_vectors, doc_ids=batch.insert_ids, shard_ids=new_sids)
                if code in REFRESH_POLICIES:
                    for sh in shards_r:
                        sh.update_candidate_summary(e)
                decision = ctrl.step_epoch(
                    epoch=e, shards=shards_r, coordinator=coord, bus=bus, sample_queries=samp_q,
                    epoch_test_queries=batch.test_queries if code == "extra_search" else None)
                ret_ids, _, searched = coord.execute_queries(
                    epoch=e, queries=batch.test_queries, shards=shards_r, m=M,
                    extra_shards_per_query=decision.extra_shards_per_query, record_popularity=True)
                pushed = coord._pushed_id_set if code == "push_vectors" else None
                res = evaluate_epoch_queries(
                    epoch=e, returned_ids_list=ret_ids, searched_shards_mask=searched, gt_ids=gt_ids,
                    gt_epochs=gt_epochs, gt_shards=gt_shards, recent_window_w=WINDOW, k=TOP_K, pushed_id_set=pushed)
                spent = bus.epoch_budget_bytes(e)
                if spent > budget_bytes:
                    raise AssertionError(f"{code} spent {spent} bytes, over the budget of {budget_bytes}")

                # New best matches the method did not return because their shard was not searched.
                missed = recent & ~searched[np.arange(len(gt_shards))[:, None], gt_shards]
                if pushed:
                    missed &= ~np.isin(gt_ids, np.fromiter(pushed, dtype=np.int64))
                row = {
                    "unseen": [sh.num_new_vectors for sh in shards_r],
                    "size": [sh.size for sh in shards_r],
                    "refreshed": [int(x) for x in decision.refreshed_shards],
                    "missed": np.bincount(gt_shards[missed], minlength=NUM_SHARDS).tolist(),
                    "recent": num(res.recent_recall_at_10),
                    "overall": num(res.recall_at_10),
                    "bytes": int(spent),
                    "budget": budget_bytes,
                    "extra": int(decision.num_extra_searches),
                    "pushed": int(decision.num_pushed_vectors),
                }
                if r["saved"] is not None:
                    sv_recent, sv_overall = r["saved"][0][e], r["saved"][1][e]
                    row["saved_recent"] = num(sv_recent)
                    row["match"] = same(res.recent_recall_at_10, sv_recent) and same(res.recall_at_10, sv_overall)
                frame["methods"][code] = row

            frame["seconds"] = round(time.perf_counter() - t0, 2)
            with self.lock:
                self.frames.append(frame)
            wait = self.delay - (time.perf_counter() - t0)
            if wait > 0 and self.stop.wait(wait):
                raise Stopped

    def state(self, since):
        with self.lock:
            frames = self.frames[since:]
        return {"session": self.id, "settings": self.settings, "status": self.status,
                "paused": not self.go.is_set(), "message": self.message, "delay": self.delay,
                "total_epochs": NUM_EPOCHS, "methods": [{"code": c, "name": n} for c, n in METHODS],
                "saved_found": self.saved_found, "frames": frames}


current = {"session": None}
current_lock = threading.Lock()


def start(settings):
    with current_lock:
        old = current["session"]
        if old is not None:
            old.stop.set()
            old.go.set()
        current["session"] = Session(settings)
    return current["session"]


def clean_settings(body):
    s = dict(DEFAULT)
    s.update({k: body[k] for k in DEFAULT if k in body})
    s["budget"], s["seed"], s["delay"] = int(s["budget"]), int(s["seed"]), float(s["delay"])
    if s["placement"] not in PLACEMENTS or s["stream"] not in STREAMS or s["budget"] not in BUDGETS \
            or s["seed"] not in SEEDS or not 0 <= s["delay"] <= 10:
        raise ValueError("setting out of range")
    return s


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def send(self, code, body, ctype="application/json"):
        data = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        url = urlparse(self.path)
        if url.path in ("/", "/index.html"):
            self.send(200, PAGE.read_bytes(), "text/html; charset=utf-8")
        elif url.path == "/api/state":
            since = int(parse_qs(url.query).get("since", ["0"])[0])
            self.send(200, current["session"].state(max(0, since)))
        else:
            self.send(404, {"error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
            sess = current["session"]
            if self.path == "/api/start":
                sess = start(clean_settings(body))
            elif self.path == "/api/pause":
                sess.go.clear() if body.get("paused") else sess.go.set()
            elif self.path == "/api/speed":
                sess.delay = max(0.0, min(10.0, float(body.get("delay", 0))))
            else:
                return self.send(404, {"error": "not found"})
            self.send(200, {"ok": True, "session": sess.id})
        except (ValueError, KeyError, TypeError) as exc:
            self.send(400, {"error": str(exc)})


def main():
    set_single_thread_env()
    server = ThreadingHTTPServer(("127.0.0.1", ARGS.port), Handler)
    start(dict(DEFAULT))
    url = f"http://127.0.0.1:{ARGS.port}/"
    print(f"Live view at {url}  (Ctrl+C to stop)", flush=True)
    if ARGS.open:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
