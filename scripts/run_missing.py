"""Finish an interrupted experiment from the same commit: run only the runs that have no complete output.

Usage (cwd = repository root):  .venv\\Scripts\\python.exe scripts\\run_missing.py --config configs\\main_M1.yaml --workers 8

It wraps dvs.run.run_from_config without changing any simulation code. A run counts as complete when
summary.json, epochs.parquet, queries.npz and provenance.json exist, summary.json parses, and the
provenance commit equals the repo HEAD. Complete runs are loaded from disk, not rerun. Each run is seeded
on its own, so rerunning only the missing runs gives the same outputs as one uninterrupted pass.
The patch sits at module level so spawned pool workers (which re-import this file) also get it.
"""
import json
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path.cwd()
sys.path.insert(0, str(REPO))

import dvs.run as R  # noqa: E402

_ORIG = R._worker_run_sim_task
_HEAD = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, capture_output=True, text=True).stdout.strip()


def _complete(run_dir: Path):
    need = ["summary.json", "epochs.parquet", "queries.npz", "provenance.json"]
    if not all((run_dir / n).is_file() for n in need):
        return None
    try:
        summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
        prov = json.loads((run_dir / "provenance.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if prov.get("git_commit") != _HEAD or "config" not in prov:
        return None
    return summary, prov["config"]


def _skip_or_run(task):
    run_dir = Path(task["run_dir"])
    done = _complete(run_dir)
    if done is not None:
        summary, config = done
        return {"status": "ok", "run_id": task["run_id"], "run_dir": str(run_dir),
                "config": config, "summary": summary}
    return _ORIG(task)


R._worker_run_sim_task = _skip_or_run


def main():
    import argparse
    import yaml

    p = argparse.ArgumentParser()
    p.add_argument("--config", type=Path, required=True)
    p.add_argument("--workers", type=int, default=8)
    a = p.parse_args()
    cfg = yaml.safe_load(a.config.read_text(encoding="utf-8"))
    out_dir = REPO / "results" / str(cfg.get("experiment", a.config.stem))
    old = out_dir / "failed_runs.json"
    if old.is_file():
        keep = out_dir / "failed_runs_pass1.json"
        if not keep.exists():
            shutil.copy2(old, keep)
    print(f"HEAD {_HEAD}; resuming {a.config}", flush=True)
    R.run_from_config(config_path=a.config, num_workers=a.workers)


if __name__ == "__main__":
    main()
