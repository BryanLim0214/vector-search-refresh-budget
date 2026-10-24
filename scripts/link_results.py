"""Build a combined analysis root from `repo/results/main` and `repo-prereg2/results/main`.

Read-only with respect to both source result folders (`repo/results/main` and
`repo-prereg2/results/main`): never writes, edits, or deletes anything in the source folders.
Supports directory junctions (`--mode junction`, default on Windows) or read-only directory
copies (`--mode copy`) into `<analysis_root>/main/{M1,M2,sens,M1_online,M3}`.
"""

from __future__ import annotations

import argparse
import os
import shutil
import stat
import subprocess
from pathlib import Path
from typing import Dict, Tuple

REPO_EXPERIMENTS = ("M1", "M2", "sens")
PREREG2_EXPERIMENTS = ("M1_online", "M3")


def _resolve_source_exp_dir(results_root: Path, exp_name: str) -> Path:
    """Resolve `<results_root>/main/<exp_name>` or `<results_root>/<exp_name>` without writing."""
    cand_main = results_root / "main" / exp_name
    cand_direct = results_root / exp_name
    if cand_main.exists():
        return cand_main
    if cand_direct.exists():
        return cand_direct
    if (results_root / "main").exists():
        return cand_main
    return cand_direct if results_root.name == "main" else cand_main


def _is_reparse_point(path: Path) -> bool:
    """Return True if `path` is a Windows junction or symlink (reparse point)."""
    try:
        st = os.lstat(path)
    except OSError:
        return False
    file_attrs = getattr(st, "st_file_attributes", 0)
    if file_attrs & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400):
        return True
    return os.path.islink(path)


def _remove_existing_destination(dest: Path) -> None:
    """Safely remove an existing link/junction or copied folder inside `analysis_root`."""
    if not os.path.lexists(dest):
        return
    if _is_reparse_point(dest):
        if os.name == "nt":
            subprocess.run(
                ["cmd", "/c", "rmdir", str(dest)],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        else:
            os.unlink(dest)
    elif dest.is_dir():
        shutil.rmtree(dest)
    else:
        dest.unlink()


def build_combined_analysis_root(
    repo_results_root: Path,
    prereg2_results_root: Path,
    analysis_root: Path,
    mode: str = "junction",
) -> Dict[str, Path]:
    """Create `<analysis_root>/main/{M1,M2,sens,M1_online,M3}` from the two result roots.

    Uses read-only junctions (`mode='junction'`) or read-only directory copies (`mode='copy'`).
    Returns a dictionary mapping each experiment label to its destination path under `<analysis_root>/main`.
    """
    if mode not in ("junction", "copy"):
        raise ValueError(f"Unsupported mode {mode!r}; expected 'junction' or 'copy'")

    repo_results_root = Path(repo_results_root).resolve()
    prereg2_results_root = Path(prereg2_results_root).resolve()
    analysis_root = Path(analysis_root).resolve()

    if analysis_root == repo_results_root or analysis_root == prereg2_results_root:
        raise ValueError("analysis_root must be a separate directory from source result roots")

    main_dir = analysis_root / "main"
    main_dir.mkdir(parents=True, exist_ok=True)

    sources: Tuple[Tuple[str, Path], ...] = tuple(
        [(exp, repo_results_root) for exp in REPO_EXPERIMENTS]
        + [(exp, prereg2_results_root) for exp in PREREG2_EXPERIMENTS]
    )

    linked: Dict[str, Path] = {}
    for exp_name, src_root in sources:
        src_dir = _resolve_source_exp_dir(src_root, exp_name)
        dst_dir = main_dir / exp_name
        _remove_existing_destination(dst_dir)

        if mode == "junction":
            if os.name == "nt":
                subprocess.run(
                    ["cmd", "/c", "mklink", "/J", str(dst_dir), str(src_dir)],
                    check=True,
                    stdout=subprocess.DEVNULL,
                )
            else:
                os.symlink(src_dir, dst_dir, target_is_directory=True)
            linked[exp_name] = dst_dir
        else:
            if src_dir.exists():
                shutil.copytree(src_dir, dst_dir)
            else:
                dst_dir.mkdir(parents=True, exist_ok=True)
            linked[exp_name] = dst_dir

    return linked


def main() -> None:
    repo_prereg2_root = Path(__file__).resolve().parent.parent
    default_repo_results = repo_prereg2_root.parent / "repo" / "results"
    default_prereg2_results = repo_prereg2_root / "results"
    default_analysis_root = repo_prereg2_root.parent / "analysis_root"

    parser = argparse.ArgumentParser(
        description="Build a combined analysis root linking M1/M2/sens from repo and M1_online/M3 from repo-prereg2."
    )
    parser.add_argument(
        "--repo-results-root",
        type=Path,
        default=default_repo_results,
        help="Read-only results root for M1, M2, and sens (default: E:\\DistributedDatabaseProject\\repo\\results).",
    )
    parser.add_argument(
        "--prereg2-results-root",
        type=Path,
        default=default_prereg2_results,
        help="Read-only results root for M1_online and M3 (default: E:\\DistributedDatabaseProject\\repo-prereg2\\results).",
    )
    parser.add_argument(
        "--analysis-root",
        "--out-root",
        dest="analysis_root",
        type=Path,
        default=default_analysis_root,
        help="Destination combined analysis root (default: E:\\DistributedDatabaseProject\\analysis_root).",
    )
    parser.add_argument(
        "--mode",
        choices=("junction", "copy"),
        default="junction",
        help="Link mode: 'junction' (default, directory junctions) or 'copy' (read-only copy).",
    )
    args = parser.parse_args()
    linked = build_combined_analysis_root(
        repo_results_root=args.repo_results_root,
        prereg2_results_root=args.prereg2_results_root,
        analysis_root=args.analysis_root,
        mode=args.mode,
    )
    for exp_name, dst_path in linked.items():
        print(f"{exp_name} -> {dst_path}")


if __name__ == "__main__":
    main()
