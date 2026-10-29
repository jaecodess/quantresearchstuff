#!/usr/bin/env python3
"""Build the team submission ZIP (all paths relative to the ZIP root) and run the hygiene checks on it.

Usage:
  build_submission_zip.py --out dist/submission.zip [--dry-run]
--dry-run reports missing required deliverables as warnings instead of failing (used for CI previews).
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import check_hygiene  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
INCLUDE = [
    "README.md",
    "HANDOVER.md",
    "report",
    "notebooks",
    "models",
    "streaming",
    "hive",
    "spark",
    "modeling",
    "evaluation",
    "analytics",
    "perf",
    "config",
    "manifests",
    "evidence",
    "corpus",
    "docs",
    "scripts",
]
EXCLUDE_NAMES = {"__pycache__", ".ipynb_checkpoints", ".gitkeep", ".DS_Store"}
EXCLUDE_FILES = {"config/paths.yaml"}


def required_problems() -> list[str]:
    problems = []
    if not (ROOT / "report" / "report.pdf").exists():
        problems.append("report/report.pdf is missing")
    if not list((ROOT / "notebooks").glob("*.ipynb")):
        problems.append("notebooks/ has no .ipynb exported from EMR")
    if not any(p.is_file() and p.name != ".gitkeep" for p in (ROOT / "models").rglob("*")):
        problems.append("models/ has no saved PipelineModel")
    if not (ROOT / "docs" / "rerun").is_dir() or not any((ROOT / "docs" / "rerun").glob("*.md")):
        problems.append("docs/rerun/ has no Appendix A rerun guide")
    return problems


def stage(dest: Path) -> list[str]:
    files = []
    for item in INCLUDE:
        src = ROOT / item
        paths = [src] if src.is_file() else sorted(src.rglob("*")) if src.is_dir() else []
        for p in paths:
            rel = p.relative_to(ROOT)
            if not p.is_file() or EXCLUDE_NAMES & set(rel.parts) or rel.as_posix() in EXCLUDE_FILES:
                continue
            target = dest / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(p, target)
            files.append(rel.as_posix())
    return files


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="dist/submission.zip", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    missing = required_problems()
    for m in missing:
        print(f"::{'warning' if args.dry_run else 'error'}::{m}")
    if missing and not args.dry_run:
        return 1

    with tempfile.TemporaryDirectory() as tmp:
        stage_dir = Path(tmp) / "zipcontent"
        files = stage(stage_dir)
        # Repo CI already checks markdown links; the ZIP omits process files (CLAUDE.md, .github) that docs link to.
        problems = check_hygiene.scan(stage_dir, links=False)
        for p in problems:
            print(f"::error::{p}")
        if problems:
            return 1
        args.out.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(args.out, "w", zipfile.ZIP_DEFLATED) as z:
            for rel in files:
                z.write(stage_dir / rel, rel)
    print(f"wrote {args.out} with {len(files)} files")
    return 0


if __name__ == "__main__":
    sys.exit(main())
