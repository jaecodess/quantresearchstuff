#!/usr/bin/env python3
"""Submission hygiene checks (brief: no credentials, tokens, raw datasets or machine-specific paths;
every referenced file exists at a path relative to the ZIP root).

Usage:
  check_hygiene.py [--root DIR]         scan git-tracked files (or every file if DIR is not a git repo)
Add `hygiene: allow` on a line to silence the text rules for that line.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

SELF_EXCLUDE = {"scripts/check_hygiene.py", "tests/test_hygiene.py"}
BAD_EXTENSIONS = {".zip", ".gz", ".bz2", ".7z", ".tar", ".pem", ".key", ".p12", ".pfx", ".jks"}
BAD_NAMES = {".env", "paths.yaml", "credentials", "credentials.json", "id_rsa", "id_ed25519"}
DATA_ALLOWED_DIRS = {
    ".csv": ("config/", "manifests/", "evidence/", "tests/", "analytics/", "docs/", "report/", "streaming/"),
    ".parquet": ("models/", "tests/"),
}
MAX_BYTES = 10 * 1024 * 1024
MODEL_DIR = "models/"

TEXT_RULES = [
    ("AWS access key", re.compile(r"\b(AKIA|ASIA)[0-9A-Z]{16}\b")),
    ("AWS secret key", re.compile(r"aws_secret_access_key\s*[=:]", re.I)),
    ("private key", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
    ("GitHub token", re.compile(r"\bgh[pousr]_[A-Za-z0-9]{36,}\b")),
    ("Slack token", re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}")),
    ("macOS/Linux home path", re.compile(r"/(Users|home)/(?!hadoop\b)[A-Za-z0-9._-]+/")),
    ("Windows user path", re.compile(r"[A-Za-z]:\\Users\\", re.I)),
    ("EC2 public hostname", re.compile(r"ec2-\d+-\d+-\d+-\d+\.[\w.-]*amazonaws\.com")),
]
LINK_RE = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")


def list_files(root: Path) -> list[str]:
    try:
        out = subprocess.run(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        return [f for f in out.split("\0") if f and (root / f).is_file()]
    except (subprocess.CalledProcessError, FileNotFoundError):
        return [str(p.relative_to(root)) for p in root.rglob("*") if p.is_file() and ".git" not in p.parts]


def check_file_policy(rel: str, size: int) -> list[str]:
    path = Path(rel)
    problems = []
    if path.suffix.lower() in BAD_EXTENSIONS or path.name in BAD_NAMES:
        problems.append(f"{rel}: forbidden file type (archive, key or local config must not be committed)")
    allowed = DATA_ALLOWED_DIRS.get(path.suffix.lower())
    if allowed is not None and not rel.startswith(allowed):
        problems.append(f"{rel}: data file outside {', '.join(allowed)}; raw datasets do not belong in the repo")
    if size > MAX_BYTES and not rel.startswith(MODEL_DIR):
        problems.append(f"{rel}: {size / 1e6:.1f} MB exceeds {MAX_BYTES // 1024 // 1024} MB limit")
    return problems


def check_text(rel: str, text: str) -> list[str]:
    problems = []
    for n, line in enumerate(text.splitlines(), 1):
        if "hygiene: allow" in line:
            continue
        for label, rx in TEXT_RULES:
            if rx.search(line):
                problems.append(f"{rel}:{n}: {label}")
    return problems


def check_links(rel: str, text: str, root: Path) -> list[str]:
    problems = []
    base = (root / rel).parent
    for n, line in enumerate(text.splitlines(), 1):
        for target in LINK_RE.findall(line):
            if target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            if not (base / target.split("#")[0]).exists():
                problems.append(f"{rel}:{n}: broken relative link {target}")
    return problems


def scan(root: Path, links: bool = True) -> list[str]:
    problems = []
    for rel in sorted(list_files(root)):
        if rel in SELF_EXCLUDE:
            continue
        path = root / rel
        problems += check_file_policy(rel, path.stat().st_size)
        if path.stat().st_size > MAX_BYTES:
            continue
        try:
            text = path.read_text()
        except (UnicodeDecodeError, OSError):
            continue
        problems += check_text(rel, text)
        if links and rel.endswith(".md"):
            problems += check_links(rel, text, root)
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=Path(__file__).resolve().parent.parent, type=Path)
    args = parser.parse_args(argv)
    problems = scan(args.root)
    for p in problems:
        print(f"::error::{p}")
    print(f"hygiene: {len(problems)} problem(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
