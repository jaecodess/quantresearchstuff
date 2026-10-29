#!/usr/bin/env python3
"""Progress tracking for the 5-workstream team.

Task files (docs/progress/*.md) are the delegation + tracking source of truth.
This script validates them, renders docs/progress/STATUS.md, checks that a PR
updated the right task files, and generates .github/CODEOWNERS.

Usage:
  progress.py status              rewrite docs/progress/STATUS.md
  progress.py check               validate task files; fail if STATUS.md / CODEOWNERS are stale
  progress.py check-pr --base A --head B [--body-env PR_BODY]
  progress.py overdue [--today YYYY-MM-DD]
  progress.py codeowners          rewrite .github/CODEOWNERS
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / ".github" / "workstreams.json"
STATUS_FILE = ROOT / "docs" / "progress" / "STATUS.md"
CODEOWNERS_FILE = ROOT / ".github" / "CODEOWNERS"
AI_LOG = "docs/progress/ai-usage.md"

TASK_RE = re.compile(r"^- \[(?P<mark>[ x~!])\] \*\*(?P<id>[A-Z]+-\d+)\*\* (?P<rest>.+)$")
LOG_RE = re.compile(r"^- (?P<date>\d{4}-\d{2}-\d{2}) (?P<who>@[\w-]+): (?P<text>.+)$")
HANDLE_RE = re.compile(r"^@[\w-]+(/[\w-]+)?$")
STATES = {" ": "todo", "~": "in progress", "x": "done", "!": "blocked"}


@dataclass
class Task:
    id: str
    ws: str
    state: str
    title: str
    fields: dict = field(default_factory=dict)
    line: int = 0

    @property
    def needs(self) -> list[str]:
        return [n.strip() for n in self.fields.get("needs", "").split(",") if n.strip()]

    @property
    def owner(self) -> str:
        return self.fields.get("owner", "@TODO")

    @property
    def has_real_owner(self) -> bool:
        return bool(HANDLE_RE.match(self.owner)) and self.owner != "@TODO"


@dataclass
class LogEntry:
    date: str
    who: str
    text: str
    ws: str


def load_config() -> dict:
    return json.loads(CONFIG.read_text())


def parse_file(path: Path, ws: str) -> tuple[list[Task], list[LogEntry]]:
    tasks, logs, in_log = [], [], False
    for n, raw in enumerate(path.read_text().splitlines(), 1):
        line = raw.rstrip()
        if line.startswith("## "):
            in_log = line.lower().startswith("## progress log")
            continue
        if in_log:
            m = LOG_RE.match(line)
            if m:
                logs.append(LogEntry(m["date"], m["who"], m["text"], ws))
            continue
        m = TASK_RE.match(line)
        if not m:
            continue
        title, *rest = (p.strip() for p in m["rest"].split(" · "))
        fields = {}
        for part in rest:
            key, _, value = part.partition(":")
            fields[key.strip().lower()] = value.strip()
        tasks.append(Task(m["id"], ws, STATES[m["mark"]], title, fields, n))
    return tasks, logs


def load_all(cfg: dict) -> tuple[list[Task], list[LogEntry], list[str]]:
    tasks, logs, errors = [], [], []
    for ws, spec in cfg["workstreams"].items():
        path = ROOT / spec["file"]
        if not path.exists():
            errors.append(f"{spec['file']}: missing task file for {ws}")
            continue
        t, entries = parse_file(path, ws)
        tasks += t
        logs += entries
    return tasks, logs, errors


def evidence_ok(item: str) -> bool:
    if item.startswith(("http://", "https://")):
        return True
    return (ROOT / item.split("#")[0]).exists()


def validate(cfg: dict, tasks: list[Task]) -> tuple[list[str], list[str]]:
    errors, warnings = [], []
    ids = {}
    for t in tasks:
        where = f"{cfg['workstreams'][t.ws]['file']}:{t.line} {t.id}"
        if t.id in ids:
            errors.append(f"{where}: duplicate task id")
        ids[t.id] = t
        if t.id.split("-")[0] != t.ws:
            errors.append(f"{where}: id prefix must be {t.ws}")
        due = t.fields.get("due")
        if due:
            try:
                dt.date.fromisoformat(due)
            except ValueError:
                errors.append(f"{where}: due '{due}' is not YYYY-MM-DD")
        if t.state in ("in progress", "done") and not t.has_real_owner:
            errors.append(f"{where}: {t.state} task needs 'owner: @github-handle' (not @TODO)")
        if t.state == "blocked" and not t.fields.get("blocked"):
            errors.append(f"{where}: blocked task needs 'blocked: <reason>'")
        if t.state == "done":
            evidence = [e.strip() for e in t.fields.get("evidence", "").split(",") if e.strip()]
            if not evidence:
                errors.append(f"{where}: done task needs 'evidence: <path or URL>' (marks go to verifiable evidence)")
            for item in evidence:
                if not evidence_ok(item):
                    errors.append(f"{where}: evidence path not found: {item}")
    for t in tasks:
        where = f"{cfg['workstreams'][t.ws]['file']}:{t.line} {t.id}"
        for dep in t.needs:
            if dep not in ids:
                errors.append(f"{where}: needs unknown task {dep}")
            elif t.state == "done" and ids[dep].state != "done":
                warnings.append(f"{where}: done while dependency {dep} is {ids[dep].state}")
    return errors, warnings


def render_status(cfg: dict, tasks: list[Task], logs: list[LogEntry]) -> str:
    by_id = {t.id: t for t in tasks}
    link = lambda t: f"[{t.id}]({Path(cfg['workstreams'][t.ws]['file']).name})"  # noqa: E731
    due = lambda t: f" · due {t.fields['due']}" if t.fields.get("due") else ""  # noqa: E731
    out = [
        "# Project status",
        "",
        "<!-- GENERATED by scripts/progress.py; do not edit. Run `make status` after changing a task file. -->",
        "",
        "Source of truth: the per-workstream task files in this folder.",
        "Legend: `[ ]` todo, `[~]` in progress, `[x]` done, `[!]` blocked.",
        "",
        "## Summary",
        "",
        "| Workstream | Owner | Done | In progress | Blocked | Todo | Total |",
        "|---|---|---|---|---|---|---|",
    ]
    for ws, spec in cfg["workstreams"].items():
        mine = [t for t in tasks if t.ws == ws]

        def count(state: str, mine: list[Task] = mine) -> int:
            return sum(t.state == state for t in mine)

        out.append(
            f"| [{ws}]({Path(spec['file']).name}) {spec['name']} | {spec['owner']} | {count('done')} | "
            f"{count('in progress')} | {count('blocked')} | {count('todo')} | {len(mine)} |"
        )
    done = sum(t.state == "done" for t in tasks)
    out += ["", f"**Overall: {done}/{len(tasks)} tasks done.**", ""]

    def section(title: str, rows: list[str], empty: str) -> None:
        out.extend([f"## {title}", ""] + (rows or [f"_{empty}_"]) + [""])

    section(
        "In progress",
        [f"- {link(t)} {t.title}: {t.owner}{due(t)}" for t in tasks if t.state == "in progress"],
        "Nothing claimed yet.",
    )
    section(
        "Blocked",
        [f"- {link(t)} {t.title}: {t.owner}: **{t.fields.get('blocked')}**" for t in tasks if t.state == "blocked"],
        "Nothing blocked.",
    )
    ready = [t for t in tasks if t.state == "todo" and all(by_id[d].state == "done" for d in t.needs if d in by_id)]
    ready.sort(key=lambda t: (t.fields.get("due", "9999-12-31"), t.id))
    section(
        "Ready to start (dependencies satisfied, nobody has claimed it)",
        [f"- {link(t)} {t.title}: {t.owner}{due(t)}" for t in ready],
        "Nothing ready.",
    )
    recent = sorted(logs, key=lambda e: e.date, reverse=True)[:15]
    section(
        "Recent activity (newest 15 progress-log entries)",
        [f"- {e.date} {e.who} [{e.ws}]: {e.text}" for e in recent],
        "No log entries yet.",
    )
    return "\n".join(out).rstrip() + "\n"


def owners_for(cfg: dict, ws: str) -> list[str]:
    keys = [ws] + cfg["workstreams"][ws].get("cross_reviewers", [])
    handles = [cfg["workstreams"][k]["owner"] for k in keys]
    return list(dict.fromkeys(h for h in handles if HANDLE_RE.match(h) and h != "@TODO"))


def render_codeowners(cfg: dict) -> str:
    out = [
        "# GENERATED from .github/workstreams.json by `make codeowners`; do not edit.",
        "# Lines appear only for workstreams whose owner is a real GitHub handle.",
        "# Cross-reviewers listed here are alternatives: any ONE code owner's approval satisfies the rule.",
        "",
    ]
    for ws, spec in cfg["workstreams"].items():
        owners = owners_for(cfg, ws)
        if not owners:
            continue
        for path in spec["paths"] + [spec["file"]]:
            out.append(f"/{path} {' '.join(owners)}")
    return "\n".join(out) + "\n"


def match_workstream(cfg: dict, path: str) -> str | None:
    best, best_len = None, -1
    for ws, spec in cfg["workstreams"].items():
        for prefix in spec["paths"]:
            if (path == prefix or path.startswith(prefix)) and len(prefix) > best_len:
                best, best_len = ws, len(prefix)
    return best


def evaluate_pr(cfg: dict, changed: list[str], body: str) -> tuple[bool, list[str]]:
    """Return (ok, messages). A PR must update the task file of every workstream it touches."""
    task_files = {spec["file"]: ws for ws, spec in cfg["workstreams"].items()}
    msgs: list[str] = []
    relevant = [f for f in changed if not any(f.startswith(p) for p in cfg["exempt_paths"])]
    if not relevant:
        return True, ["Only progress files changed; nothing more required."]
    exempt = re.search(r"^Progress-Exempt:\s*(\S.*)$", body or "", re.I | re.M)
    ok = True
    touched = {w for w in (match_workstream(cfg, f) for f in relevant) if w}
    updated = {task_files[f] for f in changed if f in task_files}
    for ws in sorted(touched - updated):
        ok = False
        msgs.append(f"PR changes files owned by {ws} but {cfg['workstreams'][ws]['file']} was not updated.")
    if not touched and not updated:
        ok = False
        msgs.append("PR changes shared files; update at least one task file (e.g. add a progress-log line).")
    if re.search(r"^\s*[-*] \[[xX]\] .*AI assistance was used", body or "", re.M) and AI_LOG not in changed:
        ok = False
        msgs.append(f"PR says AI assistance was used but {AI_LOG} was not updated.")
    if not ok and exempt:
        return True, [f"Progress check waived by 'Progress-Exempt: {exempt.group(1)}'; reviewer should confirm."]
    return ok, msgs or ["Task files updated for every touched workstream."]


def changed_files(base: str, head: str) -> list[str]:
    out = subprocess.run(
        ["git", "diff", "--name-only", f"{base}...{head}"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout
    return [line for line in out.splitlines() if line]


def cmd_status(_: argparse.Namespace) -> int:
    cfg = load_config()
    tasks, logs, errors = load_all(cfg)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    STATUS_FILE.write_text(render_status(cfg, tasks, logs))
    print(f"wrote {STATUS_FILE.relative_to(ROOT)}")
    return 0


def cmd_check(_: argparse.Namespace) -> int:
    cfg = load_config()
    tasks, logs, errors = load_all(cfg)
    more_errors, warnings = validate(cfg, tasks)
    errors += more_errors
    if not errors:
        if not STATUS_FILE.exists() or STATUS_FILE.read_text() != render_status(cfg, tasks, logs):
            errors.append("docs/progress/STATUS.md is stale: run `make status` and commit it.")
        if CODEOWNERS_FILE.exists() and CODEOWNERS_FILE.read_text() != render_codeowners(cfg):
            errors.append(".github/CODEOWNERS is stale: run `make codeowners` and commit it.")
    for w in warnings:
        print(f"::warning::{w}")
    for e in errors:
        print(f"::error::{e}")
    print(f"{len(tasks)} tasks checked, {len(errors)} errors, {len(warnings)} warnings")
    return 1 if errors else 0


def cmd_check_pr(args: argparse.Namespace) -> int:
    cfg = load_config()
    ok, msgs = evaluate_pr(cfg, changed_files(args.base, args.head), os.environ.get(args.body_env, ""))
    for m in msgs:
        print(f"::{'notice' if ok else 'error'}::{m}")
    if not ok:
        print(
            "Fix: update your workstream file in docs/progress/ (task state, evidence, progress log), "
            "then run `make status`. See CONTRIBUTING.md."
        )
    return 0 if ok else 1


def cmd_overdue(args: argparse.Namespace) -> int:
    today = dt.date.fromisoformat(args.today) if args.today else dt.date.today()
    cfg = load_config()
    tasks, _, _ = load_all(cfg)
    for t in tasks:
        if t.state != "done" and t.fields.get("due") and dt.date.fromisoformat(t.fields["due"]) < today:
            print(f"::warning::{t.id} ({t.state}, {t.owner}) was due {t.fields['due']}: {t.title}")
    return 0


def cmd_codeowners(_: argparse.Namespace) -> int:
    CODEOWNERS_FILE.write_text(render_codeowners(load_config()))
    print(f"wrote {CODEOWNERS_FILE.relative_to(ROOT)}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status").set_defaults(fn=cmd_status)
    sub.add_parser("check").set_defaults(fn=cmd_check)
    sub.add_parser("codeowners").set_defaults(fn=cmd_codeowners)
    p = sub.add_parser("check-pr")
    p.add_argument("--base", required=True)
    p.add_argument("--head", required=True)
    p.add_argument("--body-env", default="PR_BODY")
    p.set_defaults(fn=cmd_check_pr)
    p = sub.add_parser("overdue")
    p.add_argument("--today")
    p.set_defaults(fn=cmd_overdue)
    args = parser.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
