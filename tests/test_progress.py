import progress

CFG = {
    "exempt_paths": ["docs/progress/"],
    "workstreams": {
        "PIPE": {
            "name": "Pipeline",
            "file": "docs/progress/pipeline.md",
            "owner": "@a",
            "cross_reviewers": ["MODEL"],
            "paths": ["spark/", "config/split_events.csv"],
        },
        "MODEL": {
            "name": "Model",
            "file": "docs/progress/model.md",
            "owner": "@b",
            "cross_reviewers": [],
            "paths": ["evaluation/"],
        },
        "TEAM": {
            "name": "Team",
            "file": "docs/progress/team.md",
            "owner": "@TODO",
            "cross_reviewers": [],
            "paths": ["config/", ".github/"],
        },
    },
}


def test_parse_task_fields_and_log(tmp_path):
    f = tmp_path / "pipeline.md"
    f.write_text(
        "- [~] **PIPE-01** Do thing · owner: @a · due: 2026-11-01 · needs: PIPE-00, MODEL-01\n"
        "- [x] **PIPE-02** Done thing · owner: @a · evidence: README.md\n"
        "## Progress log\n- 2026-11-01 @a: did it\n"
    )
    tasks, logs = progress.parse_file(f, "PIPE")
    assert [t.state for t in tasks] == ["in progress", "done"]
    assert tasks[0].needs == ["PIPE-00", "MODEL-01"] and tasks[0].fields["due"] == "2026-11-01"
    assert logs[0].who == "@a" and logs[0].text == "did it"


def test_validate_rules():
    mk = progress.Task
    tasks = [
        mk("PIPE-01", "PIPE", "done", "x", {"owner": "@a"}),  # no evidence
        mk("PIPE-02", "PIPE", "in progress", "x", {}),  # no owner
        mk("PIPE-03", "PIPE", "blocked", "x", {"owner": "@a"}),  # no reason
        mk("PIPE-03", "PIPE", "todo", "x", {"needs": "NOPE-9", "due": "tomorrow"}),  # dup, bad dep, bad due
        mk("MODEL-01", "PIPE", "todo", "x", {}),  # wrong prefix
        mk("PIPE-04", "PIPE", "done", "x", {"owner": "@a", "evidence": "does/not/exist.md"}),
    ]
    errors, _ = progress.validate(CFG, tasks)
    text = "\n".join(errors)
    for needle in (
        "needs 'evidence",
        "needs 'owner",
        "needs 'blocked",
        "duplicate",
        "unknown task NOPE-9",
        "not YYYY-MM-DD",
        "id prefix must be PIPE",
        "evidence path not found",
    ):
        assert needle in text, needle


def test_valid_done_task_passes():
    t = progress.Task("PIPE-01", "PIPE", "done", "x", {"owner": "@a", "evidence": "README.md"})
    assert progress.validate(CFG, [t])[0] == []


def test_pr_requires_owned_workstream_file():
    ok, msgs = progress.evaluate_pr(CFG, ["spark/clean.py"], "")
    assert not ok and "PIPE" in msgs[0]
    ok, _ = progress.evaluate_pr(CFG, ["spark/clean.py", "docs/progress/pipeline.md"], "")
    assert ok


def test_pr_touching_two_workstreams_needs_both():
    files = ["spark/a.py", "evaluation/b.py", "docs/progress/pipeline.md"]
    ok, msgs = progress.evaluate_pr(CFG, files, "")
    assert not ok and "MODEL" in msgs[0]


def test_longest_prefix_wins():
    assert progress.match_workstream(CFG, "config/split_events.csv") == "PIPE"
    assert progress.match_workstream(CFG, "config/project.yaml") == "TEAM"
    assert progress.match_workstream(CFG, "README.md") is None


def test_shared_files_need_some_task_file():
    ok, _ = progress.evaluate_pr(CFG, ["README.md"], "")
    assert not ok
    ok, _ = progress.evaluate_pr(CFG, ["README.md", "docs/progress/team.md"], "")
    assert ok


def test_progress_only_pr_and_exemption():
    assert progress.evaluate_pr(CFG, ["docs/progress/pipeline.md"], "")[0]
    ok, msgs = progress.evaluate_pr(CFG, ["spark/x.py"], "Progress-Exempt: typo fix")
    assert ok and "waived" in msgs[0]


def test_ai_usage_checkbox_requires_log():
    body = "- [x] AI assistance was used"
    files = ["spark/x.py", "docs/progress/pipeline.md"]
    assert not progress.evaluate_pr(CFG, files, body)[0]
    assert progress.evaluate_pr(CFG, files + ["docs/progress/ai-usage.md"], body)[0]
    assert progress.evaluate_pr(CFG, files, "- [ ] AI assistance was used")[0]


def test_codeowners_skips_placeholder_owners():
    text = progress.render_codeowners(CFG)
    assert "/spark/ @a @b" in text and "/evaluation/ @b" in text
    assert "TEAM" not in text and "@TODO" not in text


def test_repo_task_files_are_valid_and_status_is_fresh():
    cfg = progress.load_config()
    tasks, logs, errors = progress.load_all(cfg)
    assert errors == []
    assert progress.validate(cfg, tasks)[0] == []
    assert progress.STATUS_FILE.read_text() == progress.render_status(cfg, tasks, logs)
