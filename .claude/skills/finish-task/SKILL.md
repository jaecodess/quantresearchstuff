---
name: finish-task
description: Use when you have completed, paused or blocked a delegated task and are about to commit or open a PR. Updates the workstream task file, progress log, AI-usage log and STATUS.md so the team's agents can track progress, then runs the checks CI runs.
---

# Finish a delegated task

1. Identify the task ID(s) you worked on and the workstream task file in `docs/progress/` (`corpus.md`, `pipeline.md`, `model.md`, `analytics.md`, `performance.md`, `team.md`).
2. Update only your own task lines:
   - Finished: `[x]`, `owner: @handle` (real GitHub handle), `evidence:` repo paths that exist (code, outputs, `evidence/` records, notebook locators) or URLs.
   - Partly done: `[~]` with `owner:`. Blocked: `[!]` with `blocked: <reason and who/what unblocks it>`.
   - New work discovered: add a line with the next free ID, `needs:` and `due:`.
3. Append one line to `## Progress log`: `- YYYY-MM-DD @handle: what changed. Evidence: path (TASK-ID)`.
4. If AI assistance shaped the work, append a row to `docs/progress/ai-usage.md` (tool and purpose; no model identifiers in repo files).
5. If a table contract, grid or success rule changed, record it in `docs/decisions/decision-log.md`.
6. Run `make status`, then `make check`. Fix every failure; never skip or weaken a check.
7. In the PR, tick the progress boxes in the template and name the cross-review workstream.
