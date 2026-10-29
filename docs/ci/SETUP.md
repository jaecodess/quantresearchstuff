# One-time repository setup (admin)

CI can only block a merge if branch protection requires it. Do these once (task TEAM-09).

## 1. Assign owners

Edit `owner` for each workstream in [.github/workstreams.json](../../.github/workstreams.json) to a real GitHub handle (for example `"@alice"`), then:

```bash
make codeowners
git add .github && git commit -m "Assign workstream owners"
```

`CODEOWNERS` is generated. Cross-reviewers are listed on each path as alternatives, so one approval from any listed owner satisfies the rule.

## 2. Protect `main`

Settings → Branches → Add rule for `main`:

- Require a pull request before merging, 1 approval, dismiss stale approvals
- Require review from Code Owners
- Require status checks to pass: **CI OK** (the single aggregate job in `ci.yml`)
- Require branches to be up to date before merging
- Do not allow bypassing

Or with the GitHub CLI (replace `OWNER/REPO`):

```bash
gh api -X PUT repos/OWNER/REPO/branches/main/protection --input - <<'JSON'
{
  "required_status_checks": {"strict": true, "contexts": ["CI OK"]},
  "enforce_admins": true,
  "required_pull_request_reviews": {"required_approving_review_count": 1, "require_code_owner_reviews": true, "dismiss_stale_reviews": true},
  "restrictions": null
}
JSON
```

Also enable "Automatically delete head branches" and allow squash merging only.

## 3. Secrets

CI needs none. Do not add AWS credentials to GitHub: EMR runs are done by hand from the one agreed AWS account, and their evidence is committed under `evidence/`.

## 4. Submission build

Preview any time: Actions → "Package submission" → Run workflow (dry run). The final strict build runs when you push a tag:

```bash
git tag submission-1 && git push origin submission-1
```

It fails unless `report/report.pdf`, a notebook in `notebooks/`, a saved model in `models/` and a rerun guide in `docs/rerun/` exist, and it re-runs the hygiene checks on the staged ZIP contents. Download the ZIP from the run's artifacts.
