.PHONY: help setup check status codeowners lint test spark-test hygiene streaming sql zip

PY ?= python3

help:
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-12s %s\n", $$1, $$2}'

setup: ## install dev tools (pytest, ruff, sqlfluff)
	$(PY) -m pip install -r requirements-dev.txt

status: ## regenerate docs/progress/STATUS.md from the task files (run before every PR)
	$(PY) scripts/progress.py status

codeowners: ## regenerate .github/CODEOWNERS from .github/workstreams.json
	$(PY) scripts/progress.py codeowners

check: ## everything CI runs, locally (except Spark tests)
	$(PY) scripts/progress.py check
	$(PY) scripts/check_hygiene.py
	ruff check .
	ruff format --check scripts tests
	$(PY) -m pytest -q
	bash scripts/streaming_smoke.sh
	sqlfluff lint hive analytics --dialect hive

lint: ## ruff lint + format
	ruff check . && ruff format scripts tests

test: ## unit tests (no Spark)
	$(PY) -m pytest -q

spark-test: ## Spark tests in local mode (needs Java 17 and requirements-spark.txt)
	$(PY) -m pytest -q -m spark

hygiene: ## secrets, machine paths, raw data, broken markdown links
	$(PY) scripts/check_hygiene.py

streaming: ## local mapper | sort | reducer smoke tests
	bash scripts/streaming_smoke.sh

zip: ## build dist/submission.zip (strict: needs report.pdf, notebook, model, rerun guide)
	$(PY) scripts/build_submission_zip.py --out dist/submission.zip
