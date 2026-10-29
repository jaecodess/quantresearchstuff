# Cross-sectional signal research (INF2006 Big Data project)

Predicting weekly relative stock performance from price, volume and volatility indicators on AWS EMR (HDFS, Hadoop Streaming, Hive, PySpark / Spark ML). Deadline: team ZIP by 22 Nov 2026, 23:59 SGT.

- Project plan and brief summary: [HANDOVER.md](HANDOVER.md)
- **Who does what, and where we are:** [docs/progress/STATUS.md](docs/progress/STATUS.md)
- How we work in parallel and what CI enforces: [CONTRIBUTING.md](CONTRIBUTING.md)
- Instructions for AI agents: [CLAUDE.md](CLAUDE.md)
- Decisions and pre-registered gates: [docs/decisions/decision-log.md](docs/decisions/decision-log.md)
- Admin setup (branch protection, owners): [docs/ci/SETUP.md](docs/ci/SETUP.md)

```bash
make setup && make check
```

Scope: signal quality only. No claims about profit, trading costs or investability.
