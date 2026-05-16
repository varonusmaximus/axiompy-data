# GitHub Actions (axiompy-data)

| Workflow | When | Purpose |
|----------|------|---------|
| [`python-ci.yml`](python-ci.yml) | Push/PR to `main` or `develop`, or **workflow_dispatch** | **Ruff** (lint + format on Python 3.12), **pytest + coverage** on **Python 3.12** (80% gate; **axiompy[fastapi]** from GitHub `main` (PEP 508 direct ref), then `pip install -e ".[dev,test-all]"`), **Bandit** + **pip-audit** |

There is no Artifactory or private PyPI index. Install **`axiompy`** from PyPI before this package, or from Git if core is not yet published (see root [`README.md`](../../README.md)).

## Java (PySpark tests)

The **test** job installs **Temurin 21** via `actions/setup-java` so the full PySpark suite runs on Linux. Locally, use a real JDK (`java -version` must succeed); see root README.

## Secrets

| Secret | Notes |
|--------|--------|
| `CODECOV_TOKEN` | Optional; Codecov upload is non-blocking if unset. |

## Local parity

```bash
make ci-local
```

Or step by step:

```bash
make lint
make coverage
make security
pip install -e ".[dev]"
pre-commit install
pre-commit install --hook-type pre-push
pre-commit run --all-files
```
