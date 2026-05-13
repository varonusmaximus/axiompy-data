# GitHub Actions (axiompy-data)

| Workflow | When | Purpose |
|----------|------|---------|
| [`python-ci.yml`](python-ci.yml) | Push/PR to `main` or `develop`, or **workflow_dispatch** | **Ruff** (lint + format on Python 3.11), **pytest + coverage** on **Python 3.12** (80% gate), **Bandit** + **pip-audit** (failing) |

There is no Artifactory or private PyPI index. Install **`axiompy`** from PyPI before this package, or from Git if core is not yet published (see root [`README.md`](../../README.md)).

## Secrets

| Secret | Notes |
|--------|--------|
| `CODECOV_TOKEN` | Optional; Codecov upload is non-blocking if unset. |

## Local parity

```bash
make lint
make test
make coverage
make security
```

```bash
pip install pre-commit
pre-commit install
pre-commit install --hook-type pre-push
pre-commit run --all-files
```
