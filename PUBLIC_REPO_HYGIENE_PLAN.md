# axiompy-data: public repo hygiene plan

This document records the hygiene goals applied to this repository (CI, Makefile, pre-commit, metadata, and disallowed legacy identifiers).

## 1. String and metadata cleanup

- **Hard rule:** No disallowed legacy vendor or internal-package substrings in any tracked file (including commit messages if you rewrite history). Verify with the repository’s agreed search checklist from the repo root (excluding `.git`). The PyPI build dependency `poetry-core` is unrelated to any disallowed token.
- **`pyproject.toml`:** Remove Artifactory `[[tool.poetry.source]]`. Set Poetry + PEP 621 `authors` to a public identity. Set `[project.urls]` to `varonusmaximus/axiompy-data` (or your org).
- **`README.md`:** Install and clone URLs aligned with public GitHub and PyPI-only flow.

## 2. GitHub Actions (`python-ci.yml`)

| Job | Behavior |
|-----|----------|
| **lint** | Python **3.11**, `ruff==0.14.14`, `ruff check` + `ruff format --check`. |
| **test** | Python **3.12** only; `pip install -e ".[test-all]"`; pytest + coverage; **`coverage report --fail-under=80`**; Codecov optional, flag `python-3.12`. |
| **security-scan** | Python **3.11**; `bandit -r axiompy/ -ll` and **`pip-audit`** after editable install; no Safety; no masked failures. |

No `PIP_EXTRA_INDEX_URL`. Optional `workflow_dispatch`. No optional mypy job in CI.

## 3. Makefile (root)

- `make lint`, `make test`, `make coverage`, `make security` (bandit + pip-audit). No Artifactory publish targets.

## 4. Pre-commit

- Ruff `v0.14.14`, pre-commit-hooks, Bandit on `^axiompy/`, pip-audit and pytest on **pre-push**.

## 5. Dependency on `axiompy`

`pyproject.toml` pins `axiompy>=2.0.0,<3.0.0`. CI assumes PyPI install works; if core is not published, install from Git first (see root README).

## 6. Git history (optional)

For a single clean root: `git checkout --orphan`, `git add -A`, one commit with a neutral message (no disallowed substrings), replace `main`, `git push --force-with-lease origin main`.

## 7. Verification checklist

- CI green: lint, test (3.12), security.
- Repository-wide search per team checklist: no disallowed identifiers.
- If history rewritten: `git rev-list --count main` is `1`; pickaxe searches for disallowed tokens empty on `main`.
