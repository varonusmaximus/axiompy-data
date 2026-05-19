# axiompy-data

Distribution **`axiompy-data`** adds the `axiompy.data` namespace (pip merges it with core `axiompy`).

Requires **Python 3.12+** and **`axiompy`>=2** (core). Install from PyPI when published, or install core from source first.

```bash
pip install "axiompy>=2,<3"
pip install "axiompy-data[test-all]>=3,<4"
```

For local development with sibling checkouts:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e "../axiompy[io]"
pip install -e ".[dev]"
axiompy-skills --project
```

Use **`[test-all]`** when you need the full integration stack (Spark, Arrow drivers, streaming, cloud mocks):

```bash
pip install -e ".[test-all]"
```

Repository: [https://github.com/varonusmaximus/axiompy-data](https://github.com/varonusmaximus/axiompy-data)

If **`axiompy` is not yet on PyPI**, install it from Git before installing this package (`web` is core; `[io]` adds database/HTTP/storage clients):

```bash
pip install "axiompy[io] @ git+https://github.com/varonusmaximus/axiompy.git@main"
pip install -e ".[dev]"
axiompy-skills --project
```

## Local checks

```bash
make lint
make coverage    # requires Java for full PySpark suite; see below
make security
make ci-local    # lint + pre-commit + coverage + security
```

Optional: `make typecheck` (mypy on `axiompy/`; not run in CI).

## Java / PySpark (full test suite)

PySpark tests need a **real JRE** (macOS `/usr/bin/java` is often a stub). CI uses **Temurin 21** via `actions/setup-java`.

- **macOS (Homebrew):** `brew install openjdk@21` then
  `export PATH="/opt/homebrew/opt/openjdk@21/bin:$PATH"`
  (or set `JAVA_HOME` to the JDK home).
- **Linux:** install Temurin or OpenJDK 21 and ensure `java -version` succeeds.

Without Java, root [`conftest.py`](conftest.py) skips Spark-heavy collection; with Java, all Spark tests run.

## Cursor agents and skills

This repo follows the same **bundled Cursor skills** as core `axiompy` (review, style, design patterns, documentation, testing). After installing `axiompy` (sibling editable or from PyPI), sync skills into this workspace:

```bash
axiompy-skills --project
```

That writes under **`.cursor/skills/`** (ignored in git; regenerate after clone or `axiompy` upgrade). See core [Install for Cursor agents (library + skills)](https://github.com/varonusmaximus/axiompy/blob/main/README.md#install-for-cursor-agents-library--skills) for CLI options (`--show-config`, destinations, and `[tool.axiompy.skills]` in `pyproject.toml`).

Workspace pointers: **[`AGENTS.md`](AGENTS.md)** (short index) and **`.cursor/rules/`** (e.g. branch-first workflow).

**Data-library-specific skills** for `axiompy-data` will be added in a later iteration; until then, use the core bundle only.
