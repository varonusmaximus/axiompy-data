# Core axiompy install (for axiompy-data)

axiompy **2.x** (see [axiompy `docs/CORE_WEB_AND_EXTRAS_PLAN.md`](https://github.com/varonusmaximus/axiompy/blob/main/docs/CORE_WEB_AND_EXTRAS_PLAN.md)):

- **`axiompy.web`** is **core** (requires **Pydantic** on base install; no FastAPI).
- **`axiompy[io]`** — HTTP clients, databases, object storage, YAML (`axiompy.io`).
- **`axiompy[servers]`** — Flask/FastAPI/MCP/JSON-RPC hosting only.

axiompy-data tests and modules use `axiompy.io`, `loggers`, and `validators`, so CI and local dev install:

```bash
pip install "axiompy[io] @ git+https://github.com/varonusmaximus/axiompy.git@main"
```

Sibling checkout:

```bash
pip install -e "../axiompy[io]"
pip install -e ".[dev]"
axiompy-skills --project
```

CI installs from `@main` until core 2.x is published on PyPI.
