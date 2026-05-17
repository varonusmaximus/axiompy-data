# axiompy-data (Cursor workspace)

## Where the rules live

1. **Cursor skills** — Install or refresh with `axiompy-skills --project` from this repo root so `./.cursor/skills/` matches the **core `axiompy` bundle** (same playbooks as the main library). Canonical depth for design, review, style, docs, and testing is in those `SKILL.md` files and their sidecars.
2. **`.cursor/rules/*.mdc`** — Workflow gates only (e.g. branch-first). Not a duplicate of the full style guide.
3. **Core history** — In the sibling [`axiompy`](https://github.com/varonusmaximus/axiompy) repo, [`docs/ARCHIVED_AGENTS.md`](https://github.com/varonusmaximus/axiompy/blob/main/docs/ARCHIVED_AGENTS.md) holds the old monolithic ruleset for reference only.

## Quick constraints

- **Python 3.12+** (matches core `axiompy`); **ruff** line length **100** with the same **`select` / `ignore` profile as core** (see `pyproject.toml`).
- **Mypy** is optional locally (`make typecheck`); it is **not** run in GitHub Actions for this repo.
- **Branch-first:** do not commit directly to `main` (see [`.cursor/rules/branch-first-workflow.mdc`](.cursor/rules/branch-first-workflow.mdc)).
- **Public `axiompy/data/`** — same architectural bar as core: explicit factories/settings, validators at boundaries, `LoggerFactory`, `Result` where appropriate, no hardcoded secrets.

For anything beyond this blurb, open the relevant skill under `.cursor/skills/<name>/SKILL.md` after syncing.
