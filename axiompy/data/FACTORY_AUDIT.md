# Factory / engine dispatch audit (design-patterns)

Read-only review against the **design-patterns** skill: prefer `DataEngine` enums, factories, and explicit settings over stringly-typed engine switches.

## In good shape

| Area | Pattern |
|------|---------|
| Public API | `DataEngine` enum in `types.py`; factories expose `create(engine)` and `create_auto(data)` |
| Arrow | `ArrowDatabaseFactory` + typed settings (`DuckDBArrowSettings`, etc.) in **`axiompy.data.consuming`**; public façade **`AbstractConsumingClientFactory`** |
| Observability | **`SinkFactory`** (`noop`, `otel`, `elastic`, `splunk_hec`) in **`axiompy.data.observability`** |
| Streaming | `StreamEngine` + producer/consumer factories |
| Pipeline | `TaskStatus` enum; explicit task graph |

## Candidates (non-breaking follow-ups)

| Location | Current | Suggested | Breaking? |
|----------|---------|-----------|-----------|
| `batch.py`, `cdc.py`, `dataframe.py`, `export.py`, `lineage.py`, `partition.py`, `quality.py`, `transform.py` — `create_auto()` | `if "pandas" in module_name` (and similar) for runtime detection | Keep for auto-detect; ensure all branches map to `DataEngine` before `create()` | No if behavior unchanged |
| `processing/{quality,lineage,cdc}.py` | Governance-style capabilities | Prefer **`axiompy.data.processing`** imports | — |
| Polars | Mentioned in package docstring; limited `create_auto` support | Register `DataEngine.POLARS` adapters or document as future | API doc only |
| `__init__.py` `__version__` | should match distribution | Keep in sync with `pyproject` | Low risk |

## Deferred

- Replacing module-name heuristics with `isinstance` checks where optional deps are installed (larger refactor, test-heavy).
- Data-specific Cursor skills bundle (separate roadmap).
