# `axiompy.data` package hierarchy

Domain **subpackages** with barrel `__init__.py` exports, plus **shared primitives** at the `axiompy.data` root (`types`, `error`, `result`).

`axiompy.data` uses `pkgutil.extend_path` so the namespace can span wheels, consistent with `axiompy.__path__`.

**Interchange** lives only at the **`axiompy/data/`** root: **`dataframe`**, **`export`**, **`compression`**. This distribution does not ship an **`axiompy.data.io`** package (stdlib `io` collision; core **`axiompy.io`** remains the I/O namespace on the core distribution).

**Governance** is not a separate package: **quality**, **lineage**, and **CDC** are **`axiompy.data.processing`** capabilities. Auditable outcomes can flow through **`axiompy.data.observability`** (signals → sink adapters).

## Package tree

```
axiompy/data/
  __init__.py          # barrel export + extend_path
  types.py
  error.py
  result.py
  dataframe.py         # interchange (Rule C: port-first transforms)
  export.py
  compression.py
  consuming/
    ports.py            # (optional / incremental)
    factory.py
    adapters/           # vendor Arrow-protocol clients
    base.py
    settings.py
    errors.py
    mock.py
  streaming/
    ports.py
    factory.py
    adapters/
    types.py
    handler.py
  processing/
    ports.py
    factory.py          # re-exports domain *Factory classes
    adapters/
      batch_processors.py
      partitioners.py
      transformers.py
      profilers.py
      lineage_trackers.py
      change_detectors.py
    batch.py
    pipeline.py
    partition.py
    transform.py
    quality.py
    lineage.py
    cdc.py
  observability/
    ports.py
    emit.py
    factory.py
    sinks/
```

## Canonical imports

| Intent | Import |
|--------|--------|
| Consumption | `from axiompy.data.consuming import ...` |
| Streaming | `from axiompy.data.streaming import ...` |
| Processing (incl. quality / lineage / CDC) | `from axiompy.data.processing import ...` |
| Interchange | `from axiompy.data.dataframe import ...`, `export`, `compression` |
| Observability | `from axiompy.data.observability import ...` |

## Rule C (port-first transforms)

Engine-neutral **transform** operations are declared on **ports** (e.g. `DataFrameAdapter` or dedicated transform protocols); **Pandas / Spark / Polars / …** adapters implement behavior. **`processing`** composes sequences of port calls instead of hard-coding engine-specific APIs in generic modules.

## Naming

- **`consuming`** — analytical stores / bulk columnar clients (PyArrow is an implementation detail of adapters).
- **`processing`** — batch, pipeline, partition, transform, profiler, lineage, CDC.
- **`observability`** — outbound signal sinks; optional extras in `pyproject.toml` (`otel`, `consuming-*`, etc.).

Arrow remains the **wire/table protocol** inside **consuming** adapters (`QueryResult.data`, `Arrow*` settings names where relevant). Public API: `Factory`, `Client`, `QueryResult`, `Platform`, `Settings`; conversion via `QueryResult.to()` / [`consuming/interchange.py`](axiompy/data/consuming/interchange.py).
