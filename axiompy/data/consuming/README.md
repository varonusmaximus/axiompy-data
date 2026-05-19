# Arrow Database

Arrow-native database abstraction for analytics and ETL workloads.

## Overview

The `axiompy.data.consuming` module provides an abstraction optimized for **bulk columnar data transfer** using Apache Arrow, designed for analytics and ETL workflows rather than CRUD operations.

### Key Differences from `axiompy.io.database`

| Feature | `axiompy.io.database` | `axiompy.data.consuming` |
|---------|---------------------|---------------------|
| Returns | `List[Dict]` (rows) | `QueryResult` (Arrow in `.data`) |
| Optimized for | Small result sets, CRUD | Large result sets (1M+ rows) |
| Pattern | OLTP | OLAP/Analytics |
| Integration | Web APIs, applications | DataFrames, Spark, batch processing |

### Use Cases

- ✅ **Data Migration**: Bulk transfer between databases
- ✅ **Analytics Queries**: Large result sets for analysis
- ✅ **ETL Pipelines**: Columnar processing with DuckDB/Polars/Spark
- ✅ **Data Quality**: Bulk validation on large datasets

## Installation

```bash
# Core (DuckDB)
pip install duckdb pyarrow

# Snowflake support
pip install adbc-driver-snowflake

# PostgreSQL support
pip install adbc-driver-postgresql

# Optional: DataFrame integration
pip install pandas polars
```

## Quick Start

```python
from axiompy.data.consuming import Factory, DuckDBArrowSettings

# Create an in-memory DuckDB database
settings = DuckDBArrowSettings()
client = Factory.create(settings)

# Execute SQL and get QueryResult (canonical columnar payload in .data)
result = client.query("SELECT * FROM 'data.parquet'")
print(f"Fetched {result.row_count} rows, {result.nbytes / 1024 / 1024:.2f} MB")

# Convert to pandas, polars, or Spark
df = result.to_pandas()
# result.to_polars(); result.to_spark(spark_session)
```

## Supported Databases

### DuckDB (Native Arrow Support)

DuckDB has native Arrow support - no ADBC driver needed. Ideal for:
- In-memory analytics
- Reading/writing Parquet, CSV, JSON
- Zero-copy data exchange

```python
from axiompy.data.consuming import Factory, DuckDBArrowSettings

settings = DuckDBArrowSettings(
    database=":memory:",  # or path to .duckdb file
    extensions=["httpfs", "parquet"],  # optional extensions
)
db = Factory.create(settings)

# Read files directly
table = db.read_parquet("s3://bucket/data/*.parquet")
table = db.read_csv("data.csv", header=True)

# Register in-memory tables for SQL (DuckDB only)
db.register_table("my_data", existing_arrow_table)
result = db.query("SELECT * FROM my_data WHERE x > 10")
df = result.to_pandas()
```

### Snowflake (ADBC)

Uses ADBC (Arrow Database Connectivity) for columnar transfer:

```python
from axiompy.data.consuming import ArrowDatabaseFactory, SnowflakeArrowSettings

settings = SnowflakeArrowSettings(
    account="my_account",
    warehouse="COMPUTE_WH",
    database="MY_DB",
    schema="PUBLIC",
    user="user",
    password="password",  # or use password_secret with SecretsManager
)
db = Factory.create(settings)
table = db.query("SELECT * FROM events LIMIT 1000000")
```

### PostgreSQL (ADBC)

Uses ADBC for columnar transfer:

```python
from axiompy.data.consuming import ArrowDatabaseFactory, PostgresArrowSettings

settings = PostgresArrowSettings(
    host="localhost",
    port=5432,
    database="mydb",
    user="postgres",
    password="password",
)
db = Factory.create(settings)
table = db.query("SELECT * FROM events")
```

## API Reference

### Factory

| Method | Description | Returns |
|--------|-------------|---------|
| `ArrowDatabaseFactory.create(settings)` | Create database connection | `ArrowDatabase` |
| `ArrowDatabaseFactory.create_mock()` | Create mock for testing | `MockArrowDatabase` |

### Client methods

| Method | Description | Returns |
|--------|-------------|---------|
| `query(sql, params)` | Execute SQL, return result | `QueryResult` |
| `execute(sql, params)` | Execute SQL without results (DDL/DML) | `None` |
| `get_schema(table)` | Get table schema | `pa.Schema` |
| `get_table_names(schema)` | List table names | `list[str]` |
| `validate_connection()` | Check connection health | `bool` |
| `close()` | Close connection | `None` |

### QueryResult

| Member / method | Description |
|-----------------|-------------|
| `data` | PyArrow table (canonical payload) |
| `row_count`, `schema`, `nbytes` | Convenience accessors |
| `to_pandas()`, `to_polars()`, `to_spark(spark)` | Convert to other engines |
| `to(DataEngine, **kwargs)` | Unified conversion via `interchange.convert` |

### DuckDB-only

| Method | Description |
|--------|-------------|
| `register_table(name, table)` | Register in-memory table for SQL |
| `read_parquet(path)` | Read Parquet as `QueryResult` |
| `read_csv(path, **options)` | Read CSV as `QueryResult` |

### DuckDB-Specific Methods

| Method | Description |
|--------|-------------|
| `read_parquet(path)` | Read Parquet files as Arrow table |
| `read_csv(path, **options)` | Read CSV files as Arrow table |
| `read_json(path)` | Read JSON files as Arrow table |
| `write_parquet(sql, path)` | Write query results to Parquet |

## Settings Classes

### DuckDBArrowSettings

```python
@dataclass
class DuckDBArrowSettings:
    database: str = ":memory:"        # Path or :memory:
    read_only: bool = False           # Read-only mode
    extensions: list[str] = []        # Extensions to load
```

### SnowflakeArrowSettings

```python
@dataclass
class SnowflakeArrowSettings:
    account: str                      # Account identifier
    warehouse: str                    # Compute warehouse
    database: str                     # Database name
    schema: str                       # Schema name
    user: str                         # Username
    password: Optional[str] = None    # Password (or use secret)
    password_secret: Optional[str] = None  # SecretsManager key
    role: Optional[str] = None        # Optional role
    arrow_batch_size: int = 100_000   # Streaming batch size
```

### PostgresArrowSettings

```python
@dataclass
class PostgresArrowSettings:
    host: str                         # Database host
    port: int                         # Database port
    database: str                     # Database name
    user: str                         # Username
    password: Optional[str] = None    # Password (or use secret)
    password_secret: Optional[str] = None  # SecretsManager key
    schema: str = "public"            # Default schema
    ssl_mode: str = "prefer"          # SSL mode
```

## Error Handling

```python
from axiompy.data.consuming import (
    ArrowDatabaseError,    # Base exception
    ArrowConnectionError,  # Connection failures
    ArrowQueryError,       # Query execution errors
)

try:
    table = db.query("SELECT * FROM nonexistent")
except ArrowQueryError as e:
    print(f"Query failed: {e}")
except ArrowConnectionError as e:
    print(f"Connection error: {e}")
```

## Testing with MockArrowDatabase

```python
from axiompy.data.consuming import ArrowDatabaseFactory
import pyarrow as pa

# Create mock
mock = ArrowDatabaseFactory.create_mock()

# Set predefined response
mock.set_response(
    "SELECT * FROM users",
    pa.table({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
)

# Use in tests
result = mock.query("SELECT * FROM users")
assert result.num_rows == 3

# Verify calls were made
assert ("_fetch_table_core", "SELECT * FROM users", None) in mock.calls
```

## Integration Patterns

### ETL Pipeline: Snowflake → DuckDB → Parquet

```python
from axiompy.data.consuming import ArrowDatabaseFactory, SnowflakeArrowSettings, DuckDBArrowSettings

# Extract from Snowflake
sf_settings = SnowflakeArrowSettings(...)
sf_db = ArrowDatabaseFactory.create(sf_settings)
source_data = sf_db.query("SELECT * FROM events WHERE date = '2026-01-01'")

# Transform with DuckDB
duckdb_settings = DuckDBArrowSettings()
local_db = ArrowDatabaseFactory.create(duckdb_settings)
local_db.register_table("events", source_data)
transformed = local_db.query("""
    SELECT
        event_type,
        COUNT(*) as event_count,
        AVG(duration_ms) as avg_duration
    FROM events
    GROUP BY event_type
""")

# Load to Parquet
local_db.register_table("summary", transformed)
local_db.write_parquet("SELECT * FROM summary", "output/summary.parquet")
```

### Integration with axiompy.data

```python
from axiompy.data.consuming import Factory, DuckDBArrowSettings
from axiompy.data import DataProfilerFactory, BatchProcessorFactory

# Query data
db = ArrowDatabaseFactory.create(DuckDBArrowSettings())
table = db.query("SELECT * FROM 'data.parquet'")

# Profile with pandas conversion
profiler = DataProfilerFactory.create_auto(result.to_pandas())
report = profiler.profile(result.to_pandas())

# Or process with polars
df = db.to_polars("SELECT * FROM 'data.parquet'")
```

## Best Practices

1. **Use DuckDB for local analytics** - Zero-copy Arrow exchange, no external dependencies
2. **Use context managers** - Ensures connections are properly closed
3. **Parameterize queries** - Pass `params` dict for safe query construction
4. **Register tables for complex queries** - Use `register_table()` on DuckDB to query in-memory data
5. **Batch large operations** - Use streaming for very large result sets

## Module Architecture

### File Structure

```
axiompy/data/consuming/
├── __init__.py       # Public API exports + factories
├── factory.py        # AbstractConsumingClientFactory + ArrowDatabaseFactory
├── base.py           # ArrowDatabase abstract base class
├── settings.py       # Settings protocol + vendor Settings dataclasses
├── errors.py         # Error class hierarchy
├── adapters/
│   ├── duckdb.py     # DuckDB implementation
│   ├── snowflake.py  # Snowflake ADBC implementation
│   ├── postgres.py   # PostgreSQL ADBC implementation
│   └── mock.py       # MockArrowDatabase for testing
└── README.md         # This file
```

### Why This Structure? (Avoiding Circular Imports)

The module is deliberately split into multiple files to **avoid circular imports** without relying on lazy imports.

**The Problem with a Single File:**

If all code lived in `database.py`:
```
database.py (Factory)  ←→  adapters/duckdb.py (imports ArrowDatabase from base.py)
     ↓ imports                              ↑ Factory lazy-imports DuckDBArrowDatabase
```
This creates a cycle: `database.py` → `adapters/duckdb.py` → `database.py`

**The Solution: Leaf Modules**

By splitting into separate files, we create a clean dependency graph with no cycles:

```
factory.py (ArrowDatabaseFactory)
    ↓ lazy-imports implementations
adapters/duckdb.py / adapters/snowflake.py / adapters/postgres.py
    ↓ imports (no cycle - these are leaf modules)
base.py, settings.py, errors.py  ← No internal imports!
```

**Key Design Decisions:**

1. **Leaf modules have no internal consuming imports**: `base.py`, `settings.py`, and `errors.py` only import from external packages (axiompy.validators, axiompy.loggers, etc.)

2. **Implementations import from leaf modules**: `adapters/duckdb.py` imports from `base.py`, `settings.py`, `errors.py` - never from `factory.py`

3. **Factory lives in `factory.py`**: The factory lazy-imports implementations. Since implementations don't import `factory.py`, there's no cycle.

4. **Lazy imports in `factory.py`**: Keeps optional vendor drivers off the import path until needed.

**Benefits:**

- ✅ No pylint R0401 (cyclic-import) warnings
- ✅ Optional backends load only when constructed
- ✅ Better IDE support for leaf modules (autocomplete, type checking)
- ✅ Easier to test individual components
- ✅ Follows Python best practices for module organization

## See Also

- `examples/arrow_database_examples.py` - Comprehensive examples
- `tests/test_arrow_database.py` - Unit tests
