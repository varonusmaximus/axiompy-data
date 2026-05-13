# Arrow Database

Arrow-native database abstraction for analytics and ETL workloads.

## Overview

The `axiompy.data.arrow` module provides an abstraction optimized for **bulk columnar data transfer** using Apache Arrow, designed for analytics and ETL workflows rather than CRUD operations.

### Key Differences from `axiompy.io.database`

| Feature | `axiompy.io.database` | `axiompy.data.arrow` |
|---------|---------------------|---------------------|
| Returns | `List[Dict]` (rows) | `pa.Table` (Arrow) |
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
from axiompy.data.arrow import ArrowDatabaseFactory, DuckDBArrowSettings

# Create an in-memory DuckDB database
settings = DuckDBArrowSettings()
db = ArrowDatabaseFactory.create(settings)

# Execute SQL and get Arrow table
table = db.execute_arrow("SELECT * FROM 'data.parquet'")
print(f"Fetched {table.num_rows} rows, {table.nbytes / 1024 / 1024:.2f} MB")

# Convert to pandas
df = db.to_pandas("SELECT * FROM users LIMIT 1000")
```

## Supported Databases

### DuckDB (Native Arrow Support)

DuckDB has native Arrow support - no ADBC driver needed. Ideal for:
- In-memory analytics
- Reading/writing Parquet, CSV, JSON
- Zero-copy data exchange

```python
from axiompy.data.arrow import ArrowDatabaseFactory, DuckDBArrowSettings

settings = DuckDBArrowSettings(
    database=":memory:",  # or path to .duckdb file
    extensions=["httpfs", "parquet"],  # optional extensions
)
db = ArrowDatabaseFactory.create(settings)

# Read files directly
table = db.read_parquet("s3://bucket/data/*.parquet")
table = db.read_csv("data.csv", header=True)

# Register Arrow tables for SQL queries
db.register_arrow_table("my_data", existing_arrow_table)
result = db.execute_arrow("SELECT * FROM my_data WHERE x > 10")
```

### Snowflake (ADBC)

Uses ADBC (Arrow Database Connectivity) for columnar transfer:

```python
from axiompy.data.arrow import ArrowDatabaseFactory, SnowflakeArrowSettings

settings = SnowflakeArrowSettings(
    account="my_account",
    warehouse="COMPUTE_WH",
    database="MY_DB",
    schema="PUBLIC",
    user="user",
    password="password",  # or use password_secret with SecretsManager
)
db = ArrowDatabaseFactory.create(settings)
table = db.execute_arrow("SELECT * FROM events LIMIT 1000000")
```

### PostgreSQL (ADBC)

Uses ADBC for columnar transfer:

```python
from axiompy.data.arrow import ArrowDatabaseFactory, PostgresArrowSettings

settings = PostgresArrowSettings(
    host="localhost",
    port=5432,
    database="mydb",
    user="postgres",
    password="password",
)
db = ArrowDatabaseFactory.create(settings)
table = db.execute_arrow("SELECT * FROM events")
```

## API Reference

### Factory

| Method | Description | Returns |
|--------|-------------|---------|
| `ArrowDatabaseFactory.create(settings)` | Create database connection | `ArrowDatabase` |
| `ArrowDatabaseFactory.create_mock()` | Create mock for testing | `MockArrowDatabase` |

### ArrowDatabase Methods

| Method | Description | Returns |
|--------|-------------|---------|
| `execute_arrow(sql, params)` | Execute SQL, return Arrow table | `pa.Table` |
| `execute(sql, params)` | Execute SQL without results (DDL/DML) | `None` |
| `register_arrow_table(name, table)` | Register Arrow table for SQL queries | `None` |
| `get_schema(table)` | Get table schema | `pa.Schema` |
| `get_table_names(schema)` | List table names | `list[str]` |
| `validate_connection()` | Check connection health | `bool` |
| `to_pandas(sql, params)` | Execute and return pandas DataFrame | `pd.DataFrame` |
| `to_polars(sql, params)` | Execute and return polars DataFrame | `pl.DataFrame` |
| `close()` | Close connection | `None` |

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
from axiompy.data.arrow import (
    ArrowDatabaseError,    # Base exception
    ArrowConnectionError,  # Connection failures
    ArrowQueryError,       # Query execution errors
)

try:
    table = db.execute_arrow("SELECT * FROM nonexistent")
except ArrowQueryError as e:
    print(f"Query failed: {e}")
except ArrowConnectionError as e:
    print(f"Connection error: {e}")
```

## Testing with MockArrowDatabase

```python
from axiompy.data.arrow import ArrowDatabaseFactory
import pyarrow as pa

# Create mock
mock = ArrowDatabaseFactory.create_mock()

# Set predefined response
mock.set_response(
    "SELECT * FROM users",
    pa.table({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
)

# Use in tests
result = mock.execute_arrow("SELECT * FROM users")
assert result.num_rows == 3

# Verify calls were made
assert ("execute_arrow", "SELECT * FROM users", None) in mock.calls
```

## Integration Patterns

### ETL Pipeline: Snowflake → DuckDB → Parquet

```python
from axiompy.data.arrow import ArrowDatabaseFactory, SnowflakeArrowSettings, DuckDBArrowSettings

# Extract from Snowflake
sf_settings = SnowflakeArrowSettings(...)
sf_db = ArrowDatabaseFactory.create(sf_settings)
source_data = sf_db.execute_arrow("SELECT * FROM events WHERE date = '2026-01-01'")

# Transform with DuckDB
duckdb_settings = DuckDBArrowSettings()
local_db = ArrowDatabaseFactory.create(duckdb_settings)
local_db.register_arrow_table("events", source_data)
transformed = local_db.execute_arrow("""
    SELECT 
        event_type,
        COUNT(*) as event_count,
        AVG(duration_ms) as avg_duration
    FROM events
    GROUP BY event_type
""")

# Load to Parquet
local_db.register_arrow_table("summary", transformed)
local_db.write_parquet("SELECT * FROM summary", "output/summary.parquet")
```

### Integration with axiompy.data

```python
from axiompy.data.arrow import ArrowDatabaseFactory, DuckDBArrowSettings
from axiompy.data import DataProfilerFactory, BatchProcessorFactory

# Query data
db = ArrowDatabaseFactory.create(DuckDBArrowSettings())
table = db.execute_arrow("SELECT * FROM 'data.parquet'")

# Profile with pandas conversion
profiler = DataProfilerFactory.create_auto(table.to_pandas())
report = profiler.profile(table.to_pandas())

# Or process with polars
df = db.to_polars("SELECT * FROM 'data.parquet'")
```

## Best Practices

1. **Use DuckDB for local analytics** - Zero-copy Arrow exchange, no external dependencies
2. **Use context managers** - Ensures connections are properly closed
3. **Parameterize queries** - Pass `params` dict for safe query construction
4. **Register tables for complex queries** - Use `register_arrow_table()` to query in-memory data
5. **Batch large operations** - Use streaming for very large result sets

## Module Architecture

### File Structure

```
axiompy/data/arrow/
├── __init__.py       # Public API exports + ArrowDatabaseFactory
├── base.py           # ArrowDatabase abstract base class
├── settings.py       # Settings protocol + vendor Settings dataclasses
├── errors.py         # Error class hierarchy
├── mock.py           # MockArrowDatabase for testing
├── _duckdb.py        # DuckDB implementation
├── _snowflake.py     # Snowflake ADBC implementation
├── _postgres.py      # PostgreSQL ADBC implementation
└── README.md         # This file
```

### Why This Structure? (Avoiding Circular Imports)

The module is deliberately split into multiple files to **avoid circular imports** without relying on lazy imports.

**The Problem with a Single File:**

If all code lived in `database.py`:
```
database.py (Factory)  ←→  _duckdb.py (imports ArrowDatabase from database.py)
     ↓ imports                    ↑ Factory imports DuckDBArrowDatabase
```
This creates a cycle: `database.py` → `_duckdb.py` → `database.py`

**The Solution: Leaf Modules**

By splitting into separate files, we create a clean dependency graph with no cycles:

```
__init__.py (Factory)
    ↓ imports implementations
_duckdb.py / _snowflake.py / _postgres.py
    ↓ imports (no cycle - these are leaf modules)
base.py, settings.py, errors.py  ← No internal imports!
```

**Key Design Decisions:**

1. **Leaf modules have no internal arrow imports**: `base.py`, `settings.py`, `errors.py`, and `mock.py` only import from external packages (axiompy.validators, axiompy.loggers, etc.)

2. **Implementations import from leaf modules**: `_duckdb.py` imports from `base.py`, `settings.py`, `errors.py` - never from `__init__.py`

3. **Factory lives in `__init__.py`**: The factory imports implementations directly. Since implementations don't import `__init__.py`, there's no cycle.

4. **All imports at module level**: No lazy imports needed - the clean structure allows standard Python imports.

**Benefits:**

- ✅ No pylint R0401 (cyclic-import) warnings
- ✅ No lazy imports - all dependencies visible at import time
- ✅ Better IDE support (autocomplete, type checking)
- ✅ Easier to test individual components
- ✅ Follows Python best practices for module organization

## See Also

- `examples/arrow_database_examples.py` - Comprehensive examples
- `tests/test_arrow_database.py` - Unit tests

