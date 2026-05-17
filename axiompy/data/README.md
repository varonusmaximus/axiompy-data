# axiompy.data — Data engineering utilities

Engine-agnostic data utilities for **Pandas**, **Spark**, and optional backends, shipped in the **`axiompy-data`** distribution (namespace `axiompy.data`).

| Topic | Document |
|-------|----------|
| Package domains & migration notes | [PACKAGE_LAYOUT.md](PACKAGE_LAYOUT.md) |
| Interchange — `dataframe`, `export`, `compression` (at package root) | [dataframe.py](dataframe.py), [export.py](export.py), [compression.py](compression.py) |
| `processing` — batch, pipeline, partition, transform, quality, lineage, CDC | [processing/](processing/) |
| `consuming` — Arrow-style analytical DB clients | [consuming/README.md](consuming/README.md) |
| `observability` — signal sinks (OTel, Elastic, Splunk HEC, …) | [observability/](observability/) |
| Streaming (Kafka, Kinesis, Redis, RabbitMQ) | [streaming/README.md](streaming/README.md) |
| Factory / engine dispatch notes | [FACTORY_AUDIT.md](FACTORY_AUDIT.md) |
| Repository dev setup | [../../README.md](../../README.md) |

## Features

- Engine abstraction via factories and `DataEngine`
- Data quality profiling and expectations
- ETL transforms, batch processing, partitioning
- Pipelines, lineage, change data capture
- Format conversion and compression
- Real-time streaming adapters

## Installation

Requires core **`axiompy`** and this wheel:

```bash
pip install "axiompy>=2,<3"
pip install "axiompy-data[data]"      # pandas
pip install "axiompy-data[spark]"     # pyspark
pip install "axiompy-data[streaming-all]"
pip install "axiompy-data[test-all]"  # full test stack
```

## Quick start

### Auto-Detection (Recommended)

The framework automatically detects whether you're using Pandas or Spark:

```python
from axiompy.data import DataProfilerFactory
import pandas as pd

# Works with Pandas
df = pd.read_csv("data.csv")
profiler = DataProfilerFactory.create_auto(df)  # Auto-detects Pandas
report = profiler.profile(df)

# Same code works with Spark!
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark_df = spark.read.csv("data.csv", header=True)
profiler = DataProfilerFactory.create_auto(spark_df)  # Auto-detects Spark
report = profiler.profile(spark_df)
```

### Explicit Engine Selection

```python
from axiompy.data import DataProfilerFactory, DataEngine

# Force specific engine
profiler = DataProfilerFactory.create(DataEngine.PANDAS)
report = profiler.profile(pandas_df)

profiler = DataProfilerFactory.create(DataEngine.SPARK)
report = profiler.profile(spark_df)
```

---

## Module documentation

### 1. Data Quality & Validation

Profile data quality and validate expectations:

```python
from axiompy.data import DataProfilerFactory, DataExpectation

# Profile data
profiler = DataProfilerFactory.create_auto(df)
report = profiler.profile(df)

print(f"Rows: {report.row_count}")
print(f"Nulls: {report.null_counts}")
print(f"Issues: {report.issues}")

# Validate expectations
expectations = [
    DataExpectation(name="id_not_null", column="id", condition="not_null"),
    DataExpectation(name="id_unique", column="id", condition="unique"),
    DataExpectation(name="age_range", column="age", condition="in_range", 
                   params={"min": 0, "max": 150}),
    DataExpectation(name="status_valid", column="status", condition="in_set",
                   params={"values": ["active", "inactive"]}),
]

results = profiler.validate_expectations(df, expectations)
print(f"Passed: {results['passed']}, Failed: {results['failed']}")
```

**Available Conditions:**
- `not_null` - Column has no null values
- `unique` - Column has no duplicates
- `in_range` - Values within min/max range
- `in_set` - Values in allowed set
- `regex_match` - Values match regex pattern

#### Railway-Oriented Data Quality (Result Types)

Use Result types for functional error handling without exceptions:

```python
from axiompy.data import DataProfilerFactory

profiler = DataProfilerFactory.create_auto(df)

# Profile with Result type - never raises exceptions
result = profiler.try_profile(df)

# Handle result with chainable operations
profiler.try_profile(df) \
    .map(lambda r: r.issues) \
    .then(lambda issues: check_critical_issues(issues)) \
    .unwrap_or([])  # Default to [] on error

# Or unpack directly with error handling
if result.is_ok():
    report = result.unwrap()
    print(f"Profile OK: {report.row_count} rows")
else:
    error = result.get_error()
    print(f"Profiling failed: {error}")

# Validate expectations with Result type
expectations = [DataExpectation(...)]
validation_result = profiler.try_validate_expectations(df, expectations)

# Chain multiple operations
validation_result \
    .map(lambda r: r['passed']) \
    .then(lambda passed: log_validation_passed(passed))

# Check schema with Result type
schema = {"id": "int", "name": "string"}
schema_result = profiler.try_check_schema(df, schema)

schema_result \
    .map(lambda r: r['matches']) \
    .or_else(lambda e: handle_schema_error(e))
```

**Key Benefits:**
- No exceptions - all errors in Result
- Chainable operations (`.then()`, `.map()`, `.unwrap_or()`)
- Error recovery with `.or_else()`, `.map_error()`
- Clean functional pipelines
- Type-safe error handling

---

### 2. Data Transformations

Common transformation patterns:

```python
from axiompy.data import DataTransformerFactory

transformer = DataTransformerFactory.create_auto(df)

# Rename columns
df = transformer.rename_columns(df, {"old_name": "new_name"})

# Drop nulls
df = transformer.drop_nulls(df, how="any")

# Fill nulls
df = transformer.fill_nulls(df, strategy="mean", columns=["age", "score"])

# Deduplicate
df = transformer.deduplicate(df, subset=["user_id"])

# Filter rows
df = transformer.filter_rows(df, "age > 18 and status == 'active'")

# Add computed column
df = transformer.add_computed_column(df, "full_name", 
                                    lambda d: d["first_name"] + " " + d["last_name"])

# Cast column type
df = transformer.cast_column(df, "age", "int")
```

**Fill Strategies:**
- `value` - Fill with specific value
- `mean` - Fill with column mean
- `median` - Fill with column median
- `mode` - Fill with most common value
- `forward` - Forward fill (propagate last valid value)
- `backward` - Backward fill

---

### 3. DataFrame Adapters

Unified DataFrame operations:

```python
from axiompy.data import DataFrameAdapterFactory
from axiompy.io import DatabaseFactory, DatabaseType, DatabaseSettings

adapter = DataFrameAdapterFactory.create_auto(df)

# Read from database
db_settings = DatabaseSettings(host="localhost", database="mydb", 
                               username="user", password="pass")
db = DatabaseFactory.create(DatabaseType.POSTGRES, db_settings)
df = adapter.read_table(db, "users", columns=["id", "name"], limit=1000)

# Write to database
adapter.write_table(df, db, "processed_users", mode="append")

# Read/write files
df = adapter.read_file("data.parquet", format="parquet")
adapter.write_file(df, "output.csv", format="csv")

# Get schema
schema = adapter.get_schema(df)
print(schema)  # {"id": "int64", "name": "string", ...}

# Get shape
rows, cols = adapter.get_shape(df)
```

---

### 4. Batch Processing

Process large datasets in chunks:

```python
from axiompy.data import BatchProcessorFactory

processor = BatchProcessorFactory.create_auto(
    df,
    batch_size=1000,
    max_workers=4,  # Parallel processing
    show_progress=True  # tqdm progress bar
)

# Define transformation
def transform_batch(batch):
    # Your transformation logic
    batch = clean_data(batch)
    batch = enrich_data(batch)
    return batch

# Process with sink
results = processor.process_batches(
    data=df,
    transform_func=transform_batch,
    sink=lambda batch: save_to_database(batch),
    fail_fast=False  # Continue on errors
)

print(f"Processed: {results['batches_processed']}")
print(f"Failed: {results['batches_failed']}")
```

---

### 5. Data Partitioning

Organize data with time/hash/range partitioning:

```python
from axiompy.data import DataPartitionerFactory, PartitionStrategy

partitioner = DataPartitionerFactory.create_auto(
    df,
    partition_key="created_at",
    strategy=PartitionStrategy.TIME_DAILY,
    base_path="s3://bucket/data/"
)

# Write partitioned data
paths = partitioner.write_partitioned(df, storage=s3_storage, format="parquet")
# Creates: s3://bucket/data/year=2024/month=10/day=28/data.parquet

# Read specific partitions
data = partitioner.read_partitions(
    partitions=["s3://bucket/data/year=2024/month=10/day=28"],
    storage=s3_storage
)

# List all partitions
all_partitions = partitioner.list_partitions(storage=s3_storage)
```

**Partition Strategies:**
- `TIME_DAILY` - Partition by year/month/day
- `TIME_MONTHLY` - Partition by year/month
- `TIME_YEARLY` - Partition by year
- `TIME_HOURLY` - Partition by year/month/day/hour
- `HASH` - Hash-based partitioning
- `RANGE` - Range-based partitioning

---

### 6. ETL Pipelines

Build and orchestrate workflows:

```python
from axiompy.data import Pipeline, Task

# Define tasks
extract = Task(
    name="extract",
    func=lambda: load_data_from_source(),
    retry_count=2
)

transform = Task(
    name="transform",
    func=lambda context: clean_data(context["extract"]),
    depends_on=["extract"]
)

load = Task(
    name="load",
    func=lambda context: save_data(context["transform"]),
    depends_on=["transform"]
)

# Build pipeline
pipeline = Pipeline("etl_pipeline")
pipeline.add_tasks([extract, transform, load])

# Run pipeline
results = pipeline.run(fail_fast=True)

if results["success"]:
    print("Pipeline succeeded!")
else:
    print(f"Pipeline failed: {results['errors']}")

# Visualize
print(pipeline.visualize())
```

---

### 7. Lineage Tracking

Track data provenance:

```python
from axiompy.data import LineageTrackerFactory

tracker = LineageTrackerFactory.create_auto(df, storage=db)

# Track transformation
record = tracker.track_transformation(
    job_name="user_enrichment",
    input_sources=["raw_users", "user_profiles"],
    output_targets=["enriched_users"],
    transformation="Join and aggregate user data",
    data_in=input_df,
    data_out=output_df,
    metadata={"version": "1.0", "env": "production"}
)

# Query lineage
records = tracker.get_lineage_records(job_name="user_enrichment", limit=10)
upstream = tracker.get_upstream_sources("enriched_users")
downstream = tracker.get_downstream_targets("raw_users")

# Decorator for automatic tracking
from axiompy.data.processing.lineage import track_lineage

@track_lineage("daily_etl", inputs=["raw"], outputs=["processed"])
def my_etl_job(data):
    return process(data)
```

---

### 8. Change Data Capture (CDC)

Detect data changes:

```python
from axiompy.data import ChangeDetectorFactory

detector = ChangeDetectorFactory.create_auto(
    df,
    key_columns=["user_id"]
)

# Detect all changes
changes = detector.detect_changes(old_df, new_df)

print(f"Inserts: {changes['summary']['inserts_count']}")
print(f"Updates: {changes['summary']['updates_count']}")
print(f"Deletes: {changes['summary']['deletes_count']}")

# Access change sets
inserts = changes["inserts"]  # New records
updates = changes["updates"]  # Modified records
deletes = changes["deletes"]  # Removed records
unchanged = changes["unchanged"]  # No changes

# Or get specific change types
inserts = detector.get_inserts(old_df, new_df)
updates = detector.get_updates(old_df, new_df, compare_columns=["name", "email"])
deletes = detector.get_deletes(old_df, new_df)
```

---

### 9. Format Conversion

Convert between data formats:

```python
from axiompy.data import FormatConverter, DataFormat

converter = FormatConverter()

# Convert between formats
converter.convert(
    "input.csv",
    from_format=DataFormat.CSV,
    to_format=DataFormat.PARQUET,
    output_path="output.parquet"
)

# Convenience methods
FormatConverter.csv_to_parquet("data.csv", "data.parquet")
FormatConverter.parquet_to_csv("data.parquet", "data.csv")
FormatConverter.json_to_parquet("data.json", "data.parquet")
FormatConverter.excel_to_csv("data.xlsx", "data.csv")

# Convert DataFrame directly
result = converter.convert(
    df,  # Pass DataFrame directly
    from_format=DataFormat.CSV,
    to_format=DataFormat.JSON,
    output_path="output.json"
)
```

**Supported Formats:**
- CSV
- JSON
- Parquet
- Excel (.xlsx)
- ORC (Spark only)
- Avro (future)

---

### 10. Data Compression

Compress/decompress data:

```python
from axiompy.data import DataCompressor, CompressionFormat

compressor = DataCompressor(default_format=CompressionFormat.GZIP)

# Compress data
data = b"Large data payload..."
compressed = compressor.compress(data, format=CompressionFormat.ZSTD, level=9)

# Decompress
decompressed = compressor.decompress(compressed)

# Compress files
compressor.compress_file("large_file.csv", "large_file.csv.gz")

# Decompress files
compressor.decompress_file("large_file.csv.gz", "large_file.csv")

# Compare compression formats
results = compressor.compare_formats(data)
for format_name, stats in results.items():
    print(f"{format_name}: {stats['compression_ratio']:.1f}% reduction")
```

**Supported Formats:**
- GZIP (standard, good balance)
- BZIP2 (better compression, slower)
- ZSTD (modern, very fast)
- LZ4 (fastest, lower compression)
- SNAPPY (fast, moderate compression)

---

### 11. Real-Time Streaming

Produce and consume messages from streaming platforms:

```python
from axiompy.data.streaming import (
    StreamProducerFactory, 
    StreamConsumerFactory,
    StreamHandler
)
from axiompy.data.streaming.types import StreamSettings, StreamEngine, StreamMessage
from typing import Optional
from dataclasses import dataclass

# Configure (works with Kafka, Kinesis, Redis, RabbitMQ)
settings = StreamSettings(
    engine=StreamEngine.KAFKA,
    bootstrap_servers=["localhost:9092"],
    topic="my-topic",
    group_id="my-group"
)

# Producer
with StreamProducerFactory.create(settings) as producer:
    # Send single message
    result = producer.send("Hello, Stream!", key="msg-1")
    
    # Send DataFrame
    import pandas as pd
    df = pd.DataFrame({'user_id': [1, 2, 3], 'action': ['login', 'purchase', 'logout']})
    results = producer.send_dataframe(df, key_column='user_id', format='json')

# Consumer - Option 1: Simple iterator
with StreamConsumerFactory.create(settings) as consumer:
    for message in consumer.consume(max_messages=10):
        print(message.value.decode('utf-8'))
        consumer.commit(message)
    
    # Consume to DataFrame
    df = consumer.consume_to_dataframe(max_messages=100, parse_json=True)

# Consumer - Option 2: Type-safe StreamHandler (recommended for production)
@dataclass
class UserEvent:
    user_id: int
    action: str
    amount: float = 0.0

class UserEventHandler(StreamHandler[UserEvent]):
    """Handler that combines deserialization + processing."""
    
    def deserialize(self, message: StreamMessage) -> Optional[UserEvent]:
        """Deserialize JSON to domain object."""
        import json
        try:
            data = json.loads(message.value.decode('utf-8'))
            return UserEvent(**data)
        except Exception:
            return None
    
    def handle(self, event: UserEvent) -> None:
        """Process deserialized event with type safety."""
        save_to_database(event)
        
        if event.amount > 100:
            send_alert(event)

# Use handler with consumer
handler = UserEventHandler()
with StreamConsumerFactory.create(settings) as consumer:
    stats = consumer.consume_with_handler(
        handler=handler.process_message,
        max_messages=1000
    )
    print(f"Processed {stats.messages_processed} messages")
```

**Supported Platforms:**
- Apache Kafka
- AWS Kinesis
- Redis Streams
- RabbitMQ

**StreamHandler Benefits:**
- Type-safe message processing with generic type support
- Separation of deserialization from business logic
- Easy to test deserializers and handlers independently
- Reusable across different consumers
- Swap serialization formats without changing processing code

See [`streaming/README.md`](streaming/README.md) for comprehensive streaming documentation.

---

## 🎭 Databricks/Spark Integration

All utilities work seamlessly in Databricks notebooks:

```python
# Databricks Notebook Cell 1: Setup
from axiompy.data import (
    DataProfilerFactory, 
    DataTransformerFactory,
    LineageTrackerFactory,
    DataEngine
)

# Databricks Notebook Cell 2: Load data
df = spark.table("raw_users")

# Databricks Notebook Cell 3: Profile quality
profiler = DataProfilerFactory.create(DataEngine.SPARK)
report = profiler.profile(df)
display(report.issues)  # Databricks display

# Databricks Notebook Cell 4: Transform
transformer = DataTransformerFactory.create(DataEngine.SPARK)
df = transformer.drop_nulls(df)
df = transformer.deduplicate(df, subset=["user_id"])

# Databricks Notebook Cell 5: Track lineage
tracker = LineageTrackerFactory.create(DataEngine.SPARK, storage=lineage_db)
tracker.track_transformation(
    job_name="user_cleanup",
    input_sources=["raw_users"],
    output_targets=["clean_users"],
    transformation="Remove nulls and duplicates",
    data_in=spark.table("raw_users"),
    data_out=df
)

# Databricks Notebook Cell 6: Save
df.write.mode("overwrite").saveAsTable("clean_users")
```

---

## 🔧 Best Practices

### 1. Use Auto-Detection for Portability

```python
# ✅ Good: Works everywhere
profiler = DataProfilerFactory.create_auto(df)

# ❌ Less portable: Hardcoded engine
profiler = DataProfilerFactory.create(DataEngine.PANDAS)
```

### 2. Track Lineage in Production

```python
tracker = LineageTrackerFactory.create_auto(df, storage=metadata_db)
tracker.track_transformation(
    job_name=f"{job_name}_{datetime.now().strftime('%Y%m%d')}",
    input_sources=inputs,
    output_targets=outputs,
    transformation=description,
    data_in=input_df,
    data_out=output_df,
    metadata={"env": "prod", "user": username}
)
```

### 3. Validate Data Quality Early

```python
# Validate at pipeline entry points
profiler = DataProfilerFactory.create_auto(df)
results = profiler.validate_expectations(df, critical_expectations)

if not results["success"]:
    raise ValueError(f"Data quality check failed: {results['details']}")
```

### 4. Use Batch Processing for Large Data

```python
# For very large Pandas DataFrames
processor = BatchProcessorFactory.create(
    DataEngine.PANDAS,
    batch_size=10000,
    max_workers=4,
    show_progress=True
)
```

### 5. Partition Data for Scalability

```python
# Time-based partitioning for time-series data
partitioner = DataPartitionerFactory.create_auto(
    df,
    partition_key="event_timestamp",
    strategy=PartitionStrategy.TIME_DAILY,
    base_path="s3://data-lake/events/"
)
```

---

## 🧪 Testing with axiompy.data

Mock implementations for unit testing:

```python
from axiompy.data import DataProfiler, DataEngine
from axiompy.data.types import DataQualityReport

class MockDataProfiler(DataProfiler):
    def __init__(self):
        super().__init__(DataEngine.PANDAS, {})
    
    def profile(self, data):
        return DataQualityReport(
            row_count=100,
            column_count=5,
            null_counts={},
            duplicate_count=0,
            schema={},
            statistics={},
            issues=[]
        )
    
    def validate_expectations(self, data, expectations):
        return {"success": True, "passed": len(expectations), "failed": 0}
    
    def check_schema(self, data, expected_schema):
        return {"valid": True, "issues": []}

# Use in tests
def test_my_pipeline():
    mock_profiler = MockDataProfiler()
    # Test your code...
```

---

## 📊 Performance Tips

### Pandas Performance
- Use vectorized operations
- Batch process large DataFrames
- Use appropriate dtypes (category for strings with low cardinality)
- Profile with `memory_usage(deep=True)`

### Spark Performance
- Avoid `collect()` on large DataFrames
- Use native Spark operations over UDFs
- Partition data appropriately
- Cache intermediate results: `df.cache()`
- Monitor with Spark UI

---

## 🤝 Contributing

Extend axiompy.data with custom implementations:

```python
from axiompy.data import DataProfiler, DataProfilerFactory, DataEngine

class CustomProfiler(DataProfiler):
    def profile(self, data):
        # Your custom implementation
        pass
    
    # Implement other abstract methods...

# Register
DataProfilerFactory.register_profiler(DataEngine.POLARS, CustomProfiler)
```

---

## 📖 Additional Resources

- [axiompy Documentation](../README.md)
- [Streaming Module](streaming/README.md)
- [Example Notebooks](../../examples/)

---

## 🐛 Troubleshooting

### Import Error: "No module named 'pyspark'"
```bash
pip install "axiompy[spark]"
```

### "Cannot auto-detect engine"
Pass data object or specify engine explicitly:
```python
profiler = DataProfilerFactory.create(DataEngine.PANDAS)
```

### Spark "collect() out of memory"
Use native Spark operations or batch processing:
```python
# Bad: Collects entire DataFrame
results = df.collect()

# Good: Use Spark operations
df.write.parquet("output")
```

---

**Made with ❤️ for data engineers working across local and distributed environments.**

---

**Last Updated:** 2025-12-03

