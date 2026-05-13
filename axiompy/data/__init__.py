"""
Data engineering utilities for axiompy.

Provides engine-agnostic data processing utilities with support for multiple
backends (Pandas, Spark/PySpark, Polars) through abstract interfaces and factory patterns.

Key Features:
    - Arrow Database: Arrow-native database abstraction for analytics/ETL
    - Data Quality & Validation: Profile and validate data quality
    - Data Transformation: Common ETL transformation patterns
    - DataFrame Adapters: Unified API across engines
    - Batch Processing: Process large datasets in chunks
    - Partitioning: Manage time/hash-based data partitions
    - ETL Pipelines: Build and orchestrate data pipelines
    - Lineage Tracking: Track data provenance and transformations
    - Change Data Capture: Detect and process data changes
    - Format Conversion: Convert between data formats
    - Compression: Compress/decompress data efficiently
    - Real-Time Streaming: Unified interface for Kafka, Kinesis, Redis, RabbitMQ

Usage:
    >>> from axiompy.data import DataProfilerFactory, DataEngine
    >>>
    >>> # Auto-detect engine from data
    >>> profiler = DataProfilerFactory.create_auto(df)
    >>> report = profiler.profile(df)
    >>>
    >>> # Or explicitly specify engine
    >>> profiler = DataProfilerFactory.create(DataEngine.SPARK)
    >>> report = profiler.profile(spark_df)

Arrow Database Usage:
    >>> from axiompy.data import ArrowDatabaseFactory, DuckDBArrowSettings
    >>>
    >>> settings = DuckDBArrowSettings()
    >>> db = ArrowDatabaseFactory.create(settings)
    >>> table = db.execute_arrow("SELECT * FROM 'data.parquet'")
    >>> print(f"Fetched {table.num_rows} rows")
"""

# Arrow Database
from axiompy.data.arrow import (
    ArrowConnectionError,
    ArrowDatabase,
    ArrowDatabaseError,
    ArrowDatabaseFactory,
    ArrowQueryError,
    DuckDBArrowSettings,
    MockArrowDatabase,
    PostgresArrowSettings,
    SnowflakeArrowSettings,
)

# Batch
from axiompy.data.batch import (
    BatchProcessor,
    BatchProcessorFactory,
)

# CDC
from axiompy.data.cdc import (
    ChangeDetector,
    ChangeDetectorFactory,
    ChangeType,
)

# Compression
from axiompy.data.compression import (
    CompressionFormat,
    DataCompressor,
)

# DataFrame
from axiompy.data.dataframe import (
    DataFrameAdapter,
    DataFrameAdapterFactory,
)

# Export
from axiompy.data.export import (
    DataFormat,
    FormatConverter,
)

# Lineage
from axiompy.data.lineage import (
    LineageRecord,
    LineageTracker,
    LineageTrackerFactory,
)

# Partition
from axiompy.data.partition import (
    DataPartitioner,
    DataPartitionerFactory,
    PartitionStrategy,
)

# Pipeline
from axiompy.data.pipeline import (
    Pipeline,
    Task,
    TaskStatus,
)

# Quality
from axiompy.data.quality import (
    DataExpectation,
    DataProfiler,
    DataProfilerFactory,
    DataQualityReport,
)

# Transform
from axiompy.data.transform import (
    DataTransformer,
    DataTransformerFactory,
)
from axiompy.data.types import DataEngine

__all__ = [
    # Types
    "DataEngine",
    # Arrow Database
    "ArrowDatabase",
    "ArrowDatabaseFactory",
    "ArrowDatabaseError",
    "ArrowConnectionError",
    "ArrowQueryError",
    "DuckDBArrowSettings",
    "SnowflakeArrowSettings",
    "PostgresArrowSettings",
    "MockArrowDatabase",
    # Quality
    "DataProfiler",
    "DataProfilerFactory",
    "DataQualityReport",
    "DataExpectation",
    # Transform
    "DataTransformer",
    "DataTransformerFactory",
    # DataFrame
    "DataFrameAdapter",
    "DataFrameAdapterFactory",
    # Batch
    "BatchProcessor",
    "BatchProcessorFactory",
    # Partition
    "DataPartitioner",
    "DataPartitionerFactory",
    "PartitionStrategy",
    # Pipeline
    "Pipeline",
    "Task",
    "TaskStatus",
    # Lineage
    "LineageTracker",
    "LineageTrackerFactory",
    "LineageRecord",
    # CDC
    "ChangeDetector",
    "ChangeDetectorFactory",
    "ChangeType",
    # Export
    "FormatConverter",
    "DataFormat",
    # Compression
    "DataCompressor",
    "CompressionFormat",
]

__version__ = "0.1.0"
