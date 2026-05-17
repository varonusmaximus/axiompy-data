"""
Data engineering utilities for axiompy (axiompy-data distribution).

Provides engine-agnostic data processing utilities with support for multiple
backends (Pandas, Spark/PySpark, Polars) through abstract interfaces and factory patterns.

Layout: **``consuming``**, **``streaming``**, **``processing``** (quality/lineage/CDC live under **processing**),
**``observability``** (signal sinks), **interchange** modules at this level (**``dataframe``**, **``export``**,
**``compression``**), plus **``types``**, **``error``**, **``result``**.

Key Features:
    - Consuming (analytical DB clients): Arrow-protocol database clients for analytics/ETL
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

Analytical consuming (bulk columnar clients):
    >>> from axiompy.data import ArrowDatabaseFactory, DuckDBArrowSettings
    >>>
    >>> settings = DuckDBArrowSettings()
    >>> db = ArrowDatabaseFactory.create(settings)
    >>> result = db.query("SELECT * FROM 'data.parquet'")
    >>> print(f"Fetched {result.row_count} rows")
"""

from __future__ import annotations

from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

from axiompy.data.compression import DataCompressor
from axiompy.data.consuming import (
    AbstractConsumingClientFactory,
    ArrowConnectionError,
    ArrowDatabase,
    ArrowDatabaseError,
    ArrowDatabaseFactory,
    ArrowQueryError,
    Client,
    DatabricksArrowSettings,
    DuckDBArrowSettings,
    Factory,
    MockArrowDatabase,
    MockClient,
    Platform,
    PostgresArrowSettings,
    QueryResult,
    Settings,
    SnowflakeArrowSettings,
    convert,
)
from axiompy.data.dataframe import DataFrameAdapter, DataFrameAdapterFactory
from axiompy.data.export import DataFormat, FormatConverter
from axiompy.data.processing import (
    BatchProcessor,
    BatchProcessorFactory,
    ChangeDetector,
    ChangeDetectorFactory,
    ChangeType,
    DataExpectation,
    DataPartitioner,
    DataPartitionerFactory,
    DataProfiler,
    DataProfilerFactory,
    DataQualityReport,
    DataTransformer,
    DataTransformerFactory,
    LineageRecord,
    LineageTracker,
    LineageTrackerFactory,
    PartitionStrategy,
    Pipeline,
    Task,
    TaskStatus,
)
from axiompy.data.types import CompressionFormat, DataEngine

__all__ = [
    # Types
    "DataEngine",
    # Analytical consuming clients
    "Client",
    "Factory",
    "Platform",
    "QueryResult",
    "Settings",
    "convert",
    "AbstractConsumingClientFactory",
    "ArrowDatabase",
    "ArrowDatabaseFactory",
    "ArrowDatabaseError",
    "ArrowConnectionError",
    "ArrowQueryError",
    "DuckDBArrowSettings",
    "SnowflakeArrowSettings",
    "PostgresArrowSettings",
    "DatabricksArrowSettings",
    "MockArrowDatabase",
    "MockClient",
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

__version__ = "3.0.0"
