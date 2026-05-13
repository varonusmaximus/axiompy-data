"""
Common types, enums, and dataclasses for the data engineering module.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class DataEngine(Enum):
    """Supported data processing engines."""

    PANDAS = "pandas"
    SPARK = "spark"
    POLARS = "polars"


class PartitionStrategy(Enum):
    """Data partitioning strategies."""

    TIME_DAILY = "daily"
    TIME_MONTHLY = "monthly"
    TIME_YEARLY = "yearly"
    TIME_HOURLY = "hourly"
    HASH = "hash"
    RANGE = "range"


class TaskStatus(Enum):
    """Pipeline task execution status."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class ChangeType(Enum):
    """Types of changes in CDC."""

    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


class DataFormat(Enum):
    """Supported data formats for conversion."""

    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    AVRO = "avro"
    EXCEL = "excel"
    ORC = "orc"


class CompressionFormat(Enum):
    """Supported compression formats."""

    GZIP = "gzip"
    BZIP2 = "bzip2"
    ZSTD = "zstd"
    LZ4 = "lz4"
    SNAPPY = "snappy"
    NONE = "none"


@dataclass
class DataQualityReport:
    """Standard data quality report structure."""

    row_count: int
    column_count: int
    null_counts: Dict[str, int]
    duplicate_count: int
    schema: Dict[str, Any]
    statistics: Dict[str, Dict[str, Any]]
    issues: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataExpectation:
    """Data quality expectation/assertion."""

    name: str
    column: Optional[str] = None
    condition: str = ""
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LineageRecord:
    """Record of data lineage/provenance."""

    job_name: str
    input_sources: List[str]
    output_targets: List[str]
    transformation: str
    timestamp: datetime
    row_count_in: Optional[int] = None
    row_count_out: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ChangeRecord:
    """Record of a data change (CDC)."""

    change_type: ChangeType
    key: Any
    old_value: Optional[Dict[str, Any]] = None
    new_value: Optional[Dict[str, Any]] = None
    timestamp: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
