"""
Data partitioning utilities for organizing large datasets.

Provides utilities for time-based, hash-based, and range-based partitioning
to organize data efficiently in storage systems.
"""

import hashlib
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from axiompy.io import ObjectStorage
from axiompy.loggers import LoggerFactory

from axiompy.data.observability.ports import SignalSink
from axiompy.data.types import DataEngine, PartitionStrategy

logger = LoggerFactory.create_logger(__name__)


class DataPartitioner(ABC):
    """
    Abstract base class for data partitioning.

    Handles partitioning data by time, hash, or custom strategies.
    """

    def __init__(
        self,
        engine: DataEngine,
        partition_key: str,
        strategy: PartitionStrategy = PartitionStrategy.TIME_DAILY,
        base_path: str = "",
        settings: Optional[Dict] = None,
        signal_sink: Optional[SignalSink] = None,
    ):
        """
        Initialize the partitioner.

        Args:
            engine: Data processing engine
            partition_key: Column to use for partitioning
            strategy: Partitioning strategy
            base_path: Base path for partition storage
            settings: Optional configuration settings
            signal_sink: Optional observability sink for lifecycle signals
        """
        self.engine = engine
        self.partition_key = partition_key
        self.strategy = strategy
        self.base_path = base_path
        self.settings = settings or {}
        self._signal_sink = signal_sink

    @abstractmethod
    def write_partitioned(
        self, data: Any, storage: Optional[ObjectStorage] = None, format: str = "parquet"
    ) -> List[str]:  # pragma: no cover
        """
        Write data in partitioned format.

        Args:
            data: Data to partition and write
            storage: Optional object storage (otherwise uses local filesystem)
            format: Data format (parquet, csv, json)

        Returns:
            List of partition paths written
        """
        pass

    @abstractmethod
    def read_partitions(
        self,
        partitions: Optional[List[str]] = None,
        storage: Optional[ObjectStorage] = None,
        format: str = "parquet",
    ) -> Any:  # pragma: no cover
        """
        Read partitioned data.

        Args:
            partitions: Specific partitions to read (None = all)
            storage: Optional object storage
            format: Data format

        Returns:
            Combined data from partitions
        """
        pass

    @abstractmethod
    def list_partitions(
        self, storage: Optional[ObjectStorage] = None
    ) -> List[str]:  # pragma: no cover
        """
        List all available partitions.

        Args:
            storage: Optional object storage

        Returns:
            List of partition paths
        """
        pass

    def _get_partition_path(self, partition_value: Any) -> str:
        """Generate partition path based on strategy."""
        if self.strategy in [
            PartitionStrategy.TIME_DAILY,
            PartitionStrategy.TIME_MONTHLY,
            PartitionStrategy.TIME_YEARLY,
            PartitionStrategy.TIME_HOURLY,
        ]:
            return self._time_partition_path(partition_value)
        elif self.strategy == PartitionStrategy.HASH:
            return self._hash_partition_path(partition_value)
        elif self.strategy == PartitionStrategy.RANGE:
            return self._range_partition_path(partition_value)
        else:
            raise ValueError(f"Unknown strategy: {self.strategy}")

    def _time_partition_path(self, dt: datetime) -> str:
        """Generate time-based partition path."""
        if isinstance(dt, str):
            dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))

        parts = []
        parts.append(f"year={dt.year}")

        if self.strategy in [
            PartitionStrategy.TIME_MONTHLY,
            PartitionStrategy.TIME_DAILY,
            PartitionStrategy.TIME_HOURLY,
        ]:
            parts.append(f"month={dt.month:02d}")

        if self.strategy in [PartitionStrategy.TIME_DAILY, PartitionStrategy.TIME_HOURLY]:
            parts.append(f"day={dt.day:02d}")

        if self.strategy == PartitionStrategy.TIME_HOURLY:
            parts.append(f"hour={dt.hour:02d}")

        return str(Path(self.base_path) / Path(*parts))

    def _hash_partition_path(self, value: Any) -> str:
        """Generate hash-based partition path."""
        hash_value = hashlib.md5(str(value).encode(), usedforsecurity=False).hexdigest()
        bucket = int(hash_value[:2], 16) % self.settings.get("num_buckets", 16)
        return str(Path(self.base_path) / f"bucket={bucket:04d}")

    def _range_partition_path(self, value: Any) -> str:
        """Generate range-based partition path."""
        ranges = self.settings.get("ranges", [])
        for i, (min_val, max_val) in enumerate(ranges):
            if min_val <= value < max_val:
                return str(Path(self.base_path) / f"range={i:04d}")
        return str(Path(self.base_path) / "range=9999")  # Default bucket


from .adapters.partitioners import PandasDataPartitioner, SparkDataPartitioner  # noqa: E402


class DataPartitionerFactory:
    """Factory for creating DataPartitioner instances."""

    _partitioner_map = {
        DataEngine.PANDAS: PandasDataPartitioner,
        DataEngine.SPARK: SparkDataPartitioner,
    }

    @classmethod
    def create(
        cls,
        engine: DataEngine,
        partition_key: str,
        strategy: PartitionStrategy = PartitionStrategy.TIME_DAILY,
        base_path: str = "",
        settings: Optional[Dict] = None,
        signal_sink: Optional[SignalSink] = None,
    ) -> DataPartitioner:
        """Create a DataPartitioner for the specified engine."""
        if engine not in cls._partitioner_map:
            raise ValueError(f"Unsupported engine: {engine}")

        partitioner_class = cls._partitioner_map[engine]
        logger.info(f"Creating {engine.value} data partitioner")
        return partitioner_class(partition_key, strategy, base_path, settings, signal_sink)

    @classmethod
    def create_auto(
        cls,
        data: Any,
        partition_key: str,
        strategy: PartitionStrategy = PartitionStrategy.TIME_DAILY,
        base_path: str = "",
        settings: Optional[Dict] = None,
        signal_sink: Optional[SignalSink] = None,
    ) -> DataPartitioner:
        """Auto-detect engine and create partitioner."""
        engine = cls._detect_engine(data)
        return cls.create(engine, partition_key, strategy, base_path, settings, signal_sink)

    @classmethod
    def _detect_engine(cls, data: Any) -> DataEngine:
        """Detect engine from data type."""
        module_name = type(data).__module__

        if "pandas" in module_name:
            return DataEngine.PANDAS
        elif "pyspark" in module_name:
            return DataEngine.SPARK
        else:
            raise ValueError(f"Cannot auto-detect engine for module: {module_name}")
