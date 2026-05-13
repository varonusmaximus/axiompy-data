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

from axiompy.data.types import DataEngine, PartitionStrategy
from axiompy.io import ObjectStorage
from axiompy.loggers import LoggerFactory

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
    ):
        """
        Initialize the partitioner.

        Args:
            engine: Data processing engine
            partition_key: Column to use for partitioning
            strategy: Partitioning strategy
            base_path: Base path for partition storage
            settings: Optional configuration settings
        """
        self.engine = engine
        self.partition_key = partition_key
        self.strategy = strategy
        self.base_path = base_path
        self.settings = settings or {}

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
        hash_value = hashlib.md5(str(value).encode()).hexdigest()
        bucket = int(hash_value[:2], 16) % self.settings.get("num_buckets", 16)
        return str(Path(self.base_path) / f"bucket={bucket:04d}")

    def _range_partition_path(self, value: Any) -> str:
        """Generate range-based partition path."""
        ranges = self.settings.get("ranges", [])
        for i, (min_val, max_val) in enumerate(ranges):
            if min_val <= value < max_val:
                return str(Path(self.base_path) / f"range={i:04d}")
        return str(Path(self.base_path) / "range=9999")  # Default bucket


class PandasDataPartitioner(DataPartitioner):
    """Data partitioner for Pandas DataFrames."""

    def __init__(
        self,
        partition_key: str,
        strategy: PartitionStrategy = PartitionStrategy.TIME_DAILY,
        base_path: str = "",
        settings: Optional[Dict] = None,
    ):
        super().__init__(DataEngine.PANDAS, partition_key, strategy, base_path, settings)
        try:
            import pandas as pd

            self.pd = pd
        except ImportError:
            raise ImportError("Pandas is required. Install with: pip install pandas")

    def write_partitioned(
        self, data: "pd.DataFrame", storage: Optional[ObjectStorage] = None, format: str = "parquet"
    ) -> List[str]:
        """Write Pandas DataFrame in partitioned format."""
        logger.info(f"Writing partitioned data using {self.strategy.value} strategy")

        written_paths = []

        # Group by partition
        grouped = data.groupby(
            data[self.partition_key].apply(lambda x: self._get_partition_path(x))
        )

        for partition_path, partition_data in grouped:
            file_path = str(Path(partition_path) / f"data.{format}")

            if storage:
                # Write to object storage
                if format == "parquet":
                    buffer = partition_data.to_parquet()
                elif format == "csv":
                    buffer = partition_data.to_csv(index=False).encode()
                elif format == "json":
                    buffer = partition_data.to_json().encode()
                else:
                    raise ValueError(f"Unsupported format: {format}")

                storage.put_object(file_path, buffer)
            else:
                # Write to local filesystem
                Path(partition_path).mkdir(parents=True, exist_ok=True)
                if format == "parquet":
                    partition_data.to_parquet(file_path)
                elif format == "csv":
                    partition_data.to_csv(file_path, index=False)
                elif format == "json":
                    partition_data.to_json(file_path)
                else:
                    raise ValueError(f"Unsupported format: {format}")

            written_paths.append(file_path)
            logger.debug(f"Wrote partition: {file_path}")

        logger.info(f"Wrote {len(written_paths)} partitions")
        return written_paths

    def read_partitions(
        self,
        partitions: Optional[List[str]] = None,
        storage: Optional[ObjectStorage] = None,
        format: str = "parquet",
    ) -> "pd.DataFrame":
        """Read partitioned Pandas data."""
        logger.info("Reading partitioned data")

        if partitions is None:
            partitions = self.list_partitions(storage)

        dfs = []
        for partition_path in partitions:
            file_path = str(Path(partition_path) / f"data.{format}")

            try:
                if storage:
                    content = storage.get_object(file_path)
                    if format == "parquet":
                        df = self.pd.read_parquet(content)
                    elif format == "csv":
                        df = self.pd.read_csv(content)
                    elif format == "json":
                        df = self.pd.read_json(content)
                    else:
                        raise ValueError(f"Unsupported format: {format}")
                else:
                    if format == "parquet":
                        df = self.pd.read_parquet(file_path)
                    elif format == "csv":
                        df = self.pd.read_csv(file_path)
                    elif format == "json":
                        df = self.pd.read_json(file_path)
                    else:
                        raise ValueError(f"Unsupported format: {format}")

                dfs.append(df)
            except Exception as e:
                logger.warning(f"Failed to read partition {file_path}: {e}")

        if not dfs:
            return self.pd.DataFrame()

        return self.pd.concat(dfs, ignore_index=True)

    def list_partitions(self, storage: Optional[ObjectStorage] = None) -> List[str]:
        """List all partitions."""
        if storage:
            objects = storage.list_objects(prefix=self.base_path)
            # Extract partition directories
            partitions = set()
            for obj in objects:
                # Get parent directory
                parent = str(Path(obj).parent)
                if parent != self.base_path:
                    partitions.add(parent)
            return sorted(partitions)
        else:
            # Local filesystem
            base = Path(self.base_path)
            if not base.exists():
                return []

            partitions = []
            for path in base.rglob("data.*"):
                partitions.append(str(path.parent))
            return sorted(set(partitions))


class SparkDataPartitioner(DataPartitioner):
    """Data partitioner for Spark DataFrames (uses native Spark partitioning)."""

    def __init__(
        self,
        partition_key: str,
        strategy: PartitionStrategy = PartitionStrategy.TIME_DAILY,
        base_path: str = "",
        settings: Optional[Dict] = None,
    ):
        super().__init__(DataEngine.SPARK, partition_key, strategy, base_path, settings)
        try:
            from pyspark.sql import DataFrame, SparkSession
            from pyspark.sql import functions as F

            self.DataFrame = DataFrame
            self.SparkSession = SparkSession
            self.F = F
        except ImportError:
            raise ImportError("PySpark is required. Install with: pip install pyspark")

    def write_partitioned(
        self, data: "DataFrame", storage: Optional[ObjectStorage] = None, format: str = "parquet"
    ) -> List[str]:
        """Write Spark DataFrame using native partitioning."""
        logger.info("Writing Spark partitioned data")

        # Add partition columns based on strategy
        if self.strategy in [
            PartitionStrategy.TIME_DAILY,
            PartitionStrategy.TIME_MONTHLY,
            PartitionStrategy.TIME_YEARLY,
        ]:
            data = data.withColumn("year", self.F.year(self.partition_key))
            partition_cols = ["year"]

            if self.strategy in [PartitionStrategy.TIME_MONTHLY, PartitionStrategy.TIME_DAILY]:
                data = data.withColumn("month", self.F.month(self.partition_key))
                partition_cols.append("month")

            if self.strategy == PartitionStrategy.TIME_DAILY:
                data = data.withColumn("day", self.F.dayofmonth(self.partition_key))
                partition_cols.append("day")
        else:
            partition_cols = [self.partition_key]

        # Write partitioned data
        writer = data.write.mode("overwrite")

        if format == "parquet":
            writer.partitionBy(*partition_cols).parquet(self.base_path)
        elif format == "csv":
            writer.partitionBy(*partition_cols).csv(self.base_path)
        elif format == "json":
            writer.partitionBy(*partition_cols).json(self.base_path)
        elif format == "orc":
            writer.partitionBy(*partition_cols).orc(self.base_path)
        else:
            raise ValueError(f"Unsupported format: {format}")

        logger.info(f"Wrote partitioned data to {self.base_path}")
        return [self.base_path]

    def read_partitions(
        self,
        partitions: Optional[List[str]] = None,
        storage: Optional[ObjectStorage] = None,
        format: str = "parquet",
    ) -> "DataFrame":
        """Read Spark partitioned data."""
        logger.info("Reading Spark partitioned data")

        spark = self.SparkSession.getActiveSession() or self.SparkSession.builder.getOrCreate()

        if format == "parquet":
            df = spark.read.parquet(self.base_path)
        elif format == "csv":
            df = spark.read.csv(self.base_path, header=True)
        elif format == "json":
            df = spark.read.json(self.base_path)
        elif format == "orc":
            df = spark.read.orc(self.base_path)
        else:
            raise ValueError(f"Unsupported format: {format}")

        # Apply partition filter if specified
        if partitions:
            # This is simplified - would need more sophisticated filtering
            logger.warning("Partition filtering not fully implemented for Spark")

        return df

    def list_partitions(self, storage: Optional[ObjectStorage] = None) -> List[str]:
        """List Spark partitions."""
        # For Spark, we can use SHOW PARTITIONS
        logger.info("Listing Spark partitions")

        spark = self.SparkSession.getActiveSession() or self.SparkSession.builder.getOrCreate()

        try:
            # Try to read partition info
            df = spark.read.parquet(self.base_path)
            partitions = df.select(df.columns[0]).distinct().collect()
            return [str(p[0]) for p in partitions]
        except Exception as e:
            logger.warning(f"Could not list partitions: {e}")
            return []


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
    ) -> DataPartitioner:
        """Create a DataPartitioner for the specified engine."""
        if engine not in cls._partitioner_map:
            raise ValueError(f"Unsupported engine: {engine}")

        partitioner_class = cls._partitioner_map[engine]
        logger.info(f"Creating {engine.value} data partitioner")
        return partitioner_class(partition_key, strategy, base_path, settings)

    @classmethod
    def create_auto(
        cls,
        data: Any,
        partition_key: str,
        strategy: PartitionStrategy = PartitionStrategy.TIME_DAILY,
        base_path: str = "",
        settings: Optional[Dict] = None,
    ) -> DataPartitioner:
        """Auto-detect engine and create partitioner."""
        engine = cls._detect_engine(data)
        return cls.create(engine, partition_key, strategy, base_path, settings)

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
