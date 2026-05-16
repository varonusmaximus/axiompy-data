"""Pandas and Spark data partitioners."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from axiompy.io import ObjectStorage

from axiompy.data.observability.ports import SignalKind, SignalSink
from axiompy.data.processing.partition import DataPartitioner, logger
from axiompy.data.processing.signals import emit_signal
from axiompy.data.types import DataEngine, PartitionStrategy


class PandasDataPartitioner(DataPartitioner):
    """Data partitioner for Pandas DataFrames."""

    def __init__(
        self,
        partition_key: str,
        strategy: PartitionStrategy = PartitionStrategy.TIME_DAILY,
        base_path: str = "",
        settings: Optional[Dict] = None,
        signal_sink: Optional[SignalSink] = None,
    ):
        super().__init__(
            DataEngine.PANDAS, partition_key, strategy, base_path, settings, signal_sink=signal_sink
        )
        try:
            import pandas as pd

            self.pd = pd
        except ImportError:
            raise ImportError("Pandas is required. Install with: pip install pandas")

    def write_partitioned(
        self, data: pd.DataFrame, storage: Optional[ObjectStorage] = None, format: str = "parquet"
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
        emit_signal(
            self._signal_sink,
            SignalKind.LIFECYCLE,
            "partition.write_partitioned",
            {"engine": "pandas", "partition_count": len(written_paths)},
        )
        return written_paths

    def read_partitions(
        self,
        partitions: Optional[List[str]] = None,
        storage: Optional[ObjectStorage] = None,
        format: str = "parquet",
    ) -> pd.DataFrame:
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
        signal_sink: Optional[SignalSink] = None,
    ):
        super().__init__(
            DataEngine.SPARK, partition_key, strategy, base_path, settings, signal_sink=signal_sink
        )
        try:
            from pyspark.sql import DataFrame, SparkSession
            from pyspark.sql import functions as F

            self.DataFrame = DataFrame
            self.SparkSession = SparkSession
            self.F = F
        except ImportError:
            raise ImportError("PySpark is required. Install with: pip install pyspark")

    def write_partitioned(
        self, data: DataFrame, storage: Optional[ObjectStorage] = None, format: str = "parquet"
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
        emit_signal(
            self._signal_sink,
            SignalKind.LIFECYCLE,
            "partition.write_partitioned",
            {"engine": "spark", "base_path": self.base_path},
        )
        return [self.base_path]

    def read_partitions(
        self,
        partitions: Optional[List[str]] = None,
        storage: Optional[ObjectStorage] = None,
        format: str = "parquet",
    ) -> DataFrame:
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
