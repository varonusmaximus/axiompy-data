"""Pandas, Spark, and list batch processor implementations."""

from __future__ import annotations

from typing import Any, Dict, Generator, Iterable, List, Optional

from axiompy.data.observability.ports import SignalSink
from axiompy.data.processing.batch import BatchProcessor, logger
from axiompy.data.types import DataEngine


class PandasBatchProcessor(BatchProcessor):
    """Batch processor for Pandas DataFrames."""

    def __init__(
        self,
        batch_size: int = 1000,
        max_workers: Optional[int] = None,
        show_progress: bool = False,
        settings: Optional[Dict] = None,
        signal_sink: Optional[SignalSink] = None,
    ):
        super().__init__(
            DataEngine.PANDAS,
            batch_size,
            max_workers,
            show_progress,
            settings,
            signal_sink=signal_sink,
        )
        try:
            import pandas as pd

            self.pd = pd
        except ImportError as err:
            raise ImportError("Pandas is required. Install with: pip install pandas") from err

    def iter_batches(
        self, data: Any, batch_size: Optional[int] = None
    ) -> Generator[Any, None, None]:
        """Iterate over Pandas DataFrame in batches."""
        batch_size = batch_size or self.batch_size
        total_rows = len(data)

        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            yield data.iloc[start:end]


class SparkBatchProcessor(BatchProcessor):
    """Batch processor for Spark DataFrames."""

    def __init__(
        self,
        batch_size: int = 10000,
        max_workers: Optional[int] = None,
        show_progress: bool = False,
        settings: Optional[Dict] = None,
        signal_sink: Optional[SignalSink] = None,
    ):
        super().__init__(
            DataEngine.SPARK,
            batch_size,
            max_workers,
            show_progress,
            settings,
            signal_sink=signal_sink,
        )
        try:
            from pyspark.sql import DataFrame

            self.DataFrame = DataFrame
        except ImportError as err:
            raise ImportError("PySpark is required. Install with: pip install pyspark") from err

    def iter_batches(
        self, data: Any, batch_size: Optional[int] = None
    ) -> Generator[Any, None, None]:
        """
        Iterate over Spark DataFrame in batches.

        Note: Spark is designed for distributed processing. This batching
        is mainly useful for collecting results or interfacing with non-Spark systems.
        """
        batch_size = batch_size or self.batch_size
        total_rows = data.count()

        logger.warning("Batching Spark DataFrames is less efficient than native Spark operations")

        for offset in range(0, total_rows, batch_size):
            yield data.limit(batch_size).offset(offset)


class ListBatchProcessor(BatchProcessor):
    """Batch processor for Python lists/iterables."""

    def __init__(
        self,
        batch_size: int = 1000,
        max_workers: Optional[int] = None,
        show_progress: bool = False,
        settings: Optional[Dict] = None,
        signal_sink: Optional[SignalSink] = None,
    ):
        super().__init__(
            DataEngine.PANDAS,
            batch_size,
            max_workers,
            show_progress,
            settings,
            signal_sink=signal_sink,
        )

    def iter_batches(
        self, data: Iterable, batch_size: Optional[int] = None
    ) -> Generator[List, None, None]:
        """Iterate over list/iterable in batches."""
        batch_size = batch_size or self.batch_size

        if isinstance(data, list):
            for i in range(0, len(data), batch_size):
                yield data[i : i + batch_size]
        else:
            batch = []
            for item in data:
                batch.append(item)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch
