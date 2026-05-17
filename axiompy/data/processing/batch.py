"""
Batch processing utilities for handling large datasets.

Provides utilities to process large datasets in manageable chunks with
support for parallel processing and progress tracking.
"""

from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional

from axiompy.loggers import LoggerFactory

from axiompy.data.observability.ports import SignalKind, SignalSink
from axiompy.data.processing.signals import emit_signal
from axiompy.data.types import DataEngine

logger = LoggerFactory.create_logger(__name__)

# Optional tqdm support
try:
    from tqdm import tqdm

    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


class BatchProcessor(ABC):
    """
    Abstract base class for batch processing across different engines.
    """

    def __init__(
        self,
        engine: DataEngine,
        batch_size: int = 1000,
        max_workers: Optional[int] = None,
        show_progress: bool = False,
        settings: Optional[Dict] = None,
        signal_sink: Optional[SignalSink] = None,
    ):
        """
        Initialize the batch processor.

        Args:
            engine: Data processing engine
            batch_size: Number of records per batch
            max_workers: Number of parallel workers (None = sequential)
            show_progress: Whether to show progress bar
            settings: Optional configuration settings
            signal_sink: Optional observability sink
        """
        self.engine = engine
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.show_progress = show_progress and HAS_TQDM
        self.settings = settings or {}
        self._signal_sink = signal_sink

    @abstractmethod
    def iter_batches(
        self, data: Any, batch_size: Optional[int] = None
    ) -> Generator[Any, None, None]:  # pragma: no cover
        """
        Iterate over data in batches.

        Args:
            data: Data source to batch
            batch_size: Override default batch size

        Yields:
            Batches of data
        """
        pass

    def process_batches(
        self,
        data: Any,
        transform_func: Callable,
        sink: Optional[Callable] = None,
        batch_size: Optional[int] = None,
        fail_fast: bool = False,
    ) -> Dict[str, Any]:  # pragma: no cover
        """
        Process data in batches with optional parallel execution.

        Args:
            data: Data source
            transform_func: Function to apply to each batch
            sink: Optional function to write results (receives transformed batch)
            batch_size: Override default batch size
            fail_fast: If True, stop on first error

        Returns:
            Dictionary with processing results and statistics
        """
        batch_size = batch_size or self.batch_size
        results = {"batches_processed": 0, "batches_failed": 0, "total_records": 0, "errors": []}

        batches = list(self.iter_batches(data, batch_size))
        total_batches = len(batches)

        logger.info(f"Processing {total_batches} batches")

        iterator = tqdm(batches, desc="Processing batches") if self.show_progress else batches

        if self.max_workers and self.max_workers > 1:
            # Parallel processing
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(transform_func, batch): idx for idx, batch in enumerate(batches)
                }

                for future in as_completed(futures):
                    idx = futures[future]
                    try:
                        transformed = future.result()
                        results["batches_processed"] += 1

                        if sink:
                            sink(transformed)
                    except Exception as e:
                        results["batches_failed"] += 1
                        results["errors"].append({"batch": idx, "error": str(e)})
                        logger.error(f"Batch {idx} failed: {e}")
                        if fail_fast:
                            raise
        else:
            # Sequential processing
            for idx, batch in enumerate(iterator):
                try:
                    transformed = transform_func(batch)
                    results["batches_processed"] += 1

                    if sink:
                        sink(transformed)
                except Exception as e:
                    results["batches_failed"] += 1
                    results["errors"].append({"batch": idx, "error": str(e)})
                    logger.error(f"Batch {idx} failed: {e}")
                    if fail_fast:
                        raise

        logger.info(
            f"Batch processing complete: {results['batches_processed']} succeeded, "
            f"{results['batches_failed']} failed"
        )
        emit_signal(
            self._signal_sink,
            SignalKind.LIFECYCLE,
            "batch.process_batches",
            {
                "engine": self.engine.value,
                "batches_processed": results["batches_processed"],
                "batches_failed": results["batches_failed"],
            },
        )
        return results


from .adapters.batch_processors import (  # noqa: E402
    ListBatchProcessor,
    PandasBatchProcessor,
    SparkBatchProcessor,
)


class BatchProcessorFactory:
    """
    Factory for creating BatchProcessor instances.

    Usage:
        >>> processor = BatchProcessorFactory.create(DataEngine.PANDAS, batch_size=500)
        >>> results = processor.process_batches(df, transform_func=my_transform)
    """

    _processor_map = {
        DataEngine.PANDAS: PandasBatchProcessor,
        DataEngine.SPARK: SparkBatchProcessor,
    }

    @classmethod
    def create(
        cls,
        engine: DataEngine,
        batch_size: int = 1000,
        max_workers: Optional[int] = None,
        show_progress: bool = False,
        settings: Optional[Dict] = None,
        signal_sink: Optional[SignalSink] = None,
    ) -> BatchProcessor:
        """Create a BatchProcessor for the specified engine."""
        if engine not in cls._processor_map:
            raise ValueError(
                f"Unsupported engine: {engine}. Supported: {list(cls._processor_map.keys())}"
            )

        processor_class = cls._processor_map[engine]
        logger.info(f"Creating {engine.value} batch processor")
        return processor_class(batch_size, max_workers, show_progress, settings, signal_sink)

    @classmethod
    def create_auto(
        cls,
        data: Any,
        batch_size: int = 1000,
        max_workers: Optional[int] = None,
        show_progress: bool = False,
        settings: Optional[Dict] = None,
        signal_sink: Optional[SignalSink] = None,
    ) -> BatchProcessor:
        """Auto-detect engine and create processor."""
        # Check if it's a list or iterator first
        if isinstance(data, list | tuple | Generator):
            logger.info("Creating list batch processor")
            return ListBatchProcessor(batch_size, max_workers, show_progress, settings, signal_sink)

        engine = cls._detect_engine(data)
        return cls.create(engine, batch_size, max_workers, show_progress, settings, signal_sink)

    @classmethod
    def _detect_engine(cls, data: Any) -> DataEngine:
        """Detect engine from data type."""
        module_name = type(data).__module__

        if "pandas" in module_name:
            return DataEngine.PANDAS
        elif "pyspark" in module_name:
            return DataEngine.SPARK
        elif "polars" in module_name:
            return DataEngine.POLARS
        else:
            raise ValueError(f"Cannot auto-detect engine for module: {module_name}")

    @classmethod
    def register_processor(cls, engine: DataEngine, processor_class: type) -> None:
        """Register a custom processor implementation."""
        if not issubclass(processor_class, BatchProcessor):
            raise TypeError("processor_class must inherit from BatchProcessor")

        cls._processor_map[engine] = processor_class
        logger.info(f"Registered custom batch processor for engine: {engine.value}")
