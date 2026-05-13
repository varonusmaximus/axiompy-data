"""
Data lineage tracking for monitoring data provenance and transformations.

Provides utilities to track where data comes from, how it's transformed,
and where it goes in your data pipelines.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional

from axiompy.data.types import DataEngine, LineageRecord
from axiompy.io import Database
from axiompy.loggers import LoggerFactory

logger = LoggerFactory.create_logger(__name__)


class LineageTracker(ABC):
    """
    Abstract base class for tracking data lineage.

    Tracks data transformations, sources, and destinations for
    audit and debugging purposes.
    """

    def __init__(
        self,
        engine: DataEngine,
        storage: Optional[Database] = None,
        settings: Optional[Dict] = None,
    ):
        """
        Initialize the lineage tracker.

        Args:
            engine: Data processing engine
            storage: Optional database for storing lineage records
            settings: Optional configuration settings
        """
        self.engine = engine
        self.storage = storage
        self.settings = settings or {}
        self._lineage_records: List[LineageRecord] = []

    @abstractmethod
    def track_transformation(
        self,
        job_name: str,
        input_sources: List[str],
        output_targets: List[str],
        transformation: str,
        data_in: Optional[Any] = None,
        data_out: Optional[Any] = None,
        metadata: Optional[Dict] = None,
    ) -> LineageRecord:  # pragma: no cover
        """
        Track a data transformation.

        Args:
            job_name: Name of the job/transformation
            input_sources: List of input data sources
            output_targets: List of output data targets
            transformation: Description of transformation
            data_in: Optional input data (for row count tracking)
            data_out: Optional output data (for row count tracking)
            metadata: Additional metadata to track

        Returns:
            LineageRecord for this transformation
        """
        pass

    def get_lineage_records(
        self, job_name: Optional[str] = None, limit: int = 100
    ) -> List[LineageRecord]:  # pragma: no cover
        """
        Retrieve lineage records.

        Args:
            job_name: Optional filter by job name
            limit: Maximum number of records to return

        Returns:
            List of lineage records
        """
        if job_name:
            records = [r for r in self._lineage_records if r.job_name == job_name]
        else:
            records = self._lineage_records

        return records[-limit:]

    def get_upstream_sources(self, target: str) -> List[str]:
        """
        Get all upstream sources for a target.

        Args:
            target: Target data source

        Returns:
            List of upstream source names
        """
        sources = set()
        for record in self._lineage_records:
            if target in record.output_targets:
                sources.update(record.input_sources)
        return sorted(sources)

    def get_downstream_targets(self, source: str) -> List[str]:
        """
        Get all downstream targets for a source.

        Args:
            source: Source data

        Returns:
            List of downstream target names
        """
        targets = set()
        for record in self._lineage_records:
            if source in record.input_sources:
                targets.update(record.output_targets)
        return sorted(targets)


class PandasLineageTracker(LineageTracker):
    """Lineage tracker for Pandas transformations."""

    def __init__(self, storage: Optional[Database] = None, settings: Optional[Dict] = None):
        super().__init__(DataEngine.PANDAS, storage, settings)
        try:
            import pandas as pd

            self.pd = pd
        except ImportError:
            raise ImportError("Pandas is required. Install with: pip install pandas")

    def track_transformation(
        self,
        job_name: str,
        input_sources: List[str],
        output_targets: List[str],
        transformation: str,
        data_in: Optional["pd.DataFrame"] = None,
        data_out: Optional["pd.DataFrame"] = None,
        metadata: Optional[Dict] = None,
    ) -> LineageRecord:
        """Track Pandas DataFrame transformation."""
        logger.info(f"Tracking lineage for job: {job_name}")

        row_count_in = len(data_in) if data_in is not None else None
        row_count_out = len(data_out) if data_out is not None else None

        record = LineageRecord(
            job_name=job_name,
            input_sources=input_sources,
            output_targets=output_targets,
            transformation=transformation,
            timestamp=datetime.now(),
            row_count_in=row_count_in,
            row_count_out=row_count_out,
            metadata=metadata or {},
        )

        self._lineage_records.append(record)

        # Store in database if configured
        if self.storage:
            try:
                self.storage.set(
                    "lineage",
                    {
                        "job_name": job_name,
                        "input_sources": ",".join(input_sources),
                        "output_targets": ",".join(output_targets),
                        "transformation": transformation,
                        "timestamp": record.timestamp.isoformat(),
                        "row_count_in": row_count_in,
                        "row_count_out": row_count_out,
                        "metadata": str(metadata) if metadata else None,
                    },
                )
            except Exception as e:
                logger.warning(f"Failed to store lineage record: {e}")

        logger.info(f"Lineage tracked: {row_count_in} → {row_count_out} rows")
        return record


class SparkLineageTracker(LineageTracker):
    """Lineage tracker for Spark transformations."""

    def __init__(self, storage: Optional[Database] = None, settings: Optional[Dict] = None):
        super().__init__(DataEngine.SPARK, storage, settings)
        try:
            from pyspark.sql import DataFrame

            self.DataFrame = DataFrame
        except ImportError:
            raise ImportError("PySpark is required. Install with: pip install pyspark")

    def track_transformation(
        self,
        job_name: str,
        input_sources: List[str],
        output_targets: List[str],
        transformation: str,
        data_in: Optional["DataFrame"] = None,
        data_out: Optional["DataFrame"] = None,
        metadata: Optional[Dict] = None,
    ) -> LineageRecord:
        """Track Spark DataFrame transformation."""
        logger.info(f"Tracking lineage for Spark job: {job_name}")

        row_count_in = data_in.count() if data_in is not None else None
        row_count_out = data_out.count() if data_out is not None else None

        record = LineageRecord(
            job_name=job_name,
            input_sources=input_sources,
            output_targets=output_targets,
            transformation=transformation,
            timestamp=datetime.now(),
            row_count_in=row_count_in,
            row_count_out=row_count_out,
            metadata=metadata or {},
        )

        self._lineage_records.append(record)

        # Store in database if configured
        if self.storage:
            try:
                self.storage.set(
                    "lineage",
                    {
                        "job_name": job_name,
                        "input_sources": ",".join(input_sources),
                        "output_targets": ",".join(output_targets),
                        "transformation": transformation,
                        "timestamp": record.timestamp.isoformat(),
                        "row_count_in": row_count_in,
                        "row_count_out": row_count_out,
                        "metadata": str(metadata) if metadata else None,
                    },
                )
            except Exception as e:
                logger.warning(f"Failed to store lineage record: {e}")

        logger.info(f"Lineage tracked: {row_count_in} → {row_count_out} rows")
        return record


class LineageTrackerFactory:
    """Factory for creating LineageTracker instances."""

    _tracker_map = {
        DataEngine.PANDAS: PandasLineageTracker,
        DataEngine.SPARK: SparkLineageTracker,
    }

    @classmethod
    def create(
        cls, engine: DataEngine, storage: Optional[Database] = None, settings: Optional[Dict] = None
    ) -> LineageTracker:
        """Create a LineageTracker for the specified engine."""
        if engine not in cls._tracker_map:
            raise ValueError(f"Unsupported engine: {engine}")

        tracker_class = cls._tracker_map[engine]
        logger.info(f"Creating {engine.value} lineage tracker")
        return tracker_class(storage, settings)

    @classmethod
    def create_auto(
        cls, data: Any, storage: Optional[Database] = None, settings: Optional[Dict] = None
    ) -> LineageTracker:
        """Auto-detect engine and create tracker."""
        engine = cls._detect_engine(data)
        return cls.create(engine, storage, settings)

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


def track_lineage(
    job_name: str, inputs: List[str], outputs: List[str], metadata: Optional[Dict] = None
):
    """
    Decorator for automatic lineage tracking.

    Usage:
        >>> @track_lineage("etl_job", inputs=["raw_data"], outputs=["clean_data"])
        >>> def my_etl_function(data):
        >>>     return transform(data)
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            logger.info(f"Starting tracked job: {job_name}")
            start_time = datetime.now()

            result = func(*args, **kwargs)

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            logger.info(f"Job '{job_name}' completed in {duration:.2f}s")
            logger.info(f"Inputs: {inputs} → Outputs: {outputs}")

            return result

        return wrapper

    return decorator
