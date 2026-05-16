"""Pandas and Spark lineage trackers."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from axiompy.io import Database

from axiompy.data.observability.ports import SignalKind, SignalSink
from axiompy.data.processing.lineage import LineageTracker, logger
from axiompy.data.processing.signals import emit_signal
from axiompy.data.types import DataEngine, LineageRecord


class PandasLineageTracker(LineageTracker):
    """Lineage tracker for Pandas transformations."""

    def __init__(
        self,
        storage: Optional[Database] = None,
        settings: Optional[Dict] = None,
        signal_sink: Optional[SignalSink] = None,
    ):
        super().__init__(DataEngine.PANDAS, storage, settings, signal_sink)
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
        data_in: Optional[pd.DataFrame] = None,
        data_out: Optional[pd.DataFrame] = None,
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
        emit_signal(
            self._signal_sink,
            SignalKind.LINEAGE,
            "lineage.track_transformation",
            {"job_name": job_name, "engine": "pandas"},
        )
        return record


class SparkLineageTracker(LineageTracker):
    """Lineage tracker for Spark transformations."""

    def __init__(
        self,
        storage: Optional[Database] = None,
        settings: Optional[Dict] = None,
        signal_sink: Optional[SignalSink] = None,
    ):
        super().__init__(DataEngine.SPARK, storage, settings, signal_sink)
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
        data_in: Optional[DataFrame] = None,
        data_out: Optional[DataFrame] = None,
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
        emit_signal(
            self._signal_sink,
            SignalKind.LINEAGE,
            "lineage.track_transformation",
            {"job_name": job_name, "engine": "spark"},
        )
        return record
