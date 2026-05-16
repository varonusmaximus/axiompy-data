"""
Change Data Capture (CDC) utilities for detecting data changes.

Provides utilities to identify inserts, updates, and deletes between
two datasets for incremental data processing.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from axiompy.loggers import LoggerFactory

from axiompy.data.observability.ports import SignalSink
from axiompy.data.types import ChangeRecord, ChangeType, DataEngine

logger = LoggerFactory.create_logger(__name__)


class ChangeDetector(ABC):
    """
    Abstract base class for change data capture.

    Detects changes (inserts, updates, deletes) between two datasets.
    """

    def __init__(
        self,
        engine: DataEngine,
        key_columns: List[str],
        settings: Optional[Dict] = None,
        signal_sink: Optional[SignalSink] = None,
    ):
        """
        Initialize the change detector.

        Args:
            engine: Data processing engine
            key_columns: Columns to use as primary key for comparison
            settings: Optional configuration settings
            signal_sink: Optional observability sink
        """
        self.engine = engine
        self.key_columns = key_columns
        self.settings = settings or {}
        self._signal_sink = signal_sink

    @abstractmethod
    def detect_changes(
        self, old_data: Any, new_data: Any, compare_columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:  # pragma: no cover
        """
        Detect changes between old and new datasets.

        Args:
            old_data: Old/baseline dataset
            new_data: New/current dataset
            compare_columns: Optional list of columns to compare (None = all)

        Returns:
            Dictionary with inserts, updates, deletes, and unchanged records
        """
        pass

    @abstractmethod
    def get_inserts(self, old_data: Any, new_data: Any) -> Any:  # pragma: no cover
        """Get records that exist in new_data but not in old_data."""
        pass

    @abstractmethod
    def get_deletes(self, old_data: Any, new_data: Any) -> Any:  # pragma: no cover
        """Get records that exist in old_data but not in new_data."""
        pass

    @abstractmethod
    def get_updates(
        self, old_data: Any, new_data: Any, compare_columns: Optional[List[str]] = None
    ) -> Any:  # pragma: no cover
        """Get records that exist in both but have changed values."""
        pass


from .adapters.change_detectors import PandasChangeDetector, SparkChangeDetector  # noqa: E402


class ChangeDetectorFactory:
    """Factory for creating ChangeDetector instances."""

    _detector_map = {
        DataEngine.PANDAS: PandasChangeDetector,
        DataEngine.SPARK: SparkChangeDetector,
    }

    @classmethod
    def create(
        cls,
        engine: DataEngine,
        key_columns: List[str],
        settings: Optional[Dict] = None,
        signal_sink: Optional[SignalSink] = None,
    ) -> ChangeDetector:
        """Create a ChangeDetector for the specified engine."""
        if engine not in cls._detector_map:
            raise ValueError(f"Unsupported engine: {engine}")

        detector_class = cls._detector_map[engine]
        logger.info(f"Creating {engine.value} change detector")
        return detector_class(key_columns, settings, signal_sink)

    @classmethod
    def create_auto(
        cls,
        data: Any,
        key_columns: List[str],
        settings: Optional[Dict] = None,
        signal_sink: Optional[SignalSink] = None,
    ) -> ChangeDetector:
        """Auto-detect engine and create detector."""
        engine = cls._detect_engine(data)
        return cls.create(engine, key_columns, settings, signal_sink)

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


__all__ = [
    "ChangeType",
    "ChangeRecord",
    "ChangeDetector",
    "PandasChangeDetector",
    "SparkChangeDetector",
    "ChangeDetectorFactory",
]
