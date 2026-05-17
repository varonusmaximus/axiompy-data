"""
Data quality and validation utilities with support for multiple engines.

Provides data profiling, schema validation, and expectation testing across
Pandas, Spark, and other data processing engines.
"""

from abc import ABC, abstractmethod
from inspect import signature
from typing import Any, Dict, List, Optional

from axiompy.loggers import LoggerFactory
from axiompy.result import Err, Ok, Result
from axiompy.validators import ensure_not_empty, ensure_not_none, ensure_positive

from axiompy.data.observability.ports import SignalSink
from axiompy.data.types import DataEngine, DataExpectation, DataQualityReport

logger = LoggerFactory.create_logger(__name__)


class DataProfiler(ABC):
    """
    Abstract base class for data profiling across different engines.

    Provides a unified interface for profiling DataFrames regardless of the
    underlying engine (Pandas, Spark, etc.).
    """

    def __init__(
        self,
        engine: DataEngine,
        settings: Optional[Dict] = None,
        signal_sink: Optional[SignalSink] = None,
    ):
        """
        Initialize the profiler.

        Args:
            engine: Data processing engine
            settings: Optional configuration settings
            signal_sink: Optional observability sink
        """
        self.engine = engine
        self.settings = settings or {}
        self._signal_sink = signal_sink

    @abstractmethod
    def profile(self, data: Any) -> DataQualityReport:  # pragma: no cover
        """
        Generate comprehensive data quality report.

        Args:
            data: DataFrame-like object

        Returns:
            DataQualityReport with profiling results
        """
        pass

    @abstractmethod
    def validate_expectations(
        self, data: Any, expectations: List[DataExpectation]
    ) -> Dict[str, Any]:  # pragma: no cover
        """
        Validate data against expectations.

        Args:
            data: DataFrame-like object
            expectations: List of expectations to validate

        Returns:
            Dictionary with validation results
        """
        pass

    @abstractmethod
    def check_schema(
        self, data: Any, expected_schema: Dict[str, Any]
    ) -> Dict[str, Any]:  # pragma: no cover
        """
        Validate data schema against expected schema.

        Args:
            data: DataFrame-like object
            expected_schema: Expected schema definition

        Returns:
            Schema validation results
        """
        pass

    # ===== Result-Based Methods (Railway-Oriented Programming) =====

    def try_profile(self, data: Any) -> Result[DataQualityReport, str]:
        """
        Generate data quality report using Result type.

        Never raises exceptions - all errors are returned in Err.

        Args:
            data: DataFrame-like object

        Returns:
            Result[DataQualityReport, str]: Ok with report or Err with error message
        """
        try:
            ensure_not_none(data, "DataFrame cannot be None")
            report = self.profile(data)
            logger.debug("Data profiling succeeded via Result API")
            return Ok(report)
        except Exception as e:
            error_msg = f"Data profiling failed: {str(e)}"
            logger.error(error_msg)
            return Err(error_msg)

    def try_validate_expectations(
        self, data: Any, expectations: List[DataExpectation]
    ) -> Result[Dict[str, Any], str]:
        """
        Validate data against expectations using Result type.

        Never raises exceptions - all errors are returned in Err.

        Args:
            data: DataFrame-like object
            expectations: List of expectations to validate

        Returns:
            Result[Dict[str, Any], str]: Ok with results or Err with error message
        """
        try:
            ensure_not_none(data, "DataFrame cannot be None")
            ensure_not_none(expectations, "expectations list cannot be None")
            ensure_not_empty(expectations, "expectations list cannot be empty")

            results = self.validate_expectations(data, expectations)
            logger.debug("Expectation validation succeeded via Result API")
            return Ok(results)
        except Exception as e:
            error_msg = f"Expectation validation failed: {str(e)}"
            logger.error(error_msg)
            return Err(error_msg)

    def try_check_schema(
        self, data: Any, expected_schema: Dict[str, Any]
    ) -> Result[Dict[str, Any], str]:
        """
        Validate schema using Result type.

        Never raises exceptions - all errors are returned in Err.

        Args:
            data: DataFrame-like object
            expected_schema: Expected schema definition

        Returns:
            Result[Dict[str, Any], str]: Ok with results or Err with error message
        """
        try:
            ensure_not_none(data, "DataFrame cannot be None")
            ensure_not_none(expected_schema, "expected_schema cannot be None")
            ensure_not_empty(expected_schema, "expected_schema cannot be empty")

            results = self.check_schema(data, expected_schema)
            logger.debug("Schema check succeeded via Result API")
            return Ok(results)
        except Exception as e:
            error_msg = f"Schema check failed: {str(e)}"
            logger.error(error_msg)
            return Err(error_msg)


from .adapters.profilers import PandasDataProfiler, SparkDataProfiler  # noqa: E402


class DataProfilerFactory:
    """
    Factory for creating DataProfiler instances.

    Usage:
        >>> # Explicit engine selection
        >>> profiler = DataProfilerFactory.create(DataEngine.PANDAS)
        >>> report = profiler.profile(pandas_df)
        >>>
        >>> # Auto-detection
        >>> profiler = DataProfilerFactory.create_auto(df)
        >>> report = profiler.profile(df)
    """

    _profiler_map = {
        DataEngine.PANDAS: PandasDataProfiler,
        DataEngine.SPARK: SparkDataProfiler,
    }

    @classmethod
    def create(
        cls,
        engine: DataEngine,
        settings: Optional[Dict] = None,
        signal_sink: Optional[SignalSink] = None,
    ) -> DataProfiler:
        """
        Create a DataProfiler for the specified engine.

        Args:
            engine: Data processing engine
            settings: Optional configuration settings

        Returns:
            DataProfiler instance

        Raises:
            ValueError: If engine is not supported
        """
        if engine not in cls._profiler_map:
            raise ValueError(
                f"Unsupported engine: {engine}. Supported: {list(cls._profiler_map.keys())}"
            )

        profiler_class = cls._profiler_map[engine]
        logger.info(f"Creating {engine.value} data profiler")
        if "signal_sink" in signature(profiler_class.__init__).parameters:
            return profiler_class(settings, signal_sink)
        return profiler_class(settings)

    @classmethod
    def create_auto(
        cls, data: Any, settings: Optional[Dict] = None, signal_sink: Optional[SignalSink] = None
    ) -> DataProfiler:
        """
        Auto-detect engine from data type and create appropriate profiler.

        Args:
            data: DataFrame-like object
            settings: Optional configuration settings

        Returns:
            DataProfiler instance

        Raises:
            ValueError: If engine cannot be detected
        """
        engine = cls._detect_engine(data)
        return cls.create(engine, settings, signal_sink)

    @classmethod
    def _detect_engine(cls, data: Any) -> DataEngine:
        """Detect the data engine from the data object type."""
        type_name = type(data).__name__
        module_name = type(data).__module__

        if "pandas" in module_name:
            logger.debug(f"Detected pandas engine from type {type_name}")
            return DataEngine.PANDAS
        elif "pyspark" in module_name:
            logger.debug(f"Detected spark engine from type {type_name}")
            return DataEngine.SPARK
        elif "polars" in module_name:
            logger.debug(f"Detected polars engine from type {type_name}")
            return DataEngine.POLARS
        else:
            raise ValueError(
                f"Cannot auto-detect engine for type: {type_name} from module: {module_name}"
            )

    @classmethod
    def register_profiler(cls, engine: DataEngine, profiler_class: type) -> None:
        """
        Register a custom profiler implementation.

        Args:
            engine: Engine type
            profiler_class: Class implementing DataProfiler interface

        Raises:
            TypeError: If profiler_class doesn't inherit from DataProfiler
        """
        if not issubclass(profiler_class, DataProfiler):
            raise TypeError("profiler_class must inherit from DataProfiler")

        cls._profiler_map[engine] = profiler_class
        logger.info(f"Registered custom profiler for engine: {engine.value}")
