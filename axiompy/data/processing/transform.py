"""
Data transformation utilities with support for multiple engines.

Provides common ETL transformation patterns that work across Pandas, Spark,
and other data processing engines.
"""

from abc import ABC, abstractmethod
from inspect import signature
from typing import Any, Callable, Dict, List, Optional, Union

from axiompy.loggers import LoggerFactory

from axiompy.data.observability.ports import SignalSink
from axiompy.data.types import DataEngine

logger = LoggerFactory.create_logger(__name__)


class DataTransformer(ABC):
    """
    Abstract base class for data transformations across different engines.

    Provides a unified interface for common transformation patterns regardless
    of the underlying engine (Pandas, Spark, etc.).
    """

    def __init__(
        self,
        engine: DataEngine,
        settings: Optional[Dict] = None,
        signal_sink: Optional[SignalSink] = None,
    ):
        """
        Initialize the transformer.

        Args:
            engine: Data processing engine
            settings: Optional configuration settings
            signal_sink: Optional observability sink
        """
        self.engine = engine
        self.settings = settings or {}
        self._signal_sink = signal_sink

    @abstractmethod
    def rename_columns(self, data: Any, mapping: Dict[str, str]) -> Any:  # pragma: no cover
        """
        Rename columns according to mapping.

        Args:
            data: DataFrame-like object
            mapping: Dictionary mapping old column names to new names

        Returns:
            Transformed data with renamed columns
        """
        pass

    @abstractmethod
    def select_columns(self, data: Any, columns: List[str]) -> Any:  # pragma: no cover
        """
        Select subset of columns.

        Args:
            data: DataFrame-like object
            columns: List of column names to select

        Returns:
            Data with only specified columns
        """
        pass

    @abstractmethod
    def drop_columns(self, data: Any, columns: List[str]) -> Any:  # pragma: no cover
        """
        Drop specified columns.

        Args:
            data: DataFrame-like object
            columns: List of column names to drop

        Returns:
            Data with columns removed
        """
        pass

    @abstractmethod
    def fill_nulls(
        self,
        data: Any,
        strategy: str = "value",
        value: Any = None,
        columns: Optional[List[str]] = None,  # pragma: no cover
    ) -> Any:  # pragma: no cover
        """
        Fill null values using specified strategy.

        Args:
            data: DataFrame-like object
            strategy: Fill strategy ("value", "mean", "median", "mode", "forward", "backward")
            value: Value to use for "value" strategy
            columns: Optional list of columns to fill (None = all columns)

        Returns:
            Data with nulls filled
        """
        pass

    @abstractmethod
    def drop_nulls(
        self, data: Any, how: str = "any", subset: Optional[List[str]] = None
    ) -> Any:  # pragma: no cover
        """
        Drop rows with null values.

        Args:
            data: DataFrame-like object
            how: "any" drops if any null, "all" drops if all nulls
            subset: Optional list of columns to consider

        Returns:
            Data with null rows removed
        """
        pass

    @abstractmethod
    def deduplicate(
        self, data: Any, subset: Optional[List[str]] = None, keep: str = "first"
    ) -> Any:  # pragma: no cover
        """
        Remove duplicate rows.

        Args:
            data: DataFrame-like object
            subset: Optional columns to consider for deduplication
            keep: Which duplicate to keep ("first", "last", False for drop all)

        Returns:
            Deduplicated data
        """
        pass

    @abstractmethod
    def filter_rows(self, data: Any, condition: Union[str, Callable]) -> Any:  # pragma: no cover
        """
        Filter rows based on condition.

        Args:
            data: DataFrame-like object
            condition: Filter condition (SQL-like string or callable)

        Returns:
            Filtered data
        """
        pass

    @abstractmethod
    def cast_column(self, data: Any, column: str, dtype: Any) -> Any:  # pragma: no cover
        """
        Cast column to specified data type.

        Args:
            data: DataFrame-like object
            column: Column name to cast
            dtype: Target data type

        Returns:
            Data with column cast to new type
        """
        pass

    @abstractmethod
    def add_computed_column(
        self, data: Any, column_name: str, expression: Union[str, Callable]
    ) -> Any:  # pragma: no cover
        """
        Add a computed column based on expression.

        Args:
            data: DataFrame-like object
            column_name: Name for new column
            expression: Expression to compute values (SQL or function)

        Returns:
            Data with new computed column
        """
        pass


from .adapters.transformers import PandasDataTransformer, SparkDataTransformer  # noqa: E402


class DataTransformerFactory:
    """
    Factory for creating DataTransformer instances.

    Usage:
        >>> transformer = DataTransformerFactory.create(DataEngine.PANDAS)
        >>> df = transformer.rename_columns(df, {"old": "new"})
        >>>
        >>> # Auto-detection
        >>> transformer = DataTransformerFactory.create_auto(df)
        >>> df = transformer.drop_nulls(df)
    """

    _transformer_map = {
        DataEngine.PANDAS: PandasDataTransformer,
        DataEngine.SPARK: SparkDataTransformer,
    }

    @classmethod
    def create(
        cls,
        engine: DataEngine,
        settings: Optional[Dict] = None,
        signal_sink: Optional[SignalSink] = None,
    ) -> DataTransformer:
        """
        Create a DataTransformer for the specified engine.

        Args:
            engine: Data processing engine
            settings: Optional configuration settings

        Returns:
            DataTransformer instance

        Raises:
            ValueError: If engine is not supported
        """
        if engine not in cls._transformer_map:
            raise ValueError(
                f"Unsupported engine: {engine}. Supported: {list(cls._transformer_map.keys())}"
            )

        transformer_class = cls._transformer_map[engine]
        logger.info(f"Creating {engine.value} data transformer")
        if "signal_sink" in signature(transformer_class.__init__).parameters:
            return transformer_class(settings, signal_sink)
        return transformer_class(settings)

    @classmethod
    def create_auto(
        cls, data: Any, settings: Optional[Dict] = None, signal_sink: Optional[SignalSink] = None
    ) -> DataTransformer:
        """
        Auto-detect engine from data type and create appropriate transformer.

        Args:
            data: DataFrame-like object
            settings: Optional configuration settings

        Returns:
            DataTransformer instance
        """
        engine = cls._detect_engine(data)
        return cls.create(engine, settings, signal_sink)

    @classmethod
    def _detect_engine(cls, data: Any) -> DataEngine:
        """Detect the data engine from the data object type."""
        type_name = type(data).__name__
        module_name = type(data).__module__

        if "pandas" in module_name:
            return DataEngine.PANDAS
        elif "pyspark" in module_name:
            return DataEngine.SPARK
        elif "polars" in module_name:
            return DataEngine.POLARS
        else:
            raise ValueError(
                f"Cannot auto-detect engine for type: {type_name} from module: {module_name}"
            )

    @classmethod
    def register_transformer(cls, engine: DataEngine, transformer_class: type) -> None:
        """
        Register a custom transformer implementation.

        Args:
            engine: Engine type
            transformer_class: Class implementing DataTransformer interface

        Raises:
            TypeError: If transformer_class doesn't inherit from DataTransformer
        """
        if not issubclass(transformer_class, DataTransformer):
            raise TypeError("transformer_class must inherit from DataTransformer")

        cls._transformer_map[engine] = transformer_class
        logger.info(f"Registered custom transformer for engine: {engine.value}")
