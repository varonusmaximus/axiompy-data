"""
Data transformation utilities with support for multiple engines.

Provides common ETL transformation patterns that work across Pandas, Spark,
and other data processing engines.
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Union

from axiompy.data.types import DataEngine
from axiompy.loggers import LoggerFactory

logger = LoggerFactory.create_logger(__name__)


class DataTransformer(ABC):
    """
    Abstract base class for data transformations across different engines.

    Provides a unified interface for common transformation patterns regardless
    of the underlying engine (Pandas, Spark, etc.).
    """

    def __init__(self, engine: DataEngine, settings: Optional[Dict] = None):
        """
        Initialize the transformer.

        Args:
            engine: Data processing engine
            settings: Optional configuration settings
        """
        self.engine = engine
        self.settings = settings or {}

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


class PandasDataTransformer(DataTransformer):
    """Data transformer for Pandas DataFrames."""

    def __init__(self, settings: Optional[Dict] = None):
        super().__init__(DataEngine.PANDAS, settings)
        try:
            import pandas as pd

            self.pd = pd
        except ImportError:
            raise ImportError("Pandas is required. Install with: pip install pandas")

    def rename_columns(
        self, data: "pd.DataFrame", mapping: Dict[str, str]
    ) -> "pd.DataFrame":  # pragma: no cover
        """Rename columns in Pandas DataFrame."""
        logger.debug(f"Renaming {len(mapping)} columns in Pandas DataFrame")
        return data.rename(columns=mapping)

    def select_columns(self, data: "pd.DataFrame", columns: List[str]) -> "pd.DataFrame":
        """Select columns in Pandas DataFrame."""
        logger.debug(f"Selecting {len(columns)} columns from Pandas DataFrame")
        return data[columns]

    def drop_columns(self, data: "pd.DataFrame", columns: List[str]) -> "pd.DataFrame":
        """Drop columns in Pandas DataFrame."""
        logger.debug(f"Dropping {len(columns)} columns from Pandas DataFrame")
        return data.drop(columns=columns)

    def fill_nulls(
        self,
        data: "pd.DataFrame",
        strategy: str = "value",
        value: Any = None,
        columns: Optional[List[str]] = None,  # pragma: no cover
    ) -> "pd.DataFrame":
        """Fill nulls in Pandas DataFrame."""
        logger.debug(f"Filling nulls using strategy: {strategy}")

        if columns:
            data = data.copy()
            target = data[columns]
        else:
            data = data.copy()
            target = data

        if strategy == "value":
            result = target.fillna(value)
        elif strategy == "mean":
            result = target.fillna(target.mean())
        elif strategy == "median":
            result = target.fillna(target.median())
        elif strategy == "mode":
            result = target.fillna(target.mode().iloc[0] if len(target.mode()) > 0 else value)
        elif strategy == "forward":
            result = target.fillna(method="ffill")
        elif strategy == "backward":
            result = target.fillna(method="bfill")
        else:
            raise ValueError(f"Unknown fill strategy: {strategy}")

        if columns:
            data[columns] = result
            return data
        else:
            return result

    def drop_nulls(
        self, data: "pd.DataFrame", how: str = "any", subset: Optional[List[str]] = None
    ) -> "pd.DataFrame":
        """Drop rows with nulls in Pandas DataFrame."""
        logger.debug(f"Dropping rows with nulls (how={how})")
        return data.dropna(how=how, subset=subset)

    def deduplicate(
        self, data: "pd.DataFrame", subset: Optional[List[str]] = None, keep: str = "first"
    ) -> "pd.DataFrame":
        """Remove duplicates in Pandas DataFrame."""
        logger.debug(f"Deduplicating rows (keep={keep})")
        return data.drop_duplicates(subset=subset, keep=keep)

    def filter_rows(self, data: "pd.DataFrame", condition: Union[str, Callable]) -> "pd.DataFrame":
        """Filter rows in Pandas DataFrame."""
        logger.debug("Filtering rows")

        if callable(condition):
            mask = condition(data)
            return data[mask]
        elif isinstance(condition, str):
            # Use query for string expressions
            return data.query(condition)
        else:
            raise ValueError("Condition must be callable or string expression")

    def cast_column(self, data: "pd.DataFrame", column: str, dtype: Any) -> "pd.DataFrame":
        """Cast column type in Pandas DataFrame."""
        logger.debug(f"Casting column '{column}' to {dtype}")
        data = data.copy()
        data[column] = data[column].astype(dtype)
        return data

    def add_computed_column(
        self, data: "pd.DataFrame", column_name: str, expression: Union[str, Callable]
    ) -> "pd.DataFrame":
        """Add computed column in Pandas DataFrame."""
        logger.debug(f"Adding computed column '{column_name}'")
        data = data.copy()

        if callable(expression):
            data[column_name] = expression(data)
        elif isinstance(expression, str):
            # Use eval for string expressions
            data[column_name] = data.eval(expression)
        else:
            raise ValueError("Expression must be callable or string")

        return data


class SparkDataTransformer(DataTransformer):
    """Data transformer for PySpark DataFrames."""

    def __init__(self, settings: Optional[Dict] = None):
        super().__init__(DataEngine.SPARK, settings)
        try:
            from pyspark.sql import DataFrame
            from pyspark.sql import functions as F

            self.DataFrame = DataFrame
            self.F = F
        except ImportError:
            raise ImportError("PySpark is required. Install with: pip install pyspark")

    def rename_columns(self, data: "DataFrame", mapping: Dict[str, str]) -> "DataFrame":
        """Rename columns in Spark DataFrame."""
        logger.debug(f"Renaming {len(mapping)} columns in Spark DataFrame")

        for old_name, new_name in mapping.items():
            data = data.withColumnRenamed(old_name, new_name)
        return data

    def select_columns(self, data: "DataFrame", columns: List[str]) -> "DataFrame":
        """Select columns in Spark DataFrame."""
        logger.debug(f"Selecting {len(columns)} columns from Spark DataFrame")
        return data.select(*columns)

    def drop_columns(self, data: "DataFrame", columns: List[str]) -> "DataFrame":
        """Drop columns in Spark DataFrame."""
        logger.debug(f"Dropping {len(columns)} columns from Spark DataFrame")
        return data.drop(*columns)

    def fill_nulls(
        self,
        data: "DataFrame",
        strategy: str = "value",
        value: Any = None,
        columns: Optional[List[str]] = None,  # pragma: no cover
    ) -> "DataFrame":
        """Fill nulls in Spark DataFrame."""
        logger.debug(f"Filling nulls using strategy: {strategy}")

        if strategy == "value":
            if columns:
                fill_dict = dict.fromkeys(columns, value)
                return data.fillna(fill_dict)
            else:
                return data.fillna(value)

        elif strategy in ["mean", "median"]:
            # Calculate aggregates
            target_cols = (
                columns
                if columns
                else [
                    f.name
                    for f in data.schema.fields
                    if "int" in str(f.dataType).lower()
                    or "double" in str(f.dataType).lower()
                    or "float" in str(f.dataType).lower()
                ]
            )

            if strategy == "mean":
                agg_exprs = [self.F.mean(col).alias(col) for col in target_cols]
            else:  # median
                agg_exprs = [self.F.percentile_approx(col, 0.5).alias(col) for col in target_cols]

            fill_values = data.select(agg_exprs).collect()[0].asDict()
            return data.fillna(fill_values)

        elif strategy == "forward":
            # Forward fill in Spark requires window functions
            from pyspark.sql.window import Window

            target_cols = columns if columns else data.columns
            window = Window.orderBy(self.F.monotonically_increasing_id()).rowsBetween(
                Window.unboundedPreceding, 0
            )

            for col in target_cols:
                data = data.withColumn(col, self.F.last(col, ignorenulls=True).over(window))
            return data

        else:
            raise ValueError(
                f"Strategy '{strategy}' not fully supported for Spark "
                "(use 'value', 'mean', or 'median')"
            )

    def drop_nulls(
        self, data: "DataFrame", how: str = "any", subset: Optional[List[str]] = None
    ) -> "DataFrame":
        """Drop rows with nulls in Spark DataFrame."""
        logger.debug(f"Dropping rows with nulls (how={how})")
        return data.dropna(how=how, subset=subset)

    def deduplicate(
        self, data: "DataFrame", subset: Optional[List[str]] = None, keep: str = "first"
    ) -> "DataFrame":
        """Remove duplicates in Spark DataFrame."""
        logger.debug("Deduplicating rows")

        if subset:
            return data.dropDuplicates(subset)
        else:
            return data.dropDuplicates()

    def filter_rows(self, data: "DataFrame", condition: Union[str, Callable]) -> "DataFrame":
        """Filter rows in Spark DataFrame."""
        logger.debug("Filtering rows")

        if callable(condition):
            # Assume it returns a Column expression
            return data.filter(condition(data))
        elif isinstance(condition, str):
            # Use SQL expression
            return data.filter(condition)
        else:
            raise ValueError("Condition must be callable or SQL string expression")

    def cast_column(self, data: "DataFrame", column: str, dtype: Any) -> "DataFrame":
        """Cast column type in Spark DataFrame."""
        logger.debug(f"Casting column '{column}' to {dtype}")
        return data.withColumn(column, data[column].cast(dtype))

    def add_computed_column(
        self, data: "DataFrame", column_name: str, expression: Union[str, Callable]
    ) -> "DataFrame":
        """Add computed column in Spark DataFrame."""
        logger.debug(f"Adding computed column '{column_name}'")

        if callable(expression):
            # Assume expression returns a Column
            return data.withColumn(column_name, expression(data))
        elif isinstance(expression, str):
            # Use SQL expression
            return data.withColumn(column_name, self.F.expr(expression))
        else:
            raise ValueError("Expression must be callable or SQL string")


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
    def create(cls, engine: DataEngine, settings: Optional[Dict] = None) -> DataTransformer:
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
        return transformer_class(settings)

    @classmethod
    def create_auto(cls, data: Any, settings: Optional[Dict] = None) -> DataTransformer:
        """
        Auto-detect engine from data type and create appropriate transformer.

        Args:
            data: DataFrame-like object
            settings: Optional configuration settings

        Returns:
            DataTransformer instance
        """
        engine = cls._detect_engine(data)
        return cls.create(engine, settings)

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
