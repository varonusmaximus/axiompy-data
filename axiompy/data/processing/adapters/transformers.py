"""Pandas and Spark data transformers."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Union

from axiompy.data.observability.ports import SignalKind, SignalSink
from axiompy.data.processing.signals import emit_signal
from axiompy.data.processing.transform import DataTransformer, logger
from axiompy.data.types import DataEngine


class PandasDataTransformer(DataTransformer):
    """Data transformer for Pandas DataFrames."""

    def __init__(self, settings: Optional[Dict] = None, signal_sink: Optional[SignalSink] = None):
        super().__init__(DataEngine.PANDAS, settings, signal_sink)
        try:
            import pandas as pd

            self.pd = pd
        except ImportError:
            raise ImportError("Pandas is required. Install with: pip install pandas")

    def rename_columns(
        self, data: pd.DataFrame, mapping: Dict[str, str]
    ) -> pd.DataFrame:  # pragma: no cover
        """Rename columns in Pandas DataFrame."""
        logger.debug(f"Renaming {len(mapping)} columns in Pandas DataFrame")
        return data.rename(columns=mapping)

    def select_columns(self, data: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """Select columns in Pandas DataFrame."""
        logger.debug(f"Selecting {len(columns)} columns from Pandas DataFrame")
        return data[columns]

    def drop_columns(self, data: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """Drop columns in Pandas DataFrame."""
        logger.debug(f"Dropping {len(columns)} columns from Pandas DataFrame")
        return data.drop(columns=columns)

    def fill_nulls(
        self,
        data: pd.DataFrame,
        strategy: str = "value",
        value: Any = None,
        columns: Optional[List[str]] = None,  # pragma: no cover
    ) -> pd.DataFrame:
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
        self, data: pd.DataFrame, how: str = "any", subset: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Drop rows with nulls in Pandas DataFrame."""
        logger.debug(f"Dropping rows with nulls (how={how})")
        return data.dropna(how=how, subset=subset)

    def deduplicate(
        self, data: pd.DataFrame, subset: Optional[List[str]] = None, keep: str = "first"
    ) -> pd.DataFrame:
        """Remove duplicates in Pandas DataFrame."""
        logger.debug(f"Deduplicating rows (keep={keep})")
        return data.drop_duplicates(subset=subset, keep=keep)

    def filter_rows(self, data: pd.DataFrame, condition: Union[str, Callable]) -> pd.DataFrame:
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

    def cast_column(self, data: pd.DataFrame, column: str, dtype: Any) -> pd.DataFrame:
        """Cast column type in Pandas DataFrame."""
        logger.debug(f"Casting column '{column}' to {dtype}")
        data = data.copy()
        data[column] = data[column].astype(dtype)
        return data

    def add_computed_column(
        self, data: pd.DataFrame, column_name: str, expression: Union[str, Callable]
    ) -> pd.DataFrame:
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

        emit_signal(
            self._signal_sink,
            SignalKind.LIFECYCLE,
            "transform.add_computed_column",
            {"column": column_name, "engine": "pandas"},
        )
        return data


class SparkDataTransformer(DataTransformer):
    """Data transformer for PySpark DataFrames."""

    def __init__(self, settings: Optional[Dict] = None, signal_sink: Optional[SignalSink] = None):
        super().__init__(DataEngine.SPARK, settings, signal_sink)
        try:
            from pyspark.sql import DataFrame
            from pyspark.sql import functions as F

            self.DataFrame = DataFrame
            self.F = F
        except ImportError:
            raise ImportError("PySpark is required. Install with: pip install pyspark")

    def rename_columns(self, data: DataFrame, mapping: Dict[str, str]) -> DataFrame:
        """Rename columns in Spark DataFrame."""
        logger.debug(f"Renaming {len(mapping)} columns in Spark DataFrame")

        for old_name, new_name in mapping.items():
            data = data.withColumnRenamed(old_name, new_name)
        return data

    def select_columns(self, data: DataFrame, columns: List[str]) -> DataFrame:
        """Select columns in Spark DataFrame."""
        logger.debug(f"Selecting {len(columns)} columns from Spark DataFrame")
        return data.select(*columns)

    def drop_columns(self, data: DataFrame, columns: List[str]) -> DataFrame:
        """Drop columns in Spark DataFrame."""
        logger.debug(f"Dropping {len(columns)} columns from Spark DataFrame")
        return data.drop(*columns)

    def fill_nulls(
        self,
        data: DataFrame,
        strategy: str = "value",
        value: Any = None,
        columns: Optional[List[str]] = None,  # pragma: no cover
    ) -> DataFrame:
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
        self, data: DataFrame, how: str = "any", subset: Optional[List[str]] = None
    ) -> DataFrame:
        """Drop rows with nulls in Spark DataFrame."""
        logger.debug(f"Dropping rows with nulls (how={how})")
        return data.dropna(how=how, subset=subset)

    def deduplicate(
        self, data: DataFrame, subset: Optional[List[str]] = None, keep: str = "first"
    ) -> DataFrame:
        """Remove duplicates in Spark DataFrame."""
        logger.debug("Deduplicating rows")

        if subset:
            return data.dropDuplicates(subset)
        else:
            return data.dropDuplicates()

    def filter_rows(self, data: DataFrame, condition: Union[str, Callable]) -> DataFrame:
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

    def cast_column(self, data: DataFrame, column: str, dtype: Any) -> DataFrame:
        """Cast column type in Spark DataFrame."""
        logger.debug(f"Casting column '{column}' to {dtype}")
        return data.withColumn(column, data[column].cast(dtype))

    def add_computed_column(
        self, data: DataFrame, column_name: str, expression: Union[str, Callable]
    ) -> DataFrame:
        """Add computed column in Spark DataFrame."""
        logger.debug(f"Adding computed column '{column_name}'")

        if callable(expression):
            out = data.withColumn(column_name, expression(data))
        elif isinstance(expression, str):
            out = data.withColumn(column_name, self.F.expr(expression))
        else:
            raise ValueError("Expression must be callable or SQL string")

        emit_signal(
            self._signal_sink,
            SignalKind.LIFECYCLE,
            "transform.add_computed_column",
            {"column": column_name, "engine": "spark"},
        )
        return out
