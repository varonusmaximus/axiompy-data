"""Convert Arrow tables to pandas, polars, Spark, and other engines."""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from axiompy.data.types import DataEngine


def convert(table: pa.Table, engine: DataEngine, **kwargs: Any) -> Any:
    """
    Convert a PyArrow table to the requested data engine representation.

    Args:
        table: Source Arrow table.
        engine: Target engine (``DataEngine.PANDAS``, ``POLARS``, ``SPARK``).
        **kwargs: Engine-specific options (``spark`` session required for Spark).

    Returns:
        Engine-native object (e.g. ``pd.DataFrame``, ``polars.DataFrame``).

    Raises:
        ValueError: Unknown engine or missing required kwargs.
        ImportError: Optional dependency not installed.
    """
    match engine:
        case DataEngine.PANDAS:
            return table.to_pandas()
        case DataEngine.POLARS:
            try:
                import polars as pl
            except ImportError as e:
                raise ImportError("polars not installed. Run: pip install polars") from e
            return pl.from_arrow(table)
        case DataEngine.SPARK:
            spark = kwargs.get("spark")
            if spark is None:
                raise ValueError("Spark conversion requires spark=SparkSession keyword argument")
            try:
                return spark.createDataFrame(table)
            except (TypeError, ValueError):
                return spark.createDataFrame(table.to_pandas())
        case _:
            raise ValueError(f"Unsupported DataEngine for Arrow conversion: {engine!r}")


__all__ = ["convert"]
