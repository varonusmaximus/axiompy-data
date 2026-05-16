"""
DataFrame adapter with unified API across engines.

Provides a consistent interface for common DataFrame operations that works
seamlessly with Pandas, Spark, and other data processing engines.
"""

import contextlib
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

from axiompy.io import Database
from axiompy.loggers import LoggerFactory

from axiompy.data.types import DataEngine

logger = LoggerFactory.create_logger(__name__)


class DataFrameAdapter(ABC):
    """
    Abstract base class for DataFrame adapters across different engines.

    Provides a unified API for common DataFrame operations and integrations
    with axiompy's Database and ObjectStorage abstractions.
    """

    def __init__(self, engine: DataEngine, settings: Optional[Dict] = None):
        """
        Initialize the adapter.

        Args:
            engine: Data processing engine
            settings: Optional configuration settings
        """
        self.engine = engine
        self.settings = settings or {}

    @abstractmethod
    def read_table(
        self,
        source: Union[Database, str],
        table: str,
        columns: Optional[List[str]] = None,
        filters: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Any:
        """
        Read table into DataFrame.

        Args:
            source: Database instance or connection string
            table: Table name
            columns: Optional list of columns to read
            filters: Optional filter expression
            limit: Optional row limit

        Returns:
            DataFrame with table data
        """
        pass

    @abstractmethod
    def write_table(
        self, data: Any, target: Union[Database, str], table: str, mode: str = "append"
    ) -> None:
        """
        Write DataFrame to table.

        Args:
            data: DataFrame to write
            target: Database instance or connection string
            table: Table name
            mode: Write mode ("append", "overwrite", "error")
        """
        pass

    @abstractmethod
    def read_file(self, path: str, format: str = "csv", options: Optional[Dict] = None) -> Any:
        """
        Read file into DataFrame.

        Args:
            path: File path
            format: File format (csv, json, parquet, etc.)
            options: Format-specific options

        Returns:
            DataFrame with file data
        """
        pass

    @abstractmethod
    def write_file(
        self,
        data: Any,
        path: str,
        format: str = "csv",
        mode: str = "overwrite",
        options: Optional[Dict] = None,
    ) -> None:
        """
        Write DataFrame to file.

        Args:
            data: DataFrame to write
            path: File path
            format: File format (csv, json, parquet, etc.)
            mode: Write mode
            options: Format-specific options
        """
        pass

    @abstractmethod
    def get_schema(self, data: Any) -> Dict[str, str]:
        """
        Get DataFrame schema.

        Args:
            data: DataFrame

        Returns:
            Dictionary mapping column names to types
        """
        pass

    @abstractmethod
    def get_shape(self, data: Any) -> tuple:
        """
        Get DataFrame shape (rows, columns).

        Args:
            data: DataFrame

        Returns:
            Tuple of (row_count, column_count)
        """
        pass


class PandasDataFrameAdapter(DataFrameAdapter):
    """DataFrame adapter for Pandas."""

    def __init__(self, settings: Optional[Dict] = None):
        super().__init__(DataEngine.PANDAS, settings)
        try:
            import pandas as pd

            self.pd = pd
        except ImportError:
            raise ImportError("Pandas is required. Install with: pip install pandas")

    def read_table(
        self,
        source: Union[Database, str],
        table: str,
        columns: Optional[List[str]] = None,
        filters: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> "pd.DataFrame":
        """Read table into Pandas DataFrame."""
        logger.info(f"Reading table '{table}' into Pandas DataFrame")

        # Build query
        cols = ", ".join(columns) if columns else "*"
        query = f"SELECT {cols} FROM {table}"
        if filters:
            query += f" WHERE {filters}"
        if limit:
            query += f" LIMIT {limit}"

        if isinstance(source, Database):
            # Use axiompy Database abstraction
            results = source.execute(query)
            return self.pd.DataFrame(results)
        else:
            # Use pandas read_sql
            return self.pd.read_sql(query, source)

    def write_table(
        self, data: "pd.DataFrame", target: Union[Database, str], table: str, mode: str = "append"
    ) -> None:
        """Write Pandas DataFrame to table."""
        logger.info(f"Writing Pandas DataFrame to table '{table}' (mode={mode})")

        if isinstance(target, Database):
            # Use axiompy Database abstraction
            records = data.to_dict("records")

            if mode == "overwrite":
                # Delete existing data (simplified)
                with contextlib.suppress(Exception):
                    target.execute(f"DELETE FROM {table}")

            for record in records:
                target.set(table, record)
        else:
            # Use pandas to_sql
            if_exists = "replace" if mode == "overwrite" else "append"
            data.to_sql(table, target, if_exists=if_exists, index=False)

    def read_file(
        self, path: str, format: str = "csv", options: Optional[Dict] = None
    ) -> "pd.DataFrame":
        """Read file into Pandas DataFrame."""
        logger.info(f"Reading {format} file: {path}")
        options = options or {}

        if format == "csv":
            return self.pd.read_csv(path, **options)
        elif format == "json":
            return self.pd.read_json(path, **options)
        elif format == "parquet":
            return self.pd.read_parquet(path, **options)
        elif format == "excel":
            return self.pd.read_excel(path, **options)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def write_file(
        self,
        data: "pd.DataFrame",
        path: str,
        format: str = "csv",
        mode: str = "overwrite",
        options: Optional[Dict] = None,
    ) -> None:
        """Write Pandas DataFrame to file."""
        logger.info(f"Writing {format} file: {path}")
        options = options or {}

        if format == "csv":
            data.to_csv(path, index=False, **options)
        elif format == "json":
            data.to_json(path, **options)
        elif format == "parquet":
            data.to_parquet(path, **options)
        elif format == "excel":
            data.to_excel(path, index=False, **options)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def get_schema(self, data: "pd.DataFrame") -> Dict[str, str]:
        """Get Pandas DataFrame schema."""
        return {col: str(dtype) for col, dtype in data.dtypes.items()}

    def get_shape(self, data: "pd.DataFrame") -> tuple:
        """Get Pandas DataFrame shape."""
        return data.shape


class SparkDataFrameAdapter(DataFrameAdapter):
    """DataFrame adapter for PySpark."""

    def __init__(self, settings: Optional[Dict] = None):
        super().__init__(DataEngine.SPARK, settings)
        try:
            from pyspark.sql import DataFrame, SparkSession

            self.DataFrame = DataFrame
            self.SparkSession = SparkSession
        except ImportError:
            raise ImportError("PySpark is required. Install with: pip install pyspark")

    def read_table(
        self,
        source: Union[Database, str],
        table: str,
        columns: Optional[List[str]] = None,
        filters: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> "DataFrame":
        """Read table into Spark DataFrame."""
        logger.info(f"Reading table '{table}' into Spark DataFrame")

        spark = self.SparkSession.getActiveSession() or self.SparkSession.builder.getOrCreate()

        if isinstance(source, str):
            # JDBC connection
            df = spark.read.jdbc(source, table)
        else:
            # For Database abstraction, we'll use query results
            query = f"SELECT {', '.join(columns) if columns else '*'} FROM {table}"
            if filters:
                query += f" WHERE {filters}"
            if limit:
                query += f" LIMIT {limit}"

            results = source.execute(query)
            # Convert to Spark DataFrame
            df = spark.createDataFrame(results)
            return df

        if columns:
            df = df.select(*columns)
        if filters:
            df = df.filter(filters)
        if limit:
            df = df.limit(limit)

        return df

    def write_table(
        self, data: "DataFrame", target: Union[Database, str], table: str, mode: str = "append"
    ) -> None:
        """Write Spark DataFrame to table."""
        logger.info(f"Writing Spark DataFrame to table '{table}' (mode={mode})")

        if isinstance(target, str):
            # JDBC connection
            data.write.jdbc(target, table, mode=mode)
        else:
            # For Database abstraction, convert to records
            records = [row.asDict() for row in data.collect()]

            if mode == "overwrite":
                with contextlib.suppress(Exception):
                    target.execute(f"DELETE FROM {table}")

            for record in records:
                target.set(table, record)

    def read_file(
        self, path: str, format: str = "csv", options: Optional[Dict] = None
    ) -> "DataFrame":
        """Read file into Spark DataFrame."""
        logger.info(f"Reading {format} file: {path}")

        spark = self.SparkSession.getActiveSession() or self.SparkSession.builder.getOrCreate()
        options = options or {}

        if format == "csv":
            return spark.read.csv(path, **options)
        elif format == "json":
            return spark.read.json(path, **options)
        elif format == "parquet":
            return spark.read.parquet(path, **options)
        elif format == "orc":
            return spark.read.orc(path, **options)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def write_file(
        self,
        data: "DataFrame",
        path: str,
        format: str = "csv",
        mode: str = "overwrite",
        options: Optional[Dict] = None,
    ) -> None:
        """Write Spark DataFrame to file."""
        logger.info(f"Writing {format} file: {path}")
        options = options or {}

        writer = data.write.mode(mode)

        if format == "csv":
            writer.csv(path, **options)
        elif format == "json":
            writer.json(path, **options)
        elif format == "parquet":
            writer.parquet(path, **options)
        elif format == "orc":
            writer.orc(path, **options)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def get_schema(self, data: "DataFrame") -> Dict[str, str]:
        """Get Spark DataFrame schema."""
        return {field.name: str(field.dataType) for field in data.schema.fields}

    def get_shape(self, data: "DataFrame") -> tuple:
        """Get Spark DataFrame shape."""
        return (data.count(), len(data.columns))


class DataFrameAdapterFactory:
    """
    Factory for creating DataFrameAdapter instances.

    Usage:
        >>> adapter = DataFrameAdapterFactory.create(DataEngine.PANDAS)
        >>> df = adapter.read_file("data.csv")
        >>>
        >>> # Auto-detection
        >>> adapter = DataFrameAdapterFactory.create_auto(df)
        >>> schema = adapter.get_schema(df)
    """

    _adapter_map = {
        DataEngine.PANDAS: PandasDataFrameAdapter,
        DataEngine.SPARK: SparkDataFrameAdapter,
    }

    @classmethod
    def create(cls, engine: DataEngine, settings: Optional[Dict] = None) -> DataFrameAdapter:
        """
        Create a DataFrameAdapter for the specified engine.

        Args:
            engine: Data processing engine
            settings: Optional configuration settings

        Returns:
            DataFrameAdapter instance
        """
        if engine not in cls._adapter_map:
            raise ValueError(
                f"Unsupported engine: {engine}. Supported: {list(cls._adapter_map.keys())}"
            )

        adapter_class = cls._adapter_map[engine]
        logger.info(f"Creating {engine.value} DataFrame adapter")
        return adapter_class(settings)

    @classmethod
    def create_auto(cls, data: Any, settings: Optional[Dict] = None) -> DataFrameAdapter:
        """Auto-detect engine and create adapter."""
        engine = cls._detect_engine(data)
        return cls.create(engine, settings)

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
    def register_adapter(cls, engine: DataEngine, adapter_class: type) -> None:
        """Register a custom adapter implementation."""
        if not issubclass(adapter_class, DataFrameAdapter):
            raise TypeError("adapter_class must inherit from DataFrameAdapter")

        cls._adapter_map[engine] = adapter_class
        logger.info(f"Registered custom adapter for engine: {engine.value}")
