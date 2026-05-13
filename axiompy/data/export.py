"""
Data format conversion utilities.

Provides utilities to convert data between different formats (CSV, JSON, Parquet,
Excel, etc.) with support for multiple engines.
"""

from io import BytesIO
from typing import Any, Dict, Optional

from axiompy.data.types import DataEngine, DataFormat
from axiompy.loggers import LoggerFactory

logger = LoggerFactory.create_logger(__name__)


class FormatConverter:
    """
    Utility for converting data between formats.

    Supports conversion between CSV, JSON, Parquet, Excel, and other formats
    with automatic engine detection.
    """

    def __init__(self, engine: Optional[DataEngine] = None):
        """
        Initialize the converter.

        Args:
            engine: Optional explicit engine (auto-detected if None)
        """
        self.engine = engine

    def convert(
        self,
        data: Any,
        from_format: DataFormat,
        to_format: DataFormat,
        output_path: Optional[str] = None,
        options: Optional[Dict] = None,
    ) -> Any:
        """
        Convert data from one format to another.

        Args:
            data: Input data (path string or DataFrame)
            from_format: Source format
            to_format: Target format
            output_path: Optional path to write output
            options: Format-specific options

        Returns:
            Converted data (DataFrame or bytes if output_path not specified)
        """
        logger.info(f"Converting from {from_format.value} to {to_format.value}")
        options = options or {}

        # Detect engine if not specified
        engine = self.engine
        if engine is None and not isinstance(data, str):
            engine = self._detect_engine(data)

        # Read data if it's a path
        if isinstance(data, str):
            df = self._read_file(data, from_format, options)
            if engine is None:
                engine = self._detect_engine(df)
        else:
            df = data

        # Write to target format
        if output_path:
            self._write_file(df, output_path, to_format, engine, options)
            logger.info(f"Converted data written to {output_path}")
            return output_path
        else:
            return self._to_bytes(df, to_format, engine, options)

    def _read_file(self, path: str, format: DataFormat, options: Dict) -> Any:
        """Read file into DataFrame."""
        # Try pandas first
        try:
            import pandas as pd

            if format == DataFormat.CSV:
                return pd.read_csv(path, **options)
            elif format == DataFormat.JSON:
                return pd.read_json(path, **options)
            elif format == DataFormat.PARQUET:
                return pd.read_parquet(path, **options)
            elif format == DataFormat.EXCEL:
                return pd.read_excel(path, **options)
            else:
                raise ValueError(f"Unsupported format: {format}")

        except ImportError:
            # Try Spark
            try:
                from pyspark.sql import SparkSession

                spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

                if format == DataFormat.CSV:
                    return spark.read.csv(path, header=True, **options)
                elif format == DataFormat.JSON:
                    return spark.read.json(path, **options)
                elif format == DataFormat.PARQUET:
                    return spark.read.parquet(path, **options)
                elif format == DataFormat.ORC:
                    return spark.read.orc(path, **options)
                else:
                    raise ValueError(f"Unsupported format: {format}")

            except ImportError:
                raise ImportError("Neither pandas nor pyspark is available")

    def _write_file(
        self, data: Any, path: str, format: DataFormat, engine: DataEngine, options: Dict
    ) -> None:
        """Write DataFrame to file."""
        if engine == DataEngine.PANDAS:
            if format == DataFormat.CSV:
                data.to_csv(path, index=False, **options)
            elif format == DataFormat.JSON:
                data.to_json(path, **options)
            elif format == DataFormat.PARQUET:
                data.to_parquet(path, **options)
            elif format == DataFormat.EXCEL:
                data.to_excel(path, index=False, **options)
            else:
                raise ValueError(f"Unsupported format for pandas: {format}")

        elif engine == DataEngine.SPARK:
            writer = data.write.mode(options.pop("mode", "overwrite"))

            if format == DataFormat.CSV:
                writer.csv(path, header=True, **options)
            elif format == DataFormat.JSON:
                writer.json(path, **options)
            elif format == DataFormat.PARQUET:
                writer.parquet(path, **options)
            elif format == DataFormat.ORC:
                writer.orc(path, **options)
            else:
                raise ValueError(f"Unsupported format for Spark: {format}")

        else:
            raise ValueError(f"Unsupported engine: {engine}")

    def _to_bytes(self, data: Any, format: DataFormat, engine: DataEngine, options: Dict) -> bytes:
        """Convert DataFrame to bytes."""
        if engine == DataEngine.PANDAS:
            if format == DataFormat.CSV:
                return data.to_csv(index=False, **options).encode("utf-8")
            elif format == DataFormat.JSON:
                return data.to_json(**options).encode("utf-8")
            elif format == DataFormat.PARQUET:
                buffer = BytesIO()
                data.to_parquet(buffer, **options)
                return buffer.getvalue()
            elif format == DataFormat.EXCEL:
                buffer = BytesIO()
                data.to_excel(buffer, index=False, **options)
                return buffer.getvalue()
            else:
                raise ValueError(f"Unsupported format: {format}")

        elif engine == DataEngine.SPARK:
            # For Spark, we need to collect and convert
            logger.warning("Converting Spark DataFrame to bytes requires collection - may be slow")
            pdf = data.toPandas()

            if format == DataFormat.CSV:
                return pdf.to_csv(index=False, **options).encode("utf-8")
            elif format == DataFormat.JSON:
                return pdf.to_json(**options).encode("utf-8")
            elif format == DataFormat.PARQUET:
                buffer = BytesIO()
                pdf.to_parquet(buffer, **options)
                return buffer.getvalue()
            else:
                raise ValueError(f"Unsupported format: {format}")

        else:
            raise ValueError(f"Unsupported engine: {engine}")

    def _detect_engine(self, data: Any) -> DataEngine:
        """Detect engine from data type."""
        module_name = type(data).__module__

        if "pandas" in module_name:
            return DataEngine.PANDAS
        elif "pyspark" in module_name:
            return DataEngine.SPARK
        else:
            raise ValueError(f"Cannot detect engine for module: {module_name}")

    @staticmethod
    def csv_to_parquet(input_path: str, output_path: str, options: Optional[Dict] = None) -> str:
        """Convenience method: Convert CSV to Parquet."""
        converter = FormatConverter()
        return converter.convert(
            input_path, DataFormat.CSV, DataFormat.PARQUET, output_path, options
        )

    @staticmethod
    def parquet_to_csv(input_path: str, output_path: str, options: Optional[Dict] = None) -> str:
        """Convenience method: Convert Parquet to CSV."""
        converter = FormatConverter()
        return converter.convert(
            input_path, DataFormat.PARQUET, DataFormat.CSV, output_path, options
        )

    @staticmethod
    def json_to_parquet(input_path: str, output_path: str, options: Optional[Dict] = None) -> str:
        """Convenience method: Convert JSON to Parquet."""
        converter = FormatConverter()
        return converter.convert(
            input_path, DataFormat.JSON, DataFormat.PARQUET, output_path, options
        )

    @staticmethod
    def excel_to_csv(input_path: str, output_path: str, options: Optional[Dict] = None) -> str:
        """Convenience method: Convert Excel to CSV."""
        converter = FormatConverter()
        return converter.convert(input_path, DataFormat.EXCEL, DataFormat.CSV, output_path, options)
