"""
Comprehensive tests for data export/format conversion module.

Tests FormatConverter and all format conversion methods.
"""

import tempfile
from pathlib import Path

import pytest

from axiompy.data.export import FormatConverter
from axiompy.data.types import DataEngine, DataFormat


class TestFormatConverterComprehensive:
    """Comprehensive tests for FormatConverter."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame."""
        pytest.importorskip("pandas")
        import pandas as pd

        return pd.DataFrame(
            {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "value": [10.5, 20.3, 30.8]}
        )

    @pytest.fixture
    def converter(self):
        """Create converter instance."""
        return FormatConverter()

    # ====== CSV Format Tests ======

    def test_csv_to_parquet_file(self, sample_df):
        """Test converting CSV file to Parquet."""
        pytest.importorskip("pyarrow")

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test.csv"
            parquet_path = Path(tmpdir) / "test.parquet"

            sample_df.to_csv(str(csv_path), index=False)

            result = FormatConverter.csv_to_parquet(str(csv_path), str(parquet_path))

            assert result == str(parquet_path)
            assert parquet_path.exists()

    def test_csv_to_json_file(self, sample_df):
        """Test converting CSV file to JSON."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test.csv"
            json_path = Path(tmpdir) / "test.json"

            sample_df.to_csv(str(csv_path), index=False)

            result = FormatConverter.csv_to_parquet(str(csv_path), str(json_path))

            assert result == str(json_path)

    def test_read_csv_with_options(self, sample_df):
        """Test reading CSV with format-specific options."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test.csv"
            sample_df.to_csv(str(csv_path), index=False, sep=";")

            converter = FormatConverter()
            df = converter._read_file(str(csv_path), DataFormat.CSV, {"sep": ";"})

            assert len(df) == 3

    def test_write_csv_with_options(self, sample_df):
        """Test writing CSV with options."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test.csv"

            converter = FormatConverter()
            converter._write_file(sample_df, str(csv_path), DataFormat.CSV, DataEngine.PANDAS, {})

            assert csv_path.exists()

    # ====== JSON Format Tests ======

    def test_read_json_format(self, sample_df):
        """Test reading JSON format."""
        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "test.json"
            sample_df.to_json(str(json_path))

            converter = FormatConverter()
            df = converter._read_file(str(json_path), DataFormat.JSON, {})

            assert len(df) == 3

    def test_write_json_format(self, sample_df):
        """Test writing JSON format."""
        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "test.json"

            converter = FormatConverter()
            converter._write_file(sample_df, str(json_path), DataFormat.JSON, DataEngine.PANDAS, {})

            assert json_path.exists()

    def test_json_to_parquet_file(self, sample_df):
        """Test converting JSON to Parquet."""
        pytest.importorskip("pyarrow")

        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "test.json"
            parquet_path = Path(tmpdir) / "test.parquet"

            sample_df.to_json(str(json_path))

            result = FormatConverter.json_to_parquet(str(json_path), str(parquet_path))

            assert result == str(parquet_path)
            assert parquet_path.exists()

    # ====== Parquet Format Tests ======

    def test_read_parquet_format(self, sample_df):
        """Test reading Parquet format."""
        pytest.importorskip("pyarrow")

        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = Path(tmpdir) / "test.parquet"
            sample_df.to_parquet(str(parquet_path), index=False)

            converter = FormatConverter()
            df = converter._read_file(str(parquet_path), DataFormat.PARQUET, {})

            assert len(df) == 3

    def test_write_parquet_format(self, sample_df):
        """Test writing Parquet format."""
        pytest.importorskip("pyarrow")

        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = Path(tmpdir) / "test.parquet"

            converter = FormatConverter()
            converter._write_file(
                sample_df, str(parquet_path), DataFormat.PARQUET, DataEngine.PANDAS, {}
            )

            assert parquet_path.exists()

    def test_parquet_to_csv_file(self, sample_df):
        """Test converting Parquet to CSV."""
        pytest.importorskip("pyarrow")

        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = Path(tmpdir) / "test.parquet"
            csv_path = Path(tmpdir) / "test.csv"

            sample_df.to_parquet(str(parquet_path), index=False)

            result = FormatConverter.parquet_to_csv(str(parquet_path), str(csv_path))

            assert result == str(csv_path)
            assert csv_path.exists()

    # ====== Excel Format Tests ======

    def test_read_excel_format(self, sample_df):
        """Test reading Excel format."""
        pytest.importorskip("openpyxl")

        with tempfile.TemporaryDirectory() as tmpdir:
            excel_path = Path(tmpdir) / "test.xlsx"
            sample_df.to_excel(str(excel_path), index=False)

            converter = FormatConverter()
            df = converter._read_file(str(excel_path), DataFormat.EXCEL, {})

            assert len(df) == 3

    def test_write_excel_format(self, sample_df):
        """Test writing Excel format."""
        pytest.importorskip("openpyxl")

        with tempfile.TemporaryDirectory() as tmpdir:
            excel_path = Path(tmpdir) / "test.xlsx"

            converter = FormatConverter()
            converter._write_file(
                sample_df, str(excel_path), DataFormat.EXCEL, DataEngine.PANDAS, {}
            )

            assert excel_path.exists()

    def test_excel_to_csv_file(self, sample_df):
        """Test converting Excel to CSV."""
        pytest.importorskip("openpyxl")

        with tempfile.TemporaryDirectory() as tmpdir:
            excel_path = Path(tmpdir) / "test.xlsx"
            csv_path = Path(tmpdir) / "test.csv"

            sample_df.to_excel(str(excel_path), index=False)

            result = FormatConverter.excel_to_csv(str(excel_path), str(csv_path))

            assert result == str(csv_path)
            assert csv_path.exists()

    # ====== Convert Method Tests ======

    def test_convert_dataframe_to_bytes_csv(self, sample_df):
        """Test converting DataFrame to bytes as CSV."""
        converter = FormatConverter(engine=DataEngine.PANDAS)

        result = converter.convert(sample_df, DataFormat.CSV, DataFormat.CSV)

        assert isinstance(result, bytes)
        assert b"id,name,value" in result

    def test_convert_dataframe_to_bytes_json(self, sample_df):
        """Test converting DataFrame to bytes as JSON."""
        converter = FormatConverter(engine=DataEngine.PANDAS)

        result = converter.convert(sample_df, DataFormat.CSV, DataFormat.JSON)

        assert isinstance(result, bytes)

    def test_convert_dataframe_to_bytes_parquet(self, sample_df):
        """Test converting DataFrame to bytes as Parquet."""
        pytest.importorskip("pyarrow")

        converter = FormatConverter(engine=DataEngine.PANDAS)

        result = converter.convert(sample_df, DataFormat.CSV, DataFormat.PARQUET)

        assert isinstance(result, bytes)

    def test_convert_dataframe_to_bytes_excel(self, sample_df):
        """Test converting DataFrame to bytes as Excel."""
        pytest.importorskip("openpyxl")

        converter = FormatConverter(engine=DataEngine.PANDAS)

        result = converter.convert(sample_df, DataFormat.CSV, DataFormat.EXCEL)

        assert isinstance(result, bytes)

    def test_convert_file_to_file(self, sample_df):
        """Test converting file from one format to another."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "input.csv"
            json_path = Path(tmpdir) / "output.json"

            sample_df.to_csv(str(csv_path), index=False)

            converter = FormatConverter()
            result = converter.convert(
                str(csv_path), DataFormat.CSV, DataFormat.JSON, str(json_path)
            )

            assert result == str(json_path)
            assert json_path.exists()

    def test_convert_with_auto_engine_detection(self, sample_df):
        """Test converting with auto-detected engine."""
        converter = FormatConverter()  # No engine specified

        result = converter.convert(sample_df, DataFormat.CSV, DataFormat.JSON)

        assert isinstance(result, bytes)

    # ====== _to_bytes Tests ======

    def test_to_bytes_csv(self, sample_df):
        """Test converting to bytes as CSV."""
        converter = FormatConverter()

        result = converter._to_bytes(sample_df, DataFormat.CSV, DataEngine.PANDAS, {})

        assert isinstance(result, bytes)
        assert b"id" in result

    def test_to_bytes_json(self, sample_df):
        """Test converting to bytes as JSON."""
        converter = FormatConverter()

        result = converter._to_bytes(sample_df, DataFormat.JSON, DataEngine.PANDAS, {})

        assert isinstance(result, bytes)

    def test_to_bytes_parquet(self, sample_df):
        """Test converting to bytes as Parquet."""
        pytest.importorskip("pyarrow")

        converter = FormatConverter()

        result = converter._to_bytes(sample_df, DataFormat.PARQUET, DataEngine.PANDAS, {})

        assert isinstance(result, bytes)

    def test_to_bytes_excel(self, sample_df):
        """Test converting to bytes as Excel."""
        pytest.importorskip("openpyxl")

        converter = FormatConverter()

        result = converter._to_bytes(sample_df, DataFormat.EXCEL, DataEngine.PANDAS, {})

        assert isinstance(result, bytes)

    # ====== Error Handling Tests ======

    def test_read_unsupported_format(self):
        """Test reading unsupported format."""
        converter = FormatConverter()

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.xyz"
            path.write_text("test")

            with pytest.raises(ValueError, match="Unsupported format"):
                converter._read_file(str(path), DataFormat.ORC, {})

    def test_write_unsupported_format_pandas(self, sample_df):
        """Test writing unsupported format with Pandas."""
        converter = FormatConverter()

        with pytest.raises(ValueError, match="Unsupported format"):
            converter._write_file(sample_df, "test.xyz", DataFormat.ORC, DataEngine.PANDAS, {})

    def test_to_bytes_unsupported_format(self, sample_df):
        """Test converting to bytes with unsupported format."""
        converter = FormatConverter()

        with pytest.raises(ValueError, match="Unsupported format"):
            converter._to_bytes(sample_df, DataFormat.ORC, DataEngine.PANDAS, {})

    def test_to_bytes_unsupported_engine(self, sample_df):
        """Test converting to bytes with unsupported engine."""
        converter = FormatConverter()

        mock_engine = type("MockEngine", (), {"value": "mock"})()

        with pytest.raises(ValueError, match="Unsupported engine"):
            converter._to_bytes(sample_df, DataFormat.CSV, mock_engine, {})

    def test_write_unsupported_engine(self, sample_df):
        """Test writing with unsupported engine."""
        converter = FormatConverter()

        mock_engine = type("MockEngine", (), {"value": "mock"})()

        with pytest.raises(ValueError, match="Unsupported engine"):
            converter._write_file(sample_df, "test.csv", DataFormat.CSV, mock_engine, {})

    # ====== Engine Detection Tests ======

    def test_detect_engine_pandas(self, sample_df):
        """Test detecting Pandas engine."""
        converter = FormatConverter()

        engine = converter._detect_engine(sample_df)

        assert engine == DataEngine.PANDAS

    def test_detect_engine_unknown_type(self):
        """Test detecting engine with unknown type."""
        converter = FormatConverter()

        with pytest.raises(ValueError, match="Cannot detect engine"):
            converter._detect_engine([1, 2, 3])

    # ====== Format Conversion Chain Tests ======

    def test_convert_chain_csv_to_json_to_csv(self, sample_df):
        """Test converting through multiple formats."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv1_path = Path(tmpdir) / "original.csv"
            json_path = Path(tmpdir) / "intermediate.json"
            csv2_path = Path(tmpdir) / "final.csv"

            sample_df.to_csv(str(csv1_path), index=False)

            # CSV -> JSON
            converter = FormatConverter()
            converter.convert(str(csv1_path), DataFormat.CSV, DataFormat.JSON, str(json_path))

            # JSON -> CSV
            converter.convert(str(json_path), DataFormat.JSON, DataFormat.CSV, str(csv2_path))

            assert csv2_path.exists()
            result_df = pytest.importorskip("pandas").read_csv(str(csv2_path))
            assert len(result_df) == 3

    # ====== Spark Integration Tests ======

    def test_spark_dataframe_to_bytes_csv(self):
        """Test converting Spark DataFrame to bytes as CSV."""
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.master("local").getOrCreate()

        try:
            sdf = spark.createDataFrame(
                [(1, "Alice", 10.5), (2, "Bob", 20.3)], ["id", "name", "value"]
            )

            converter = FormatConverter(engine=DataEngine.SPARK)
            result = converter._to_bytes(sdf, DataFormat.CSV, DataEngine.SPARK, {})

            assert isinstance(result, bytes)
        finally:
            spark.stop()

    def test_spark_dataframe_to_bytes_json(self):
        """Test converting Spark DataFrame to bytes as JSON."""
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.master("local").getOrCreate()

        try:
            sdf = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])

            converter = FormatConverter(engine=DataEngine.SPARK)
            result = converter._to_bytes(sdf, DataFormat.JSON, DataEngine.SPARK, {})

            assert isinstance(result, bytes)
        finally:
            spark.stop()

    # ====== Options Tests ======

    def test_convert_with_options(self, sample_df):
        """Test converting with format-specific options."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test.csv"

            converter = FormatConverter(engine=DataEngine.PANDAS)
            converter.convert(
                sample_df, DataFormat.CSV, DataFormat.CSV, str(csv_path), {"sep": ";"}
            )

            assert csv_path.exists()
            content = csv_path.read_text()
            # Should use semicolon separator
            assert ";" in content
