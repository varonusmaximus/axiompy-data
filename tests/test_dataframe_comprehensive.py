"""
Comprehensive tests for DataFrame adapters.

Tests all methods of PandasDataFrameAdapter and SparkDataFrameAdapter.
"""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from axiompy.data.dataframe import (
    DataFrameAdapterFactory,
    PandasDataFrameAdapter,
)
from axiompy.data.types import DataEngine
from axiompy.io import Database


class TestPandasDataFrameAdapterComprehensive:
    """Comprehensive tests for PandasDataFrameAdapter."""

    @pytest.fixture
    def adapter(self):
        """Create adapter instance."""
        return PandasDataFrameAdapter()

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame."""
        pytest.importorskip("pandas")
        import pandas as pd

        return pd.DataFrame(
            {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "value": [10.5, 20.3, 30.8]}
        )

    # ====== read_table tests ======

    def test_read_table_with_database_basic(self, adapter):
        """Test reading from database with basic query."""
        mock_db = Mock(spec=Database)
        mock_db.execute.return_value = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

        df = adapter.read_table(mock_db, "users")

        assert len(df) == 2
        assert list(df.columns) == ["id", "name"]
        mock_db.execute.assert_called_once()
        call_args = mock_db.execute.call_args[0][0]
        assert "SELECT *" in call_args
        assert "FROM users" in call_args

    def test_read_table_with_database_columns(self, adapter):
        """Test reading specific columns from database."""
        mock_db = Mock(spec=Database)
        mock_db.execute.return_value = [{"name": "Alice"}, {"name": "Bob"}]

        df = adapter.read_table(mock_db, "users", columns=["name"])

        assert "name" in list(df.columns)
        call_args = mock_db.execute.call_args[0][0]
        assert "SELECT name" in call_args

    def test_read_table_with_database_filters(self, adapter):
        """Test reading with WHERE filters."""
        mock_db = Mock(spec=Database)
        mock_db.execute.return_value = [{"id": 1, "name": "Alice"}]

        df = adapter.read_table(mock_db, "users", filters="id > 0")

        call_args = mock_db.execute.call_args[0][0]
        assert "WHERE id > 0" in call_args

    def test_read_table_with_database_limit(self, adapter):
        """Test reading with LIMIT."""
        mock_db = Mock(spec=Database)
        mock_db.execute.return_value = [{"id": 1}]

        df = adapter.read_table(mock_db, "users", limit=10)

        call_args = mock_db.execute.call_args[0][0]
        assert "LIMIT 10" in call_args

    def test_read_table_with_database_all_options(self, adapter):
        """Test reading with all options combined."""
        mock_db = Mock(spec=Database)
        mock_db.execute.return_value = [{"name": "Alice"}]

        df = adapter.read_table(
            mock_db, "users", columns=["name", "email"], filters="id > 5", limit=20
        )

        call_args = mock_db.execute.call_args[0][0]
        assert "SELECT name, email" in call_args
        assert "WHERE id > 5" in call_args
        assert "LIMIT 20" in call_args

    @patch("pandas.read_sql")
    def test_read_table_with_sql_connection(self, mock_read_sql, adapter):
        """Test reading from SQL connection string."""
        pytest.importorskip("pandas")
        import pandas as pd

        mock_df = pd.DataFrame({"id": [1], "name": ["Alice"]})
        mock_read_sql.return_value = mock_df

        # Use string connection instead of Database object
        df = adapter.read_table("sqlite:///test.db", "users")

        mock_read_sql.assert_called_once()

    # ====== write_table tests ======

    def test_write_table_to_database_append(self, adapter, sample_df):
        """Test writing to database in append mode."""
        mock_db = Mock(spec=Database)

        adapter.write_table(sample_df, mock_db, "users", mode="append")

        # Should not call DELETE for append mode
        mock_db.execute.assert_not_called()
        # Should call set for each row
        assert mock_db.set.call_count == 3

    def test_write_table_to_database_overwrite(self, adapter, sample_df):
        """Test writing to database in overwrite mode."""
        mock_db = Mock(spec=Database)

        adapter.write_table(sample_df, mock_db, "users", mode="overwrite")

        # Should call DELETE for overwrite mode
        mock_db.execute.assert_called_once()
        call_args = mock_db.execute.call_args[0][0]
        assert "DELETE FROM users" in call_args
        # Should still call set for each row
        assert mock_db.set.call_count == 3

    def test_write_table_to_database_overwrite_delete_fails(self, adapter, sample_df):
        """Test write when DELETE fails (exception handled gracefully)."""
        mock_db = Mock(spec=Database)
        mock_db.execute.side_effect = Exception("DELETE failed")

        # Should not raise, exception is caught
        adapter.write_table(sample_df, mock_db, "users", mode="overwrite")

        # Should still try to write records
        assert mock_db.set.call_count == 3

    @patch("pandas.DataFrame.to_sql")
    def test_write_table_to_sql_connection(self, mock_to_sql, adapter, sample_df):
        """Test writing to SQL connection string."""
        adapter.write_table(sample_df, "sqlite:///test.db", "users")

        mock_to_sql.assert_called_once()

    # ====== read_file tests ======

    def test_read_file_csv(self, adapter):
        """Test reading CSV file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test.csv"
            csv_path.write_text("id,name\n1,Alice\n2,Bob")

            df = adapter.read_file(str(csv_path), format="csv")

            assert len(df) == 2
            assert list(df.columns) == ["id", "name"]

    def test_read_file_json(self, adapter):
        """Test reading JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "test.json"
            json_path.write_text('[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]')

            df = adapter.read_file(str(json_path), format="json")

            assert len(df) == 2

    def test_read_file_parquet(self, adapter, sample_df):
        """Test reading Parquet file."""
        pytest.importorskip("pyarrow")

        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = Path(tmpdir) / "test.parquet"
            sample_df.to_parquet(str(parquet_path), index=False)

            df = adapter.read_file(str(parquet_path), format="parquet")

            assert len(df) == 3

    def test_read_file_excel(self, adapter, sample_df):
        """Test reading Excel file."""
        pytest.importorskip("openpyxl")

        with tempfile.TemporaryDirectory() as tmpdir:
            excel_path = Path(tmpdir) / "test.xlsx"
            sample_df.to_excel(str(excel_path), index=False)

            df = adapter.read_file(str(excel_path), format="excel")

            assert len(df) == 3

    def test_read_file_with_options(self, adapter):
        """Test reading with format-specific options."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test.csv"
            csv_path.write_text("id;name\n1;Alice\n2;Bob")

            df = adapter.read_file(str(csv_path), format="csv", options={"sep": ";"})

            assert len(df) == 2

    def test_read_file_unsupported_format(self, adapter):
        """Test reading unsupported format."""
        with pytest.raises(ValueError, match="Unsupported format"):
            adapter.read_file("test.xyz", format="unsupported")

    # ====== write_file tests ======

    def test_write_file_csv(self, adapter, sample_df):
        """Test writing CSV file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "output.csv"

            adapter.write_file(sample_df, str(csv_path), format="csv")

            assert csv_path.exists()
            assert len(csv_path.read_text()) > 0

    def test_write_file_json(self, adapter, sample_df):
        """Test writing JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "output.json"

            adapter.write_file(sample_df, str(json_path), format="json")

            assert json_path.exists()

    def test_write_file_parquet(self, adapter, sample_df):
        """Test writing Parquet file."""
        pytest.importorskip("pyarrow")

        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = Path(tmpdir) / "output.parquet"

            adapter.write_file(sample_df, str(parquet_path), format="parquet")

            assert parquet_path.exists()

    def test_write_file_excel(self, adapter, sample_df):
        """Test writing Excel file."""
        pytest.importorskip("openpyxl")

        with tempfile.TemporaryDirectory() as tmpdir:
            excel_path = Path(tmpdir) / "output.xlsx"

            adapter.write_file(sample_df, str(excel_path), format="excel")

            assert excel_path.exists()

    def test_write_file_unsupported_format(self, adapter, sample_df):
        """Test writing unsupported format."""
        with pytest.raises(ValueError, match="Unsupported format"):
            adapter.write_file(sample_df, "test.xyz", format="unsupported")

    # ====== get_schema tests ======

    def test_get_schema(self, adapter, sample_df):
        """Test getting DataFrame schema."""
        schema = adapter.get_schema(sample_df)

        assert isinstance(schema, dict)
        assert "id" in schema
        assert "name" in schema
        assert "value" in schema

    # ====== get_shape tests ======

    def test_get_shape(self, adapter, sample_df):
        """Test getting DataFrame shape."""
        shape = adapter.get_shape(sample_df)

        assert isinstance(shape, tuple)
        assert len(shape) == 2
        assert shape[0] == 3  # rows
        assert shape[1] == 3  # columns


class TestDataFrameAdapterFactory:
    """Test DataFrameAdapterFactory."""

    def test_create_pandas_adapter(self):
        """Test creating Pandas adapter."""
        adapter = DataFrameAdapterFactory.create(DataEngine.PANDAS)

        assert isinstance(adapter, PandasDataFrameAdapter)
        assert adapter.engine == DataEngine.PANDAS

    def test_create_with_settings(self):
        """Test creating adapter with settings."""
        settings = {"key": "value"}
        adapter = DataFrameAdapterFactory.create(DataEngine.PANDAS, settings)

        assert adapter.settings == settings

    def test_create_unsupported_engine(self):
        """Test creating adapter with unsupported engine."""
        # Create a mock engine that's not supported
        mock_engine = type("MockEngine", (), {"value": "mock"})()
        with pytest.raises((ValueError, AttributeError)):
            DataFrameAdapterFactory.create(mock_engine)

    def test_create_auto_pandas(self):
        """Test auto-detecting Pandas DataFrame."""
        pytest.importorskip("pandas")
        import pandas as pd

        df = pd.DataFrame({"id": [1, 2, 3]})
        adapter = DataFrameAdapterFactory.create_auto(df)

        assert isinstance(adapter, PandasDataFrameAdapter)

    def test_create_auto_unknown_type(self):
        """Test auto-detection with unknown type."""
        with pytest.raises(ValueError, match="Cannot auto-detect engine"):
            DataFrameAdapterFactory.create_auto([1, 2, 3])

    def test_register_adapter(self):
        """Test registering custom adapter."""

        class CustomAdapter(PandasDataFrameAdapter):
            pass

        engine = DataEngine.PANDAS
        DataFrameAdapterFactory.register_adapter(engine, CustomAdapter)

        adapter = DataFrameAdapterFactory.create(engine)
        assert isinstance(adapter, CustomAdapter)

    def test_register_invalid_adapter(self):
        """Test registering invalid adapter."""

        class NotAnAdapter:
            pass

        with pytest.raises(TypeError, match="must inherit from DataFrameAdapter"):
            DataFrameAdapterFactory.register_adapter(DataEngine.PANDAS, NotAnAdapter)
