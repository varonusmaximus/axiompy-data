"""
Unit tests for partition and dataframe modules.

Tests DataPartitioner, PandasDataPartitioner, and DataFrameAdapter implementations.
"""

import os
import tempfile
from unittest.mock import Mock

import pandas as pd
import pytest

from axiompy.data.dataframe import (
    DataFrameAdapterFactory,
    PandasDataFrameAdapter,
)
from axiompy.data.partition import (
    DataPartitionerFactory,
    PandasDataPartitioner,
)
from axiompy.data.types import DataEngine, PartitionStrategy
from axiompy.io import Database

# ============================================================================
# Partition Tests
# ============================================================================


class TestPandasDataPartitioner:
    """Test PandasDataPartitioner implementation."""

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame with dates."""
        dates = pd.date_range("2023-01-01", periods=100, freq="D")
        return pd.DataFrame({"date": dates, "value": range(100, 200), "id": range(1, 101)})

    @pytest.fixture
    def partitioner(self):
        """Create partitioner instance."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield PandasDataPartitioner(
                partition_key="date", strategy=PartitionStrategy.TIME_DAILY, base_path=tmpdir
            )

    def test_init(self):
        """Test partitioner initialization."""
        with tempfile.TemporaryDirectory() as tmpdir:
            partitioner = PandasDataPartitioner(
                partition_key="date", strategy=PartitionStrategy.TIME_DAILY, base_path=tmpdir
            )

            assert partitioner.partition_key == "date"
            assert partitioner.strategy == PartitionStrategy.TIME_DAILY
            assert partitioner.base_path == tmpdir
            assert partitioner.engine == DataEngine.PANDAS

    def test_write_partitioned_time_daily(self, partitioner, sample_df):
        """Test writing partitioned data with TIME_DAILY strategy."""
        partitioner.strategy = PartitionStrategy.TIME_DAILY

        paths = partitioner.write_partitioned(sample_df, format="csv")

        assert len(paths) > 0
        for path in paths:
            assert os.path.exists(path)

    def test_write_partitioned_time_monthly(self, partitioner, sample_df):
        """Test writing partitioned data with TIME_MONTHLY strategy."""
        partitioner.strategy = PartitionStrategy.TIME_MONTHLY

        paths = partitioner.write_partitioned(sample_df, format="csv")

        assert len(paths) > 0

    def test_write_partitioned_hash(self, sample_df):
        """Test writing partitioned data with HASH strategy."""
        with tempfile.TemporaryDirectory() as tmpdir:
            partitioner = PandasDataPartitioner(
                partition_key="id", strategy=PartitionStrategy.HASH, base_path=tmpdir
            )

            paths = partitioner.write_partitioned(sample_df, format="csv")

            assert len(paths) > 0

    def test_write_partitioned_parquet(self, partitioner, sample_df):
        """Test writing partitioned data in parquet format."""
        paths = partitioner.write_partitioned(sample_df, format="parquet")

        assert len(paths) > 0

    def test_read_partitions(self, partitioner, sample_df):
        """Test reading partitioned data."""
        # Write first
        partitioner.write_partitioned(sample_df, format="csv")

        # Read back
        result = partitioner.read_partitions(format="csv")

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_list_partitions(self, partitioner, sample_df):
        """Test listing partitions."""
        partitioner.write_partitioned(sample_df, format="csv")

        partitions = partitioner.list_partitions()

        assert isinstance(partitions, list)
        assert len(partitions) > 0

    def test_list_partitions_empty(self, partitioner):
        """Test listing partitions when none exist."""
        partitions = partitioner.list_partitions()

        assert isinstance(partitions, list)


class TestDataPartitionerFactory:
    """Test DataPartitionerFactory."""

    def test_create_pandas_partitioner(self):
        """Test creating Pandas partitioner."""
        with tempfile.TemporaryDirectory() as tmpdir:
            partitioner = DataPartitionerFactory.create(
                engine=DataEngine.PANDAS, partition_key="date", base_path=tmpdir
            )

            assert isinstance(partitioner, PandasDataPartitioner)

    def test_create_auto_pandas(self):
        """Test auto-detection for Pandas DataFrame."""
        df = pd.DataFrame({"date": [1, 2, 3], "value": [10, 20, 30]})

        with tempfile.TemporaryDirectory() as tmpdir:
            partitioner = DataPartitionerFactory.create_auto(
                data=df, partition_key="date", base_path=tmpdir
            )

            assert isinstance(partitioner, PandasDataPartitioner)

    def test_create_unsupported_engine(self):
        """Test creating partitioner with unsupported engine."""
        # Use a mock engine that's not in the support map
        mock_engine = type("MockEngine", (), {"value": "mock"})()
        with pytest.raises((ValueError, AttributeError)):
            DataPartitionerFactory.create(engine=mock_engine, partition_key="date")


# ============================================================================
# DataFrame Adapter Tests
# ============================================================================


class TestPandasDataFrameAdapter:
    """Test PandasDataFrameAdapter implementation."""

    @pytest.fixture
    def adapter(self):
        """Create adapter instance."""
        return PandasDataFrameAdapter()

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame."""
        return pd.DataFrame(
            {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "value": [10, 20, 30]}
        )

    def test_init(self):
        """Test adapter initialization."""
        adapter = PandasDataFrameAdapter()

        assert adapter.engine == DataEngine.PANDAS
        assert adapter.settings == {}

    def test_init_with_settings(self):
        """Test adapter initialization with settings."""
        settings = {"option1": "value1"}
        adapter = PandasDataFrameAdapter(settings=settings)

        assert adapter.settings == settings

    def test_read_file_csv(self, adapter, sample_df):
        """Test reading CSV file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "test.csv")
            sample_df.to_csv(csv_path, index=False)

            df = adapter.read_file(csv_path, format="csv")

            assert isinstance(df, pd.DataFrame)
            assert len(df) == 3
            assert list(df.columns) == ["id", "name", "value"]

    def test_read_file_json(self, adapter, sample_df):
        """Test reading JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = os.path.join(tmpdir, "test.json")
            sample_df.to_json(json_path)

            df = adapter.read_file(json_path, format="json")

            assert isinstance(df, pd.DataFrame)

    def test_read_file_parquet(self, adapter, sample_df):
        """Test reading Parquet file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = os.path.join(tmpdir, "test.parquet")
            sample_df.to_parquet(parquet_path, index=False)

            df = adapter.read_file(parquet_path, format="parquet")

            assert isinstance(df, pd.DataFrame)
            assert len(df) == 3

    def test_read_file_unsupported_format(self, adapter):
        """Test reading unsupported file format."""
        with pytest.raises(ValueError):
            adapter.read_file("test.xyz", format="unsupported")

    def test_write_file_csv(self, adapter, sample_df):
        """Test writing CSV file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "output.csv")

            adapter.write_file(sample_df, csv_path, format="csv")

            assert os.path.exists(csv_path)
            df = pd.read_csv(csv_path)
            assert len(df) == 3

    def test_write_file_json(self, adapter, sample_df):
        """Test writing JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = os.path.join(tmpdir, "output.json")

            adapter.write_file(sample_df, json_path, format="json")

            assert os.path.exists(json_path)

    def test_write_file_parquet(self, adapter, sample_df):
        """Test writing Parquet file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = os.path.join(tmpdir, "output.parquet")

            adapter.write_file(sample_df, parquet_path, format="parquet")

            assert os.path.exists(parquet_path)

    def test_get_schema(self, adapter, sample_df):
        """Test getting DataFrame schema."""
        schema = adapter.get_schema(sample_df)

        assert isinstance(schema, dict)
        assert "id" in schema
        assert "name" in schema
        assert "value" in schema

    def test_get_shape(self, adapter, sample_df):
        """Test getting DataFrame shape."""
        shape = adapter.get_shape(sample_df)

        assert isinstance(shape, tuple)
        assert shape == (3, 3)

    def test_read_table_with_database(self, adapter):
        """Test reading table from database."""
        mock_db = Mock(spec=Database)
        mock_db.execute.return_value = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

        df = adapter.read_table(source=mock_db, table="users")

        assert isinstance(df, pd.DataFrame)
        mock_db.execute.assert_called_once()

    def test_read_table_with_filters(self, adapter):
        """Test reading table with filters."""
        mock_db = Mock(spec=Database)
        mock_db.execute.return_value = [{"id": 1}]

        df = adapter.read_table(source=mock_db, table="users", filters="id > 0")

        assert isinstance(df, pd.DataFrame)
        call_args = mock_db.execute.call_args[0][0]
        assert "WHERE" in call_args

    def test_read_table_with_limit(self, adapter):
        """Test reading table with limit."""
        mock_db = Mock(spec=Database)
        mock_db.execute.return_value = [{"id": 1}]

        df = adapter.read_table(source=mock_db, table="users", limit=10)

        assert isinstance(df, pd.DataFrame)
        call_args = mock_db.execute.call_args[0][0]
        assert "LIMIT" in call_args

    def test_write_table_with_database_append(self, adapter, sample_df):
        """Test writing table to database in append mode."""
        mock_db = Mock(spec=Database)

        adapter.write_table(data=sample_df, target=mock_db, table="users", mode="append")

        assert mock_db.set.called

    def test_write_table_with_database_overwrite(self, adapter, sample_df):
        """Test writing table to database in overwrite mode."""
        mock_db = Mock(spec=Database)

        adapter.write_table(data=sample_df, target=mock_db, table="users", mode="overwrite")

        # Should call delete first
        assert mock_db.execute.called
        assert mock_db.set.called


class TestDataFrameAdapterFactory:
    """Test DataFrameAdapterFactory."""

    def test_create_pandas_adapter(self):
        """Test creating Pandas adapter."""
        adapter = DataFrameAdapterFactory.create(engine=DataEngine.PANDAS)

        assert isinstance(adapter, PandasDataFrameAdapter)

    def test_create_auto_pandas(self):
        """Test auto-detection for Pandas DataFrame."""
        df = pd.DataFrame({"a": [1, 2, 3]})

        adapter = DataFrameAdapterFactory.create_auto(data=df)

        assert isinstance(adapter, PandasDataFrameAdapter)

    def test_create_with_settings(self):
        """Test creating adapter with settings."""
        settings = {"option": "value"}
        adapter = DataFrameAdapterFactory.create(engine=DataEngine.PANDAS, settings=settings)

        assert adapter.settings == settings

    def test_create_unsupported_engine(self):
        """Test creating adapter with unsupported engine."""
        # Use a mock engine that's not in the support map
        mock_engine = type("MockEngine", (), {"value": "mock"})()
        with pytest.raises((ValueError, AttributeError)):
            DataFrameAdapterFactory.create(engine=mock_engine)


# ============================================================================
# Strategy-specific Partition Tests
# ============================================================================


class TestPartitionStrategies:
    """Test different partition strategies."""

    @pytest.fixture
    def sample_df(self):
        """Create DataFrame with various data types."""
        return pd.DataFrame(
            {
                "timestamp": pd.date_range("2023-01-01", periods=50, freq="h"),
                "hash_key": list(range(50)),
                "range_key": list(range(50, 100)),
                "value": range(100, 150),
            }
        )

    def test_time_daily_strategy(self, sample_df):
        """Test TIME_DAILY partitioning."""
        with tempfile.TemporaryDirectory() as tmpdir:
            partitioner = PandasDataPartitioner(
                partition_key="timestamp", strategy=PartitionStrategy.TIME_DAILY, base_path=tmpdir
            )

            paths = partitioner.write_partitioned(sample_df, format="csv")

            assert len(paths) > 0

    def test_time_monthly_strategy(self, sample_df):
        """Test TIME_MONTHLY partitioning."""
        with tempfile.TemporaryDirectory() as tmpdir:
            partitioner = PandasDataPartitioner(
                partition_key="timestamp", strategy=PartitionStrategy.TIME_MONTHLY, base_path=tmpdir
            )

            paths = partitioner.write_partitioned(sample_df, format="csv")

            assert len(paths) > 0

    def test_hash_strategy(self, sample_df):
        """Test HASH partitioning."""
        with tempfile.TemporaryDirectory() as tmpdir:
            partitioner = PandasDataPartitioner(
                partition_key="hash_key", strategy=PartitionStrategy.HASH, base_path=tmpdir
            )

            paths = partitioner.write_partitioned(sample_df, format="csv")

            assert len(paths) > 0

    def test_range_strategy(self, sample_df):
        """Test RANGE partitioning."""
        with tempfile.TemporaryDirectory() as tmpdir:
            partitioner = PandasDataPartitioner(
                partition_key="range_key", strategy=PartitionStrategy.RANGE, base_path=tmpdir
            )

            paths = partitioner.write_partitioned(sample_df, format="csv")

            assert len(paths) > 0
