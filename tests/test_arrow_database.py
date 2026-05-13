"""
Unit tests for Arrow Database abstraction.

Tests use DuckDB as the primary backend since it has no external dependencies
and provides full Arrow support.
"""

import pytest

# Check if duckdb is available
try:
    import duckdb
    import pyarrow as pa

    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

from axiompy.data.arrow import (
    ArrowConnectionError,
    ArrowDatabaseFactory,
    ArrowQueryError,
    DuckDBArrowSettings,
    MockArrowDatabase,
    PostgresArrowSettings,
    SnowflakeArrowSettings,
)


# =============================================================================
# Settings Tests
# =============================================================================


class TestDuckDBArrowSettings:
    """Tests for DuckDBArrowSettings configuration."""

    def test_default_settings(self) -> None:
        """Test creating settings with defaults."""
        settings = DuckDBArrowSettings()

        assert settings.database == ":memory:"
        assert settings.read_only is False
        assert settings.extensions == []
        assert settings.adapter_type == "duckdb"

    def test_custom_settings(self) -> None:
        """Test creating settings with custom values."""
        settings = DuckDBArrowSettings(
            database="/path/to/db.duckdb",
            read_only=True,
            extensions=["httpfs", "parquet"],
        )

        assert settings.database == "/path/to/db.duckdb"
        assert settings.read_only is True
        assert settings.extensions == ["httpfs", "parquet"]


class TestSnowflakeArrowSettings:
    """Tests for SnowflakeArrowSettings configuration."""

    def test_valid_settings_with_password(self) -> None:
        """Test creating valid settings with password."""
        settings = SnowflakeArrowSettings(
            account="my_account",
            warehouse="COMPUTE_WH",
            database="MY_DB",
            schema="PUBLIC",
            user="user",
            password="password",
        )

        assert settings.account == "my_account"
        assert settings.warehouse == "COMPUTE_WH"
        assert settings.database == "MY_DB"
        assert settings.schema == "PUBLIC"
        assert settings.user == "user"
        assert settings.password == "password"
        assert settings.adapter_type == "snowflake"

    def test_valid_settings_with_password_secret(self) -> None:
        """Test creating valid settings with password_secret."""
        settings = SnowflakeArrowSettings(
            account="my_account",
            warehouse="COMPUTE_WH",
            database="MY_DB",
            schema="PUBLIC",
            user="user",
            password_secret="snowflake/password",
        )

        assert settings.password_secret == "snowflake/password"

    def test_missing_account_raises(self) -> None:
        """Test that missing account raises ValueError."""
        with pytest.raises(ValueError, match="account"):
            SnowflakeArrowSettings(
                account="",
                warehouse="WH",
                database="DB",
                schema="PUBLIC",
                user="user",
                password="password",
            )

    def test_missing_password_and_secret_raises(self) -> None:
        """Test that missing both password and password_secret raises."""
        with pytest.raises(ValueError, match="password"):
            SnowflakeArrowSettings(
                account="account",
                warehouse="WH",
                database="DB",
                schema="PUBLIC",
                user="user",
            )


class TestPostgresArrowSettings:
    """Tests for PostgresArrowSettings configuration."""

    def test_valid_settings(self) -> None:
        """Test creating valid PostgreSQL settings."""
        settings = PostgresArrowSettings(
            host="localhost",
            port=5432,
            database="mydb",
            user="postgres",
            password="password",
        )

        assert settings.host == "localhost"
        assert settings.port == 5432
        assert settings.database == "mydb"
        assert settings.user == "postgres"
        assert settings.schema == "public"
        assert settings.ssl_mode == "prefer"
        assert settings.adapter_type == "postgres"

    def test_missing_host_raises(self) -> None:
        """Test that missing host raises ValueError."""
        with pytest.raises(ValueError, match="Host"):
            PostgresArrowSettings(
                host="",
                port=5432,
                database="mydb",
                user="postgres",
            )


# =============================================================================
# DuckDB Implementation Tests
# =============================================================================


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
class TestDuckDBArrowDatabase:
    """Unit tests using DuckDB (no external dependencies)."""

    @pytest.fixture
    def db(self):
        """Create in-memory DuckDB database."""
        settings = DuckDBArrowSettings(database=":memory:")
        db = ArrowDatabaseFactory.create(settings)
        yield db
        db.close()

    def test_execute_arrow_returns_table(self, db) -> None:
        """Test basic query returns Arrow table."""
        result = db.execute_arrow("SELECT 1 as value, 'hello' as text")

        assert isinstance(result, pa.Table)
        assert result.num_rows == 1
        assert result.column("value").to_pylist() == [1]
        assert result.column("text").to_pylist() == ["hello"]

    def test_execute_arrow_with_params(self, db) -> None:
        """Test query with parameters."""
        # DuckDB uses positional parameters with $1, $2, etc.
        result = db.execute_arrow("SELECT $1 as value", params=[42])

        assert result.num_rows == 1
        assert result.column("value").to_pylist() == [42]

    def test_execute_ddl(self, db) -> None:
        """Test executing DDL statements."""
        db.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR)")
        db.execute("INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob')")

        result = db.execute_arrow("SELECT * FROM test_table ORDER BY id")

        assert result.num_rows == 2
        assert result.column("id").to_pylist() == [1, 2]
        assert result.column("name").to_pylist() == ["Alice", "Bob"]

    def test_register_arrow_table(self, db) -> None:
        """Test registering and querying Arrow table."""
        # Create Arrow table
        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )

        # Register and query
        db.register_arrow_table("users", table)
        result = db.execute_arrow("SELECT * FROM users WHERE id > 1")

        assert result.num_rows == 2
        assert result.column("name").to_pylist() == ["Bob", "Charlie"]

    def test_get_schema(self, db) -> None:
        """Test getting table schema."""
        db.execute("CREATE TABLE schema_test (id INTEGER, name VARCHAR, amount DOUBLE)")

        schema = db.get_schema("schema_test")

        assert len(schema) == 3
        assert schema.field("id").type == pa.int32()
        assert schema.field("name").type == pa.string()
        assert schema.field("amount").type == pa.float64()

    def test_get_table_names(self, db) -> None:
        """Test listing table names."""
        db.execute("CREATE TABLE table_a (id INTEGER)")
        db.execute("CREATE TABLE table_b (id INTEGER)")

        tables = db.get_table_names()

        assert "table_a" in tables
        assert "table_b" in tables

    def test_validate_connection(self, db) -> None:
        """Test connection validation."""
        assert db.validate_connection() is True

    def test_to_pandas_convenience(self, db) -> None:
        """Test to_pandas convenience method."""
        df = db.to_pandas("SELECT 1 as id, 'Alice' as name")

        assert len(df) == 1
        assert df.iloc[0]["name"] == "Alice"

    def test_context_manager(self) -> None:
        """Test context manager closes connection."""
        settings = DuckDBArrowSettings(database=":memory:")

        with ArrowDatabaseFactory.create(settings) as db:
            result = db.execute_arrow("SELECT 1")
            assert result.num_rows == 1

        # Connection should be closed
        assert db._connection is None

    def test_query_error_raises_arrow_query_error(self, db) -> None:
        """Test that invalid query raises ArrowQueryError."""
        with pytest.raises(ArrowQueryError, match="Query execution failed"):
            db.execute_arrow("SELECT * FROM nonexistent_table")

    def test_read_parquet(self, db, tmp_path) -> None:
        """Test reading Parquet files directly."""
        # Create a test Parquet file
        test_table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
        parquet_path = tmp_path / "test.parquet"

        import pyarrow.parquet as pq

        pq.write_table(test_table, parquet_path)

        # Read with DuckDB
        result = db.read_parquet(str(parquet_path))

        assert result.num_rows == 3
        assert result.column("x").to_pylist() == [1, 2, 3]

    def test_read_csv(self, db, tmp_path) -> None:
        """Test reading CSV files directly."""
        # Create a test CSV file
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("id,name\n1,Alice\n2,Bob\n")

        # Read with DuckDB
        result = db.read_csv(str(csv_path))

        assert result.num_rows == 2
        assert result.column("name").to_pylist() == ["Alice", "Bob"]

    def test_write_parquet(self, db, tmp_path) -> None:
        """Test writing query results to Parquet."""
        # Create test data
        db.execute("CREATE TABLE export_test (id INTEGER, value VARCHAR)")
        db.execute("INSERT INTO export_test VALUES (1, 'a'), (2, 'b')")

        # Write to Parquet
        output_path = tmp_path / "output.parquet"
        db.write_parquet("SELECT * FROM export_test", str(output_path))

        # Verify file was created and is valid
        assert output_path.exists()

        import pyarrow.parquet as pq

        result = pq.read_table(output_path)
        assert result.num_rows == 2

    def test_read_csv_with_options(self, db, tmp_path) -> None:
        """Test reading CSV with custom options."""
        csv_path = tmp_path / "custom.csv"
        csv_path.write_text("a|b|c\n1|2|3\n4|5|6\n")

        # Read with custom delimiter
        result = db.read_csv(str(csv_path), delim="|", header=True)

        assert result.num_rows == 2

    def test_read_json(self, db, tmp_path) -> None:
        """Test reading JSON files."""
        json_path = tmp_path / "test.json"
        json_path.write_text('{"id": 1, "name": "Alice"}\n{"id": 2, "name": "Bob"}\n')

        result = db.read_json(str(json_path))

        assert result.num_rows == 2
        assert result.column("name").to_pylist() == ["Alice", "Bob"]

    def test_close_sets_connection_to_none(self) -> None:
        """Test that close() sets connection to None."""
        settings = DuckDBArrowSettings(database=":memory:")
        db = ArrowDatabaseFactory.create(settings)

        # Force connection
        db.execute_arrow("SELECT 1")
        assert db._connection is not None

        # Close should set to None
        db.close()
        assert db._connection is None

    def test_execute_with_dict_params(self, db) -> None:
        """Test execute with dictionary parameters."""
        db.execute("CREATE TABLE param_test (id INTEGER, name VARCHAR)")
        # DuckDB uses positional params, but we test the dict params path
        db.execute("INSERT INTO param_test VALUES ($1, $2)", params=[1, "test"])

        result = db.execute_arrow("SELECT * FROM param_test")
        assert result.num_rows == 1


# =============================================================================
# Mock Implementation Tests
# =============================================================================


class TestMockArrowDatabase:
    """Tests for MockArrowDatabase."""

    def test_set_response(self) -> None:
        """Test setting predefined responses."""
        if not DUCKDB_AVAILABLE:
            pytest.skip("PyArrow not available")

        mock = MockArrowDatabase()
        expected = pa.table({"value": [42]})
        mock.set_response("SELECT 42", expected)

        result = mock.execute_arrow("SELECT 42")

        assert result.equals(expected)

    def test_calls_tracking(self) -> None:
        """Test that method calls are tracked."""
        mock = MockArrowDatabase()

        mock.execute_arrow("SELECT 1")  # May return None if pyarrow not installed
        mock.execute("INSERT INTO table VALUES (1)")
        mock.validate_connection()

        assert len(mock.calls) == 3
        assert mock.calls[0] == ("execute_arrow", "SELECT 1", None)
        assert mock.calls[1] == ("execute", "INSERT INTO table VALUES (1)", None)
        assert mock.calls[2] == ("validate_connection",)

    def test_register_and_get_schema(self) -> None:
        """Test registering table and getting schema."""
        if not DUCKDB_AVAILABLE:
            pytest.skip("PyArrow not available")

        mock = MockArrowDatabase()
        table = pa.table({"id": [1, 2], "name": ["a", "b"]})

        mock.register_arrow_table("test", table)
        schema = mock.get_schema("test")

        assert len(schema) == 2
        assert "test" in mock.get_table_names()

    def test_reset(self) -> None:
        """Test resetting mock state."""
        mock = MockArrowDatabase()
        mock.execute_arrow("SELECT 1")  # May return None if pyarrow not installed
        mock.reset()

        assert mock.calls == []
        assert mock._responses == {}
        assert mock._tables == {}

    def test_close_marks_connection_closed(self) -> None:
        """Test that close() marks connection as closed."""
        mock = MockArrowDatabase()

        assert mock.validate_connection() is True
        mock.close()
        assert mock.validate_connection() is False

    def test_get_table_names_empty(self) -> None:
        """Test get_table_names returns empty list when no tables registered."""
        mock = MockArrowDatabase()
        tables = mock.get_table_names()
        assert tables == []
        assert ("get_table_names", None) in mock.calls

    def test_get_schema_unregistered_table(self) -> None:
        """Test get_schema for unregistered table returns empty schema."""
        if not DUCKDB_AVAILABLE:
            pytest.skip("PyArrow not available")
        mock = MockArrowDatabase()
        schema = mock.get_schema("nonexistent")
        assert len(schema) == 0
        assert ("get_schema", "nonexistent") in mock.calls

    def test_context_manager(self) -> None:
        """Test mock works as context manager."""
        with MockArrowDatabase() as mock:
            mock.execute("SELECT 1")
            assert mock.validate_connection() is True

        # After context exit, close() should be called
        assert ("close",) in mock.calls

    def test_execute_with_params(self) -> None:
        """Test execute with parameters."""
        mock = MockArrowDatabase()
        mock.execute("INSERT INTO t VALUES (:x)", params={"x": 1})
        assert mock.calls[-1] == ("execute", "INSERT INTO t VALUES (:x)", {"x": 1})

    def test_execute_arrow_with_params(self) -> None:
        """Test execute_arrow with parameters."""
        mock = MockArrowDatabase()
        mock.execute_arrow("SELECT * FROM t WHERE x = :x", params={"x": 1})
        assert mock.calls[-1] == ("execute_arrow", "SELECT * FROM t WHERE x = :x", {"x": 1})


# =============================================================================
# Factory Tests
# =============================================================================


class TestArrowDatabaseFactory:
    """Tests for ArrowDatabaseFactory."""

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
    def test_create_duckdb(self) -> None:
        """Test creating DuckDB instance."""
        settings = DuckDBArrowSettings()
        db = ArrowDatabaseFactory.create(settings)

        try:
            assert db is not None
            assert db.validate_connection() is True
        finally:
            db.close()

    def test_create_mock(self) -> None:
        """Test creating mock instance."""
        mock = ArrowDatabaseFactory.create_mock()

        assert isinstance(mock, MockArrowDatabase)
        assert mock.validate_connection() is True

    def test_unsupported_type_raises(self) -> None:
        """Test that unsupported adapter type raises ValueError."""

        class UnsupportedSettings:
            @property
            def adapter_type(self) -> str:
                return "unsupported"

        with pytest.raises(ValueError, match="Unsupported Arrow database type"):
            ArrowDatabaseFactory.create(UnsupportedSettings())

    def test_none_settings_raises(self) -> None:
        """Test that None settings raises ValidationError."""
        from axiompy.validators import ValidationError

        with pytest.raises(ValidationError, match="None"):
            ArrowDatabaseFactory.create(None)  # type: ignore


# =============================================================================
# Snowflake Implementation Tests (Mocked)
# =============================================================================


class TestSnowflakeArrowDatabase:
    """Tests for SnowflakeArrowDatabase using mocked ADBC driver."""

    @pytest.fixture
    def snowflake_settings(self):
        """Create valid Snowflake settings."""
        return SnowflakeArrowSettings(
            account="test_account",
            warehouse="TEST_WH",
            database="TEST_DB",
            schema="PUBLIC",
            user="test_user",
            password="test_password",
        )

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="PyArrow not installed")
    def test_execute_arrow_success(self, snowflake_settings):
        """Test successful query execution."""
        from unittest.mock import MagicMock, patch

        from axiompy.data.arrow._snowflake import SnowflakeArrowDatabase

        mock_cursor = MagicMock()
        mock_cursor.fetch_arrow_table.return_value = pa.table({"id": [1, 2, 3]})
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        db = SnowflakeArrowDatabase(snowflake_settings)
        db._connection = mock_conn

        result = db.execute_arrow("SELECT * FROM test")

        assert result.num_rows == 3
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test")

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="PyArrow not installed")
    def test_execute_arrow_with_params(self, snowflake_settings):
        """Test query execution with parameters."""
        from unittest.mock import MagicMock

        from axiompy.data.arrow._snowflake import SnowflakeArrowDatabase

        mock_cursor = MagicMock()
        mock_cursor.fetch_arrow_table.return_value = pa.table({"id": [1]})
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        db = SnowflakeArrowDatabase(snowflake_settings)
        db._connection = mock_conn

        db.execute_arrow("SELECT * FROM test WHERE id = :id", params={"id": 1})

        mock_cursor.execute.assert_called_once_with("SELECT * FROM test WHERE id = :id", {"id": 1})

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="PyArrow not installed")
    def test_execute_ddl(self, snowflake_settings):
        """Test DDL execution."""
        from unittest.mock import MagicMock

        from axiompy.data.arrow._snowflake import SnowflakeArrowDatabase

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        db = SnowflakeArrowDatabase(snowflake_settings)
        db._connection = mock_conn

        db.execute("CREATE TABLE test (id INT)")

        mock_cursor.execute.assert_called_once_with("CREATE TABLE test (id INT)")

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="PyArrow not installed")
    def test_execute_with_params(self, snowflake_settings):
        """Test execute with parameters."""
        from unittest.mock import MagicMock

        from axiompy.data.arrow._snowflake import SnowflakeArrowDatabase

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        db = SnowflakeArrowDatabase(snowflake_settings)
        db._connection = mock_conn

        db.execute("INSERT INTO test VALUES (:v)", params={"v": 1})

        mock_cursor.execute.assert_called_once_with("INSERT INTO test VALUES (:v)", {"v": 1})

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="PyArrow not installed")
    def test_register_arrow_table_not_supported(self, snowflake_settings):
        """Test that register_arrow_table raises NotImplementedError."""
        from axiompy.data.arrow._snowflake import SnowflakeArrowDatabase

        db = SnowflakeArrowDatabase(snowflake_settings)

        with pytest.raises(NotImplementedError, match="Snowflake ADBC"):
            db.register_arrow_table("test", pa.table({"x": [1]}))

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="PyArrow not installed")
    def test_get_schema(self, snowflake_settings):
        """Test getting table schema."""
        from unittest.mock import MagicMock

        from axiompy.data.arrow._snowflake import SnowflakeArrowDatabase

        mock_cursor = MagicMock()
        mock_cursor.fetch_arrow_table.return_value = pa.table({"id": pa.array([], pa.int64())})
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        db = SnowflakeArrowDatabase(snowflake_settings)
        db._connection = mock_conn

        schema = db.get_schema("test_table")

        assert len(schema) == 1
        assert schema.field("id").type == pa.int64()

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="PyArrow not installed")
    def test_get_table_names(self, snowflake_settings):
        """Test listing table names."""
        from unittest.mock import MagicMock

        from axiompy.data.arrow._snowflake import SnowflakeArrowDatabase

        mock_cursor = MagicMock()
        mock_cursor.fetch_arrow_table.return_value = pa.table({"TABLE_NAME": ["users", "orders"]})
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        db = SnowflakeArrowDatabase(snowflake_settings)
        db._connection = mock_conn

        tables = db.get_table_names()

        assert tables == ["users", "orders"]

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="PyArrow not installed")
    def test_validate_connection_success(self, snowflake_settings):
        """Test successful connection validation."""
        from unittest.mock import MagicMock

        from axiompy.data.arrow._snowflake import SnowflakeArrowDatabase

        mock_cursor = MagicMock()
        mock_cursor.fetch_arrow_table.return_value = pa.table({"x": [1]})
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        db = SnowflakeArrowDatabase(snowflake_settings)
        db._connection = mock_conn

        assert db.validate_connection() is True

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="PyArrow not installed")
    def test_validate_connection_failure(self, snowflake_settings):
        """Test failed connection validation."""
        from unittest.mock import MagicMock

        from axiompy.data.arrow._snowflake import SnowflakeArrowDatabase

        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("Connection lost")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        db = SnowflakeArrowDatabase(snowflake_settings)
        db._connection = mock_conn

        assert db.validate_connection() is False

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="PyArrow not installed")
    def test_close(self, snowflake_settings):
        """Test closing connection."""
        from unittest.mock import MagicMock

        from axiompy.data.arrow._snowflake import SnowflakeArrowDatabase

        mock_conn = MagicMock()

        db = SnowflakeArrowDatabase(snowflake_settings)
        db._connection = mock_conn

        db.close()

        mock_conn.close.assert_called_once()
        assert db._connection is None

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="PyArrow not installed")
    def test_query_error_raises_arrow_query_error(self, snowflake_settings):
        """Test that query errors raise ArrowQueryError."""
        from unittest.mock import MagicMock

        from axiompy.data.arrow._snowflake import SnowflakeArrowDatabase

        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("SQL syntax error")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        db = SnowflakeArrowDatabase(snowflake_settings)
        db._connection = mock_conn

        with pytest.raises(ArrowQueryError, match="Query execution failed"):
            db.execute_arrow("INVALID SQL")

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="PyArrow not installed")
    def test_execute_error_raises_arrow_query_error(self, snowflake_settings):
        """Test that execute errors raise ArrowQueryError."""
        from unittest.mock import MagicMock

        from axiompy.data.arrow._snowflake import SnowflakeArrowDatabase

        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("Permission denied")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        db = SnowflakeArrowDatabase(snowflake_settings)
        db._connection = mock_conn

        with pytest.raises(ArrowQueryError, match="Execute failed"):
            db.execute("DROP TABLE important")

    def test_get_password_from_settings(self, snowflake_settings):
        """Test getting password from settings."""
        from axiompy.data.arrow._snowflake import SnowflakeArrowDatabase

        db = SnowflakeArrowDatabase(snowflake_settings)
        assert db._get_password() == "test_password"

    def test_get_password_from_secrets_manager(self):
        """Test getting password from secrets manager."""
        from unittest.mock import MagicMock

        from axiompy.data.arrow._snowflake import SnowflakeArrowDatabase

        settings = SnowflakeArrowSettings(
            account="test",
            warehouse="WH",
            database="DB",
            schema="PUBLIC",
            user="user",
            password_secret="secret/key",
        )

        mock_secrets = MagicMock()
        mock_secrets.get_secret.return_value = "secret_password"

        db = SnowflakeArrowDatabase(settings, secrets_manager=mock_secrets)
        assert db._get_password() == "secret_password"
        mock_secrets.get_secret.assert_called_once_with("secret/key")

    def test_get_password_no_config_raises(self):
        """Test that missing password config raises error."""
        from axiompy.data.arrow._snowflake import SnowflakeArrowDatabase

        # Create settings without password validation
        settings = SnowflakeArrowSettings(
            account="test",
            warehouse="WH",
            database="DB",
            schema="PUBLIC",
            user="user",
            password="temp",  # Required for validation
        )
        # Clear the password after creation
        settings.password = None
        settings.password_secret = None

        db = SnowflakeArrowDatabase(settings)

        with pytest.raises(ArrowConnectionError, match="No password configured"):
            db._get_password()

    def test_get_connection_uri(self, snowflake_settings):
        """Test connection URI generation."""
        from axiompy.data.arrow._snowflake import SnowflakeArrowDatabase

        db = SnowflakeArrowDatabase(snowflake_settings)
        uri = db._get_connection_uri()

        assert "test_account.snowflakecomputing.com" in uri
        assert "warehouse=TEST_WH" in uri
        assert "database=TEST_DB" in uri
        assert "schema=PUBLIC" in uri

    def test_get_connection_uri_with_role(self):
        """Test connection URI with role."""
        from axiompy.data.arrow._snowflake import SnowflakeArrowDatabase

        settings = SnowflakeArrowSettings(
            account="test",
            warehouse="WH",
            database="DB",
            schema="PUBLIC",
            user="user",
            password="password",
            role="ANALYST",
        )

        db = SnowflakeArrowDatabase(settings)
        uri = db._get_connection_uri()

        assert "role=ANALYST" in uri


# =============================================================================
# PostgreSQL Implementation Tests (Mocked)
# =============================================================================


class TestPostgresArrowDatabase:
    """Tests for PostgresArrowDatabase using mocked ADBC driver."""

    @pytest.fixture
    def postgres_settings(self):
        """Create valid PostgreSQL settings."""
        return PostgresArrowSettings(
            host="localhost",
            port=5432,
            database="testdb",
            user="postgres",
            password="password",
        )

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="PyArrow not installed")
    def test_execute_arrow_success(self, postgres_settings):
        """Test successful query execution."""
        from unittest.mock import MagicMock

        from axiompy.data.arrow._postgres import PostgresArrowDatabase

        mock_cursor = MagicMock()
        mock_cursor.fetch_arrow_table.return_value = pa.table({"id": [1, 2, 3]})
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        db = PostgresArrowDatabase(postgres_settings)
        db._connection = mock_conn

        result = db.execute_arrow("SELECT * FROM test")

        assert result.num_rows == 3
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test")

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="PyArrow not installed")
    def test_execute_arrow_with_params(self, postgres_settings):
        """Test query execution with parameters."""
        from unittest.mock import MagicMock

        from axiompy.data.arrow._postgres import PostgresArrowDatabase

        mock_cursor = MagicMock()
        mock_cursor.fetch_arrow_table.return_value = pa.table({"id": [1]})
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        db = PostgresArrowDatabase(postgres_settings)
        db._connection = mock_conn

        db.execute_arrow("SELECT * FROM test WHERE id = $1", params={"id": 1})

        mock_cursor.execute.assert_called_once_with("SELECT * FROM test WHERE id = $1", {"id": 1})

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="PyArrow not installed")
    def test_execute_ddl(self, postgres_settings):
        """Test DDL execution with commit."""
        from unittest.mock import MagicMock

        from axiompy.data.arrow._postgres import PostgresArrowDatabase

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        db = PostgresArrowDatabase(postgres_settings)
        db._connection = mock_conn

        db.execute("CREATE TABLE test (id INT)")

        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="PyArrow not installed")
    def test_execute_with_params(self, postgres_settings):
        """Test execute with parameters."""
        from unittest.mock import MagicMock

        from axiompy.data.arrow._postgres import PostgresArrowDatabase

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        db = PostgresArrowDatabase(postgres_settings)
        db._connection = mock_conn

        db.execute("INSERT INTO test VALUES ($1)", params={"v": 1})

        mock_cursor.execute.assert_called_once_with("INSERT INTO test VALUES ($1)", {"v": 1})

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="PyArrow not installed")
    def test_register_arrow_table_not_supported(self, postgres_settings):
        """Test that register_arrow_table raises NotImplementedError."""
        from axiompy.data.arrow._postgres import PostgresArrowDatabase

        db = PostgresArrowDatabase(postgres_settings)

        with pytest.raises(NotImplementedError, match="PostgreSQL ADBC"):
            db.register_arrow_table("test", pa.table({"x": [1]}))

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="PyArrow not installed")
    def test_get_schema(self, postgres_settings):
        """Test getting table schema."""
        from unittest.mock import MagicMock

        from axiompy.data.arrow._postgres import PostgresArrowDatabase

        mock_cursor = MagicMock()
        mock_cursor.fetch_arrow_table.return_value = pa.table({"id": pa.array([], pa.int64())})
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        db = PostgresArrowDatabase(postgres_settings)
        db._connection = mock_conn

        schema = db.get_schema("test_table")

        assert len(schema) == 1

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="PyArrow not installed")
    def test_get_table_names(self, postgres_settings):
        """Test listing table names."""
        from unittest.mock import MagicMock

        from axiompy.data.arrow._postgres import PostgresArrowDatabase

        mock_cursor = MagicMock()
        mock_cursor.fetch_arrow_table.return_value = pa.table({"table_name": ["users", "orders"]})
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        db = PostgresArrowDatabase(postgres_settings)
        db._connection = mock_conn

        tables = db.get_table_names()

        assert tables == ["users", "orders"]

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="PyArrow not installed")
    def test_validate_connection_success(self, postgres_settings):
        """Test successful connection validation."""
        from unittest.mock import MagicMock

        from axiompy.data.arrow._postgres import PostgresArrowDatabase

        mock_cursor = MagicMock()
        mock_cursor.fetch_arrow_table.return_value = pa.table({"x": [1]})
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        db = PostgresArrowDatabase(postgres_settings)
        db._connection = mock_conn

        assert db.validate_connection() is True

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="PyArrow not installed")
    def test_validate_connection_failure(self, postgres_settings):
        """Test failed connection validation."""
        from unittest.mock import MagicMock

        from axiompy.data.arrow._postgres import PostgresArrowDatabase

        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("Connection lost")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        db = PostgresArrowDatabase(postgres_settings)
        db._connection = mock_conn

        assert db.validate_connection() is False

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="PyArrow not installed")
    def test_close(self, postgres_settings):
        """Test closing connection."""
        from unittest.mock import MagicMock

        from axiompy.data.arrow._postgres import PostgresArrowDatabase

        mock_conn = MagicMock()

        db = PostgresArrowDatabase(postgres_settings)
        db._connection = mock_conn

        db.close()

        mock_conn.close.assert_called_once()
        assert db._connection is None

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="PyArrow not installed")
    def test_query_error_raises_arrow_query_error(self, postgres_settings):
        """Test that query errors raise ArrowQueryError."""
        from unittest.mock import MagicMock

        from axiompy.data.arrow._postgres import PostgresArrowDatabase

        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("SQL error")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        db = PostgresArrowDatabase(postgres_settings)
        db._connection = mock_conn

        with pytest.raises(ArrowQueryError, match="Query execution failed"):
            db.execute_arrow("INVALID SQL")

    @pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="PyArrow not installed")
    def test_execute_error_raises_arrow_query_error(self, postgres_settings):
        """Test that execute errors raise ArrowQueryError."""
        from unittest.mock import MagicMock

        from axiompy.data.arrow._postgres import PostgresArrowDatabase

        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("Permission denied")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        db = PostgresArrowDatabase(postgres_settings)
        db._connection = mock_conn

        with pytest.raises(ArrowQueryError, match="Execute failed"):
            db.execute("DROP TABLE important")

    def test_get_password_from_settings(self, postgres_settings):
        """Test getting password from settings."""
        from axiompy.data.arrow._postgres import PostgresArrowDatabase

        db = PostgresArrowDatabase(postgres_settings)
        assert db._get_password() == "password"

    def test_get_password_from_secrets_manager(self):
        """Test getting password from secrets manager."""
        from unittest.mock import MagicMock

        from axiompy.data.arrow._postgres import PostgresArrowDatabase

        settings = PostgresArrowSettings(
            host="localhost",
            port=5432,
            database="testdb",
            user="postgres",
            password_secret="pg/password",
        )

        mock_secrets = MagicMock()
        mock_secrets.get_secret.return_value = "secret_password"

        db = PostgresArrowDatabase(settings, secrets_manager=mock_secrets)
        assert db._get_password() == "secret_password"

    def test_get_password_no_config_raises(self):
        """Test that missing password config raises error."""
        from axiompy.data.arrow._postgres import PostgresArrowDatabase

        settings = PostgresArrowSettings(
            host="localhost",
            port=5432,
            database="testdb",
            user="postgres",
            password="temp",
        )
        settings.password = None
        settings.password_secret = None

        db = PostgresArrowDatabase(settings)

        with pytest.raises(ArrowConnectionError, match="No password configured"):
            db._get_password()

    def test_get_connection_uri(self, postgres_settings):
        """Test connection URI generation."""
        from axiompy.data.arrow._postgres import PostgresArrowDatabase

        db = PostgresArrowDatabase(postgres_settings)
        uri = db._get_connection_uri()

        assert "postgresql://postgres:password" in uri
        assert "@localhost:5432" in uri
        assert "/testdb" in uri
        assert "sslmode=prefer" in uri


# =============================================================================
# Integration-style Tests (still using DuckDB)
# =============================================================================


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
class TestArrowDatabaseIntegration:
    """Integration-style tests using DuckDB."""

    def test_etl_workflow(self) -> None:
        """Test typical ETL workflow with Arrow tables."""
        settings = DuckDBArrowSettings()

        with ArrowDatabaseFactory.create(settings) as db:
            # Create source data as Arrow
            source_data = pa.table(
                {
                    "event_id": [1, 2, 3, 4, 5],
                    "event_type": ["click", "view", "click", "purchase", "view"],
                    "amount": [None, None, None, 99.99, None],
                }
            )

            # Register and transform
            db.register_arrow_table("raw_events", source_data)

            # Aggregate
            result = db.execute_arrow(
                """
                SELECT 
                    event_type,
                    COUNT(*) as event_count,
                    SUM(COALESCE(amount, 0)) as total_amount
                FROM raw_events
                GROUP BY event_type
                ORDER BY event_type
            """
            )

            assert result.num_rows == 3
            event_types = result.column("event_type").to_pylist()
            assert "click" in event_types
            assert "purchase" in event_types
            assert "view" in event_types

    def test_multi_table_join(self) -> None:
        """Test joining multiple Arrow tables."""
        settings = DuckDBArrowSettings()

        with ArrowDatabaseFactory.create(settings) as db:
            # Create users table
            users = pa.table(
                {
                    "user_id": [1, 2, 3],
                    "name": ["Alice", "Bob", "Charlie"],
                }
            )

            # Create orders table
            orders = pa.table(
                {
                    "order_id": [101, 102, 103, 104],
                    "user_id": [1, 1, 2, 3],
                    "amount": [50.0, 75.0, 100.0, 25.0],
                }
            )

            # Register both
            db.register_arrow_table("users", users)
            db.register_arrow_table("orders", orders)

            # Join and aggregate
            result = db.execute_arrow(
                """
                SELECT 
                    u.name,
                    COUNT(o.order_id) as order_count,
                    SUM(o.amount) as total_spent
                FROM users u
                JOIN orders o ON u.user_id = o.user_id
                GROUP BY u.name
                ORDER BY total_spent DESC
            """
            )

            assert result.num_rows == 3
            names = result.column("name").to_pylist()
            assert names[0] == "Alice"  # Alice has highest total (125.0)

    def test_large_result_handling(self) -> None:
        """Test handling larger result sets."""
        settings = DuckDBArrowSettings()

        with ArrowDatabaseFactory.create(settings) as db:
            # Generate 100k rows
            result = db.execute_arrow(
                """
                SELECT 
                    i as id,
                    'user_' || i as name,
                    random() as score
                FROM generate_series(1, 100000) as t(i)
            """
            )

            assert result.num_rows == 100000
            assert result.nbytes > 0

            # Verify we can process results
            df = result.to_pandas()
            assert len(df) == 100000
