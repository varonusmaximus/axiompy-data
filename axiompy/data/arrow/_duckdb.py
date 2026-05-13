"""
DuckDB Arrow database implementation.

DuckDB has native Arrow support - no ADBC driver needed.

Requires: pip install duckdb

DuckDB excels at:
    - In-memory analytics on Arrow tables
    - Reading/writing Parquet, CSV, JSON
    - Joining data from multiple sources
    - Zero-copy data exchange with Arrow

Example:
    >>> from axiompy.data.arrow import ArrowDatabaseFactory, DuckDBArrowSettings
    >>>
    >>> settings = DuckDBArrowSettings()
    >>> db = ArrowDatabaseFactory.create(settings)
    >>> table = db.execute_arrow("SELECT * FROM 'data.parquet'")
"""

from typing import Any, Optional

import pyarrow as pa

from axiompy.decorators import LogExecutionTime
from axiompy.loggers import LoggerFactory
from axiompy.validators import ensure_not_none

from .base import ArrowDatabase
from .errors import ArrowConnectionError, ArrowQueryError
from .settings import DuckDBArrowSettings

logger = LoggerFactory.create_logger(__name__)


class DuckDBArrowDatabase(ArrowDatabase):
    """
    DuckDB implementation with native Arrow support.

    DuckDB provides zero-copy data exchange with Arrow, making it ideal for:
    - Analytical queries on columnar data
    - Reading files directly (Parquet, CSV, JSON)
    - In-memory data processing
    - Joining data from multiple sources

    Attributes:
        settings: DuckDB configuration settings
        _connection: Lazy-initialized DuckDB connection

    Example:
        >>> settings = DuckDBArrowSettings(database=":memory:")
        >>> db = DuckDBArrowDatabase(settings)
        >>>
        >>> # Query Parquet files directly
        >>> table = db.execute_arrow("SELECT * FROM 'data/*.parquet'")
        >>>
        >>> # Register and query Arrow tables
        >>> db.register_arrow_table("my_data", arrow_table)
        >>> result = db.execute_arrow("SELECT * FROM my_data WHERE x > 10")
    """

    def __init__(self, settings: DuckDBArrowSettings) -> None:
        """
        Initialize DuckDB Arrow database.

        Args:
            settings: DuckDB configuration settings

        Raises:
            ValueError: If settings is None
        """
        ensure_not_none(settings, "DuckDBArrowSettings required")
        self.settings = settings
        self._connection: Optional[Any] = None

    def _connect(self) -> Any:
        """
        Create DuckDB connection (lazy).

        Returns:
            DuckDB connection object

        Raises:
            ArrowConnectionError: If DuckDB is not installed
        """
        if self._connection is None:
            try:
                import duckdb
            except ImportError as e:
                raise ArrowConnectionError("duckdb not installed. Run: pip install duckdb") from e

            self._connection = duckdb.connect(
                self.settings.database,
                read_only=self.settings.read_only,
            )

            # Install/load extensions
            for ext in self.settings.extensions:
                self._connection.execute(f"INSTALL {ext}; LOAD {ext};")
                logger.debug(f"Loaded DuckDB extension: {ext}")

            logger.info(f"DuckDB connection established: {self.settings.database}")

        return self._connection

    @LogExecutionTime(logger)
    def execute_arrow(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
    ) -> pa.Table:
        """
        Execute SQL and return Arrow table.

        Args:
            sql: SQL query to execute
            params: Optional query parameters

        Returns:
            PyArrow Table with query results

        Raises:
            ArrowQueryError: If query execution fails
        """
        ensure_not_none(sql, "SQL cannot be None")

        try:
            conn = self._connect()
            result = conn.execute(sql, params) if params else conn.execute(sql)
            table = result.fetch_arrow_table()

            logger.info(f"Fetched {table.num_rows} rows, {table.nbytes / 1024 / 1024:.2f} MB")
            return table

        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise ArrowQueryError(f"Query execution failed: {e}") from e

    def execute(self, sql: str, params: Optional[dict[str, Any]] = None) -> None:
        """
        Execute SQL without returning results.

        Args:
            sql: SQL statement to execute
            params: Optional query parameters

        Raises:
            ArrowQueryError: If execution fails
        """
        try:
            conn = self._connect()
            if params:
                conn.execute(sql, params)
            else:
                conn.execute(sql)
        except Exception as e:
            logger.error(f"Execute failed: {e}")
            raise ArrowQueryError(f"Execute failed: {e}") from e

    def register_arrow_table(self, name: str, table: pa.Table) -> None:
        """
        Register Arrow table as virtual table.

        This enables querying in-memory Arrow data with SQL.

        Args:
            name: Virtual table name
            table: PyArrow Table to register

        Example:
            >>> import pyarrow as pa
            >>> table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
            >>> db.register_arrow_table("my_table", table)
            >>> result = db.execute_arrow("SELECT * FROM my_table WHERE id > 1")
        """
        ensure_not_none(name, "Table name required")
        ensure_not_none(table, "Arrow table required")

        conn = self._connect()
        conn.register(name, table)
        logger.info(f"Registered Arrow table '{name}' ({table.num_rows} rows)")

    def get_schema(self, table: str) -> pa.Schema:
        """
        Get table schema as Arrow schema.

        Args:
            table: Table name (can be schema-qualified)

        Returns:
            PyArrow Schema with column names and types
        """
        result = self.execute_arrow(f"SELECT * FROM {table} LIMIT 0")
        return result.schema

    def get_table_names(self, schema: Optional[str] = None) -> list[str]:
        """
        List table names in database.

        Args:
            schema: Optional schema to filter by (not used in DuckDB in-memory)

        Returns:
            List of table names
        """
        result = self.execute_arrow("SHOW TABLES")
        return result.column("name").to_pylist()

    def validate_connection(self) -> bool:
        """
        Validate connection is healthy.

        Returns:
            True if connection is valid, False otherwise
        """
        try:
            self.execute_arrow("SELECT 1")
            return True
        except Exception:
            return False

    def close(self) -> None:
        """Close DuckDB connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("DuckDB connection closed")

    # =========================================================================
    # DuckDB-specific convenience methods
    # =========================================================================

    def read_parquet(self, path: str) -> pa.Table:
        """
        Read Parquet file(s) directly as Arrow table.

        DuckDB can read from:
        - Local files: "/path/to/file.parquet"
        - Glob patterns: "/path/to/*.parquet"
        - S3: "s3://bucket/key.parquet" (requires httpfs extension)
        - HTTP: "https://example.com/file.parquet" (requires httpfs extension)

        Args:
            path: Path to Parquet file(s)

        Returns:
            PyArrow Table with file contents

        Example:
            >>> table = db.read_parquet("data/*.parquet")
            >>> table = db.read_parquet("s3://my-bucket/data.parquet")
        """
        return self.execute_arrow(f"SELECT * FROM read_parquet('{path}')")

    def read_csv(self, path: str, **options: Any) -> pa.Table:
        """
        Read CSV file(s) directly as Arrow table.

        Args:
            path: Path to CSV file(s)
            **options: DuckDB read_csv options (header, delim, etc.)

        Returns:
            PyArrow Table with file contents

        Example:
            >>> table = db.read_csv("data.csv", header=True)
            >>> table = db.read_csv("data/*.csv", delim="|")
        """
        if options:
            opts_str = ", ".join(f"{k}={repr(v)}" for k, v in options.items())
            sql = f"SELECT * FROM read_csv('{path}', {opts_str})"
        else:
            sql = f"SELECT * FROM read_csv_auto('{path}')"

        return self.execute_arrow(sql)

    def read_json(self, path: str) -> pa.Table:
        """
        Read JSON file(s) directly as Arrow table.

        Args:
            path: Path to JSON file(s)

        Returns:
            PyArrow Table with file contents

        Example:
            >>> table = db.read_json("data.json")
            >>> table = db.read_json("data/*.ndjson")
        """
        return self.execute_arrow(f"SELECT * FROM read_json_auto('{path}')")

    def write_parquet(self, sql: str, path: str) -> None:
        """
        Write query results to Parquet file.

        Args:
            sql: SQL query to execute
            path: Output Parquet file path

        Example:
            >>> db.write_parquet(
            ...     "SELECT * FROM events WHERE date = '2026-01-01'",
            ...     "output/events_2026-01-01.parquet"
            ... )
        """
        self.execute(f"COPY ({sql}) TO '{path}' (FORMAT PARQUET)")
        logger.info(f"Wrote query results to {path}")
