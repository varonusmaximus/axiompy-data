"""
Snowflake Arrow database implementation using ADBC.

Requires: pip install adbc-driver-snowflake

ADBC (Arrow Database Connectivity) enables streaming results directly as
Arrow tables, bypassing row-by-row iteration for efficient bulk transfer.

Example:
    >>> from axiompy.data.arrow import ArrowDatabaseFactory, SnowflakeArrowSettings
    >>>
    >>> settings = SnowflakeArrowSettings(
    ...     account="my_account",
    ...     warehouse="COMPUTE_WH",
    ...     database="MY_DB",
    ...     schema="PUBLIC",
    ...     user="user",
    ...     password="password",
    ... )
    >>> db = ArrowDatabaseFactory.create(settings)
    >>> table = db.execute_arrow("SELECT * FROM events LIMIT 1000000")
"""

from typing import Any, Optional

import pyarrow as pa

from axiompy.decorators import LogExecutionTime, Retry
from axiompy.loggers import LoggerFactory
from axiompy.validators import ensure_not_none

from .base import ArrowDatabase
from .errors import ArrowConnectionError, ArrowQueryError
from .settings import SnowflakeArrowSettings

logger = LoggerFactory.create_logger(__name__)


class SnowflakeArrowDatabase(ArrowDatabase):
    """
    Snowflake implementation using ADBC for columnar transfer.

    ADBC (Arrow Database Connectivity) enables streaming results
    directly as Arrow tables, bypassing row-by-row iteration.

    Benefits:
        - Columnar transfer: Data stays in columnar format
        - Zero-copy: Minimal data transformation overhead
        - Streaming: Large results can be processed in batches
        - Native types: Arrow types map directly to Snowflake types

    Attributes:
        settings: Snowflake configuration settings
        secrets: Optional secrets manager for credential loading
        _connection: Lazy-initialized ADBC connection
        _password: Cached password

    Example:
        >>> settings = SnowflakeArrowSettings(
        ...     account="my_account",
        ...     warehouse="COMPUTE_WH",
        ...     database="MY_DB",
        ...     schema="PUBLIC",
        ...     user="user",
        ...     password="password",
        ... )
        >>> db = SnowflakeArrowDatabase(settings)
        >>> table = db.execute_arrow("SELECT * FROM events WHERE date = '2026-01-01'")
        >>> print(f"Fetched {table.num_rows} rows")
    """

    def __init__(
        self,
        settings: SnowflakeArrowSettings,
        secrets_manager: Optional[Any] = None,
    ) -> None:
        """
        Initialize Snowflake Arrow database.

        Args:
            settings: Snowflake configuration settings
            secrets_manager: Optional SecretsManager for credential loading

        Raises:
            ValueError: If settings is None
        """
        ensure_not_none(settings, "SnowflakeArrowSettings required")
        self.settings = settings
        self.secrets = secrets_manager
        self._connection: Optional[Any] = None
        self._password: Optional[str] = None

    def _get_password(self) -> str:
        """
        Load password from secrets manager or settings.

        Returns:
            Password string

        Raises:
            ArrowConnectionError: If no password is configured
        """
        if self._password is None:
            if self.settings.password:
                self._password = self.settings.password
            elif self.secrets and self.settings.password_secret:
                self._password = self.secrets.get_secret(self.settings.password_secret)
            else:
                raise ArrowConnectionError("No password configured")
        return self._password

    def _get_connection_uri(self) -> str:
        """
        Build Snowflake ADBC connection URI.

        Returns:
            Connection URI string
        """
        uri = (
            f"{self.settings.account}.snowflakecomputing.com"
            f"?warehouse={self.settings.warehouse}"
            f"&database={self.settings.database}"
            f"&schema={self.settings.schema}"
        )
        if self.settings.role:
            uri += f"&role={self.settings.role}"
        return uri

    @Retry(logger, max_attempts=3, delay=1.0)
    def _connect(self) -> Any:
        """
        Establish ADBC connection (lazy, with retry).

        Returns:
            ADBC connection object

        Raises:
            ArrowConnectionError: If connection fails or driver not installed
        """
        if self._connection is None:
            try:
                import adbc_driver_snowflake.dbapi as snowflake_dbapi
            except ImportError as e:
                raise ArrowConnectionError(
                    "adbc-driver-snowflake not installed. Run: pip install adbc-driver-snowflake"
                ) from e

            logger.info(f"Connecting to Snowflake: {self.settings.account}")
            self._connection = snowflake_dbapi.connect(
                self._get_connection_uri(),
                db_kwargs={
                    "username": self.settings.user,
                    "password": self._get_password(),
                    "adbc.snowflake.sql.client_option.arrow_batches": "true",
                },
            )
            logger.info("Snowflake connection established")
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
            cursor = conn.cursor()

            # Execute query
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            # Fetch as Arrow
            table = cursor.fetch_arrow_table()

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
            cursor = conn.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
        except Exception as e:
            logger.error(f"Execute failed: {e}")
            raise ArrowQueryError(f"Execute failed: {e}") from e

    def register_arrow_table(self, name: str, table: pa.Table) -> None:
        """
        Not supported for Snowflake ADBC.

        Snowflake ADBC does not support registering in-memory Arrow tables.
        Use DuckDB for this functionality.

        Args:
            name: Table name (unused)
            table: Arrow table (unused)

        Raises:
            NotImplementedError: Always raised
        """
        raise NotImplementedError(
            "Snowflake ADBC does not support registering in-memory Arrow tables. "
            "Use DuckDB for this functionality."
        )

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
        List table names in schema.

        Args:
            schema: Schema to list tables from (defaults to settings.schema)

        Returns:
            List of table names
        """
        schema = schema or self.settings.schema
        result = self.execute_arrow(
            f"""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{schema}'
        """
        )
        return result.column("TABLE_NAME").to_pylist()

    def validate_connection(self) -> bool:
        """
        Validate connection is healthy.

        Returns:
            True if connection is valid, False otherwise
        """
        try:
            self.execute_arrow("SELECT 1")
            return True
        except Exception as e:
            logger.warning(f"Connection validation failed: {e}")
            return False

    def close(self) -> None:
        """Close Snowflake connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Snowflake connection closed")
