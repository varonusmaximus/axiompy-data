"""
Snowflake analytical consuming client (ADBC).

Requires: pip install adbc-driver-snowflake
"""

from __future__ import annotations

from typing import Any, Optional

import pyarrow as pa
from axiompy.decorators import LogExecutionTime, Retry
from axiompy.loggers import LoggerFactory
from axiompy.validators import ensure_not_none

from axiompy.data.consuming.base import Client
from axiompy.data.consuming.errors import ArrowConnectionError, ArrowQueryError
from axiompy.data.consuming.settings import SnowflakeArrowSettings
from axiompy.data.observability.ports import SignalSink

logger = LoggerFactory.create_logger(__name__)


class SnowflakeClient(Client):
    """Snowflake implementation using ADBC for columnar transfer."""

    consuming_adapter_name = "snowflake"

    def __init__(
        self,
        settings: SnowflakeArrowSettings,
        secrets_manager: Optional[Any] = None,
        signal_sink: Optional[SignalSink] = None,
    ) -> None:
        ensure_not_none(settings, "SnowflakeArrowSettings required")
        self.settings = settings
        self.secrets = secrets_manager
        self._connection: Optional[Any] = None
        self._password: Optional[str] = None
        self._signal_sink = signal_sink

    def _get_password(self) -> str:
        if self._password is None:
            if self.settings.password:
                self._password = self.settings.password
            elif self.secrets and self.settings.password_secret:
                self._password = self.secrets.get_secret(self.settings.password_secret)
            else:
                raise ArrowConnectionError("No password configured")
        return self._password

    def _get_connection_uri(self) -> str:
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
        if self._connection is None:
            try:
                import adbc_driver_snowflake.dbapi as snowflake_dbapi
            except ImportError as e:
                raise ArrowConnectionError(
                    "adbc-driver-snowflake not installed. "
                    "Run: pip install adbc-driver-snowflake"
                ) from e

            logger.info("Connecting to Snowflake: %s", self.settings.account)
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
    def _execute_query(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
    ) -> pa.Table:
        ensure_not_none(sql, "SQL cannot be None")

        try:
            conn = self._connect()
            cursor = conn.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            table = cursor.fetch_arrow_table()
            logger.info(
                "Fetched %s rows, %.2f MB",
                table.num_rows,
                table.nbytes / 1024 / 1024,
            )
            return table
        except Exception as e:
            logger.error("Query failed: %s", e)
            raise ArrowQueryError(f"Query execution failed: {e}") from e

    def execute(self, sql: str, params: Optional[dict[str, Any]] = None) -> None:
        try:
            conn = self._connect()
            cursor = conn.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
        except Exception as e:
            logger.error("Execute failed: %s", e)
            raise ArrowQueryError(f"Execute failed: {e}") from e

    def get_schema(self, table: str) -> pa.Schema:
        result = self.query(f"SELECT * FROM {table} LIMIT 0", emit_lifecycle=False)
        return result.schema

    def get_table_names(self, schema: Optional[str] = None) -> list[str]:
        schema = schema or self.settings.schema
        result = self.query(
            f"""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema}'
            """,
            emit_lifecycle=False,
        )
        return result.data.column("TABLE_NAME").to_pylist()

    def validate_connection(self) -> bool:
        try:
            self.query("SELECT 1", emit_lifecycle=False)
            return True
        except Exception as e:
            logger.warning("Connection validation failed: %s", e)
            return False

    def close(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Snowflake connection closed")


SnowflakeArrowDatabase = SnowflakeClient
