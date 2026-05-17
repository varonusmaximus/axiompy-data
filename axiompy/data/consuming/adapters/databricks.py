"""
Databricks SQL analytical consuming client.

Requires: pip install databricks-sql-connector
"""

from __future__ import annotations

from typing import Any, Optional

import pyarrow as pa
from axiompy.decorators import LogExecutionTime, Retry
from axiompy.loggers import LoggerFactory
from axiompy.validators import ensure_not_none

from axiompy.data.consuming.base import Client
from axiompy.data.consuming.errors import ArrowConnectionError, ArrowQueryError
from axiompy.data.consuming.settings import DatabricksArrowSettings
from axiompy.data.observability.ports import SignalSink

logger = LoggerFactory.create_logger(__name__)


class DatabricksClient(Client):
    """Databricks SQL warehouse client with PyArrow results."""

    consuming_adapter_name = "databricks"

    def __init__(
        self,
        settings: DatabricksArrowSettings,
        secrets_manager: Optional[Any] = None,
        signal_sink: Optional[SignalSink] = None,
    ) -> None:
        ensure_not_none(settings, "DatabricksArrowSettings required")
        self.settings = settings
        self.secrets = secrets_manager
        self._connection: Optional[Any] = None
        self._token: Optional[str] = None
        self._signal_sink = signal_sink

    def _get_token(self) -> str:
        if self._token is None:
            if self.settings.access_token:
                self._token = self.settings.access_token
            elif self.secrets and self.settings.token_secret:
                self._token = self.secrets.get_secret(self.settings.token_secret)
            else:
                raise ArrowConnectionError("No Databricks access token configured")
        return self._token

    @Retry(logger, max_attempts=3, delay=1.0)
    def _connect(self) -> Any:
        if self._connection is None:
            try:
                from databricks import sql
            except ImportError as e:
                raise ArrowConnectionError(
                    "databricks-sql-connector not installed. "
                    "Run: pip install databricks-sql-connector"
                ) from e

            kwargs: dict[str, Any] = {
                "server_hostname": self.settings.server_hostname,
                "http_path": self.settings.http_path,
                "access_token": self._get_token(),
            }
            if self.settings.catalog:
                kwargs["catalog"] = self.settings.catalog
            if self.settings.schema:
                kwargs["schema"] = self.settings.schema

            logger.info("Connecting to Databricks: %s", self.settings.server_hostname)
            self._connection = sql.connect(**kwargs)
            logger.info("Databricks connection established")
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
                cursor.execute(sql, parameters=params)
            else:
                cursor.execute(sql)

            if hasattr(cursor, "fetchall_arrow"):
                table = cursor.fetchall_arrow()
            else:
                rows = cursor.fetchall()
                colnames = [desc[0] for desc in cursor.description]
                table = pa.Table.from_pydict(
                    {name: [row[i] for row in rows] for i, name in enumerate(colnames)}
                )

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
                cursor.execute(sql, parameters=params)
            else:
                cursor.execute(sql)
        except Exception as e:
            logger.error("Execute failed: %s", e)
            raise ArrowQueryError(f"Execute failed: {e}") from e

    def get_schema(self, table: str) -> pa.Schema:
        result = self.query(f"SELECT * FROM {table} LIMIT 0", emit_lifecycle=False)
        return result.schema

    def get_table_names(self, schema: Optional[str] = None) -> list[str]:
        schema = schema or self.settings.schema or "default"
        result = self.query(f"SHOW TABLES IN {schema}", emit_lifecycle=False)
        if result.row_count == 0:
            return []
        col_name = (
            "tableName" if "tableName" in result.data.column_names else result.data.column_names[0]
        )
        return result.data.column(col_name).to_pylist()

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
            logger.info("Databricks connection closed")


DatabricksArrowDatabase = DatabricksClient
