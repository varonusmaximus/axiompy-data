"""
DuckDB analytical consuming client (native Arrow).

Requires: pip install duckdb
"""

from __future__ import annotations

from typing import Any, Optional

import pyarrow as pa
from axiompy.decorators import LogExecutionTime
from axiompy.loggers import LoggerFactory
from axiompy.validators import ensure_not_none

from axiompy.data.consuming.base import Client
from axiompy.data.consuming.errors import ArrowConnectionError, ArrowQueryError
from axiompy.data.consuming.results import QueryResult
from axiompy.data.consuming.settings import DuckDBArrowSettings
from axiompy.data.observability.ports import SignalSink

logger = LoggerFactory.create_logger(__name__)


class DuckDBClient(Client):
    """DuckDB implementation with native Arrow support."""

    consuming_adapter_name = "duckdb"

    def __init__(
        self,
        settings: DuckDBArrowSettings,
        signal_sink: Optional[SignalSink] = None,
    ) -> None:
        ensure_not_none(settings, "DuckDBArrowSettings required")
        self.settings = settings
        self._connection: Optional[Any] = None
        self._signal_sink = signal_sink

    def _connect(self) -> Any:
        if self._connection is None:
            try:
                import duckdb
            except ImportError as e:
                raise ArrowConnectionError("duckdb not installed. Run: pip install duckdb") from e

            self._connection = duckdb.connect(
                self.settings.database,
                read_only=self.settings.read_only,
            )

            for ext in self.settings.extensions:
                self._connection.execute(f"INSTALL {ext}; LOAD {ext};")
                logger.debug("Loaded DuckDB extension: %s", ext)

            logger.info("DuckDB connection established: %s", self.settings.database)

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
            result = conn.execute(sql, params) if params else conn.execute(sql)
            table = result.fetch_arrow_table()
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
            if params:
                conn.execute(sql, params)
            else:
                conn.execute(sql)
        except Exception as e:
            logger.error("Execute failed: %s", e)
            raise ArrowQueryError(f"Execute failed: {e}") from e

    def register_table(self, name: str, table: pa.Table) -> None:
        """Register an in-memory table so it can be referenced in SQL (DuckDB-specific)."""
        ensure_not_none(name, "Table name required")
        ensure_not_none(table, "Table required")
        conn = self._connect()
        conn.register(name, table)
        logger.info("Registered table '%s' (%s rows)", name, table.num_rows)

    def register_arrow_table(self, name: str, table: pa.Table) -> None:
        """Deprecated alias for :meth:`register_table`."""
        self.register_table(name, table)

    def get_schema(self, table: str) -> pa.Schema:
        result = self.query(f"SELECT * FROM {table} LIMIT 0", emit_lifecycle=False)
        return result.schema

    def get_table_names(self, schema: Optional[str] = None) -> list[str]:
        del schema
        result = self.query("SHOW TABLES", emit_lifecycle=False)
        return result.data.column("name").to_pylist()

    def validate_connection(self) -> bool:
        try:
            self.query("SELECT 1", emit_lifecycle=False)
            return True
        except Exception:
            return False

    def close(self) -> None:
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("DuckDB connection closed")

    def read_parquet(self, path: str) -> QueryResult:
        return self.query(f"SELECT * FROM read_parquet('{path}')")

    def read_csv(self, path: str, **options: Any) -> QueryResult:
        if options:
            opts_str = ", ".join(f"{k}={repr(v)}" for k, v in options.items())
            sql = f"SELECT * FROM read_csv('{path}', {opts_str})"
        else:
            sql = f"SELECT * FROM read_csv_auto('{path}')"
        return self.query(sql)

    def read_json(self, path: str) -> QueryResult:
        return self.query(f"SELECT * FROM read_json_auto('{path}')")

    def write_parquet(self, sql: str, path: str) -> None:
        self.execute(f"COPY ({sql}) TO '{path}' (FORMAT PARQUET)")
        logger.info("Wrote query results to %s", path)


DuckDBArrowDatabase = DuckDBClient
