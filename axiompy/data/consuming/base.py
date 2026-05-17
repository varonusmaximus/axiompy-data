"""
Analytical consuming client abstract base class.

Provides the interface for columnar SQL clients (:class:`~axiompy.data.consuming.results.QueryResult`).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar, Optional

import pyarrow as pa

from axiompy.data.consuming.results import QueryResult
from axiompy.data.observability.emit import emit_signal
from axiompy.data.observability.ports import SignalKind, SignalSink


class Client(ABC):
    """
    Abstract base class for analytical SQL clients.

    Use :meth:`query` for tabular reads (returns :class:`~axiompy.data.consuming.results.QueryResult`).
    Use :meth:`execute` for DDL/DML. Introspection calls :meth:`query` with
    ``emit_lifecycle=False`` so schema probes are not counted as user queries in telemetry.
    """

    consuming_adapter_name: ClassVar[str] = "unknown"
    _signal_sink: Optional[SignalSink]

    @abstractmethod
    def _execute_query(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
    ) -> pa.Table:
        """Run SQL and return a result table (platform-specific)."""

    def query(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
        *,
        emit_lifecycle: bool = True,
    ) -> QueryResult:
        """Execute SQL and return a :class:`QueryResult`."""
        table = self._execute_query(sql, params)
        if emit_lifecycle:
            emit_signal(
                getattr(self, "_signal_sink", None),
                SignalKind.LIFECYCLE,
                "consuming.query",
                {"adapter": self.consuming_adapter_name, "rows": table.num_rows},
                source="axiompy.data.consuming",
            )
        return QueryResult(
            data=table,
            adapter=self.consuming_adapter_name,
            sql=sql,
            params=params,
        )

    @abstractmethod
    def execute(self, sql: str, params: Optional[dict[str, Any]] = None) -> None:
        """Execute SQL without returning results (DDL, DML)."""

    @abstractmethod
    def get_schema(self, table: str) -> pa.Schema:
        """Get table schema."""

    @abstractmethod
    def get_table_names(self, schema: Optional[str] = None) -> list[str]:
        """List table names in database/schema."""

    @abstractmethod
    def validate_connection(self) -> bool:
        """Validate that the connection is healthy."""

    @abstractmethod
    def close(self) -> None:
        """Close the database connection."""

    def __enter__(self) -> Client:
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> bool:
        self.close()
        return False


# Backward-compatible alias (deprecated public name)
ArrowDatabase = Client
