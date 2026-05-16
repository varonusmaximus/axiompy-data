"""Mock consuming client for testing."""

from __future__ import annotations

from typing import Any, Optional

from axiompy.data.consuming.base import Client
from axiompy.data.observability.ports import SignalSink


class MockClient(Client):
    """Mock implementation that records calls and returns predefined responses."""

    consuming_adapter_name = "mock"

    def __init__(self, signal_sink: Optional[SignalSink] = None) -> None:
        self.calls: list[tuple[str, Any, ...]] = []
        self._responses: dict[str, Any] = {}
        self._tables: dict[str, Any] = {}
        self._closed: bool = False
        self._signal_sink = signal_sink

    def set_response(self, sql: str, result: Any) -> MockClient:
        self._responses[sql] = result
        return self

    def reset(self) -> None:
        self.calls.clear()
        self._responses.clear()
        self._tables.clear()
        self._closed = False

    def _execute_query(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
    ) -> Any:
        self.calls.append(("query", sql, params))

        if sql in self._responses:
            return self._responses[sql]

        try:
            import pyarrow as pa

            return pa.table({})
        except ImportError:
            return None

    def execute(self, sql: str, params: Optional[dict[str, Any]] = None) -> None:
        self.calls.append(("execute", sql, params))

    def register_table(self, name: str, table: Any) -> None:
        self.calls.append(("register_table", name, table))
        self._tables[name] = table

    def get_schema(self, table: str) -> Any:
        import pyarrow as pa

        self.calls.append(("get_schema", table))
        if table in self._tables:
            return self._tables[table].schema
        return pa.schema([])

    def get_table_names(self, schema: Optional[str] = None) -> list[str]:
        self.calls.append(("get_table_names", schema))
        return list(self._tables.keys())

    def validate_connection(self) -> bool:
        self.calls.append(("validate_connection",))
        return not self._closed

    def close(self) -> None:
        self.calls.append(("close",))
        self._closed = True


MockArrowDatabase = MockClient
