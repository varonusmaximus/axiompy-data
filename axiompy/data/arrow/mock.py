"""
Mock Arrow database implementation for testing.

Provides a mock implementation that records calls and returns predefined responses.
"""

from typing import Any, Optional

from .base import ArrowDatabase


class MockArrowDatabase(ArrowDatabase):
    """
    Mock implementation for unit testing.

    Allows setting predefined responses and tracking method calls.

    Example:
        >>> mock = MockArrowDatabase()
        >>> mock.set_response("SELECT 1", pa.table({"value": [1]}))
        >>> result = mock.execute_arrow("SELECT 1")
        >>> assert result.num_rows == 1
        >>> assert mock.calls == [("execute_arrow", "SELECT 1", None)]
    """

    def __init__(self) -> None:
        """Initialize mock database."""
        self.calls: list[tuple[str, Any, ...]] = []
        self._responses: dict[str, Any] = {}
        self._tables: dict[str, Any] = {}
        self._closed: bool = False

    def set_response(  # type: ignore  # noqa: F821
        self, sql: str, result: "pa.Table"
    ) -> "MockArrowDatabase":
        """
        Set predefined response for a SQL query.

        Args:
            sql: SQL query to match
            result: PyArrow Table to return

        Returns:
            Self for method chaining
        """
        self._responses[sql] = result
        return self

    def reset(self) -> None:
        """Reset recorded calls and responses."""
        self.calls.clear()
        self._responses.clear()
        self._tables.clear()
        self._closed = False

    def execute_arrow(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
    ) -> Any:
        """Execute SQL and return mocked result."""
        self.calls.append(("execute_arrow", sql, params))

        if sql in self._responses:
            return self._responses[sql]

        # Default: return empty table (requires pyarrow)
        try:
            import pyarrow as pa

            return pa.table({})
        except ImportError:
            # Return None if pyarrow not available - for basic mock testing
            return None

    def execute(self, sql: str, params: Optional[dict[str, Any]] = None) -> None:
        """Execute SQL (recorded but no action)."""
        self.calls.append(("execute", sql, params))

    def register_arrow_table(  # type: ignore  # noqa: F821
        self, name: str, table: "pa.Table"
    ) -> None:
        """Register Arrow table."""
        self.calls.append(("register_arrow_table", name, table))
        self._tables[name] = table

    def get_schema(self, table: str) -> "pa.Schema":  # type: ignore  # noqa: F821
        """Get table schema."""
        import pyarrow as pa

        self.calls.append(("get_schema", table))

        if table in self._tables:
            return self._tables[table].schema

        # Default empty schema
        return pa.schema([])

    def get_table_names(self, schema: Optional[str] = None) -> list[str]:
        """List table names."""
        self.calls.append(("get_table_names", schema))
        return list(self._tables.keys())

    def validate_connection(self) -> bool:
        """Validate connection (always True for mock)."""
        self.calls.append(("validate_connection",))
        return not self._closed

    def close(self) -> None:
        """Close mock connection."""
        self.calls.append(("close",))
        self._closed = True
