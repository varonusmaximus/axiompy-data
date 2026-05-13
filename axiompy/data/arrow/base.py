"""
Arrow database abstract base class.

Provides the interface for Arrow-native database connections.
"""

from abc import ABC, abstractmethod
from typing import Any, Optional


class ArrowDatabase(ABC):
    """
    Abstract base class for Arrow-native database connections.

    Unlike Database (row-based, List[Dict]), this returns PyArrow Tables
    for efficient columnar data transfer, ideal for analytics and ETL.

    Implementations:
        - SnowflakeArrowDatabase (via adbc-driver-snowflake)
        - PostgresArrowDatabase (via adbc-driver-postgresql)
        - DuckDBArrowDatabase (native Arrow support)

    Example:
        >>> db = ArrowDatabaseFactory.create(DuckDBArrowSettings())
        >>> table = db.execute_arrow("SELECT * FROM 'data.parquet'")
        >>> print(f"Fetched {table.num_rows} rows, {table.nbytes / 1024 / 1024:.2f} MB")
    """

    @abstractmethod
    def execute_arrow(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
    ) -> "pa.Table":  # type: ignore  # noqa: F821
        """
        Execute SQL and return results as Arrow table.

        This is the primary method for analytics queries, returning
        columnar data for efficient processing.

        Args:
            sql: SQL query to execute
            params: Optional query parameters (parameterized queries)

        Returns:
            PyArrow Table with query results

        Raises:
            ArrowQueryError: If query execution fails

        Example:
            >>> table = db.execute_arrow(
            ...     "SELECT * FROM events WHERE dt = :date",
            ...     params={"date": "2026-01-01"}
            ... )
            >>> print(f"Fetched {table.num_rows} rows")
        """
        pass

    @abstractmethod
    def execute(self, sql: str, params: Optional[dict[str, Any]] = None) -> None:
        """
        Execute SQL without returning results (DDL, DML).

        Use for CREATE, INSERT, UPDATE, DELETE, etc.

        Args:
            sql: SQL statement to execute
            params: Optional query parameters

        Raises:
            ArrowQueryError: If execution fails
        """
        pass

    @abstractmethod
    def register_arrow_table(  # type: ignore  # noqa: F821
        self, name: str, table: "pa.Table"
    ) -> None:
        """
        Register an Arrow table as a virtual table.

        Allows querying in-memory Arrow data with SQL.

        Args:
            name: Virtual table name
            table: PyArrow Table to register

        Example:
            >>> db.register_arrow_table("staged_data", my_arrow_table)
            >>> result = db.execute_arrow("SELECT * FROM staged_data WHERE x > 10")
        """
        pass

    @abstractmethod
    def get_schema(self, table: str) -> "pa.Schema":  # type: ignore  # noqa: F821
        """
        Get table schema as Arrow schema.

        Args:
            table: Table name (optionally schema-qualified)

        Returns:
            PyArrow Schema with column names and types
        """
        pass

    @abstractmethod
    def get_table_names(self, schema: Optional[str] = None) -> list[str]:
        """
        List table names in database/schema.

        Args:
            schema: Optional schema to filter by

        Returns:
            List of table names
        """
        pass

    @abstractmethod
    def validate_connection(self) -> bool:
        """
        Validate that the connection is healthy.

        Returns:
            True if connection is valid, False otherwise
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the database connection."""
        pass

    def __enter__(self) -> "ArrowDatabase":
        """Context manager entry."""
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> bool:
        """Context manager exit - close connection."""
        self.close()
        return False

    # =========================================================================
    # Integration with axiompy.data
    # =========================================================================

    def to_pandas(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
    ) -> "pd.DataFrame":  # type: ignore  # noqa: F821
        """
        Execute SQL and return results as Pandas DataFrame.

        Convenience method for integration with pandas-based workflows.

        Args:
            sql: SQL query to execute
            params: Optional query parameters

        Returns:
            Pandas DataFrame with query results

        Example:
            >>> df = db.to_pandas("SELECT * FROM users LIMIT 1000")
            >>> print(df.describe())
        """
        table = self.execute_arrow(sql, params)
        return table.to_pandas()

    def to_polars(
        self,
        sql: str,
        params: Optional[dict[str, Any]] = None,
    ) -> "pl.DataFrame":  # type: ignore  # noqa: F821
        """
        Execute SQL and return results as Polars DataFrame.

        Requires: pip install polars

        Args:
            sql: SQL query to execute
            params: Optional query parameters

        Returns:
            Polars DataFrame with query results

        Example:
            >>> df = db.to_polars("SELECT * FROM events")
            >>> print(df.describe())
        """
        import polars as pl

        table = self.execute_arrow(sql, params)
        return pl.from_arrow(table)
