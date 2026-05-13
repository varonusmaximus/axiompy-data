"""
Arrow-native database abstraction for analytics and ETL workloads.

This module provides an abstraction optimized for **bulk columnar data transfer**
using Apache Arrow, designed for analytics and ETL workflows rather than CRUD operations.

Key Differences from axiompy.io.database:
    - Returns: pa.Table (Arrow) instead of List[Dict]
    - Optimized for: Large result sets (1M+ rows)
    - Integrates with: DataFrames, Spark, batch processing
    - Pattern: OLAP/Analytics rather than OLTP

Use Cases:
    - Data Migration: Bulk transfer between databases
    - Analytics Queries: Large result sets for analysis
    - ETL Pipelines: Columnar processing with DuckDB/Polars/Spark
    - Data Quality: Bulk validation on large datasets

Quick Example:
    >>> from axiompy.data.arrow import ArrowDatabaseFactory, DuckDBArrowSettings
    >>>
    >>> settings = DuckDBArrowSettings()
    >>> db = ArrowDatabaseFactory.create(settings)
    >>> table = db.execute_arrow("SELECT * FROM 'data.parquet'")
    >>> print(f"Fetched {table.num_rows} rows")

Supported Databases:
    - DuckDB (native Arrow support)
    - Snowflake (via adbc-driver-snowflake)
    - PostgreSQL (via adbc-driver-postgresql)

For comprehensive examples, see:
    - `examples/arrow_database_examples.py`
    - `tests/test_arrow_database.py`
    - `axiompy/data/arrow/README.md`
"""

from typing import Any, Optional

from axiompy.loggers import LoggerFactory
from axiompy.validators import ensure_not_none

from .base import ArrowDatabase
from .errors import ArrowConnectionError, ArrowDatabaseError, ArrowQueryError
from .mock import MockArrowDatabase
from .settings import (
    ArrowDatabaseSettingsProtocol,
    DuckDBArrowSettings,
    PostgresArrowSettings,
    SnowflakeArrowSettings,
)

logger = LoggerFactory.create_logger(__name__)


# =============================================================================
# Factory
# =============================================================================


class ArrowDatabaseFactory:
    """
    Factory for creating Arrow-native database connections.

    Infers adapter type from settings.adapter_type property.

    Usage:
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
        >>>
        >>> db = ArrowDatabaseFactory.create(settings)
        >>> table = db.execute_arrow("SELECT * FROM events LIMIT 1000000")
        >>> print(f"Fetched {table.num_rows} rows, {table.nbytes / 1024 / 1024:.2f} MB")
    """

    @staticmethod
    def create(
        settings: ArrowDatabaseSettingsProtocol,
        secrets_manager: Optional[Any] = None,
    ) -> ArrowDatabase:
        """
        Create an Arrow database connection based on settings type.

        Args:
            settings: Vendor-specific settings (SnowflakeArrowSettings, etc.)
            secrets_manager: Optional SecretsManager for credential loading

        Returns:
            ArrowDatabase implementation

        Raises:
            ValueError: If settings type is not supported
        """
        ensure_not_none(settings, "Settings cannot be None")

        adapter_type = settings.adapter_type
        logger.info(f"Creating Arrow database: {adapter_type}")

        match adapter_type:
            case "snowflake":
                from axiompy.data.arrow._snowflake import SnowflakeArrowDatabase

                return SnowflakeArrowDatabase(settings, secrets_manager)  # type: ignore

            case "postgres":
                from axiompy.data.arrow._postgres import PostgresArrowDatabase

                return PostgresArrowDatabase(settings, secrets_manager)  # type: ignore

            case "duckdb":
                from axiompy.data.arrow._duckdb import DuckDBArrowDatabase

                return DuckDBArrowDatabase(settings)  # type: ignore

            case _:
                raise ValueError(f"Unsupported Arrow database type: {adapter_type}")

    @staticmethod
    def create_mock() -> MockArrowDatabase:
        """
        Create a mock Arrow database for testing.

        Returns:
            MockArrowDatabase instance
        """
        return MockArrowDatabase()


__all__ = [
    # Base classes
    "ArrowDatabase",
    "ArrowDatabaseFactory",
    "ArrowDatabaseSettingsProtocol",
    # Settings
    "DuckDBArrowSettings",
    "SnowflakeArrowSettings",
    "PostgresArrowSettings",
    # Errors
    "ArrowDatabaseError",
    "ArrowConnectionError",
    "ArrowQueryError",
    # Mock
    "MockArrowDatabase",
]
