"""Factory for analytical consuming clients."""

from __future__ import annotations

from typing import Any, Optional

from axiompy.loggers import LoggerFactory
from axiompy.validators import ensure_not_none

from axiompy.data.observability.ports import SignalSink

from .base import Client
from .platform import Platform
from .types import Settings

logger = LoggerFactory.create_logger(__name__)


class Factory:
    """
    Create :class:`~axiompy.data.consuming.base.Client` instances from :data:`~axiompy.data.consuming.types.Settings`.

    Supported platforms: DuckDB, Snowflake, PostgreSQL, Databricks SQL, and mock (tests).
    """

    @classmethod
    def create(
        cls,
        settings: Settings,
        secrets_manager: Optional[Any] = None,
        signal_sink: Optional[SignalSink] = None,
    ) -> Client:
        ensure_not_none(settings, "Settings required")

        platform = settings.platform
        platform_key = platform.value if hasattr(platform, "value") else str(platform)
        logger.info("Creating consuming client: %s", platform_key)

        match platform:
            case Platform.SNOWFLAKE:
                from .adapters.snowflake import SnowflakeClient

                return SnowflakeClient(settings, secrets_manager, signal_sink=signal_sink)  # type: ignore[arg-type]

            case Platform.POSTGRES:
                from .adapters.postgres import PostgresClient

                return PostgresClient(settings, secrets_manager, signal_sink=signal_sink)  # type: ignore[arg-type]

            case Platform.DUCKDB:
                from .adapters.duckdb import DuckDBClient

                return DuckDBClient(settings, signal_sink=signal_sink)  # type: ignore[arg-type]

            case Platform.DATABRICKS:
                from .adapters.databricks import DatabricksClient

                return DatabricksClient(settings, secrets_manager, signal_sink=signal_sink)  # type: ignore[arg-type]

            case Platform.MOCK:
                from .adapters.mock import MockClient

                return MockClient(signal_sink=signal_sink)

            case _:
                raise ValueError(f"Unsupported platform: {platform!r}")

    @classmethod
    def create_mock(cls, signal_sink: Optional[SignalSink] = None) -> Client:
        """Return a mock client for unit tests."""
        from .adapters.mock import MockClient

        return MockClient(signal_sink=signal_sink)


# Backward-compatible aliases
AbstractConsumingClientFactory = Factory
ArrowDatabaseFactory = Factory
