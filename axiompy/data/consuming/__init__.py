"""
Analytical **consuming** clients: bulk columnar access for analytics and ETL.

Canonical import::

    from axiompy.data.consuming import Factory, Client, QueryResult, Platform
    from axiompy.data.consuming.settings import DuckDBArrowSettings

    settings = DuckDBArrowSettings()
    client = Factory.create(settings)
    result = client.query("SELECT 1")
    df = result.to_pandas()
"""

# Testing / advanced imports (not required for typical use)
from axiompy.data.consuming.adapters.mock import MockArrowDatabase, MockClient
from axiompy.data.consuming.base import ArrowDatabase, Client
from axiompy.data.consuming.errors import ArrowConnectionError, ArrowDatabaseError, ArrowQueryError
from axiompy.data.consuming.factory import (
    AbstractConsumingClientFactory,
    ArrowDatabaseFactory,
    Factory,
)
from axiompy.data.consuming.interchange import convert
from axiompy.data.consuming.platform import Platform
from axiompy.data.consuming.results import QueryResult
from axiompy.data.consuming.settings import (
    DatabricksArrowSettings,
    DuckDBArrowSettings,
    MockArrowSettings,
    PostgresArrowSettings,
    SnowflakeArrowSettings,
)
from axiompy.data.consuming.types import Settings

__all__ = [
    "Client",
    "Factory",
    "Platform",
    "QueryResult",
    "Settings",
    "convert",
    "DuckDBArrowSettings",
    "SnowflakeArrowSettings",
    "PostgresArrowSettings",
    "DatabricksArrowSettings",
    "MockArrowSettings",
    "ArrowDatabaseError",
    "ArrowConnectionError",
    "ArrowQueryError",
    "MockClient",
    # Deprecated aliases
    "ArrowDatabase",
    "ArrowDatabaseFactory",
    "AbstractConsumingClientFactory",
    "MockArrowDatabase",
]
