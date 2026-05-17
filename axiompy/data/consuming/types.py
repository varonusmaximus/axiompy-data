"""Public consuming type aliases."""

from __future__ import annotations

from axiompy.data.consuming.settings import (
    DatabricksArrowSettings,
    DuckDBArrowSettings,
    MockArrowSettings,
    PostgresArrowSettings,
    SnowflakeArrowSettings,
)

type Settings = (
    DuckDBArrowSettings
    | PostgresArrowSettings
    | SnowflakeArrowSettings
    | DatabricksArrowSettings
    | MockArrowSettings
)

__all__ = ["Settings"]
