"""Platform dispatch enum for analytical consuming clients."""

from __future__ import annotations

from enum import StrEnum


class Platform(StrEnum):
    """Analytical database / warehouse target for :class:`~axiompy.data.consuming.base.Client`."""

    DUCKDB = "duckdb"
    POSTGRES = "postgres"
    SNOWFLAKE = "snowflake"
    DATABRICKS = "databricks"
    MOCK = "mock"


__all__ = ["Platform"]
