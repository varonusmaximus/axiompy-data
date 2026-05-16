"""
Settings dataclasses for analytical consuming clients.

Import concrete settings from this module; use :data:`~axiompy.data.consuming.types.Settings`
for the union type accepted by :class:`~axiompy.data.consuming.factory.Factory`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from axiompy.validators import ValidationError, ensure_equal, ensure_not_empty

from axiompy.data.consuming.platform import Platform


@dataclass
class SnowflakeArrowSettings:
    """Snowflake-specific settings for Arrow/ADBC connections."""

    account: str
    warehouse: str
    database: str
    schema: str
    user: str
    password: Optional[str] = None
    password_secret: Optional[str] = None
    role: Optional[str] = None
    arrow_batch_size: int = 100_000

    @property
    def platform(self) -> Platform:
        return Platform.SNOWFLAKE

    @property
    def adapter_type(self) -> str:
        """Legacy dispatch key; prefer :attr:`platform`."""
        return self.platform.value

    def __post_init__(self) -> None:
        try:
            ensure_equal(self.platform, Platform.SNOWFLAKE, "Invalid platform for SnowflakeArrowSettings")
            ensure_not_empty(self.account, "Snowflake account required")
            ensure_not_empty(self.warehouse, "Snowflake warehouse required")
            ensure_not_empty(self.database, "Database required")
            ensure_not_empty(self.user, "User required")
        except ValidationError as e:
            raise ValueError(str(e)) from e

        if not self.password and not self.password_secret:
            raise ValueError("Either password or password_secret required")


@dataclass
class PostgresArrowSettings:
    """PostgreSQL-specific settings for Arrow/ADBC connections."""

    host: str
    port: int
    database: str
    user: str
    password: Optional[str] = None
    password_secret: Optional[str] = None
    schema: str = "public"
    ssl_mode: str = "prefer"

    @property
    def platform(self) -> Platform:
        return Platform.POSTGRES

    @property
    def adapter_type(self) -> str:
        return self.platform.value

    def __post_init__(self) -> None:
        try:
            ensure_equal(self.platform, Platform.POSTGRES, "Invalid platform for PostgresArrowSettings")
            ensure_not_empty(self.host, "Host required")
            ensure_not_empty(self.database, "Database required")
        except ValidationError as e:
            raise ValueError(str(e)) from e


@dataclass
class DuckDBArrowSettings:
    """DuckDB-specific settings (native Arrow support)."""

    database: str = ":memory:"
    read_only: bool = False
    extensions: list[str] = field(default_factory=list)

    @property
    def platform(self) -> Platform:
        return Platform.DUCKDB

    @property
    def adapter_type(self) -> str:
        return self.platform.value

    def __post_init__(self) -> None:
        try:
            ensure_equal(self.platform, Platform.DUCKDB, "Invalid platform for DuckDBArrowSettings")
        except ValidationError as e:
            raise ValueError(str(e)) from e


@dataclass
class DatabricksArrowSettings:
    """Databricks SQL warehouse settings (``databricks-sql-connector``)."""

    server_hostname: str
    http_path: str
    access_token: Optional[str] = None
    token_secret: Optional[str] = None
    catalog: Optional[str] = None
    schema: Optional[str] = None

    @property
    def platform(self) -> Platform:
        return Platform.DATABRICKS

    @property
    def adapter_type(self) -> str:
        return self.platform.value

    def __post_init__(self) -> None:
        try:
            ensure_equal(
                self.platform,
                Platform.DATABRICKS,
                "Invalid platform for DatabricksArrowSettings",
            )
            ensure_not_empty(self.server_hostname, "Databricks server_hostname required")
            ensure_not_empty(self.http_path, "Databricks http_path required")
        except ValidationError as e:
            raise ValueError(str(e)) from e

        if not self.access_token and not self.token_secret:
            raise ValueError("Either access_token or token_secret required")


@dataclass
class MockArrowSettings:
    """Settings placeholder for mock clients (testing)."""

    @property
    def platform(self) -> Platform:
        return Platform.MOCK

    @property
    def adapter_type(self) -> str:
        return self.platform.value

    def __post_init__(self) -> None:
        try:
            ensure_equal(self.platform, Platform.MOCK, "Invalid platform for MockArrowSettings")
        except ValidationError as e:
            raise ValueError(str(e)) from e
