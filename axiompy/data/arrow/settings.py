"""
Arrow database settings dataclasses.

Provides vendor-specific configuration for Arrow database connections.
"""

from dataclasses import dataclass, field
from typing import Optional, Protocol, runtime_checkable

from axiompy.validators import ValidationError, ensure_not_empty

# =============================================================================
# Settings Protocol
# =============================================================================


@runtime_checkable
class ArrowDatabaseSettingsProtocol(Protocol):
    """
    Protocol that all vendor-specific Arrow database settings must implement.

    The adapter_type property tells the factory which adapter to create.
    """

    @property
    def adapter_type(self) -> str:
        """Returns adapter type identifier (e.g., 'snowflake', 'postgres', 'duckdb')."""
        ...


# =============================================================================
# Vendor-Specific Settings
# =============================================================================


@dataclass
class SnowflakeArrowSettings:
    """
    Snowflake-specific settings for Arrow/ADBC connections.

    Uses ADBC (Arrow Database Connectivity) for columnar transfer.

    Attributes:
        account: Snowflake account identifier
        warehouse: Compute warehouse name
        database: Database name
        schema: Schema name
        user: Username for authentication
        password: Password (optional if using password_secret)
        password_secret: Secret key for SecretsManager (optional if using password)
        role: Optional role to use
        arrow_batch_size: Batch size for Arrow streaming (default: 100,000)

    Example:
        >>> settings = SnowflakeArrowSettings(
        ...     account="my_account",
        ...     warehouse="COMPUTE_WH",
        ...     database="MY_DB",
        ...     schema="PUBLIC",
        ...     user="user",
        ...     password="password",
        ... )
        >>> db = ArrowDatabaseFactory.create(settings)
    """

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
    def adapter_type(self) -> str:
        """Return adapter type identifier."""
        return "snowflake"

    def __post_init__(self) -> None:
        """Validate settings after initialization."""
        try:
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
    """
    PostgreSQL-specific settings for Arrow/ADBC connections.

    Uses ADBC (Arrow Database Connectivity) for columnar transfer.

    Attributes:
        host: Database host
        port: Database port
        database: Database name
        user: Username for authentication
        password: Password (optional if using password_secret)
        password_secret: Secret key for SecretsManager (optional if using password)
        schema: Schema name (default: "public")
        ssl_mode: SSL connection mode (default: "prefer")

    Example:
        >>> settings = PostgresArrowSettings(
        ...     host="localhost",
        ...     port=5432,
        ...     database="mydb",
        ...     user="postgres",
        ...     password="password",
        ... )
        >>> db = ArrowDatabaseFactory.create(settings)
    """

    host: str
    port: int
    database: str
    user: str
    password: Optional[str] = None
    password_secret: Optional[str] = None
    schema: str = "public"
    ssl_mode: str = "prefer"

    @property
    def adapter_type(self) -> str:
        """Return adapter type identifier."""
        return "postgres"

    def __post_init__(self) -> None:
        """Validate settings after initialization."""
        try:
            ensure_not_empty(self.host, "Host required")
            ensure_not_empty(self.database, "Database required")
        except ValidationError as e:
            raise ValueError(str(e)) from e


@dataclass
class DuckDBArrowSettings:
    """
    DuckDB-specific settings for Arrow connections.

    DuckDB has native Arrow support without needing ADBC.

    Attributes:
        database: Database path or ":memory:" for in-memory (default: ":memory:")
        read_only: Open database in read-only mode (default: False)
        extensions: List of DuckDB extensions to load (e.g., ["httpfs", "parquet"])

    Example:
        >>> # In-memory
        >>> settings = DuckDBArrowSettings()
        >>> db = ArrowDatabaseFactory.create(settings)
        >>>
        >>> # Persistent
        >>> settings = DuckDBArrowSettings(database="/path/to/db.duckdb")
        >>> db = ArrowDatabaseFactory.create(settings)
    """

    database: str = ":memory:"
    read_only: bool = False
    extensions: list[str] = field(default_factory=list)

    @property
    def adapter_type(self) -> str:
        """Return adapter type identifier."""
        return "duckdb"
