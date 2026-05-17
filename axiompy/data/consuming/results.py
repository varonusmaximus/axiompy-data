"""Query result value object for analytical consuming clients."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import pyarrow as pa

from axiompy.data.consuming.interchange import convert
from axiompy.data.types import DataEngine


@dataclass(frozen=True)
class QueryResult:
    """
    Default tabular result from :meth:`~axiompy.data.consuming.base.Client.query`.

    The canonical payload is columnar Arrow data in :attr:`data`. Use :meth:`to` or
    ``to_pandas`` / ``to_polars`` / ``to_spark`` to materialize other formats.

    Future extensions (not in v1): ``execution_time_ms``, warehouse ``metadata``.
    """

    data: pa.Table
    adapter: str
    sql: Optional[str] = None
    params: Optional[dict[str, Any]] = None

    @property
    def row_count(self) -> int:
        return self.data.num_rows

    @property
    def schema(self) -> pa.Schema:
        return self.data.schema

    @property
    def nbytes(self) -> int:
        return self.data.nbytes

    def to(self, engine: DataEngine, **kwargs: Any) -> Any:
        """Convert :attr:`data` to the given engine representation."""
        return convert(self.data, engine, **kwargs)

    def to_pandas(self) -> Any:
        return self.to(DataEngine.PANDAS)

    def to_polars(self) -> Any:
        return self.to(DataEngine.POLARS)

    def to_spark(self, spark: Any, **kwargs: Any) -> Any:
        return self.to(DataEngine.SPARK, spark=spark, **kwargs)


__all__ = ["QueryResult"]
