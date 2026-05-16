"""
Processing hexagonal ports (incremental).

Batch, partition, pipeline, and transform factories compose engine-specific
adapters; protocol surfaces here stay vendor-neutral.
"""

from __future__ import annotations

from typing import Any, Generator, Protocol, runtime_checkable


@runtime_checkable
class BatchIteratePort(Protocol):
    """Port for iterating a dataset in fixed-size chunks."""

    def iter_batches(self, data: Any, batch_size: int) -> Generator[Any, None, None]:
        """Yield batches of ``batch_size`` until exhausted."""
