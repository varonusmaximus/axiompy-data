"""
Streaming hexagonal ports (incremental).

Concrete adapters remain in ``producer.py`` / ``consumer.py``; this module holds
stable ``typing.Protocol`` surfaces for publish/consume operations.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class StreamPublishPort(Protocol):
    """Publish-side port implemented by transport adapters."""

    def publish(self, topic: str, message: Any, **kwargs: Any) -> None:
        """Send one message to a logical topic or stream."""


@runtime_checkable
class StreamConsumePort(Protocol):
    """Consume-side port implemented by transport adapters."""

    def poll(self, timeout_ms: float | None = None) -> Any | None:
        """Return the next message or None when idle."""
