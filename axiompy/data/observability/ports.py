"""Outbound observability: neutral signal DTOs and sink port (hexagonal)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Mapping, Optional, Protocol, runtime_checkable


class SignalKind(str, Enum):
    """High-level categories for data-plane telemetry."""

    LINEAGE = "lineage"
    QUALITY = "quality"
    CDC = "cdc"
    METRIC = "metric"
    LIFECYCLE = "lifecycle"


@dataclass(frozen=True)
class DataSignal:
    """Vendor-neutral signal emitted by consuming/streaming/processing/interchange adapters."""

    kind: SignalKind
    name: str
    payload: Mapping[str, Any]
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    source: Optional[str] = None


@runtime_checkable
class SignalSink(Protocol):
    """Port implemented by OTel/Elastic/Splunk HEC etc. adapters under ``sinks/``."""

    def emit(self, signal: DataSignal) -> None:
        """Forward or record one signal."""
