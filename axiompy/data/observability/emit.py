"""Optional one-shot signal emission (shared by domains)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Mapping, Optional

from axiompy.data.observability.ports import DataSignal, SignalKind, SignalSink


def emit_signal(
    sink: Optional[SignalSink],
    kind: SignalKind,
    name: str,
    payload: Mapping[str, Any],
    *,
    source: str = "axiompy.data",
) -> None:
    if sink is None:
        return
    sink.emit(
        DataSignal(
            kind=kind,
            name=name,
            payload=dict(payload),
            timestamp=datetime.now(UTC),
            source=source,
        )
    )
