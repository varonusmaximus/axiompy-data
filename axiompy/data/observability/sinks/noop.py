from __future__ import annotations

from axiompy.data.observability.ports import DataSignal, SignalSink


class NoOpSignalSink:
    """Default sink that discards signals (useful when no telemetry is configured)."""

    def emit(self, signal: DataSignal) -> None:
        return None
