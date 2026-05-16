"""Tests for observability signal sinks and factory."""

from __future__ import annotations

import pytest

from axiompy.data.observability import DataSignal, SignalKind, SinkFactory
from axiompy.data.observability.sinks.noop import NoOpSignalSink


def test_sink_factory_noop() -> None:
    sink = SinkFactory.create("noop")
    assert isinstance(sink, NoOpSignalSink)
    sink.emit(DataSignal(kind=SignalKind.QUALITY, name="t", payload={}))


def test_sink_factory_otel_when_available() -> None:
    try:
        import opentelemetry  # noqa: F401
    except ImportError:
        pytest.skip("opentelemetry-api not installed")
    sink = SinkFactory.create("otel")
    sink.emit(DataSignal(kind=SignalKind.METRIC, name="m", payload={"k": 1}))


def test_sink_factory_unknown() -> None:
    with pytest.raises(ValueError, match="Unknown sink"):
        SinkFactory.create("nonexistent_sink_id")
