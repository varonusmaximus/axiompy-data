"""OpenTelemetry sink: maps :class:`~axiompy.data.observability.ports.DataSignal` to spans."""

from __future__ import annotations

from axiompy.data.observability.ports import DataSignal


class OTelSignalSink:
    """Forwards each signal as span attributes (requires ``opentelemetry-api``)."""

    def __init__(self, tracer_name: str = "axiompy.data") -> None:
        try:
            from opentelemetry import trace
        except ImportError as err:
            msg = "OpenTelemetry is not installed; use pip install 'axiompy-data[otel]'."
            raise ImportError(msg) from err
        self._tracer = trace.get_tracer(tracer_name)

    def emit(self, signal: DataSignal) -> None:
        with self._tracer.start_as_current_span(f"data_signal.{signal.name}") as span:
            span.set_attribute("axiompy.signal.kind", signal.kind.value)
            span.set_attribute("axiompy.signal.name", signal.name)
            if signal.source:
                span.set_attribute("axiompy.signal.source", signal.source)
            for key, value in signal.payload.items():
                span.set_attribute(f"axiompy.signal.payload.{key}", str(value))
