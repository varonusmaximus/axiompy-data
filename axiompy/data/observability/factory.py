"""Factory for :class:`~axiompy.data.observability.ports.SignalSink` adapters (lazy extras)."""

from __future__ import annotations

from typing import Any, Type

from axiompy.data.observability.ports import SignalSink
from axiompy.data.observability.sinks.noop import NoOpSignalSink


class SinkFactory:
    """
    Selects a sink implementation by id. Heavy vendors load inside ``create`` only.

    Builtin:
        - ``noop`` — default no-op sink (always available)
        - ``otel`` — OpenTelemetry spans (``pip install 'axiompy-data[otel]'``)
        - ``elastic`` — HTTP POST to an Elastic ingest URL (stdlib)
        - ``splunk_hec`` — Splunk HTTP Event Collector (stdlib)
    """

    _registry: dict[str, Type[SignalSink]] = {
        "noop": NoOpSignalSink,
    }

    @classmethod
    def register(cls, sink_id: str, impl: Type[SignalSink]) -> None:
        cls._registry[sink_id] = impl

    @classmethod
    def create(cls, sink_id: str = "noop", **kwargs: Any) -> SignalSink:
        if sink_id == "otel":
            from axiompy.data.observability.sinks.otel import OTelSignalSink

            return OTelSignalSink(**kwargs)
        if sink_id == "elastic":
            from axiompy.data.observability.sinks.elastic import ElasticSignalSink

            return ElasticSignalSink(**kwargs)
        if sink_id in {"splunk_hec", "splunk"}:
            from axiompy.data.observability.sinks.splunk_hec import SplunkHecSignalSink

            return SplunkHecSignalSink(**kwargs)
        try:
            sink_cls = cls._registry[sink_id]
        except KeyError as err:
            raise ValueError(f"Unknown sink id: {sink_id!r}") from err
        return sink_cls(**kwargs)
