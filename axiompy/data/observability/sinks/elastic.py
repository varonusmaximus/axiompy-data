"""Elasticsearch / Elastic ingest HTTP sink (stdlib ``urllib`` only)."""

from __future__ import annotations

import json
import urllib.error
import urllib.request

from axiompy.data.observability.ports import DataSignal


class ElasticSignalSink:
    """
    POST JSON documents to a single ingest URL (index API, ingest pipeline, or proxy).

    ``index_url`` should accept JSON bodies (e.g. ``https://host:9200/target-index/_doc``).
    """

    def __init__(
        self, index_url: str, *, timeout: float = 10.0, extra_headers: dict[str, str] | None = None
    ) -> None:
        self._url = index_url
        self._timeout = timeout
        self._extra_headers = extra_headers or {}

    def emit(self, signal: DataSignal) -> None:
        doc = {
            "kind": signal.kind.value,
            "name": signal.name,
            "payload": dict(signal.payload),
            "timestamp": signal.timestamp.isoformat(),
            "source": signal.source,
        }
        data = json.dumps(doc).encode("utf-8")
        headers = {"Content-Type": "application/json", **self._extra_headers}
        req = urllib.request.Request(self._url, data=data, headers=headers, method="POST")
        try:
            urllib.request.urlopen(req, timeout=self._timeout)  # nosec B310
        except urllib.error.HTTPError as err:
            body = err.read().decode("utf-8", errors="replace")
            msg = f"Elastic ingest failed: {err.code} {err.reason}: {body}"
            raise RuntimeError(msg) from err
