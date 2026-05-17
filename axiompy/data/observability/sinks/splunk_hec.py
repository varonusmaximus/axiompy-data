"""Splunk HTTP Event Collector (HEC) sink (stdlib ``urllib``)."""

from __future__ import annotations

import json
import urllib.error
import urllib.request

from axiompy.data.observability.ports import DataSignal


class SplunkHecSignalSink:
    """Send each signal as one HEC JSON event."""

    def __init__(self, hec_url: str, hec_token: str, *, timeout: float = 10.0) -> None:
        base = hec_url.rstrip("/")
        self._url = f"{base}/services/collector/event"
        self._token = hec_token
        self._timeout = timeout

    def emit(self, signal: DataSignal) -> None:
        body = {
            "event": {
                "kind": signal.kind.value,
                "name": signal.name,
                "payload": dict(signal.payload),
                "timestamp": signal.timestamp.isoformat(),
                "source": signal.source,
            },
        }
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            self._url,
            data=data,
            method="POST",
            headers={
                "Authorization": f"Splunk {self._token}",
                "Content-Type": "application/json",
            },
        )
        try:
            urllib.request.urlopen(req, timeout=self._timeout)  # nosec B310
        except urllib.error.HTTPError as err:
            raw = err.read().decode("utf-8", errors="replace")
            msg = f"Splunk HEC failed: {err.code} {err.reason}: {raw}"
            raise RuntimeError(msg) from err
