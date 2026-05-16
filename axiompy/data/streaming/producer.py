"""
Stream producer abstraction and concrete implementations.

Provides unified producer interface for publishing to different streaming platforms.
"""

import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

from axiompy.decorators import Retry
from axiompy.loggers import LoggerFactory
from axiompy.validators import ensure_not_empty, ensure_not_none

from axiompy.data.streaming.types import ProducerResult, StreamEngine, StreamSettings


class StreamProducer(ABC):
    """Abstract base class for stream producers."""

    def __init__(self, settings: StreamSettings):
        """
        Initialize producer.

        Args:
            settings: Streaming configuration
        """
        self.settings = settings
        self.logger = LoggerFactory.create_logger(f"{self.__class__.__name__}", level=logging.INFO)

    @abstractmethod
    def send(
        self,
        message: Union[bytes, str, Dict[str, Any]],
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        partition: Optional[int] = None,
    ) -> ProducerResult:  # pragma: no cover
        """
        Send a single message.

        Args:
            message: Message payload (bytes, string, or dict)
            key: Optional partition/routing key
            headers: Optional message headers
            partition: Optional target partition

        Returns:
            ProducerResult with success status and metadata
        """
        pass

    @abstractmethod
    @Retry(max_attempts=3, delay=1.0, backoff=2.0)
    def send_batch(
        self,
        messages: List[Union[bytes, str, Dict[str, Any]]],
        keys: Optional[List[str]] = None,
        headers: Optional[List[Dict[str, str]]] = None,
    ) -> List[ProducerResult]:  # pragma: no cover
        """
        Send multiple messages in batch.

        Args:
            messages: List of message payloads (must not be None or empty)
            keys: Optional list of keys (one per message)
            headers: Optional list of headers (one per message)

        Returns:
            List of ProducerResult for each message

        Raises:
            ValidationError: If messages list is None or empty
        """
        ensure_not_none(messages, "messages list cannot be None")
        ensure_not_empty(messages, "messages list cannot be empty")
        pass

    @Retry(max_attempts=3, delay=1.0, backoff=2.0)
    def send_dataframe(
        self, data: Any, key_column: Optional[str] = None, format: str = "json"
    ) -> List[ProducerResult]:
        """
        Send DataFrame rows as messages.

        Args:
            data: DataFrame (Pandas or Spark) - must not be None
            key_column: Column to use as message key
            format: Serialization format (json, csv) - must not be empty

        Returns:
            List of ProducerResult for each row

        Raises:
            ValidationError: If data is None or format is empty
        """
        ensure_not_none(data, "DataFrame cannot be None")
        ensure_not_empty(format, "format cannot be empty")

        results = []

        # Convert to records
        if hasattr(data, "to_dict"):  # Pandas
            records = data.to_dict("records")
        elif hasattr(data, "toPandas"):  # Spark
            records = data.toPandas().to_dict("records")
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")

        # Send each record
        for record in records:
            key = str(record.get(key_column)) if key_column else None

            if format == "json":
                message = json.dumps(record).encode("utf-8")
            elif format == "csv":
                message = ",".join(str(v) for v in record.values()).encode("utf-8")
            else:
                raise ValueError(f"Unsupported format: {format}")

            result = self.send(message, key=key)
            results.append(result)

        self.logger.info(f"Sent {len(results)} DataFrame rows to stream")
        return results

    @abstractmethod
    def flush(self) -> None:  # pragma: no cover
        """Flush any pending messages."""
        pass

    @abstractmethod
    def close(self) -> None:  # pragma: no cover
        """Close producer and cleanup resources."""
        pass

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
