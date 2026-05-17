"""
Stream consumer abstraction and concrete implementations.

Provides unified consumer interface with data sink pattern for consuming from streaming platforms.
"""

import json
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Iterator, List, Optional

from axiompy.decorators import Retry
from axiompy.loggers import LoggerFactory
from axiompy.validators import ensure_positive

from axiompy.data.streaming.types import ConsumerStats, StreamEngine, StreamMessage, StreamSettings


class StreamConsumer(ABC):
    """Abstract base class for stream consumers (data sink pattern)."""

    def __init__(self, settings: StreamSettings):
        """
        Initialize consumer.

        Args:
            settings: Streaming configuration
        """
        self.settings = settings
        self.logger = LoggerFactory.create_logger(f"{self.__class__.__name__}", level=logging.INFO)
        self.stats = ConsumerStats(start_time=datetime.now())

    @abstractmethod
    def consume(
        self, max_messages: Optional[int] = None, timeout_seconds: Optional[int] = None
    ) -> Iterator[StreamMessage]:  # pragma: no cover
        """
        Consume messages as an iterator (generator pattern).

        Args:
            max_messages: Maximum number of messages to consume (None = unlimited)
            timeout_seconds: Timeout for consuming (None = no timeout)

        Yields:
            StreamMessage objects
        """
        pass

    @Retry(max_attempts=3, delay=1.0, backoff=2.0)
    def consume_batch(
        self, batch_size: int = 100, timeout_seconds: Optional[int] = None
    ) -> List[StreamMessage]:
        """
        Consume a batch of messages.

        Args:
            batch_size: Number of messages to consume (must be positive)
            timeout_seconds: Timeout for batch (must be positive if provided)

        Returns:
            List of StreamMessage objects

        Raises:
            ValidationError: If batch_size or timeout_seconds are invalid
        """
        ensure_positive(batch_size, "batch_size must be positive")
        ensure_positive(timeout_seconds, "timeout_seconds must be positive", allow_none=True)

        messages = []
        for message in self.consume(max_messages=batch_size, timeout_seconds=timeout_seconds):
            messages.append(message)
            if len(messages) >= batch_size:
                break
        return messages

    @Retry(max_attempts=3, delay=1.0, backoff=2.0)
    def consume_to_dataframe(
        self,
        max_messages: int = 1000,
        timeout_seconds: Optional[int] = None,
        parse_json: bool = True,
    ) -> Any:
        """
        Consume messages into a DataFrame.

        Args:
            max_messages: Maximum messages to consume (must be positive)
            timeout_seconds: Timeout (must be positive if provided)
            parse_json: Whether to parse JSON payloads

        Returns:
            DataFrame (Pandas by default)

        Raises:
            ValidationError: If max_messages or timeout_seconds are invalid
        """
        ensure_positive(max_messages, "max_messages must be positive")
        ensure_positive(timeout_seconds, "timeout_seconds must be positive", allow_none=True)

        import pandas as pd

        records = []
        for message in self.consume(max_messages=max_messages, timeout_seconds=timeout_seconds):
            if parse_json:
                try:
                    data = json.loads(message.value.decode("utf-8"))
                    if isinstance(data, dict):
                        records.append(data)
                    else:
                        records.append({"value": data})
                except Exception as e:
                    self.logger.debug(f"Failed to parse JSON: {e}")
                    records.append({"raw": message.value.decode("utf-8", errors="ignore")})
            else:
                records.append(
                    {
                        "key": message.key,
                        "value": message.value.decode("utf-8", errors="ignore"),
                        "timestamp": message.timestamp,
                        "offset": message.offset,
                        "partition": message.partition,
                    }
                )

        df = pd.DataFrame(records)
        self.logger.info(f"Consumed {len(df)} messages to DataFrame")
        return df

    def consume_with_handler(
        self,
        handler: Callable[[StreamMessage], None],
        max_messages: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
        fail_fast: bool = False,
    ) -> ConsumerStats:
        """
        Consume messages and apply handler function (sink pattern).

        Args:
            handler: Function to process each message. Can be a simple callable
                    or a StreamHandler.process_message method for composable,
                    type-safe message processing with deserialization.
            max_messages: Maximum messages to process
            timeout_seconds: Timeout
            fail_fast: Stop on first error

        Returns:
            ConsumerStats with processing results

        Example:
            # Option 1: Simple function handler
            def my_handler(message):
                data = json.loads(message.value)
                process(data)

            stats = consumer.consume_with_handler(my_handler, max_messages=100)

            # Option 2: StreamHandler for type-safe processing
            from axiompy.data.streaming import StreamHandler

            class UserEventHandler(StreamHandler[UserEvent]):
                def deserialize(self, message):
                    return UserEvent.from_json(message.value)

                def handle(self, event: UserEvent):
                    save_to_db(event)

            handler = UserEventHandler()
            stats = consumer.consume_with_handler(
                handler=handler.process_message,
                max_messages=100
            )
        """
        stats = ConsumerStats(start_time=datetime.now())

        for message in self.consume(max_messages=max_messages, timeout_seconds=timeout_seconds):
            stats.messages_consumed += 1
            stats.bytes_consumed += len(message.value)

            try:
                handler(message)
                stats.messages_processed += 1
                if self.settings.auto_commit:
                    self.commit(message)
            except Exception as e:
                stats.messages_failed += 1
                self.logger.error(f"Handler failed for message {message.offset}: {e}")
                if fail_fast:
                    raise

        stats.end_time = datetime.now()
        self.logger.info(
            f"Consumed {stats.messages_consumed} messages, "
            f"processed {stats.messages_processed}, "
            f"failed {stats.messages_failed} "
            f"({stats.throughput_msg_per_sec:.1f} msg/sec)"
        )
        return stats

    @abstractmethod
    def commit(self, message: Optional[StreamMessage] = None) -> None:  # pragma: no cover
        """
        Commit message offset.

        Args:
            message: Message to commit (None = commit all)
        """
        pass

    @abstractmethod
    def close(self) -> None:  # pragma: no cover
        """Close consumer and cleanup resources."""
        pass

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stats.end_time = datetime.now()
        self.close()

    def get_stats(self) -> ConsumerStats:
        """Get current consumer statistics."""
        return self.stats
