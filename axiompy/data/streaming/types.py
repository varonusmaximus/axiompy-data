"""
Type definitions for streaming module.

Defines common types, enums, and dataclasses used across all streaming implementations.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class StreamEngine(Enum):
    """Supported streaming engines."""

    KINESIS = "kinesis"
    KAFKA = "kafka"
    REDIS = "redis"
    RABBITMQ = "rabbitmq"


@dataclass
class StreamMessage:
    """
    Standard message format across all streaming platforms.

    This provides a unified interface regardless of the underlying platform.
    """

    # Core fields
    key: Optional[str] = None  # Partition/routing key
    value: bytes = b""  # Message payload
    headers: Dict[str, str] = field(default_factory=dict)  # Metadata

    # Tracking
    timestamp: Optional[datetime] = None
    offset: Optional[str] = None  # Platform-specific offset/sequence
    partition: Optional[int] = None

    # Platform-specific metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "key": self.key,
            "value": self.value,
            "headers": self.headers,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "offset": self.offset,
            "partition": self.partition,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StreamMessage":
        """Create from dictionary."""
        return cls(
            key=data.get("key"),
            value=data.get("value", b""),
            headers=data.get("headers", {}),
            timestamp=datetime.fromisoformat(data["timestamp"]) if data.get("timestamp") else None,
            offset=data.get("offset"),
            partition=data.get("partition"),
            metadata=data.get("metadata", {}),
        )


@dataclass
class StreamSettings:
    """
    Configuration for streaming connections.

    Contains settings for all supported streaming platforms.
    """

    # Connection
    engine: StreamEngine = StreamEngine.KAFKA

    # Common settings
    topic: Optional[str] = None  # Kafka topic / Kinesis stream / Redis stream
    queue: Optional[str] = None  # RabbitMQ queue
    group_id: Optional[str] = None  # Consumer group ID

    # Kafka-specific
    bootstrap_servers: Optional[List[str]] = None
    kafka_config: Dict[str, Any] = field(default_factory=dict)

    # AWS Kinesis-specific
    region: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    shard_iterator_type: str = "LATEST"  # LATEST, TRIM_HORIZON, AT_SEQUENCE_NUMBER

    # Redis-specific
    redis_host: Optional[str] = None
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: Optional[str] = None

    # RabbitMQ-specific
    rabbitmq_host: Optional[str] = None
    rabbitmq_port: int = 5672
    rabbitmq_virtual_host: str = "/"
    rabbitmq_username: Optional[str] = None
    rabbitmq_password: Optional[str] = None
    exchange: Optional[str] = None
    routing_key: Optional[str] = None

    # Behavior
    batch_size: int = 100
    max_retries: int = 3
    timeout_seconds: int = 30
    auto_commit: bool = True

    # Extra settings
    extra_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProducerResult:
    """Result of a produce operation."""

    success: bool
    message_id: Optional[str] = None  # Platform-specific message ID
    offset: Optional[str] = None
    partition: Optional[int] = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ConsumerStats:
    """Statistics for consumer operations."""

    messages_consumed: int = 0
    messages_processed: int = 0
    messages_failed: int = 0
    bytes_consumed: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

    @property
    def duration_seconds(self) -> float:
        """Calculate duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0

    @property
    def throughput_msg_per_sec(self) -> float:
        """Messages per second."""
        duration = self.duration_seconds
        return self.messages_consumed / duration if duration > 0 else 0.0

    @property
    def throughput_mb_per_sec(self) -> float:
        """Megabytes per second."""
        duration = self.duration_seconds
        return (self.bytes_consumed / 1024 / 1024) / duration if duration > 0 else 0.0
