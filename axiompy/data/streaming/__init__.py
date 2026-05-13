"""
Streaming utilities for real-time data processing.

This module provides unified interfaces for streaming platforms:
- AWS Kinesis
- Apache Kafka
- Redis Streams
- RabbitMQ

Features:
- Producer/Consumer abstractions
- DataFrame integration
- Data sink patterns
- Statistics tracking
- Batch operations
"""

from axiompy.data.streaming.consumer import (
    KafkaStreamConsumer,
    KinesisStreamConsumer,
    RabbitMQStreamConsumer,
    RedisStreamConsumer,
    StreamConsumer,
    StreamConsumerFactory,
)
from axiompy.data.streaming.handler import (
    StreamHandler,
)
from axiompy.data.streaming.producer import (
    KafkaStreamProducer,
    KinesisStreamProducer,
    RabbitMQStreamProducer,
    RedisStreamProducer,
    StreamProducer,
    StreamProducerFactory,
)
from axiompy.data.streaming.types import (
    ConsumerStats,
    ProducerResult,
    StreamEngine,
    StreamMessage,
    StreamSettings,
)

__all__ = [
    # Types
    "StreamEngine",
    "StreamMessage",
    "StreamSettings",
    "ProducerResult",
    "ConsumerStats",
    # Producers
    "StreamProducer",
    "StreamProducerFactory",
    "KafkaStreamProducer",
    "KinesisStreamProducer",
    "RedisStreamProducer",
    "RabbitMQStreamProducer",
    # Consumers
    "StreamConsumer",
    "StreamConsumerFactory",
    "KafkaStreamConsumer",
    "KinesisStreamConsumer",
    "RedisStreamConsumer",
    "RabbitMQStreamConsumer",
    # Handlers
    "StreamHandler",
]
