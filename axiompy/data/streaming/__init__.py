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

from axiompy.data.streaming.adapters.kafka import KafkaStreamConsumer, KafkaStreamProducer
from axiompy.data.streaming.adapters.kinesis import KinesisStreamConsumer, KinesisStreamProducer
from axiompy.data.streaming.adapters.rabbitmq import RabbitMQStreamConsumer, RabbitMQStreamProducer
from axiompy.data.streaming.adapters.redis import RedisStreamConsumer, RedisStreamProducer
from axiompy.data.streaming.consumer import StreamConsumer
from axiompy.data.streaming.factory import StreamConsumerFactory, StreamProducerFactory
from axiompy.data.streaming.handler import (
    StreamHandler,
)
from axiompy.data.streaming.ports import StreamConsumePort, StreamPublishPort
from axiompy.data.streaming.producer import StreamProducer
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
    # Ports (hexagonal)
    "StreamPublishPort",
    "StreamConsumePort",
]
