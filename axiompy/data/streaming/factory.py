"""Factories for stream producers and consumers (transport dispatch)."""

from __future__ import annotations

from axiompy.data.streaming.adapters.kafka import KafkaStreamConsumer, KafkaStreamProducer
from axiompy.data.streaming.adapters.kinesis import KinesisStreamConsumer, KinesisStreamProducer
from axiompy.data.streaming.adapters.rabbitmq import RabbitMQStreamConsumer, RabbitMQStreamProducer
from axiompy.data.streaming.adapters.redis import RedisStreamConsumer, RedisStreamProducer
from axiompy.data.streaming.consumer import StreamConsumer
from axiompy.data.streaming.producer import StreamProducer
from axiompy.data.streaming.types import StreamEngine, StreamSettings


class StreamProducerFactory:
    """Factory for creating stream producers."""

    _producers = {
        StreamEngine.KAFKA: KafkaStreamProducer,
        StreamEngine.KINESIS: KinesisStreamProducer,
        StreamEngine.REDIS: RedisStreamProducer,
        StreamEngine.RABBITMQ: RabbitMQStreamProducer,
    }

    @classmethod
    def register_producer(cls, engine: StreamEngine, producer_class):
        """
        Register a custom producer implementation.

        Args:
            engine: Stream engine type
            producer_class: Producer class to register
        """
        cls._producers[engine] = producer_class

    @classmethod
    def create(cls, settings: StreamSettings) -> StreamProducer:
        """
        Create a stream producer.

        Args:
            settings: Streaming configuration

        Returns:
            StreamProducer instance

        Raises:
            ValueError: If engine not supported
        """
        if settings.engine not in cls._producers:
            raise ValueError(
                f"Unsupported stream engine: {settings.engine}. "
                f"Supported: {list(cls._producers.keys())}"
            )

        producer_class = cls._producers[settings.engine]
        return producer_class(settings)


class StreamConsumerFactory:
    """Factory for creating stream consumers."""

    _consumers = {
        StreamEngine.KAFKA: KafkaStreamConsumer,
        StreamEngine.KINESIS: KinesisStreamConsumer,
        StreamEngine.REDIS: RedisStreamConsumer,
        StreamEngine.RABBITMQ: RabbitMQStreamConsumer,
    }

    @classmethod
    def register_consumer(cls, engine: StreamEngine, consumer_class):
        """
        Register a custom consumer implementation.

        Args:
            engine: Stream engine type
            consumer_class: Consumer class to register
        """
        cls._consumers[engine] = consumer_class

    @classmethod
    def create(cls, settings: StreamSettings) -> StreamConsumer:
        """
        Create a stream consumer.

        Args:
            settings: Streaming configuration

        Returns:
            StreamConsumer instance

        Raises:
            ValueError: If engine not supported
        """
        if settings.engine not in cls._consumers:
            raise ValueError(
                f"Unsupported stream engine: {settings.engine}. "
                f"Supported: {list(cls._consumers.keys())}"
            )

        consumer_class = cls._consumers[settings.engine]
        return consumer_class(settings)
