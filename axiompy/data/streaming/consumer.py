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

from axiompy.data.streaming.types import ConsumerStats, StreamEngine, StreamMessage, StreamSettings
from axiompy.decorators import Retry
from axiompy.loggers import LoggerFactory
from axiompy.validators import ensure_positive


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


class KafkaStreamConsumer(StreamConsumer):
    """Kafka stream consumer implementation."""

    def __init__(self, settings: StreamSettings):
        super().__init__(settings)
        try:
            from kafka import KafkaConsumer

            self._consumer = KafkaConsumer(
                settings.topic,
                bootstrap_servers=settings.bootstrap_servers,
                group_id=settings.group_id,
                auto_offset_reset="latest",
                enable_auto_commit=settings.auto_commit,
                **settings.kafka_config,
            )
            self.logger.info(f"Connected to Kafka consumer group: {settings.group_id}")
        except ImportError:
            raise ImportError("kafka-python required: pip install kafka-python")

    def consume(self, max_messages=None, timeout_seconds=None):
        timeout_ms = timeout_seconds * 1000 if timeout_seconds else 1000
        count = 0
        start_time = time.time()

        while True:
            # Check limits
            if max_messages and count >= max_messages:
                break
            if timeout_seconds and (time.time() - start_time) > timeout_seconds:
                break

            # Poll for messages
            records = self._consumer.poll(timeout_ms=timeout_ms, max_records=1)

            if not records:
                break

            for _topic_partition, messages in records.items():
                for msg in messages:
                    count += 1

                    # Convert to StreamMessage
                    stream_msg = StreamMessage(
                        key=msg.key.decode("utf-8") if msg.key else None,
                        value=msg.value,
                        headers={
                            k: v.decode("utf-8") if isinstance(v, bytes) else v
                            for k, v in (msg.headers or [])
                        },
                        timestamp=(
                            datetime.fromtimestamp(msg.timestamp / 1000) if msg.timestamp else None
                        ),
                        offset=str(msg.offset),
                        partition=msg.partition,
                        metadata={"topic": msg.topic},
                    )

                    yield stream_msg

                    if max_messages and count >= max_messages:
                        return

    def commit(self, message=None):
        if message:
            # Commit specific message offset
            from kafka import TopicPartition

            tp = TopicPartition(message.metadata.get("topic"), message.partition)
            self._consumer.commit({tp: int(message.offset) + 1})
        else:
            # Commit all
            self._consumer.commit()

    def close(self):
        self._consumer.close()
        self.logger.info("Closed Kafka consumer")


class KinesisStreamConsumer(StreamConsumer):
    """AWS Kinesis stream consumer implementation."""

    def __init__(self, settings: StreamSettings):
        super().__init__(settings)
        try:
            import boto3

            self._client = boto3.client(
                "kinesis",
                region_name=settings.region,
                aws_access_key_id=settings.aws_access_key_id,
                aws_secret_access_key=settings.aws_secret_access_key,
            )
            self._shard_iterators = {}
            self._initialize_shards()
            self.logger.info(f"Connected to Kinesis stream: {settings.topic}")
        except ImportError:
            raise ImportError("boto3 required: pip install boto3")

    def _initialize_shards(self):
        """Initialize shard iterators for all shards."""
        response = self._client.describe_stream(StreamName=self.settings.topic)
        shards = response["StreamDescription"]["Shards"]

        for shard in shards:
            shard_id = shard["ShardId"]
            iterator_response = self._client.get_shard_iterator(
                StreamName=self.settings.topic,
                ShardId=shard_id,
                ShardIteratorType=self.settings.shard_iterator_type,
            )
            self._shard_iterators[shard_id] = iterator_response["ShardIterator"]

    def consume(self, max_messages=None, timeout_seconds=None):
        count = 0
        start_time = time.time()

        while True:
            # Check limits
            if max_messages and count >= max_messages:
                break
            if timeout_seconds and (time.time() - start_time) > timeout_seconds:
                break

            # Read from each shard
            for shard_id, iterator in list(self._shard_iterators.items()):
                if not iterator:
                    continue

                try:
                    response = self._client.get_records(
                        ShardIterator=iterator,
                        Limit=min(
                            self.settings.batch_size,
                            max_messages - count if max_messages else self.settings.batch_size,
                        ),
                    )

                    # Update iterator
                    self._shard_iterators[shard_id] = response.get("NextShardIterator")

                    # Process records
                    for record in response["Records"]:
                        count += 1

                        stream_msg = StreamMessage(
                            key=record.get("PartitionKey"),
                            value=record["Data"],
                            timestamp=record.get("ApproximateArrivalTimestamp"),
                            offset=record["SequenceNumber"],
                            metadata={"shard_id": shard_id},
                        )

                        yield stream_msg

                        if max_messages and count >= max_messages:
                            return

                except Exception as e:
                    self.logger.error(f"Error reading from shard {shard_id}: {e}")

            # If no messages, break
            if count == 0:
                break

            time.sleep(0.1)  # Small delay between polls

    def commit(self, message=None):
        # Kinesis doesn't have explicit commit
        # Checkpointing would be handled by application
        pass

    def close(self):
        pass  # Boto3 client doesn't require closing


class RedisStreamConsumer(StreamConsumer):
    """Redis stream consumer implementation."""

    def __init__(self, settings: StreamSettings):
        super().__init__(settings)
        try:
            import redis

            self._client = redis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                db=settings.redis_db,
                password=settings.redis_password,
            )
            self._last_id = "0-0"  # Start from beginning
            self.logger.info(f"Connected to Redis stream: {settings.topic}")
        except ImportError:
            raise ImportError("redis required: pip install redis")

    def consume(self, max_messages=None, timeout_seconds=None):
        count = 0
        start_time = time.time()
        timeout_ms = timeout_seconds * 1000 if timeout_seconds else 1000

        while True:
            # Check limits
            if max_messages and count >= max_messages:
                break
            if timeout_seconds and (time.time() - start_time) > timeout_seconds:
                break

            # Read from stream
            if self.settings.group_id:
                # Consumer group mode
                messages = self._client.xreadgroup(
                    groupname=self.settings.group_id,
                    consumername="consumer",
                    streams={self.settings.topic: ">"},
                    count=self.settings.batch_size,
                    block=timeout_ms,
                )
            else:
                # Simple read mode
                messages = self._client.xread(
                    {self.settings.topic: self._last_id},
                    count=self.settings.batch_size,
                    block=timeout_ms,
                )

            if not messages:
                break

            for stream_name, stream_messages in messages:
                for msg_id, fields in stream_messages:
                    count += 1

                    # Decode fields
                    decoded_fields = {
                        k.decode("utf-8") if isinstance(k, bytes) else k: (
                            v.decode("utf-8") if isinstance(v, bytes) else v
                        )
                        for k, v in fields.items()
                    }

                    stream_msg = StreamMessage(
                        key=decoded_fields.get("key"),
                        value=decoded_fields.get("data", "").encode("utf-8"),
                        offset=msg_id.decode("utf-8") if isinstance(msg_id, bytes) else msg_id,
                        metadata={
                            "stream": (
                                stream_name.decode("utf-8")
                                if isinstance(stream_name, bytes)
                                else stream_name
                            )
                        },
                    )

                    self._last_id = stream_msg.offset

                    yield stream_msg

                    if max_messages and count >= max_messages:
                        return

    def commit(self, message=None):
        if message and self.settings.group_id:
            # Acknowledge message in consumer group
            self._client.xack(self.settings.topic, self.settings.group_id, message.offset)

    def close(self):
        self._client.close()
        self.logger.info("Closed Redis connection")


class RabbitMQStreamConsumer(StreamConsumer):
    """RabbitMQ stream consumer implementation."""

    def __init__(self, settings: StreamSettings):
        super().__init__(settings)
        try:
            import pika

            credentials = pika.PlainCredentials(
                settings.rabbitmq_username or "guest", settings.rabbitmq_password or "guest"
            )
            parameters = pika.ConnectionParameters(
                host=settings.rabbitmq_host or "localhost",
                port=settings.rabbitmq_port,
                virtual_host=settings.rabbitmq_virtual_host,
                credentials=credentials,
            )
            self._connection = pika.BlockingConnection(parameters)
            self._channel = self._connection.channel()

            # Declare queue if specified
            if settings.queue:
                self._channel.queue_declare(queue=settings.queue, durable=True)

            self._messages_buffer = []
            self.logger.info(f"Connected to RabbitMQ queue: {settings.queue}")
        except ImportError:
            raise ImportError("pika required: pip install pika")

    def consume(self, max_messages=None, timeout_seconds=None):
        count = 0
        start_time = time.time()

        while True:
            # Check limits
            if max_messages and count >= max_messages:
                break
            if timeout_seconds and (time.time() - start_time) > timeout_seconds:
                break

            # Get message
            method_frame, properties, body = self._channel.basic_get(
                queue=self.settings.queue, auto_ack=self.settings.auto_commit
            )

            if method_frame is None:
                break

            count += 1

            # Convert to StreamMessage
            headers = properties.headers or {}
            stream_msg = StreamMessage(
                value=body,
                headers={k: str(v) for k, v in headers.items()},
                timestamp=datetime.now(),
                offset=str(method_frame.delivery_tag),
                metadata={
                    "delivery_tag": method_frame.delivery_tag,
                    "exchange": method_frame.exchange,
                    "routing_key": method_frame.routing_key,
                },
            )

            yield stream_msg

    def commit(self, message=None):
        if message and not self.settings.auto_commit:
            # Acknowledge message
            delivery_tag = message.metadata.get("delivery_tag")
            if delivery_tag:
                self._channel.basic_ack(delivery_tag=delivery_tag)

    def close(self):
        self._connection.close()
        self.logger.info("Closed RabbitMQ connection")


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
