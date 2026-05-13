"""
Stream producer abstraction and concrete implementations.

Provides unified producer interface for publishing to different streaming platforms.
"""

import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

from axiompy.data.streaming.types import ProducerResult, StreamEngine, StreamSettings
from axiompy.decorators import Retry
from axiompy.loggers import LoggerFactory
from axiompy.validators import ensure_not_empty, ensure_not_none


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


class KafkaStreamProducer(StreamProducer):
    """Kafka stream producer implementation."""

    def __init__(self, settings: StreamSettings):
        super().__init__(settings)
        try:
            from kafka import KafkaProducer

            self._producer = KafkaProducer(
                bootstrap_servers=settings.bootstrap_servers, **settings.kafka_config
            )
            self.logger.info(f"Connected to Kafka: {settings.bootstrap_servers}")
        except ImportError:
            raise ImportError("kafka-python required: pip install kafka-python")

    def send(self, message, key=None, headers=None, partition=None):
        # Convert message to bytes
        if isinstance(message, str):
            message = message.encode("utf-8")
        elif isinstance(message, dict):
            message = json.dumps(message).encode("utf-8")

        # Convert key to bytes
        key_bytes = key.encode("utf-8") if key else None

        try:
            # Send to Kafka
            future = self._producer.send(
                self.settings.topic,
                value=message,
                key=key_bytes,
                headers=list(headers.items()) if headers else None,
                partition=partition,
            )

            # Wait for result
            record_metadata = future.get(timeout=self.settings.timeout_seconds)

            return ProducerResult(
                success=True,
                offset=str(record_metadata.offset),
                partition=record_metadata.partition,
                metadata={"topic": record_metadata.topic, "timestamp": record_metadata.timestamp},
            )
        except Exception as e:
            self.logger.error(f"Failed to send message to Kafka: {e}")
            return ProducerResult(success=False, error=str(e))

    def send_batch(self, messages, keys=None, headers=None):
        results = []
        for i, message in enumerate(messages):
            key = keys[i] if keys and i < len(keys) else None
            hdrs = headers[i] if headers and i < len(headers) else None
            result = self.send(message, key=key, headers=hdrs)
            results.append(result)
        self.flush()
        return results

    def flush(self):
        self._producer.flush()

    def close(self):
        self._producer.close()
        self.logger.info("Closed Kafka producer")


class KinesisStreamProducer(StreamProducer):
    """AWS Kinesis stream producer implementation."""

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
            self.logger.info(f"Connected to Kinesis stream: {settings.topic}")
        except ImportError:
            raise ImportError("boto3 required: pip install boto3")

    def send(self, message, key=None, headers=None, partition=None):
        # Convert to bytes
        if isinstance(message, str):
            message = message.encode("utf-8")
        elif isinstance(message, dict):
            message = json.dumps(message).encode("utf-8")

        try:
            # Send to Kinesis
            response = self._client.put_record(
                StreamName=self.settings.topic, Data=message, PartitionKey=key or "default"
            )

            return ProducerResult(
                success=True,
                message_id=response["SequenceNumber"],
                metadata={"shard_id": response["ShardId"]},
            )
        except Exception as e:
            self.logger.error(f"Failed to send message to Kinesis: {e}")
            return ProducerResult(success=False, error=str(e))

    def send_batch(self, messages, keys=None, headers=None):
        # Use put_records for batch
        records = []
        for i, message in enumerate(messages):
            if isinstance(message, str):
                message = message.encode("utf-8")
            elif isinstance(message, dict):
                message = json.dumps(message).encode("utf-8")

            records.append(
                {"Data": message, "PartitionKey": keys[i] if keys and i < len(keys) else f"key-{i}"}
            )

        try:
            response = self._client.put_records(StreamName=self.settings.topic, Records=records)

            results = []
            for record in response["Records"]:
                results.append(
                    ProducerResult(
                        success="ErrorCode" not in record,
                        message_id=record.get("SequenceNumber"),
                        error=record.get("ErrorMessage"),
                        metadata={"shard_id": record.get("ShardId")},
                    )
                )

            return results
        except Exception as e:
            self.logger.error(f"Failed to send batch to Kinesis: {e}")
            return [ProducerResult(success=False, error=str(e)) for _ in messages]

    def flush(self):
        pass  # Kinesis doesn't require explicit flush

    def close(self):
        pass  # Boto3 client doesn't require closing


class RedisStreamProducer(StreamProducer):
    """Redis stream producer implementation."""

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
            self.logger.info(f"Connected to Redis stream: {settings.topic}")
        except ImportError:
            raise ImportError("redis required: pip install redis")

    def send(self, message, key=None, headers=None, partition=None):
        # Convert to bytes if needed
        if isinstance(message, str):
            message = message.encode("utf-8")
        elif isinstance(message, dict):
            message = json.dumps(message).encode("utf-8")

        # Prepare fields
        fields = {"data": message}
        if key:
            fields["key"] = key
        if headers:
            fields.update(headers)

        try:
            # Add to Redis stream
            message_id = self._client.xadd(self.settings.topic, fields)

            return ProducerResult(
                success=True,
                message_id=(
                    message_id.decode("utf-8") if isinstance(message_id, bytes) else message_id
                ),
                metadata={"stream": self.settings.topic},
            )
        except Exception as e:
            self.logger.error(f"Failed to send message to Redis: {e}")
            return ProducerResult(success=False, error=str(e))

    def send_batch(self, messages, keys=None, headers=None):
        results = []
        pipeline = self._client.pipeline()

        for i, message in enumerate(messages):
            if isinstance(message, str):
                message = message.encode("utf-8")
            elif isinstance(message, dict):
                message = json.dumps(message).encode("utf-8")

            fields = {"data": message}
            if keys and i < len(keys):
                fields["key"] = keys[i]
            if headers and i < len(headers):
                fields.update(headers[i])

            pipeline.xadd(self.settings.topic, fields)

        try:
            message_ids = pipeline.execute()

            for msg_id in message_ids:
                results.append(
                    ProducerResult(
                        success=True,
                        message_id=msg_id.decode("utf-8") if isinstance(msg_id, bytes) else msg_id,
                    )
                )
        except Exception as e:
            self.logger.error(f"Failed to send batch to Redis: {e}")
            results = [ProducerResult(success=False, error=str(e)) for _ in messages]

        return results

    def flush(self):
        pass

    def close(self):
        self._client.close()
        self.logger.info("Closed Redis connection")


class RabbitMQStreamProducer(StreamProducer):
    """RabbitMQ stream producer implementation."""

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

            self.logger.info(f"Connected to RabbitMQ queue: {settings.queue}")
        except ImportError:
            raise ImportError("pika required: pip install pika")

    def send(self, message, key=None, headers=None, partition=None):
        # Convert to bytes
        if isinstance(message, str):
            message = message.encode("utf-8")
        elif isinstance(message, dict):
            message = json.dumps(message).encode("utf-8")

        try:
            import pika

            # Publish
            self._channel.basic_publish(
                exchange=self.settings.exchange or "",
                routing_key=self.settings.routing_key or self.settings.queue,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    headers=headers,
                ),
            )

            return ProducerResult(success=True)
        except Exception as e:
            self.logger.error(f"Failed to send message to RabbitMQ: {e}")
            return ProducerResult(success=False, error=str(e))

    def send_batch(self, messages, keys=None, headers=None):
        results = []
        for i, message in enumerate(messages):
            hdrs = headers[i] if headers and i < len(headers) else None
            result = self.send(message, headers=hdrs)
            results.append(result)
        return results

    def flush(self):
        pass

    def close(self):
        self._connection.close()
        self.logger.info("Closed RabbitMQ connection")


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
