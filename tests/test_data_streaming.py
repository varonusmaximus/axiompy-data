"""
Unit tests for streaming module.

Tests producer and consumer implementations with mocking.
"""

import json
from datetime import datetime
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

from axiompy.data.streaming.consumer import (
    KafkaStreamConsumer,
    KinesisStreamConsumer,
    RabbitMQStreamConsumer,
    RedisStreamConsumer,
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
    StreamProducerFactory,
)
from axiompy.data.streaming.types import (
    ConsumerStats,
    StreamEngine,
    StreamMessage,
    StreamSettings,
)

# =============================================================================
# Type Tests
# =============================================================================


class TestStreamTypes:
    """Test streaming type definitions."""

    def test_stream_message_creation(self):
        """Test creating a StreamMessage."""
        msg = StreamMessage(
            key="test-key",
            value=b"test-value",
            headers={"header1": "value1"},
            timestamp=datetime.now(),
            offset="12345",
            partition=0,
        )

        assert msg.key == "test-key"
        assert msg.value == b"test-value"
        assert msg.headers == {"header1": "value1"}
        assert msg.offset == "12345"
        assert msg.partition == 0

    def test_stream_message_to_dict(self):
        """Test converting StreamMessage to dict."""
        msg = StreamMessage(key="key", value=b"value")
        msg_dict = msg.to_dict()

        assert msg_dict["key"] == "key"
        assert msg_dict["value"] == b"value"

    def test_stream_message_from_dict(self):
        """Test creating StreamMessage from dict."""
        data = {"key": "test-key", "value": b"test-value", "offset": "123"}
        msg = StreamMessage.from_dict(data)

        assert msg.key == "test-key"
        assert msg.value == b"test-value"
        assert msg.offset == "123"

    def test_stream_settings_defaults(self):
        """Test default StreamSettings values."""
        settings = StreamSettings(engine=StreamEngine.KAFKA)

        assert settings.engine == StreamEngine.KAFKA
        assert settings.batch_size == 100
        assert settings.timeout_seconds == 30
        assert settings.auto_commit is True

    def test_consumer_stats_calculations(self):
        """Test ConsumerStats calculations."""
        stats = ConsumerStats(
            messages_consumed=1000,
            bytes_consumed=1024 * 1024 * 10,  # 10 MB
            start_time=datetime(2024, 1, 1, 12, 0, 0),
            end_time=datetime(2024, 1, 1, 12, 0, 10),  # 10 seconds
        )

        assert stats.duration_seconds == 10.0
        assert stats.throughput_msg_per_sec == 100.0
        assert stats.throughput_mb_per_sec == 1.0


# =============================================================================
# Producer Tests
# =============================================================================


class TestStreamProducerFactory:
    """Test StreamProducerFactory."""

    def test_create_kafka_producer(self):
        """Test creating Kafka producer."""
        settings = StreamSettings(
            engine=StreamEngine.KAFKA, bootstrap_servers=["localhost:9092"], topic="test-topic"
        )

        with patch("kafka.KafkaProducer"):
            producer = StreamProducerFactory.create(settings)
            assert isinstance(producer, KafkaStreamProducer)

    def test_create_kinesis_producer(self):
        """Test creating Kinesis producer."""
        settings = StreamSettings(
            engine=StreamEngine.KINESIS, topic="test-stream", region="us-east-1"
        )

        with patch("boto3.client"):
            producer = StreamProducerFactory.create(settings)
            assert isinstance(producer, KinesisStreamProducer)

    def test_create_redis_producer(self):
        """Test creating Redis producer."""
        settings = StreamSettings(
            engine=StreamEngine.REDIS, topic="test-stream", redis_host="localhost"
        )

        with patch("redis.Redis"):
            producer = StreamProducerFactory.create(settings)
            assert isinstance(producer, RedisStreamProducer)

    def test_create_rabbitmq_producer(self):
        """Test creating RabbitMQ producer."""
        settings = StreamSettings(
            engine=StreamEngine.RABBITMQ, queue="test-queue", rabbitmq_host="localhost"
        )

        with (
            patch("pika.BlockingConnection"),
            patch("pika.PlainCredentials"),
            patch("pika.ConnectionParameters"),
        ):
            producer = StreamProducerFactory.create(settings)
            assert isinstance(producer, RabbitMQStreamProducer)

    def test_unsupported_engine_raises(self):
        """Test that unsupported engine raises ValueError."""
        settings = StreamSettings(engine="unsupported")  # type: ignore

        # Remove from registry
        original_producers = StreamProducerFactory._producers.copy()
        StreamProducerFactory._producers = {}

        try:
            with pytest.raises(ValueError, match="Unsupported stream engine"):
                StreamProducerFactory.create(settings)
        finally:
            StreamProducerFactory._producers = original_producers


class TestKafkaProducer:
    """Test Kafka producer implementation."""

    @patch("kafka.KafkaProducer")
    def test_send_string_message(self, mock_kafka_producer):
        """Test sending string message to Kafka."""
        # Setup mocks
        mock_producer_instance = MagicMock()
        mock_kafka_producer.return_value = mock_producer_instance

        future = MagicMock()
        mock_metadata = MagicMock()
        mock_metadata.offset = 12345
        mock_metadata.partition = 0
        mock_metadata.topic = "test-topic"
        mock_metadata.timestamp = 1234567890
        future.get.return_value = mock_metadata
        mock_producer_instance.send.return_value = future

        # Create producer and send
        settings = StreamSettings(
            engine=StreamEngine.KAFKA, bootstrap_servers=["localhost:9092"], topic="test-topic"
        )
        producer = KafkaStreamProducer(settings)
        result = producer.send("Hello, Kafka!", key="msg-1")

        # Assertions
        assert result.success is True
        assert result.offset == "12345"
        assert result.partition == 0
        mock_producer_instance.send.assert_called_once()

    @patch("kafka.KafkaProducer")
    def test_send_batch(self, mock_kafka_producer):
        """Test sending batch of messages to Kafka."""
        # Setup mocks
        mock_producer_instance = MagicMock()
        mock_kafka_producer.return_value = mock_producer_instance

        future = MagicMock()
        mock_metadata = MagicMock()
        mock_metadata.offset = 100
        mock_metadata.partition = 0
        mock_metadata.topic = "test-topic"
        mock_metadata.timestamp = 1234567890
        future.get.return_value = mock_metadata
        mock_producer_instance.send.return_value = future

        # Create producer and send batch
        settings = StreamSettings(
            engine=StreamEngine.KAFKA, bootstrap_servers=["localhost:9092"], topic="test-topic"
        )
        producer = KafkaStreamProducer(settings)
        results = producer.send_batch(["msg1", "msg2", "msg3"])

        # Assertions
        assert len(results) == 3
        assert all(r.success for r in results)
        assert mock_producer_instance.send.call_count == 3


class TestKinesisProducer:
    """Test Kinesis producer implementation."""

    @patch("boto3.client")
    def test_send_message(self, mock_boto3_client):
        """Test sending message to Kinesis."""
        # Setup mocks
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.put_record.return_value = {
            "SequenceNumber": "12345",
            "ShardId": "shardId-000000000000",
        }

        # Create producer and send
        settings = StreamSettings(
            engine=StreamEngine.KINESIS, topic="test-stream", region="us-east-1"
        )
        producer = KinesisStreamProducer(settings)
        result = producer.send("Hello, Kinesis!")

        # Assertions
        assert result.success is True
        assert result.message_id == "12345"
        mock_client.put_record.assert_called_once()

    @patch("boto3.client")
    def test_send_batch(self, mock_boto3_client):
        """Test sending batch to Kinesis."""
        # Setup mocks
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        mock_client.put_records.return_value = {
            "Records": [
                {"SequenceNumber": "100", "ShardId": "shard-1"},
                {"SequenceNumber": "101", "ShardId": "shard-1"},
                {"SequenceNumber": "102", "ShardId": "shard-1"},
            ]
        }

        # Create producer and send batch
        settings = StreamSettings(
            engine=StreamEngine.KINESIS, topic="test-stream", region="us-east-1"
        )
        producer = KinesisStreamProducer(settings)
        results = producer.send_batch(["msg1", "msg2", "msg3"])

        # Assertions
        assert len(results) == 3
        assert all(r.success for r in results)
        mock_client.put_records.assert_called_once()


class TestDataFrameIntegration:
    """Test DataFrame integration with producers."""

    @patch("kafka.KafkaProducer")
    def test_send_dataframe_pandas(self, mock_kafka_producer):
        """Test sending Pandas DataFrame to stream."""
        import pandas as pd

        # Setup mocks
        mock_producer_instance = MagicMock()
        mock_kafka_producer.return_value = mock_producer_instance

        future = MagicMock()
        mock_metadata = MagicMock()
        mock_metadata.offset = 100
        mock_metadata.partition = 0
        mock_metadata.topic = "test-topic"
        mock_metadata.timestamp = 1234567890
        future.get.return_value = mock_metadata
        mock_producer_instance.send.return_value = future

        # Create DataFrame and send
        df = pd.DataFrame({"user_id": [1, 2, 3], "action": ["login", "purchase", "logout"]})

        settings = StreamSettings(
            engine=StreamEngine.KAFKA, bootstrap_servers=["localhost:9092"], topic="test-topic"
        )
        producer = KafkaStreamProducer(settings)
        results = producer.send_dataframe(df, key_column="user_id", format="json")

        # Assertions
        assert len(results) == 3
        assert all(r.success for r in results)
        assert mock_producer_instance.send.call_count == 3


# =============================================================================
# Consumer Tests
# =============================================================================


class TestStreamConsumerFactory:
    """Test StreamConsumerFactory."""

    def test_create_kafka_consumer(self):
        """Test creating Kafka consumer."""
        settings = StreamSettings(
            engine=StreamEngine.KAFKA,
            bootstrap_servers=["localhost:9092"],
            topic="test-topic",
            group_id="test-group",
        )

        with patch("kafka.KafkaConsumer"):
            consumer = StreamConsumerFactory.create(settings)
            assert isinstance(consumer, KafkaStreamConsumer)

    def test_create_kinesis_consumer(self):
        """Test creating Kinesis consumer."""
        settings = StreamSettings(
            engine=StreamEngine.KINESIS, topic="test-stream", region="us-east-1"
        )

        with patch("boto3.client"):
            consumer = StreamConsumerFactory.create(settings)
            assert isinstance(consumer, KinesisStreamConsumer)


class TestKafkaConsumer:
    """Test Kafka consumer implementation."""

    @patch("kafka.KafkaConsumer")
    def test_consume_messages(self, mock_kafka_consumer):
        """Test consuming messages from Kafka."""
        # Setup mocks
        mock_consumer_instance = MagicMock()
        mock_kafka_consumer.return_value = mock_consumer_instance

        # Create mock messages
        mock_msg = MagicMock()
        mock_msg.key = b"msg-1"
        mock_msg.value = b"Hello, Kafka!"
        mock_msg.headers = []
        mock_msg.timestamp = 1234567890000
        mock_msg.offset = 100
        mock_msg.partition = 0
        mock_msg.topic = "test-topic"

        from kafka import TopicPartition

        mock_consumer_instance.poll.return_value = {TopicPartition("test-topic", 0): [mock_msg]}

        # Create consumer and consume
        settings = StreamSettings(
            engine=StreamEngine.KAFKA,
            bootstrap_servers=["localhost:9092"],
            topic="test-topic",
            group_id="test-group",
        )
        consumer = KafkaStreamConsumer(settings)

        messages = list(consumer.consume(max_messages=1))

        # Assertions
        assert len(messages) == 1
        assert messages[0].key == "msg-1"
        assert messages[0].value == b"Hello, Kafka!"
        assert messages[0].offset == "100"

    @patch("kafka.KafkaConsumer")
    def test_consume_with_handler(self, mock_kafka_consumer):
        """Test consuming with handler function."""
        # Setup mocks
        mock_consumer_instance = MagicMock()
        mock_kafka_consumer.return_value = mock_consumer_instance

        # Create mock message
        mock_msg = MagicMock()
        mock_msg.key = b"key1"
        mock_msg.value = b'{"user_id": 1, "action": "login"}'
        mock_msg.headers = []
        mock_msg.timestamp = 1234567890000
        mock_msg.offset = 100
        mock_msg.partition = 0
        mock_msg.topic = "test-topic"

        from kafka import TopicPartition

        mock_consumer_instance.poll.side_effect = [
            {TopicPartition("test-topic", 0): [mock_msg]},
            {},  # Empty to stop iteration
        ]

        # Create consumer
        settings = StreamSettings(
            engine=StreamEngine.KAFKA,
            bootstrap_servers=["localhost:9092"],
            topic="test-topic",
            group_id="test-group",
            auto_commit=False,
        )
        consumer = KafkaStreamConsumer(settings)

        # Handler function
        processed_messages = []

        def handler(message):
            data = json.loads(message.value.decode("utf-8"))
            processed_messages.append(data)

        # Consume with handler
        stats = consumer.consume_with_handler(handler, max_messages=1)

        # Assertions
        assert stats.messages_consumed == 1
        assert stats.messages_processed == 1
        assert len(processed_messages) == 1
        assert processed_messages[0]["user_id"] == 1


class TestConsumerDataFrameIntegration:
    """Test DataFrame integration with consumers."""

    @patch("kafka.KafkaConsumer")
    def test_consume_to_dataframe(self, mock_kafka_consumer):
        """Test consuming to DataFrame."""
        # Setup mocks
        mock_consumer_instance = MagicMock()
        mock_kafka_consumer.return_value = mock_consumer_instance

        # Create mock messages
        messages = []
        for i in range(3):
            msg = MagicMock()
            msg.key = f"key{i}".encode()
            msg.value = json.dumps({"user_id": i, "action": "test"}).encode()
            msg.headers = []
            msg.timestamp = 1234567890000
            msg.offset = 100 + i
            msg.partition = 0
            msg.topic = "test-topic"
            messages.append(msg)

        from kafka import TopicPartition

        mock_consumer_instance.poll.side_effect = [
            {TopicPartition("test-topic", 0): messages},
            {},  # Empty to stop
        ]

        # Create consumer
        settings = StreamSettings(
            engine=StreamEngine.KAFKA,
            bootstrap_servers=["localhost:9092"],
            topic="test-topic",
            group_id="test-group",
        )
        consumer = KafkaStreamConsumer(settings)

        # Consume to DataFrame
        df = consumer.consume_to_dataframe(max_messages=10, parse_json=True)

        # Assertions
        assert len(df) == 3
        assert "user_id" in df.columns
        assert "action" in df.columns
        assert df["user_id"].tolist() == [0, 1, 2]


class TestConsumerStats:
    """Test consumer statistics tracking."""

    @patch("kafka.KafkaConsumer")
    def test_stats_tracking(self, mock_kafka_consumer):
        """Test that consumer tracks statistics correctly."""
        # Setup mocks
        mock_consumer_instance = MagicMock()
        mock_kafka_consumer.return_value = mock_consumer_instance

        # Create mock message
        mock_msg = MagicMock()
        mock_msg.key = b"key1"
        mock_msg.value = b"test message"
        mock_msg.headers = []
        mock_msg.timestamp = 1234567890000
        mock_msg.offset = 100
        mock_msg.partition = 0
        mock_msg.topic = "test-topic"

        from kafka import TopicPartition

        mock_consumer_instance.poll.side_effect = [
            {TopicPartition("test-topic", 0): [mock_msg]},
            {},
        ]

        # Create consumer
        settings = StreamSettings(
            engine=StreamEngine.KAFKA,
            bootstrap_servers=["localhost:9092"],
            topic="test-topic",
            group_id="test-group",
        )
        consumer = KafkaStreamConsumer(settings)

        # Consume with handler
        def handler(message):
            pass  # Do nothing

        stats = consumer.consume_with_handler(handler, max_messages=1)

        # Assertions
        assert stats.messages_consumed == 1
        assert stats.messages_processed == 1
        assert stats.messages_failed == 0
        assert stats.bytes_consumed == len(b"test message")


class TestContextManagers:
    """Test context manager support."""

    @patch("kafka.KafkaProducer")
    def test_producer_context_manager(self, mock_kafka_producer):
        """Test using producer as context manager."""
        mock_producer_instance = MagicMock()
        mock_kafka_producer.return_value = mock_producer_instance

        settings = StreamSettings(
            engine=StreamEngine.KAFKA, bootstrap_servers=["localhost:9092"], topic="test-topic"
        )

        with KafkaStreamProducer(settings) as producer:
            assert producer is not None

        # Close should have been called
        mock_producer_instance.close.assert_called_once()

    @patch("kafka.KafkaConsumer")
    def test_consumer_context_manager(self, mock_kafka_consumer):
        """Test using consumer as context manager."""
        mock_consumer_instance = MagicMock()
        mock_kafka_consumer.return_value = mock_consumer_instance

        settings = StreamSettings(
            engine=StreamEngine.KAFKA,
            bootstrap_servers=["localhost:9092"],
            topic="test-topic",
            group_id="test-group",
        )

        with KafkaStreamConsumer(settings) as consumer:
            assert consumer is not None

        # Close should have been called
        mock_consumer_instance.close.assert_called_once()


class TestRedisProducer:
    """Test Redis producer implementation."""

    @patch("redis.Redis")
    def test_send_message(self, mock_redis):
        """Test sending message to Redis."""
        # Setup mocks
        mock_client = MagicMock()
        mock_redis.return_value = mock_client
        mock_client.xadd.return_value = b"1234567890-0"

        # Create producer and send
        settings = StreamSettings(
            engine=StreamEngine.REDIS, topic="test-stream", redis_host="localhost"
        )
        producer = RedisStreamProducer(settings)
        result = producer.send("Hello, Redis!")

        # Assertions
        assert result.success is True
        assert result.message_id == "1234567890-0"
        mock_client.xadd.assert_called_once()

    @patch("redis.Redis")
    def test_send_batch(self, mock_redis):
        """Test sending batch to Redis."""
        # Setup mocks
        mock_client = MagicMock()
        mock_redis.return_value = mock_client
        mock_pipeline = MagicMock()
        mock_client.pipeline.return_value = mock_pipeline
        mock_pipeline.execute.return_value = [b"100-0", b"101-0", b"102-0"]

        # Create producer and send batch
        settings = StreamSettings(
            engine=StreamEngine.REDIS, topic="test-stream", redis_host="localhost"
        )
        producer = RedisStreamProducer(settings)
        results = producer.send_batch(["msg1", "msg2", "msg3"])

        # Assertions
        assert len(results) == 3
        assert all(r.success for r in results)
        mock_pipeline.execute.assert_called_once()


class TestRabbitMQProducer:
    """Test RabbitMQ producer implementation."""

    @patch("pika.BlockingConnection")
    @patch("pika.PlainCredentials")
    @patch("pika.ConnectionParameters")
    def test_send_message(self, mock_params, mock_creds, mock_connection):
        """Test sending message to RabbitMQ."""
        # Setup mocks
        mock_conn_instance = MagicMock()
        mock_connection.return_value = mock_conn_instance
        mock_channel = MagicMock()
        mock_conn_instance.channel.return_value = mock_channel

        # Create producer and send
        settings = StreamSettings(
            engine=StreamEngine.RABBITMQ, queue="test-queue", rabbitmq_host="localhost"
        )
        producer = RabbitMQStreamProducer(settings)
        result = producer.send("Hello, RabbitMQ!")

        # Assertions
        assert result.success is True
        mock_channel.basic_publish.assert_called_once()


class TestProducerErrorHandling:
    """Test producer error handling."""

    @patch("kafka.KafkaProducer")
    def test_send_error(self, mock_kafka_producer):
        """Test handling send errors."""
        # Setup mocks
        mock_producer_instance = MagicMock()
        mock_kafka_producer.return_value = mock_producer_instance

        # Simulate error
        future = MagicMock()
        future.get.side_effect = Exception("Connection failed")
        mock_producer_instance.send.return_value = future

        # Create producer and send
        settings = StreamSettings(
            engine=StreamEngine.KAFKA, bootstrap_servers=["localhost:9092"], topic="test-topic"
        )
        producer = KafkaStreamProducer(settings)
        result = producer.send("Test message")

        # Assertions
        assert result.success is False
        assert "Connection failed" in result.error


class TestDataFrameEdgeCases:
    """Test DataFrame edge cases."""

    @patch("kafka.KafkaProducer")
    def test_send_dataframe_csv_format(self, mock_kafka_producer):
        """Test sending DataFrame with CSV format."""
        import pandas as pd

        # Setup mocks
        mock_producer_instance = MagicMock()
        mock_kafka_producer.return_value = mock_producer_instance

        future = MagicMock()
        mock_metadata = MagicMock()
        mock_metadata.offset = 100
        mock_metadata.partition = 0
        mock_metadata.topic = "test-topic"
        mock_metadata.timestamp = 1234567890
        future.get.return_value = mock_metadata
        mock_producer_instance.send.return_value = future

        # Create DataFrame and send
        df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

        settings = StreamSettings(
            engine=StreamEngine.KAFKA, bootstrap_servers=["localhost:9092"], topic="test-topic"
        )
        producer = KafkaStreamProducer(settings)
        results = producer.send_dataframe(df, format="csv")

        # Assertions
        assert len(results) == 2
        assert all(r.success for r in results)

    @patch("kafka.KafkaProducer")
    def test_send_dataframe_invalid_format(self, mock_kafka_producer):
        """Test sending DataFrame with invalid format raises error."""
        import pandas as pd

        # Setup mocks
        mock_producer_instance = MagicMock()
        mock_kafka_producer.return_value = mock_producer_instance

        # Create DataFrame
        df = pd.DataFrame({"id": [1]})

        settings = StreamSettings(
            engine=StreamEngine.KAFKA, bootstrap_servers=["localhost:9092"], topic="test-topic"
        )
        producer = KafkaStreamProducer(settings)

        # Assertions
        with pytest.raises(ValueError, match="Unsupported format"):
            producer.send_dataframe(df, format="invalid")

    @patch("kafka.KafkaProducer")
    def test_send_dataframe_invalid_type(self, mock_kafka_producer):
        """Test sending invalid data type raises error."""
        # Setup mocks
        mock_producer_instance = MagicMock()
        mock_kafka_producer.return_value = mock_producer_instance

        settings = StreamSettings(
            engine=StreamEngine.KAFKA, bootstrap_servers=["localhost:9092"], topic="test-topic"
        )
        producer = KafkaStreamProducer(settings)

        # Assertions
        with pytest.raises(ValueError, match="Unsupported data type"):
            producer.send_dataframe({"not": "a dataframe"})


class TestConsumerEdgeCases:
    """Test consumer edge cases."""

    @patch("kafka.KafkaConsumer")
    def test_consume_to_dataframe_no_json(self, mock_kafka_consumer):
        """Test consuming to DataFrame without JSON parsing."""
        # Setup mocks
        mock_consumer_instance = MagicMock()
        mock_kafka_consumer.return_value = mock_consumer_instance

        # Create mock messages
        messages = []
        for i in range(3):
            msg = MagicMock()
            msg.key = f"key{i}".encode()
            msg.value = f"value{i}".encode()
            msg.headers = []
            msg.timestamp = 1234567890000
            msg.offset = 100 + i
            msg.partition = 0
            msg.topic = "test-topic"
            messages.append(msg)

        from kafka import TopicPartition

        mock_consumer_instance.poll.side_effect = [{TopicPartition("test-topic", 0): messages}, {}]

        # Create consumer
        settings = StreamSettings(
            engine=StreamEngine.KAFKA,
            bootstrap_servers=["localhost:9092"],
            topic="test-topic",
            group_id="test-group",
        )
        consumer = KafkaStreamConsumer(settings)

        # Consume to DataFrame without JSON parsing
        df = consumer.consume_to_dataframe(max_messages=10, parse_json=False)

        # Assertions
        assert len(df) == 3
        assert "key" in df.columns
        assert "value" in df.columns
        assert "offset" in df.columns

    @patch("kafka.KafkaConsumer")
    def test_consume_with_handler_errors(self, mock_kafka_consumer):
        """Test consuming with handler that raises errors."""
        # Setup mocks
        mock_consumer_instance = MagicMock()
        mock_kafka_consumer.return_value = mock_consumer_instance

        # Create mock message
        mock_msg = MagicMock()
        mock_msg.key = b"key1"
        mock_msg.value = b'{"test": "data"}'
        mock_msg.headers = []
        mock_msg.timestamp = 1234567890000
        mock_msg.offset = 100
        mock_msg.partition = 0
        mock_msg.topic = "test-topic"

        from kafka import TopicPartition

        mock_consumer_instance.poll.side_effect = [
            {TopicPartition("test-topic", 0): [mock_msg]},
            {},
        ]

        # Create consumer
        settings = StreamSettings(
            engine=StreamEngine.KAFKA,
            bootstrap_servers=["localhost:9092"],
            topic="test-topic",
            group_id="test-group",
            auto_commit=False,
        )
        consumer = KafkaStreamConsumer(settings)

        # Handler that raises error
        def failing_handler(message):
            raise ValueError("Processing failed")

        # Consume with handler (should not fail with fail_fast=False)
        stats = consumer.consume_with_handler(failing_handler, max_messages=1, fail_fast=False)

        # Assertions
        assert stats.messages_consumed == 1
        assert stats.messages_processed == 0
        assert stats.messages_failed == 1

    @patch("kafka.KafkaConsumer")
    def test_consume_batch_operation(self, mock_kafka_consumer):
        """Test batch consumption."""
        # Setup mocks
        mock_consumer_instance = MagicMock()
        mock_kafka_consumer.return_value = mock_consumer_instance

        # Create mock messages
        messages = []
        for i in range(5):
            msg = MagicMock()
            msg.key = f"key{i}".encode()
            msg.value = f"value{i}".encode()
            msg.headers = []
            msg.timestamp = 1234567890000
            msg.offset = 100 + i
            msg.partition = 0
            msg.topic = "test-topic"
            messages.append(msg)

        from kafka import TopicPartition

        mock_consumer_instance.poll.side_effect = [
            {TopicPartition("test-topic", 0): messages[:3]},
            {TopicPartition("test-topic", 0): messages[3:]},
            {},
        ]

        # Create consumer
        settings = StreamSettings(
            engine=StreamEngine.KAFKA,
            bootstrap_servers=["localhost:9092"],
            topic="test-topic",
            group_id="test-group",
        )
        consumer = KafkaStreamConsumer(settings)

        # Consume batch
        batch = consumer.consume_batch(batch_size=3, timeout_seconds=5)

        # Assertions
        assert len(batch) == 3


class TestMessageSerialization:
    """Test message serialization edge cases."""

    @patch("kafka.KafkaProducer")
    def test_send_dict_message(self, mock_kafka_producer):
        """Test sending dict message (auto JSON serialization)."""
        # Setup mocks
        mock_producer_instance = MagicMock()
        mock_kafka_producer.return_value = mock_producer_instance

        future = MagicMock()
        mock_metadata = MagicMock()
        mock_metadata.offset = 100
        mock_metadata.partition = 0
        mock_metadata.topic = "test-topic"
        mock_metadata.timestamp = 1234567890
        future.get.return_value = mock_metadata
        mock_producer_instance.send.return_value = future

        # Create producer and send dict
        settings = StreamSettings(
            engine=StreamEngine.KAFKA, bootstrap_servers=["localhost:9092"], topic="test-topic"
        )
        producer = KafkaStreamProducer(settings)
        result = producer.send({"user": "alice", "action": "login"}, key="user-1")

        # Assertions
        assert result.success is True
        # Verify dict was serialized to JSON bytes
        call_args = mock_producer_instance.send.call_args
        assert b'"user"' in call_args[1]["value"]

    @patch("kafka.KafkaProducer")
    def test_send_bytes_message(self, mock_kafka_producer):
        """Test sending bytes message directly."""
        # Setup mocks
        mock_producer_instance = MagicMock()
        mock_kafka_producer.return_value = mock_producer_instance

        future = MagicMock()
        mock_metadata = MagicMock()
        mock_metadata.offset = 100
        mock_metadata.partition = 0
        mock_metadata.topic = "test-topic"
        mock_metadata.timestamp = 1234567890
        future.get.return_value = mock_metadata
        mock_producer_instance.send.return_value = future

        # Create producer and send bytes
        settings = StreamSettings(
            engine=StreamEngine.KAFKA, bootstrap_servers=["localhost:9092"], topic="test-topic"
        )
        producer = KafkaStreamProducer(settings)
        result = producer.send(b"binary data", key="msg-1")

        # Assertions
        assert result.success is True


class TestConsumerStatsProperties:
    """Test all ConsumerStats properties."""

    def test_stats_throughput_mb_per_sec(self):
        """Test MB/sec throughput calculation."""
        from datetime import datetime

        stats = ConsumerStats(
            messages_consumed=100,
            bytes_consumed=10 * 1024 * 1024,  # 10 MB
            start_time=datetime(2024, 1, 1, 12, 0, 0),
            end_time=datetime(2024, 1, 1, 12, 0, 10),  # 10 seconds
        )

        # Assertions
        assert stats.throughput_mb_per_sec == 1.0  # 10 MB / 10 sec = 1 MB/sec

    def test_stats_zero_duration(self):
        """Test stats with zero duration."""
        stats = ConsumerStats(messages_consumed=100)

        # Assertions
        assert stats.duration_seconds == 0.0
        assert stats.throughput_msg_per_sec == 0.0
        assert stats.throughput_mb_per_sec == 0.0


class TestRedisConsumer:
    """Test Redis consumer implementation."""

    @patch("redis.Redis")
    def test_consume_messages(self, mock_redis):
        """Test consuming messages from Redis."""
        # Setup mocks
        mock_client = MagicMock()
        mock_redis.return_value = mock_client

        # Mock Redis XREAD response
        mock_client.xread.return_value = [
            (
                b"test-stream",
                [
                    (b"1234567890-0", {b"data": b"Hello, Redis!", b"key": b"msg-1"}),
                ],
            )
        ]

        # Create consumer
        settings = StreamSettings(
            engine=StreamEngine.REDIS, topic="test-stream", redis_host="localhost"
        )
        consumer = RedisStreamConsumer(settings)

        # Consume messages
        messages = list(consumer.consume(max_messages=1))

        # Assertions
        assert len(messages) == 1
        assert messages[0].value == b"Hello, Redis!"
        assert messages[0].offset == "1234567890-0"


class TestRabbitMQConsumer:
    """Test RabbitMQ consumer implementation."""

    @patch("pika.BlockingConnection")
    @patch("pika.PlainCredentials")
    @patch("pika.ConnectionParameters")
    def test_consume_messages(self, mock_params, mock_creds, mock_connection):
        """Test consuming messages from RabbitMQ."""
        # Setup mocks
        mock_conn_instance = MagicMock()
        mock_connection.return_value = mock_conn_instance
        mock_channel = MagicMock()
        mock_conn_instance.channel.return_value = mock_channel

        # Mock message
        mock_method = MagicMock()
        mock_method.delivery_tag = 1
        mock_method.exchange = ""
        mock_method.routing_key = "test-queue"

        mock_properties = MagicMock()
        mock_properties.headers = {"source": "test"}

        mock_channel.basic_get.side_effect = [
            (mock_method, mock_properties, b"Hello, RabbitMQ!"),
            (None, None, None),  # No more messages
        ]

        # Create consumer
        settings = StreamSettings(
            engine=StreamEngine.RABBITMQ, queue="test-queue", rabbitmq_host="localhost"
        )
        consumer = RabbitMQStreamConsumer(settings)

        # Consume messages
        messages = list(consumer.consume(max_messages=1))

        # Assertions
        assert len(messages) == 1
        assert messages[0].value == b"Hello, RabbitMQ!"


class TestStreamHandler:
    """Test StreamHandler implementation."""

    def test_handler_process_message_success(self):
        """Test successful message processing with handler."""
        from dataclasses import dataclass

        @dataclass
        class UserEvent:
            user_id: int
            action: str

        # Create concrete handler
        processed_events = []

        class TestHandler(StreamHandler[UserEvent]):
            def deserialize(self, message: StreamMessage) -> Optional[UserEvent]:
                data = json.loads(message.value.decode("utf-8"))
                return UserEvent(user_id=data["user_id"], action=data["action"])

            def handle(self, event: UserEvent) -> None:
                processed_events.append(event)

        # Create message
        message = StreamMessage(
            key="user-1", value=json.dumps({"user_id": 1, "action": "login"}).encode("utf-8")
        )

        # Process message
        handler = TestHandler()
        result = handler.process_message(message)

        # Assertions
        assert result is True
        assert len(processed_events) == 1
        assert processed_events[0].user_id == 1
        assert processed_events[0].action == "login"

    def test_handler_deserialization_failure(self):
        """Test handler when deserialization fails."""
        from dataclasses import dataclass

        @dataclass
        class UserEvent:
            user_id: int

        class TestHandler(StreamHandler[UserEvent]):
            def deserialize(self, message: StreamMessage) -> Optional[UserEvent]:
                # Simulating deserialization failure
                return None

            def handle(self, event: UserEvent) -> None:
                pass  # Should not be called

        # Create message
        message = StreamMessage(key="user-1", value=b"invalid json")

        # Process message
        handler = TestHandler()
        result = handler.process_message(message)

        # Assertions
        assert result is False

    def test_handler_processing_exception(self):
        """Test handler when handle() raises exception."""
        from dataclasses import dataclass

        @dataclass
        class UserEvent:
            user_id: int

        class FailingHandler(StreamHandler[UserEvent]):
            def deserialize(self, message: StreamMessage) -> Optional[UserEvent]:
                data = json.loads(message.value.decode("utf-8"))
                return UserEvent(user_id=data["user_id"])

            def handle(self, event: UserEvent) -> None:
                raise ValueError("Processing failed!")

        # Create message
        message = StreamMessage(key="user-1", value=json.dumps({"user_id": 1}).encode("utf-8"))

        # Process message
        handler = FailingHandler()
        result = handler.process_message(message)

        # Assertions
        assert result is False

    def test_handler_with_consumer(self):
        """Test using StreamHandler with consumer."""
        from dataclasses import dataclass

        @dataclass
        class UserEvent:
            user_id: int
            action: str

        processed_events = []

        class JsonUserEventHandler(StreamHandler[UserEvent]):
            def deserialize(self, message: StreamMessage) -> Optional[UserEvent]:
                try:
                    data = json.loads(message.value.decode("utf-8"))
                    return UserEvent(user_id=data["user_id"], action=data["action"])
                except Exception:
                    return None

            def handle(self, event: UserEvent) -> None:
                processed_events.append(event)

        # Create handler
        handler = JsonUserEventHandler()

        # Mock Kafka consumer
        with patch("kafka.KafkaConsumer") as mock_kafka_consumer:
            # Setup mocks
            mock_consumer_instance = MagicMock()
            mock_kafka_consumer.return_value = mock_consumer_instance

            # Create mock messages
            messages = []
            for i in range(3):
                msg = MagicMock()
                msg.key = f"user-{i}".encode()
                msg.value = json.dumps(
                    {"user_id": i, "action": "login" if i % 2 == 0 else "logout"}
                ).encode()
                msg.headers = []
                msg.timestamp = 1234567890000
                msg.offset = 100 + i
                msg.partition = 0
                msg.topic = "test-topic"
                messages.append(msg)

            from kafka import TopicPartition

            mock_consumer_instance.poll.side_effect = [
                {TopicPartition("test-topic", 0): messages},
                {},
            ]

            # Create consumer
            settings = StreamSettings(
                engine=StreamEngine.KAFKA,
                bootstrap_servers=["localhost:9092"],
                topic="test-topic",
                group_id="test-group",
                auto_commit=False,
            )
            consumer = KafkaStreamConsumer(settings)

            # Use handler with consumer
            stats = consumer.consume_with_handler(
                handler=lambda msg: handler.process_message(msg), max_messages=3, fail_fast=False
            )

            # Assertions
            assert stats.messages_consumed == 3
            assert stats.messages_processed == 3
            assert len(processed_events) == 3
            assert processed_events[0].user_id == 0
            assert processed_events[1].action == "logout"

    def test_handler_graceful_error_handling(self):
        """Test handler gracefully handles various error types."""
        from dataclasses import dataclass

        @dataclass
        class Event:
            data: str

        class RobustHandler(StreamHandler[Event]):
            def deserialize(self, message: StreamMessage) -> Optional[Event]:
                try:
                    data = message.value.decode("utf-8")
                    return Event(data=data)
                except Exception:
                    return None

            def handle(self, event: Event) -> None:
                if event.data == "error":
                    raise RuntimeError("Expected error")

        handler = RobustHandler()

        # Test with invalid bytes
        invalid_message = StreamMessage(
            key="test",
            value=b"\x80\x81\x82\x83",  # Invalid UTF-8
        )
        assert handler.process_message(invalid_message) is False

        # Test with error-triggering event
        error_message = StreamMessage(key="test", value=b"error")
        assert handler.process_message(error_message) is False

        # Test with valid event
        valid_message = StreamMessage(key="test", value=b"valid")
        assert handler.process_message(valid_message) is True

    def test_handler_type_preservation(self):
        """Test that handler preserves type information."""
        from dataclasses import dataclass
        from typing import List

        @dataclass
        class ComplexEvent:
            id: int
            tags: List[str]
            metadata: dict

        processed: List[ComplexEvent] = []

        class ComplexHandler(StreamHandler[ComplexEvent]):
            def deserialize(self, message: StreamMessage) -> Optional[ComplexEvent]:
                data = json.loads(message.value.decode("utf-8"))
                return ComplexEvent(id=data["id"], tags=data["tags"], metadata=data["metadata"])

            def handle(self, event: ComplexEvent) -> None:
                processed.append(event)

        # Create complex message
        message = StreamMessage(
            key="complex-1",
            value=json.dumps(
                {
                    "id": 42,
                    "tags": ["important", "urgent"],
                    "metadata": {"source": "api", "version": "2.0"},
                }
            ).encode("utf-8"),
        )

        # Process
        handler = ComplexHandler()
        result = handler.process_message(message)

        # Assertions
        assert result is True
        assert len(processed) == 1
        event = processed[0]
        assert isinstance(event, ComplexEvent)
        assert event.id == 42
        assert event.tags == ["important", "urgent"]
        assert event.metadata["source"] == "api"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
