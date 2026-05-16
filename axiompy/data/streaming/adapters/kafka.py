"""Kafka stream producer and consumer."""

import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from axiompy.data.streaming.consumer import StreamConsumer
from axiompy.data.streaming.producer import StreamProducer
from axiompy.data.streaming.types import ProducerResult, StreamMessage, StreamSettings


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
