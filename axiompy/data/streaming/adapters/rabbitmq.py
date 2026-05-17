"""RabbitMQ stream producer and consumer."""

import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Union

from axiompy.data.streaming.consumer import StreamConsumer
from axiompy.data.streaming.producer import StreamProducer
from axiompy.data.streaming.types import ProducerResult, StreamMessage, StreamSettings


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
