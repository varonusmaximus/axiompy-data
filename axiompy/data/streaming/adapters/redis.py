"""Redis stream producer and consumer."""

import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Union

from axiompy.data.streaming.consumer import StreamConsumer
from axiompy.data.streaming.producer import StreamProducer
from axiompy.data.streaming.types import ProducerResult, StreamMessage, StreamSettings


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
