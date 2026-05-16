"""AWS Kinesis stream producer and consumer."""

import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

from axiompy.data.streaming.consumer import StreamConsumer
from axiompy.data.streaming.producer import StreamProducer
from axiompy.data.streaming.types import ProducerResult, StreamMessage, StreamSettings


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
