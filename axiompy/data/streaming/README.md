# axiompy.data.streaming - Real-Time Streaming Utilities

**Unified interface for real-time data streaming across Kafka, Kinesis, Redis, and RabbitMQ.**

The `axiompy.data.streaming` module provides production-ready streaming utilities with consistent APIs across different streaming platforms, following the same abstraction patterns as the rest of the axiompy.data module.

---

## 🎯 Key Features

- **🔄 Platform Abstraction**: Same API for Kafka, Kinesis, Redis Streams, RabbitMQ
- **📊 DataFrame Integration**: Send/receive DataFrames directly to/from streams
- **🚰 Data Sink Pattern**: Consumers act as data sinks (like BatchProcessor)
- **📈 Statistics Tracking**: Built-in performance monitoring
- **🔁 Batch Operations**: Optimized batch produce/consume
- **🎭 Context Managers**: Automatic resource cleanup
- **🔌 AxiomPy Integration**: Works with data, logging, decorators

---

## 📦 Installation

```bash
# Install specific streaming engine
pip install "axiompy[streaming-kafka]"      # Kafka support
pip install "axiompy[streaming-kinesis]"    # AWS Kinesis
pip install "axiompy[streaming-redis]"      # Redis Streams
pip install "axiompy[streaming-rabbitmq]"   # RabbitMQ

# Install all streaming engines
pip install "axiompy[streaming-all]"
```

---

## 🚀 Quick Start

### Producer Example

```python
from axiompy.data.streaming import StreamProducerFactory
from axiompy.data.streaming.types import StreamSettings, StreamEngine

# Configure
settings = StreamSettings(
    engine=StreamEngine.KAFKA,
    bootstrap_servers=["localhost:9092"],
    topic="my-topic"
)

# Create producer and send
with StreamProducerFactory.create(settings) as producer:
    # Send single message
    result = producer.send("Hello, World!", key="msg-1")
    print(f"Sent: {result.success}, Offset: {result.offset}")
    
    # Send batch
    results = producer.send_batch(["msg1", "msg2", "msg3"])
    print(f"Sent {len(results)} messages")
```

### Consumer Example

```python
from axiompy.data.streaming import StreamConsumerFactory

# Configure
settings = StreamSettings(
    engine=StreamEngine.KAFKA,
    bootstrap_servers=["localhost:9092"],
    topic="my-topic",
    group_id="my-consumer-group"
)

# Create consumer and consume
with StreamConsumerFactory.create(settings) as consumer:
    for message in consumer.consume(max_messages=10):
        print(f"Received: {message.value.decode('utf-8')}")
        consumer.commit(message)
```

---

## 🔧 Supported Platforms

| Platform | StreamEngine | Producer | Consumer | Batch | Required Package |
|----------|-------------|----------|----------|-------|------------------|
| **Apache Kafka** | `KAFKA` | ✅ | ✅ | ✅ | `kafka-python` |
| **AWS Kinesis** | `KINESIS` | ✅ | ✅ | ✅ | `boto3` |
| **Redis Streams** | `REDIS` | ✅ | ✅ | ✅ | `redis` |
| **RabbitMQ** | `RABBITMQ` | ✅ | ✅ | ❌ | `pika` |

---

## 📚 Platform Configuration

### Kafka

```python
from axiompy.data.streaming.types import StreamSettings, StreamEngine

settings = StreamSettings(
    engine=StreamEngine.KAFKA,
    bootstrap_servers=["localhost:9092"],
    topic="my-topic",
    group_id="my-group",
    kafka_config={
        "compression_type": "gzip",
        "acks": "all"
    }
)
```

### AWS Kinesis

```python
settings = StreamSettings(
    engine=StreamEngine.KINESIS,
    topic="my-stream",  # Stream name
    region="us-east-1",
    aws_access_key_id="YOUR_KEY",
    aws_secret_access_key="YOUR_SECRET",
    shard_iterator_type="LATEST"  # or "TRIM_HORIZON"
)
```

### Redis Streams

```python
settings = StreamSettings(
    engine=StreamEngine.REDIS,
    topic="my-stream",  # Stream name
    redis_host="localhost",
    redis_port=6379,
    redis_password="password",
    group_id="my-consumer-group"  # Optional for consumer groups
)
```

### RabbitMQ

```python
settings = StreamSettings(
    engine=StreamEngine.RABBITMQ,
    queue="my-queue",
    rabbitmq_host="localhost",
    rabbitmq_port=5672,
    rabbitmq_username="guest",
    rabbitmq_password="guest",
    exchange="",  # Default exchange
    routing_key="my-queue"
)
```

---

## 🔌 Producer API

### Basic Operations

```python
from axiompy.data.streaming import StreamProducerFactory

producer = StreamProducerFactory.create(settings)

# Send string
result = producer.send("Hello, World!", key="msg-1")

# Send bytes
result = producer.send(b"binary data", key="msg-2")

# Send dict (auto-serialized to JSON)
result = producer.send({"user_id": 123, "action": "login"}, key="user-123")

# Send with headers
result = producer.send(
    "message",
    key="msg-3",
    headers={"source": "api", "version": "1.0"}
)

# Flush pending messages
producer.flush()

# Cleanup
producer.close()
```

### Batch Operations

```python
# Send multiple messages
messages = ["msg1", "msg2", "msg3"]
keys = ["key1", "key2", "key3"]
results = producer.send_batch(messages, keys=keys)

print(f"Sent {sum(r.success for r in results)} messages")
```

### DataFrame Integration

```python
import pandas as pd

# Create DataFrame
df = pd.DataFrame({
    'user_id': [1, 2, 3],
    'action': ['login', 'purchase', 'logout'],
    'timestamp': pd.date_range('2024-01-01', periods=3)
})

# Send each row as JSON message
results = producer.send_dataframe(
    df,
    key_column='user_id',  # Use user_id as message key
    format='json'  # or 'csv'
)

print(f"Sent {len(results)} events to stream")
```

---

## 📥 Consumer API

### Iterator Pattern

```python
from axiompy.data.streaming import StreamConsumerFactory

consumer = StreamConsumerFactory.create(settings)

# Consume as iterator
for message in consumer.consume(max_messages=100, timeout_seconds=30):
    print(f"Key: {message.key}")
    print(f"Value: {message.value.decode('utf-8')}")
    print(f"Offset: {message.offset}")
    print(f"Timestamp: {message.timestamp}")
    
    # Process message
    # ...
    
    # Commit offset
    consumer.commit(message)

consumer.close()
```

### Batch Consumption

```python
# Consume batch of messages
messages = consumer.consume_batch(batch_size=100, timeout_seconds=10)

print(f"Consumed {len(messages)} messages")
for msg in messages:
    process(msg)

# Commit all at once
consumer.commit()
```

### Data Sink Pattern

#### Option 1: Simple Handler Function

```python
# Define handler function
def save_to_database(message):
    """Handler function - saves message to database."""
    import json
    from axiompy.io import DatabaseFactory
    
    db = DatabaseFactory.create(...)
    data = json.loads(message.value.decode('utf-8'))
    db.set("events", data)

# Process stream as data sink
stats = consumer.consume_with_handler(
    handler=save_to_database,
    max_messages=1000,
    timeout_seconds=60,
    fail_fast=False  # Continue on errors
)

print(f"Processed: {stats.messages_processed}")
print(f"Failed: {stats.messages_failed}")
print(f"Throughput: {stats.throughput_msg_per_sec:.1f} msg/sec")
```

#### Option 2: StreamHandler (Type-Safe, Composable)

For production systems, use `StreamHandler` for type-safe, composable message processing:

```python
from axiompy.data.streaming import StreamHandler
from dataclasses import dataclass
from typing import Optional
import json

# Define domain model
@dataclass
class UserEvent:
    user_id: int
    action: str
    timestamp: str
    amount: float = 0.0

# Create handler that combines deserialization + processing
class JsonUserEventHandler(StreamHandler[UserEvent]):
    """Handler that deserializes JSON messages to UserEvent objects."""
    
    def deserialize(self, message: StreamMessage) -> Optional[UserEvent]:
        """Deserialize JSON message to UserEvent."""
        try:
            data = json.loads(message.value.decode('utf-8'))
            return UserEvent(
                user_id=data['user_id'],
                action=data['action'],
                timestamp=data.get('timestamp', datetime.now().isoformat()),
                amount=data.get('amount', 0.0)
            )
        except Exception as e:
            logger.error(f"Failed to deserialize: {e}")
            return None
    
    def handle(self, event: UserEvent) -> None:
        """Process the deserialized event."""
        # Type-safe processing with domain model
        save_to_database(event)
        
        if event.action == "purchase" and event.amount > 100:
            send_high_value_alert(event)

# Use handler with consumer
handler = JsonUserEventHandler()

with StreamConsumerFactory.create(settings) as consumer:
    for message in consumer.consume(max_messages=100):
        if handler.process_message(message):
            consumer.commit(message)
        else:
            logger.error(f"Failed to process {message.key}")

# Or use with consume_with_handler
stats = consumer.consume_with_handler(
    handler=handler.process_message,
    max_messages=1000,
    fail_fast=False
)
```

**Benefits of StreamHandler:**
- **Type Safety**: Full type hints and IDE support
- **Separation of Concerns**: Deserialization separate from business logic
- **Format Flexibility**: Swap JSON → BSON → Protobuf without changing processing code
- **Testability**: Test deserializers and handlers independently
- **Reusability**: Share handlers across different consumers
- **Error Handling**: Built-in error handling in `process_message()`

### DataFrame Consumption

```python
# Consume directly to DataFrame
df = consumer.consume_to_dataframe(
    max_messages=1000,
    timeout_seconds=30,
    parse_json=True  # Parse JSON payloads
)

print(df.head())
print(f"Consumed {len(df)} messages")
```

---

## 📊 Statistics & Monitoring

```python
# Get consumer statistics
stats = consumer.get_stats()

print(f"Messages consumed: {stats.messages_consumed}")
print(f"Messages processed: {stats.messages_processed}")
print(f"Messages failed: {stats.messages_failed}")
print(f"Bytes consumed: {stats.bytes_consumed}")
print(f"Duration: {stats.duration_seconds:.2f} seconds")
print(f"Throughput: {stats.throughput_msg_per_sec:.1f} msg/sec")
print(f"Throughput: {stats.throughput_mb_per_sec:.2f} MB/sec")
```

---

## 🔗 Integration with AxiomPy

### With Data Transformers

```python
from axiompy.data import DataTransformerFactory

# Consume, transform, produce
consumer = StreamConsumerFactory.create(input_settings)
producer = StreamProducerFactory.create(output_settings)

# Batch processing
batch = consumer.consume_batch(100)
df = consumer.consume_to_dataframe(100)

# Transform
transformer = DataTransformerFactory.create_auto(df)
clean_df = transformer.fill_nulls(df, strategy="mean")
clean_df = transformer.filter_rows(clean_df, "score > 0")

# Send to output stream
producer.send_dataframe(clean_df, key_column='id')
```

### With Data Quality

```python
from axiompy.data import DataProfilerFactory

# Monitor stream quality
df = consumer.consume_to_dataframe(1000)
profiler = DataProfilerFactory.create_auto(df)
report = profiler.profile(df)

if len(report.issues) > 0:
    print(f"Quality issues detected: {report.issues}")
```

### With Lineage Tracking

```python
from axiompy.data import LineageTrackerFactory
from axiompy.io import DatabaseFactory

# Track stream processing
db = DatabaseFactory.create(...)
tracker = LineageTrackerFactory.create_auto(df, storage=db)

tracker.track_transformation(
    job_name="stream_processing",
    input_sources=["kafka://input-topic"],
    output_targets=["kafka://output-topic"],
    transformation="Real-time data cleaning and enrichment",
    data_in=input_df,
    data_out=output_df
)
```

### With Batch Processor

```python
from axiompy.data import BatchProcessorFactory

# Process large stream in batches
df = consumer.consume_to_dataframe(10000)
processor = BatchProcessorFactory.create_auto(df, batch_size=1000)

def process_batch(batch):
    # Transform batch
    return clean_data(batch)

results = processor.process_batches(
    df,
    batch_fn=process_batch,
    sink=lambda batch: producer.send_dataframe(batch),
    show_progress=True
)
```

---

## 🎯 StreamHandler Pattern

### Why Use StreamHandler?

Traditional message processing couples deserialization with business logic:

```python
# Traditional approach - tightly coupled
def process_message(message):
    # Deserialization + processing mixed together
    data = json.loads(message.value.decode('utf-8'))
    user_id = data['user_id']
    action = data['action']
    
    if action == "purchase":
        save_purchase(user_id, data['amount'])
    
    # Hard to test, hard to reuse, no type safety
```

StreamHandler separates these concerns:

```python
# StreamHandler approach - clean separation
class PurchaseEventHandler(StreamHandler[PurchaseEvent]):
    def deserialize(self, message: StreamMessage) -> Optional[PurchaseEvent]:
        """Deserialization logic - easily testable"""
        try:
            data = json.loads(message.value.decode('utf-8'))
            return PurchaseEvent.from_dict(data)
        except Exception as e:
            logger.error(f"Deserialization failed: {e}")
            return None
    
    def handle(self, event: PurchaseEvent) -> None:
        """Business logic - type-safe and testable"""
        save_purchase(event)
        
        # Type hints give you IDE autocomplete!
        if event.amount > 100:
            send_alert(event.user_id)
```

### Multiple Handlers Example

Route different message types to different handlers:

```python
from axiompy.data.streaming import StreamHandler

# Define handlers for each event type
class LoginHandler(StreamHandler[LoginEvent]):
    def deserialize(self, message):
        data = json.loads(message.value.decode('utf-8'))
        return LoginEvent(**data)
    
    def handle(self, event: LoginEvent):
        logger.info(f"User {event.user_id} logged in from {event.ip}")

class PurchaseHandler(StreamHandler[PurchaseEvent]):
    def deserialize(self, message):
        data = json.loads(message.value.decode('utf-8'))
        return PurchaseEvent(**data)
    
    def handle(self, event: PurchaseEvent):
        save_purchase(event)
        update_inventory(event.product_id)

# Route messages based on type
login_handler = LoginHandler()
purchase_handler = PurchaseHandler()

def route_message(message):
    event_type = message.headers.get('event_type')
    
    if event_type == 'login':
        return login_handler.process_message(message)
    elif event_type == 'purchase':
        return purchase_handler.process_message(message)
    else:
        logger.warning(f"Unknown event type: {event_type}")
        return False

# Process with routing
with StreamConsumerFactory.create(settings) as consumer:
    stats = consumer.consume_with_handler(
        handler=route_message,
        max_messages=1000
    )
```

### Testing StreamHandlers

StreamHandlers are easy to test in isolation:

```python
import pytest
from axiompy.data.streaming.types import StreamMessage

def test_purchase_handler_deserialization():
    """Test deserialization logic independently."""
    handler = PurchaseHandler()
    
    message = StreamMessage(
        value=json.dumps({"user_id": 1, "amount": 99.99}).encode()
    )
    
    event = handler.deserialize(message)
    
    assert event is not None
    assert event.user_id == 1
    assert event.amount == 99.99

def test_purchase_handler_processing():
    """Test business logic with mock events."""
    handler = PurchaseHandler()
    
    event = PurchaseEvent(user_id=1, product_id=42, amount=150.00)
    
    handler.handle(event)
    
    # Verify side effects
    assert_purchase_saved(event)
    assert_inventory_updated(event.product_id)

def test_purchase_handler_invalid_json():
    """Test error handling."""
    handler = PurchaseHandler()
    
    message = StreamMessage(value=b"invalid json")
    
    event = handler.deserialize(message)
    
    assert event is None  # Should return None on error
```

---

## 💡 Use Cases

### 1. Stream ETL Pipeline

```python
# Read from Kafka, transform, write to Kinesis
kafka_consumer = StreamConsumerFactory.create(kafka_settings)
kinesis_producer = StreamProducerFactory.create(kinesis_settings)

for message in kafka_consumer.consume(max_messages=1000):
    # Parse and transform
    data = json.loads(message.value)
    transformed = transform_data(data)
    
    # Send to output stream
    kinesis_producer.send(
        json.dumps(transformed),
        key=str(data['id'])
    )
```

### 2. Real-Time Analytics

```python
# Consume stream and aggregate in real-time
df = consumer.consume_to_dataframe(max_messages=1000)

# Analyze
summary = df.groupby('category').agg({
    'amount': ['sum', 'mean', 'count']
})

print(summary)
```

### 3. Stream to Database Sink

```python
def save_event(message):
    """Save stream event to database."""
    data = json.loads(message.value)
    db.set("events", {
        "user_id": data['user_id'],
        "action": data['action'],
        "timestamp": datetime.now()
    })

# Consume 10,000 messages and save to DB
stats = consumer.consume_with_handler(
    handler=save_event,
    max_messages=10000,
    fail_fast=False
)

print(f"Saved {stats.messages_processed} events")
```

### 4. Multi-Stream Processing

```python
# Consume from multiple streams
consumers = [
    StreamConsumerFactory.create(settings1),
    StreamConsumerFactory.create(settings2)
]

all_messages = []
for consumer in consumers:
    messages = consumer.consume_batch(100)
    all_messages.extend(messages)

# Process combined stream
process_messages(all_messages)
```

---

## 🔍 Error Handling

```python
from axiompy.data.streaming import StreamProducerFactory

try:
    producer = StreamProducerFactory.create(settings)
    result = producer.send("message")
    
    if not result.success:
        print(f"Failed to send: {result.error}")
    
except Exception as e:
    print(f"Producer error: {e}")
finally:
    producer.close()
```

---

## 🎭 Context Managers

```python
# Automatic cleanup with context managers
with StreamProducerFactory.create(settings) as producer:
    producer.send("message")
    # Automatically flushed and closed

with StreamConsumerFactory.create(settings) as consumer:
    for message in consumer.consume(max_messages=10):
        process(message)
    # Automatically closed
```

---

## 🧪 Testing

Mock implementations for unit testing:

```python
from unittest.mock import patch

# Mock Kafka producer
@patch('axiompy.data.streaming.adapters.kafka.KafkaProducer')
def test_my_function(mock_kafka):
    mock_producer = MagicMock()
    mock_kafka.return_value = mock_producer
    
    # Test your code
    settings = StreamSettings(...)
    producer = StreamProducerFactory.create(settings)
    result = producer.send("test")
    
    assert result.success
```

---

## ⚡ Performance Tips

1. **Use Batch Operations**: Send/consume multiple messages at once
2. **Enable Compression**: Configure compression at platform level
3. **Tune Batch Size**: Adjust `batch_size` for your workload
4. **Monitor Statistics**: Track throughput and adjust accordingly
5. **Use Consumer Groups**: Distribute load across multiple consumers (Kafka, Redis)
6. **Connection Pooling**: Reuse producer/consumer instances

---

## 🔧 Advanced Configuration

### Custom Retry Logic

```python
from axiompy.decorators import Retry
from axiompy.loggers import LoggerFactory

logger = LoggerFactory.create_logger(__name__)

@Retry(logger, max_attempts=3, delay=1.0, backoff=2.0)
def send_with_retry(producer, message):
    result = producer.send(message)
    if not result.success:
        raise Exception(f"Send failed: {result.error}")
    return result
```

### Custom Serialization

```python
import pickle

# Custom serialization
data = {"complex": "object"}
serialized = pickle.dumps(data)
producer.send(serialized, key="custom-1")

# Custom deserialization
for message in consumer.consume():
    data = pickle.loads(message.value)
    process(data)
```

---

## 📖 API Reference

See the [API documentation](../README.md) for complete details on all classes and methods.

### Key Classes

- `StreamProducer` - Abstract base for producers
- `StreamConsumer` - Abstract base for consumers
- `StreamProducerFactory` - Create producers
- `StreamConsumerFactory` - Create consumers
- `StreamHandler[T]` - Composable message handler with deserialization
- `StreamMessage` - Standard message format
- `StreamSettings` - Configuration
- `ProducerResult` - Send operation result
- `ConsumerStats` - Consumption statistics

---

## 🐛 Troubleshooting

### "Module not found" error
```bash
# Install required package
pip install kafka-python  # For Kafka
pip install boto3          # For Kinesis
pip install redis          # For Redis
pip install pika           # For RabbitMQ
```

### Connection timeouts
- Check network connectivity
- Verify credentials
- Increase `timeout_seconds` in settings

### Consumer not receiving messages
- Check consumer group ID
- Verify topic/queue exists
- Check offset position (Kafka: `auto_offset_reset`)

---

**Made with ❤️ for real-time data engineers working across multiple streaming platforms.**

---

**Last Updated:** 2025-12-03

