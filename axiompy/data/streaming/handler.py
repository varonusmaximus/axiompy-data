"""
Stream Message Handlers

Composable handlers that combine deserialization and processing logic.
This enables swapping serialization formats without changing consumer code.
"""

from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar

from axiompy.loggers import LoggerFactory

from .types import StreamMessage

logger = LoggerFactory.create_logger(__name__)

T = TypeVar("T")


class StreamHandler(ABC, Generic[T]):
    """
    Abstract handler for stream messages.

    Combines deserialization and processing in a composable unit.
    This enables swapping serialization formats without changing consumer code.

    Example:
        class JsonUserEventHandler(StreamHandler[UserEvent]):
            def deserialize(self, message: StreamMessage) -> Optional[UserEvent]:
                data = json.loads(message.value.decode('utf-8'))
                return UserEvent.from_dict(data)

            def handle(self, event: UserEvent) -> None:
                save_to_database(event)

        # Use handler
        handler = JsonUserEventHandler()
        consumer = StreamConsumerFactory.create(settings)

        for message in consumer.consume():
            if handler.process_message(message):
                consumer.commit(message)

    Benefits:
        - Format Flexibility: Swap JSON → BSON → Protobuf by changing handler
        - Separation of Concerns: Deserialization separate from processing
        - Testability: Test deserializers and processors independently
        - Reusability: Share handlers across projects
    """

    @abstractmethod
    def deserialize(self, message: StreamMessage) -> Optional[T]:  # pragma: no cover
        """
        Deserialize stream message to domain object.

        Args:
            message: Raw stream message with bytes payload

        Returns:
            Deserialized domain object, or None if deserialization fails

        Example:
            def deserialize(self, message: StreamMessage) -> Optional[UserEvent]:
                try:
                    data = json.loads(message.value.decode('utf-8'))
                    return UserEvent.from_dict(data)
                except Exception as e:
                    logger.error(f"Deserialization failed: {e}")
                    return None
        """
        pass

    @abstractmethod
    def handle(self, event: T) -> None:  # pragma: no cover
        """
        Process deserialized event.

        Args:
            event: Deserialized domain object

        Example:
            def handle(self, event: UserEvent) -> None:
                # Save to database
                db.insert(event)

                # Or send to another stream
                producer.send(event)

                # Or trigger analytics
                analytics.track(event)
        """
        pass

    def process_message(self, message: StreamMessage) -> bool:
        """
        Complete message processing: deserialize + handle.

        This is the main entry point for processing messages.
        It coordinates deserialization and handling with error handling.

        Args:
            message: Raw stream message

        Returns:
            True if message was processed successfully, False otherwise

        Example:
            handler = JsonUserEventHandler()

            for message in consumer.consume():
                if handler.process_message(message):
                    consumer.commit(message)
                else:
                    logger.error(f"Failed to process message {message.key}")
        """
        try:
            # Deserialize
            event = self.deserialize(message)

            if event is None:
                return False

            # Handle
            self.handle(event)
            return True

        except Exception as e:
            logger.error(f"Failed to process message: {e}")
            return False
