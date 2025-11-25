"""
Dead Letter Queue (DLQ) Handler - Production-grade error handling (Netflix-style).
Handles failed events by routing them to DLQ for investigation and reprocessing.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from libs.logger import get_logger
from libs.config_loader import get_kafka_config, get_config_value
from libs.exceptions import PipelineException, KafkaProduceError
from streaming.kafka_client import KafkaClient, get_kafka_client
from observability.metrics import record_dlq_event


class DeadLetterQueue:
    """
    Dead Letter Queue handler for failed events.
    Enriches events with failure metadata and publishes to DLQ topic.
    """

    def __init__(
        self,
        kafka_client: Optional[KafkaClient] = None,
        dlq_topic: Optional[str] = None,
        logger=None,
        environment: Optional[str] = None
    ):
        """
        Initialize DLQ handler.
        
        Args:
            kafka_client: Kafka client instance (creates new if None)
            dlq_topic: DLQ topic name (reads from config if None)
            logger: Logger instance
            environment: Environment name (dev/staging/prod)
        """
        self.logger = logger or get_logger(__name__)
        self.kafka_client = kafka_client or get_kafka_client(environment=environment, logger=self.logger)
        
        # Load DLQ configuration
        config = get_kafka_config(environment)
        self.dlq_topic = dlq_topic or get_config_value(config, "data_quality.dlq_topic", "user_events_dlq")
        self.enabled = get_config_value(config, "data_quality.enable_dlq", True)
        
        self.logger.info(f"DeadLetterQueue initialized (topic: {self.dlq_topic}, enabled: {self.enabled})")

    def send_to_dlq(
        self,
        event: Dict[str, Any],
        reason: str,
        error: str,
        error_type: Optional[str] = None,
        retry_count: Optional[int] = None,
        additional_context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Send failed event to Dead Letter Queue.
        
        Args:
            event: Original event payload that failed
            reason: Reason for failure (e.g., "schema_validation_failed")
            error: Error message
            error_type: Type of error (e.g., "SchemaValidationError")
            retry_count: Number of retries attempted (if applicable)
            additional_context: Additional context metadata
            
        Returns:
            True if sent to DLQ successfully, False otherwise
        """
        if not self.enabled:
            self.logger.warning("DLQ is disabled, event will not be sent to DLQ")
            return False
        
        try:
            # Enrich event with DLQ metadata
            dlq_event = self._enrich_with_dlq_metadata(
                event=event,
                reason=reason,
                error=error,
                error_type=error_type,
                retry_count=retry_count,
                additional_context=additional_context
            )
            
            # Publish to DLQ topic (bypass validation - DLQ accepts any format)
            self.logger.warning(
                f"Sending event {event.get('event_id', 'unknown')} to DLQ: {reason}"
            )
            
            self.kafka_client.publish(
                payload=dlq_event,
                topic=self.dlq_topic,
                serialize_json=True
            )
            
            # Record metrics
            record_dlq_event(reason=reason, topic=self.dlq_topic)
            
            self.logger.info(
                f"Event {event.get('event_id', 'unknown')} sent to DLQ topic: {self.dlq_topic}"
            )
            
            return True
            
        except KafkaProduceError as e:
            # Critical: DLQ publish failed - event may be lost
            self.logger.critical(
                f"CRITICAL: Failed to send event to DLQ: {str(e)}. "
                f"Original error: {error}. Event may be lost! Event ID: {event.get('event_id', 'unknown')}"
            )
            return False
            
        except Exception as e:
            # Unexpected error during DLQ publish
            self.logger.critical(
                f"CRITICAL: Unexpected error sending to DLQ: {str(e)}. "
                f"Original error: {error}. Event may be lost! Event ID: {event.get('event_id', 'unknown')}"
            )
            return False

    def _enrich_with_dlq_metadata(
        self,
        event: Dict[str, Any],
        reason: str,
        error: str,
        error_type: Optional[str],
        retry_count: Optional[int],
        additional_context: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Enrich event with DLQ metadata for debugging and reprocessing.
        
        Returns:
            Enriched event with DLQ metadata
        """
        # Create copy to avoid modifying original
        dlq_event = event.copy()
        
        # Add DLQ metadata
        dlq_event["_dlq_metadata"] = {
            "reason": reason,
            "error": error,
            "error_type": error_type or "Unknown",
            "timestamp": int(time.time() * 1000),
            "timestamp_iso": datetime.now(timezone.utc).isoformat(),
            "original_event_id": event.get("event_id", "unknown"),
            "retry_count": retry_count,
            "dlq_topic": self.dlq_topic,
            "context": additional_context or {}
        }
        
        return dlq_event

    def is_enabled(self) -> bool:
        """Check if DLQ is enabled."""
        return self.enabled

    def get_dlq_topic(self) -> str:
        """Get DLQ topic name."""
        return self.dlq_topic


def get_dlq_handler(
    environment: Optional[str] = None,
    logger=None
) -> DeadLetterQueue:
    """
    Factory function to create DLQ handler with default dependencies.
    
    Args:
        environment: Environment name (dev/staging/prod)
        logger: Logger instance
        
    Returns:
        DeadLetterQueue instance
    """
    return DeadLetterQueue(environment=environment, logger=logger)

