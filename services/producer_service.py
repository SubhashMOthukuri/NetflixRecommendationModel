"""
Producer Service - Orchestrates all components for event publishing (Netflix-style).
This is the main service layer that combines:
- Schema validation
- Kafka client
- Error handling
- Logging & metrics
"""
from __future__ import annotations

import time
from typing import Any, Dict, Optional

from libs.logger import get_logger, log_performance, log_error_with_context
from libs.config_loader import get_kafka_config, get_config_value
from libs.exceptions import (
    SchemaValidationError,
    KafkaProduceError,
    PipelineException
)
from schemas.schema_validator import SchemaValidator, get_schema_validator
from streaming.kafka_client import KafkaClient, get_kafka_client


class ProducerService:
    """
    Main service that orchestrates event publishing pipeline:
    1. Validates event against schema
    2. Publishes to Kafka via KafkaClient
    3. Handles errors, DLQ routing, logging, metrics
    
    This is the single entry point for all producer operations.
    """

    def __init__(
        self,
        kafka_client: Optional[KafkaClient] = None,
        schema_validator: Optional[SchemaValidator] = None,
        logger=None,
        environment: Optional[str] = None
    ) -> None:
        """
        Initialize producer service with dependencies.
        
        Args:
            kafka_client: Kafka client instance (creates new if None)
            schema_validator: Schema validator instance (creates new if None)
            logger: Logger instance (creates new if None)
            environment: Environment name (dev/staging/prod)
        """
        self.logger = logger or get_logger(__name__)
        self.kafka_client = kafka_client or get_kafka_client(environment=environment, logger=self.logger)
        self.schema_validator = schema_validator or get_schema_validator(logger=self.logger)
        
        # Load DLQ configuration
        self.config = get_kafka_config(environment)
        self.dlq_enabled = get_config_value(self.config, "data_quality.enable_dlq", False)
        self.dlq_topic = get_config_value(self.config, "data_quality.dlq_topic", "user_events_dlq")
        self.max_retries = get_config_value(self.config, "producer.retries", 5)
        
        self.logger.info(f"ProducerService initialized (DLQ enabled: {self.dlq_enabled})")

    def publish_event(
        self,
        event: Dict[str, Any],
        topic: Optional[str] = None,
        key: Optional[str] = None,
        validate: bool = True,
        headers: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Main method to publish an event to Kafka.
        
        Flow:
        1. Validate event against schema (if validate=True)
        2. Publish to Kafka via KafkaClient
        3. Handle errors and logging
        
        Args:
            event: Event payload (Python dict)
            topic: Kafka topic name (uses default if None)
            key: Message key for partitioning
            validate: Whether to validate schema (default: True)
            headers: Optional message headers
            
        Returns:
            True if published successfully, False otherwise
            
        Raises:
            SchemaValidationError: If validation fails
            KafkaProduceError: If Kafka publish fails
        """
        start_time = time.time()
        event_id = event.get("event_id", "unknown")
        
        try:
            # Step 1: Validate schema
            if validate:
                self.logger.debug(f"Validating event {event_id}")
                self.schema_validator.validate_user_event(event)
                self.logger.debug(f"Event {event_id} passed validation")
            
            # Step 2: Publish to Kafka
            self.logger.info(f"Publishing event {event_id} to Kafka")
            self.kafka_client.publish(
                payload=event,
                topic=topic,
                key=key,
                headers=headers
            )
            
            # Step 3: Log success
            duration_ms = (time.time() - start_time) * 1000
            log_performance(self.logger, "publish_event", duration_ms)
            self.logger.info(f"Successfully published event {event_id}")
            
            return True
            
        except SchemaValidationError as e:
            # Schema validation failed - don't send to Kafka
            duration_ms = (time.time() - start_time) * 1000
            log_error_with_context(
                self.logger,
                e,
                {"event_id": event_id, "operation": "schema_validation"},
                "publish_event"
            )
            # Route to DLQ if configured
            if self.dlq_enabled:
                self._send_to_dlq(event, reason="schema_validation_failed", error=str(e))
            raise
            
        except KafkaProduceError as e:
            # Kafka publish failed - try retry logic
            duration_ms = (time.time() - start_time) * 1000
            log_error_with_context(
                self.logger,
                e,
                {"event_id": event_id, "operation": "kafka_publish"},
                "publish_event"
            )
            
            # Retry with exponential backoff
            if self._retry_publish(event, topic, key, headers, retry_count=0):
                return True
            
            # All retries failed - send to DLQ
            if self.dlq_enabled:
                self._send_to_dlq(event, reason="kafka_publish_failed_after_retries", error=str(e))
            raise
            
        except Exception as e:
            # Unexpected error
            duration_ms = (time.time() - start_time) * 1000
            log_error_with_context(
                self.logger,
                e,
                {"event_id": event_id, "operation": "publish_event"},
                "publish_event"
            )
            # Send to DLQ for unexpected errors
            if self.dlq_enabled:
                self._send_to_dlq(event, reason="unexpected_error", error=str(e))
            raise PipelineException(f"Unexpected error publishing event: {str(e)}", context={"event_id": event_id})

    def publish_batch(
        self,
        events: list[Dict[str, Any]],
        topic: Optional[str] = None,
        validate: bool = True
    ) -> Dict[str, int]:
        """
        Publish multiple events in batch.
        
        Args:
            events: List of event payloads
            topic: Kafka topic name
            validate: Whether to validate each event
            
        Returns:
            Dictionary with success_count and failure_count
        """
        results = {"success_count": 0, "failure_count": 0}
        
        self.logger.info(f"Publishing batch of {len(events)} events")
        
        for event in events:
            try:
                self.publish_event(event, topic=topic, validate=validate)
                results["success_count"] += 1
            except Exception as e:
                results["failure_count"] += 1
                event_id = event.get("event_id", "unknown")
                self.logger.error(f"Failed to publish event {event_id} in batch: {str(e)}")
        
        self.logger.info(
            f"Batch complete: {results['success_count']} succeeded, "
            f"{results['failure_count']} failed"
        )
        
        return results

    def health_check(self) -> Dict[str, Any]:
        """
        Check health of producer service components.
        
        Returns:
            Dictionary with health status of each component
        """
        health = {
            "service": "producer_service",
            "status": "healthy",
            "components": {}
        }
        
        # Check Kafka client
        try:
            # Simple check - try to get producer config
            if self.kafka_client and self.kafka_client.producer:
                health["components"]["kafka_client"] = "healthy"
            else:
                health["components"]["kafka_client"] = "unhealthy"
                health["status"] = "degraded"
        except Exception as e:
            health["components"]["kafka_client"] = f"error: {str(e)}"
            health["status"] = "unhealthy"
        
        # Check schema validator
        try:
            if self.schema_validator and self.schema_validator.schema:
                health["components"]["schema_validator"] = "healthy"
            else:
                health["components"]["schema_validator"] = "unhealthy"
                health["status"] = "degraded"
        except Exception as e:
            health["components"]["schema_validator"] = f"error: {str(e)}"
            health["status"] = "unhealthy"
        
        return health

    def _send_to_dlq(
        self,
        event: Dict[str, Any],
        reason: str,
        error: str
    ) -> None:
        """
        Send failed event to Dead Letter Queue (DLQ).
        
        Args:
            event: Event payload that failed
            reason: Reason for failure (e.g., "schema_validation_failed")
            error: Error message
        """
        try:
            # Enrich event with DLQ metadata
            dlq_event = {
                **event,
                "_dlq_metadata": {
                    "reason": reason,
                    "error": error,
                    "timestamp": int(time.time() * 1000),
                    "original_event_id": event.get("event_id", "unknown")
                }
            }
            
            # Publish to DLQ topic (bypass validation)
            self.logger.warning(
                f"Sending event {event.get('event_id', 'unknown')} to DLQ: {reason}"
            )
            self.kafka_client.publish(
                payload=dlq_event,
                topic=self.dlq_topic,
                serialize_json=True
            )
            self.logger.info(f"Event sent to DLQ topic: {self.dlq_topic}")
            
        except Exception as dlq_error:
            # If DLQ publish fails, log critical error
            self.logger.critical(
                f"Failed to send event to DLQ: {str(dlq_error)}. "
                f"Original error: {error}. Event may be lost!"
            )

    def _retry_publish(
        self,
        event: Dict[str, Any],
        topic: Optional[str],
        key: Optional[str],
        headers: Optional[Dict[str, str]],
        retry_count: int = 0
    ) -> bool:
        """
        Retry publishing event with exponential backoff.
        
        Args:
            event: Event payload
            topic: Kafka topic
            key: Message key
            headers: Message headers
            retry_count: Current retry attempt (0-based)
            
        Returns:
            True if publish succeeded, False if all retries exhausted
        """
        if retry_count >= self.max_retries:
            self.logger.error(
                f"Max retries ({self.max_retries}) exhausted for event {event.get('event_id', 'unknown')}"
            )
            return False
        
        # Exponential backoff: 1s, 2s, 4s, 8s, 16s
        wait_time = 2 ** retry_count
        self.logger.info(
            f"Retrying publish (attempt {retry_count + 1}/{self.max_retries}) "
            f"after {wait_time}s for event {event.get('event_id', 'unknown')}"
        )
        
        time.sleep(wait_time)
        
        try:
            # Try publishing again
            self.kafka_client.publish(
                payload=event,
                topic=topic,
                key=key,
                headers=headers
            )
            self.logger.info(
                f"Successfully published event {event.get('event_id', 'unknown')} "
                f"on retry attempt {retry_count + 1}"
            )
            return True
            
        except KafkaProduceError:
            # Retry failed, try again
            return self._retry_publish(event, topic, key, headers, retry_count + 1)


def get_producer_service(
    environment: Optional[str] = None,
    logger=None
) -> ProducerService:
    """
    Factory function to create ProducerService with default dependencies.
    
    Args:
        environment: Environment name (dev/staging/prod)
        logger: Logger instance
        
    Returns:
        ProducerService instance
    """
    return ProducerService(environment=environment, logger=logger)

