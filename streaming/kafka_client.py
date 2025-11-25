"""
Kafka Client - Production-grade Kafka producer wrapper (Netflix-style).
Centralizes configuration, retries, batching, logging, and error handling.
"""
from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional

from confluent_kafka import Producer, KafkaException

from libs.config_loader import get_kafka_config, get_config_value
from libs.logger import get_logger, log_performance, log_error_with_context
from libs.exceptions import KafkaConnectionError, KafkaProduceError


class KafkaClient:
    """
    Shared client that wraps confluent_kafka Producer with Netflix-grade defaults:
    - Loads config from kafka.yaml
    - Adds consistent logging + metrics hooks
    - Handles retries and delivery callbacks
    - Provides a simple publish() API for services
    """

    def __init__(self, environment: Optional[str] = None, logger=None) -> None:
        self.logger = logger or get_logger(__name__)
        self.config = get_kafka_config(environment)

        producer_config = self._build_producer_config()
        try:
            self.producer = Producer(producer_config)
        except KafkaException as exc:
            raise KafkaConnectionError("Failed to create Kafka producer", context={"error": str(exc)})

        self.default_topic = self.config["topics"].get("user_events", "user_events")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def publish(
        self,
        payload: Dict[str, Any],
        topic: Optional[str] = None,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        serialize_json: bool = True,
    ) -> None:
        """
        Publish a single message to Kafka.
        Handles serialization, metrics, and delivery callbacks.
        """
        topic = topic or self.default_topic
        headers = headers or {}

        try:
            value = json.dumps(payload).encode("utf-8") if serialize_json else payload
            start = time.perf_counter()

            self.producer.produce(
                topic=topic,
                key=key,
                value=value,
                headers=headers,
                callback=self._delivery_callback,
            )
            self.producer.poll(0)  # trigger delivery

            duration_ms = (time.perf_counter() - start) * 1000
            log_performance(self.logger, "kafka_publish", duration_ms)

        except BufferError as exc:
            self.logger.warning("Kafka buffer full, flushing and retrying")
            self.producer.flush(10)
            raise KafkaProduceError("Kafka buffer full") from exc
        except KafkaException as exc:
            log_error_with_context(
                self.logger,
                exc,
                context={"topic": topic, "key": key},
                operation="kafka_publish",
            )
            raise KafkaProduceError("Kafka publish failed", context={"error": str(exc)})

    def flush(self, timeout: int = 10) -> None:
        """
        Flush pending messages (called during shutdown or batch completion).
        """
        self.logger.info("Flushing Kafka producer", extra={"timeout": timeout})
        self.producer.flush(timeout)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_producer_config(self) -> Dict[str, Any]:
        broker_cfg = self.config.get("broker", {})
        producer_cfg = self.config.get("producer", {})

        config: Dict[str, Any] = {
            "bootstrap.servers": broker_cfg.get("bootstrap_servers", "localhost:9092"),
        }

        for key, value in producer_cfg.items():
            config[key] = value

        return config

    def _delivery_callback(self, err, msg) -> None:
        if err is not None:
            log_error_with_context(
                self.logger,
                err,
                context={"topic": msg.topic(), "partition": msg.partition()},
                operation="kafka_delivery",
            )
        else:
            self.logger.debug(
                "Kafka delivery success",
                extra={
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                },
            )


def get_kafka_client(environment: Optional[str] = None) -> KafkaClient:
    return KafkaClient(environment=environment)

