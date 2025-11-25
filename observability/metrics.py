"""
Metrics Module - Production observability for producer pipeline (Netflix-style).
Tracks events published, latency, error rates, DLQ events for monitoring.
"""
from __future__ import annotations

import time
from typing import Dict, Any, Optional
from collections import defaultdict
from threading import Lock

from libs.logger import get_logger


class MetricsCollector:
    """
    Thread-safe metrics collector for producer pipeline.
    Tracks counters, timers, and gauges for observability.
    """

    def __init__(self, logger=None):
        """
        Initialize metrics collector.
        
        Args:
            logger: Logger instance
        """
        self.logger = logger or get_logger(__name__)
        self._lock = Lock()
        
        # Counters (events, errors, etc.)
        self._counters = defaultdict(int)
        
        # Timers (latency measurements)
        self._timers = defaultdict(list)
        
        # Gauges (current values)
        self._gauges = defaultdict(float)
        
        self.logger.info("MetricsCollector initialized")

    # ------------------------------------------------------------------
    # Counter Metrics
    # ------------------------------------------------------------------

    def increment_counter(
        self,
        metric_name: str,
        value: int = 1,
        tags: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Increment a counter metric.
        
        Args:
            metric_name: Name of the metric (e.g., "events.published")
            value: Value to increment by (default: 1)
            tags: Optional tags for filtering (e.g., {"topic": "user_events"})
        """
        with self._lock:
            key = self._build_key(metric_name, tags)
            self._counters[key] += value
            self.logger.debug(f"Counter {key} incremented by {value}")

    def get_counter(self, metric_name: str, tags: Optional[Dict[str, str]] = None) -> int:
        """Get current counter value."""
        with self._lock:
            key = self._build_key(metric_name, tags)
            return self._counters.get(key, 0)

    # ------------------------------------------------------------------
    # Timer Metrics
    # ------------------------------------------------------------------

    def record_timer(
        self,
        metric_name: str,
        duration_ms: float,
        tags: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Record a timer metric (latency).
        
        Args:
            metric_name: Name of the metric (e.g., "publish.latency")
            duration_ms: Duration in milliseconds
            tags: Optional tags
        """
        with self._lock:
            key = self._build_key(metric_name, tags)
            self._timers[key].append(duration_ms)
            # Keep only last 1000 measurements to prevent memory issues
            if len(self._timers[key]) > 1000:
                self._timers[key] = self._timers[key][-1000:]
            self.logger.debug(f"Timer {key} recorded: {duration_ms}ms")

    def get_timer_stats(
        self,
        metric_name: str,
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, float]:
        """
        Get timer statistics (min, max, avg, p95, p99).
        
        Returns:
            Dictionary with statistics
        """
        with self._lock:
            key = self._build_key(metric_name, tags)
            values = self._timers.get(key, [])
            
            if not values:
                return {"count": 0, "min": 0, "max": 0, "avg": 0, "p95": 0, "p99": 0}
            
            sorted_values = sorted(values)
            count = len(sorted_values)
            
            return {
                "count": count,
                "min": min(sorted_values),
                "max": max(sorted_values),
                "avg": sum(sorted_values) / count,
                "p95": sorted_values[int(count * 0.95)] if count > 0 else 0,
                "p99": sorted_values[int(count * 0.99)] if count > 0 else 0,
            }

    # ------------------------------------------------------------------
    # Gauge Metrics
    # ------------------------------------------------------------------

    def set_gauge(
        self,
        metric_name: str,
        value: float,
        tags: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Set a gauge metric (current value).
        
        Args:
            metric_name: Name of the metric (e.g., "queue.size")
            value: Current value
            tags: Optional tags
        """
        with self._lock:
            key = self._build_key(metric_name, tags)
            self._gauges[key] = value
            self.logger.debug(f"Gauge {key} set to {value}")

    def get_gauge(self, metric_name: str, tags: Optional[Dict[str, str]] = None) -> float:
        """Get current gauge value."""
        with self._lock:
            key = self._build_key(metric_name, tags)
            return self._gauges.get(key, 0.0)

    # ------------------------------------------------------------------
    # Helper Methods
    # ------------------------------------------------------------------

    def _build_key(self, metric_name: str, tags: Optional[Dict[str, str]] = None) -> str:
        """Build metric key with tags."""
        if not tags:
            return metric_name
        
        tag_str = ",".join([f"{k}={v}" for k, v in sorted(tags.items())])
        return f"{metric_name}[{tag_str}]"

    def get_all_metrics(self) -> Dict[str, Any]:
        """
        Get all metrics for reporting/monitoring.
        
        Returns:
            Dictionary with all metrics
        """
        with self._lock:
            return {
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "timers": {
                    key: self.get_timer_stats(key.split("[")[0])
                    for key in self._timers.keys()
                }
            }

    def reset(self) -> None:
        """Reset all metrics (useful for testing)."""
        with self._lock:
            self._counters.clear()
            self._timers.clear()
            self._gauges.clear()
            self.logger.info("All metrics reset")


# Global metrics instance
_metrics_instance: Optional[MetricsCollector] = None


def get_metrics(logger=None) -> MetricsCollector:
    """
    Get or create global metrics instance.
    
    Args:
        logger: Logger instance
        
    Returns:
        MetricsCollector instance
    """
    global _metrics_instance
    if _metrics_instance is None:
        _metrics_instance = MetricsCollector(logger=logger)
    return _metrics_instance


# ------------------------------------------------------------------
# Convenience Functions for Producer Metrics
# ------------------------------------------------------------------

def record_event_published(topic: str = "user_events", tags: Optional[Dict[str, str]] = None) -> None:
    """Record successful event publication."""
    metrics = get_metrics()
    metrics.increment_counter("events.published", tags={"topic": topic, **(tags or {})})


def record_event_failed(
    reason: str,
    topic: str = "user_events",
    tags: Optional[Dict[str, str]] = None
) -> None:
    """Record failed event publication."""
    metrics = get_metrics()
    metrics.increment_counter(
        "events.failed",
        tags={"topic": topic, "reason": reason, **(tags or {})}
    )


def record_publish_latency(duration_ms: float, topic: str = "user_events") -> None:
    """Record publish latency."""
    metrics = get_metrics()
    metrics.record_timer("publish.latency", duration_ms, tags={"topic": topic})


def record_dlq_event(reason: str, topic: str = "user_events") -> None:
    """Record event sent to DLQ."""
    metrics = get_metrics()
    metrics.increment_counter("dlq.events", tags={"topic": topic, "reason": reason})


def record_schema_validation_failed(topic: str = "user_events") -> None:
    """Record schema validation failure."""
    metrics = get_metrics()
    metrics.increment_counter("validation.failed", tags={"topic": topic})


def get_producer_metrics_summary() -> Dict[str, Any]:
    """
    Get summary of producer metrics.
    
    Returns:
        Dictionary with key metrics
    """
    metrics = get_metrics()
    
    return {
        "events_published": metrics.get_counter("events.published"),
        "events_failed": metrics.get_counter("events.failed"),
        "dlq_events": metrics.get_counter("dlq.events"),
        "validation_failures": metrics.get_counter("validation.failed"),
        "publish_latency": metrics.get_timer_stats("publish.latency"),
    }

