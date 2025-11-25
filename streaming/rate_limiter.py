"""
Rate Limiter - Production-grade rate limiting for Kafka producer (Netflix-style).
Prevents overwhelming Kafka by controlling publish rate (events per second/minute).
"""
from __future__ import annotations

import time
from threading import Lock
from typing import Optional

from libs.logger import get_logger
from libs.config_loader import get_kafka_config, get_config_value


class RateLimiter:
    """
    Token bucket rate limiter for controlling publish rate.
    Prevents overwhelming Kafka with too many events.
    """

    def __init__(
        self,
        events_per_second: Optional[float] = None,
        events_per_minute: Optional[float] = None,
        logger=None,
        environment: Optional[str] = None
    ):
        """
        Initialize rate limiter.
        
        Args:
            events_per_second: Max events per second (None = no limit)
            events_per_minute: Max events per minute (None = no limit)
            logger: Logger instance
            environment: Environment name (reads from config if None)
        """
        self.logger = logger or get_logger(__name__)
        self._lock = Lock()
        
        # Load rate limit config if not provided
        if events_per_second is None or events_per_minute is None:
            config = get_kafka_config(environment)
            rate_config = config.get("rate_limiting", {})
            events_per_second = events_per_second or rate_config.get("events_per_second")
            events_per_minute = events_per_minute or rate_config.get("events_per_minute")
        
        # Token bucket parameters
        self.events_per_second = events_per_second
        self.events_per_minute = events_per_minute
        
        # Token bucket state
        self.tokens_second = events_per_second if events_per_second else float('inf')
        self.tokens_minute = events_per_minute if events_per_minute else float('inf')
        self.last_refill_second = time.time()
        self.last_refill_minute = time.time()
        
        # Tracking
        self.total_requests = 0
        self.total_blocked = 0
        
        if events_per_second or events_per_minute:
            self.logger.info(
                f"RateLimiter initialized: {events_per_second} events/sec, "
                f"{events_per_minute} events/min"
            )
        else:
            self.logger.info("RateLimiter initialized: no limits (unlimited)")

    def acquire(self, tokens: int = 1) -> bool:
        """
        Try to acquire tokens (permit to publish event).
        
        Args:
            tokens: Number of tokens to acquire (default: 1)
            
        Returns:
            True if tokens acquired (can publish), False if rate limited
        """
        if not self.events_per_second and not self.events_per_minute:
            # No rate limiting
            return True
        
        with self._lock:
            current_time = time.time()
            
            # Refill tokens for per-second limit
            if self.events_per_second:
                elapsed = current_time - self.last_refill_second
                if elapsed >= 1.0:
                    # Refill tokens
                    self.tokens_second = min(
                        self.events_per_second,
                        self.tokens_second + (elapsed * self.events_per_second)
                    )
                    self.last_refill_second = current_time
            
            # Refill tokens for per-minute limit
            if self.events_per_minute:
                elapsed = current_time - self.last_refill_minute
                if elapsed >= 60.0:
                    # Refill tokens
                    self.tokens_minute = min(
                        self.events_per_minute,
                        self.tokens_minute + (elapsed * self.events_per_minute / 60.0)
                    )
                    self.last_refill_minute = current_time
            
            # Check if we have enough tokens
            has_tokens_second = self.tokens_second >= tokens if self.events_per_second else True
            has_tokens_minute = self.tokens_minute >= tokens if self.events_per_minute else True
            
            if has_tokens_second and has_tokens_minute:
                # Consume tokens
                if self.events_per_second:
                    self.tokens_second -= tokens
                if self.events_per_minute:
                    self.tokens_minute -= tokens
                
                self.total_requests += 1
                return True
            else:
                # Rate limited
                self.total_blocked += 1
                self.logger.debug(
                    f"Rate limited: tokens_second={self.tokens_second:.2f}, "
                    f"tokens_minute={self.tokens_minute:.2f}, requested={tokens}"
                )
                return False

    def wait_if_needed(self, tokens: int = 1) -> None:
        """
        Wait until tokens are available (blocking).
        
        Args:
            tokens: Number of tokens needed
        """
        while not self.acquire(tokens):
            # Calculate wait time
            wait_time = 0.1  # Check every 100ms
            
            if self.events_per_second and self.tokens_second < tokens:
                # Wait for per-second refill
                wait_time = max(wait_time, (tokens - self.tokens_second) / self.events_per_second)
            
            if self.events_per_minute and self.tokens_minute < tokens:
                # Wait for per-minute refill
                wait_time = max(wait_time, (tokens - self.tokens_minute) / (self.events_per_minute / 60.0))
            
            time.sleep(min(wait_time, 1.0))  # Max 1 second wait

    def get_stats(self) -> dict:
        """
        Get rate limiter statistics.
        
        Returns:
            Dictionary with stats
        """
        with self._lock:
            return {
                "events_per_second": self.events_per_second,
                "events_per_minute": self.events_per_minute,
                "tokens_available_second": self.tokens_second if self.events_per_second else "unlimited",
                "tokens_available_minute": self.tokens_minute if self.events_per_minute else "unlimited",
                "total_requests": self.total_requests,
                "total_blocked": self.total_blocked,
                "block_rate": (self.total_blocked / self.total_requests * 100) if self.total_requests > 0 else 0
            }

    def reset_stats(self) -> None:
        """Reset statistics counters."""
        with self._lock:
            self.total_requests = 0
            self.total_blocked = 0
            self.logger.info("Rate limiter statistics reset")


def get_rate_limiter(
    events_per_second: Optional[float] = None,
    events_per_minute: Optional[float] = None,
    environment: Optional[str] = None,
    logger=None
) -> RateLimiter:
    """
    Factory function to create rate limiter.
    
    Args:
        events_per_second: Max events per second
        events_per_minute: Max events per minute
        environment: Environment name
        logger: Logger instance
        
    Returns:
        RateLimiter instance
    """
    return RateLimiter(
        events_per_second=events_per_second,
        events_per_minute=events_per_minute,
        environment=environment,
        logger=logger
    )

