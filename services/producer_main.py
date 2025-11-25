"""
Producer Main - Entry point for event producer service (Netflix-style).
This is the main script that runs the producer and uses ProducerService
to publish events to Kafka.
"""
from __future__ import annotations

import sys
import time
import signal
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from libs.logger import get_logger
from libs.exceptions import SchemaValidationError, KafkaProduceError
from services.producer_service import get_producer_service

# Import event generator (from dummy_data_generator)
from data_streaming.dummy_data_generator.simulate_events import generate_video_event


class ProducerMain:
    """
    Main entry point for producer service.
    Orchestrates event generation and publishing using ProducerService.
    """

    def __init__(self, environment: str = None):
        """Initialize producer main with service."""
        self.logger = get_logger(__name__)
        self.service = get_producer_service(environment=environment, logger=self.logger)
        self.running = True
        
        # Setup graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info("ProducerMain initialized")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False

    def run(self, interval_seconds: float = 1.0):
        """
        Main loop: Generate events and publish to Kafka.
        
        Args:
            interval_seconds: Time between event generations (default: 1 second)
        """
        self.logger.info("Starting producer main loop...")
        event_count = 0
        
        try:
            while self.running:
                # Generate event (using dummy data generator)
                event = generate_video_event(int(time.time() * 1000))
                event_id = event.get("event_id", "unknown")
                
                try:
                    # Publish event using ProducerService
                    success = self.service.publish_event(
                        event=event,
                        validate=True  # Enable schema validation
                    )
                    
                    if success:
                        event_count += 1
                        self.logger.debug(f"Published event {event_id} (total: {event_count})")
                    
                except SchemaValidationError as e:
                    self.logger.error(f"Schema validation failed for event {event_id}: {str(e)}")
                    # Event sent to DLQ by service
                    
                except KafkaProduceError as e:
                    self.logger.error(f"Kafka publish failed for event {event_id}: {str(e)}")
                    # Retry logic and DLQ handled by service
                    
                except Exception as e:
                    self.logger.error(f"Unexpected error publishing event {event_id}: {str(e)}")
                
                # Wait before next event
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received")
        finally:
            self._shutdown(event_count)

    def _shutdown(self, event_count: int):
        """Graceful shutdown."""
        self.logger.info(f"Shutting down... Published {event_count} events total")
        
        # Flush Kafka producer
        try:
            self.service.kafka_client.flush()
            self.logger.info("Kafka producer flushed")
        except Exception as e:
            self.logger.error(f"Error flushing Kafka producer: {str(e)}")
        
        # Health check before shutdown
        health = self.service.health_check()
        self.logger.info(f"Final health check: {health}")
        
        self.logger.info("Producer shutdown complete")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Kafka Event Producer")
    parser.add_argument(
        "--environment",
        type=str,
        default=None,
        help="Environment name (dev/staging/prod)"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Seconds between events (default: 1.0)"
    )
    
    args = parser.parse_args()
    
    # Create and run producer
    producer = ProducerMain(environment=args.environment)
    producer.run(interval_seconds=args.interval)


if __name__ == "__main__":
    main()

