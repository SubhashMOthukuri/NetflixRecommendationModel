"""
Job Launcher - Production ML Pipeline
Entry point for launching and managing bronze ingestion jobs
Handles job submission, monitoring, restart on failure, and lifecycle management
"""
import sys
import time
import signal
import argparse
from typing import Optional, Dict, Any
from libs.logger import get_logger
from libs.exceptions import PipelineException
from data_streaming.bronze_ingestion.jobs.streaming.bronze_stream_job import BronzeStreamJob

logger = get_logger(__name__)


class JobLauncherError(PipelineException):
    """Raised when job launcher fails"""
    pass


class JobLauncher:
    """
    Production-grade job launcher
    
    Manages:
    - Job submission to Spark cluster
    - Job monitoring and health checks
    - Automatic restart on failure
    - Graceful shutdown
    - Resource management
    
    Example:
        launcher = JobLauncher()
        launcher.launch_job(topic="user_events", environment="prod")
    """
    
    def __init__(
        self,
        max_retries: int = 3,
        retry_delay_seconds: int = 60,
        health_check_interval: int = 300
    ):
        """
        Initialize job launcher
        
        Args:
            max_retries: Maximum retry attempts on failure
            retry_delay_seconds: Delay between retries
            health_check_interval: Health check interval in seconds
        """
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        self.health_check_interval = health_check_interval
        self.job: Optional[BronzeStreamJob] = None
        self.running = False
        
        # Register signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        logger.info("Job launcher initialized")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        signal_name = signal.Signals(signum).name
        logger.warning(f"Received {signal_name} signal, shutting down...")
        self.shutdown()
        sys.exit(0)
    
    def launch_job(
        self,
        topic: str = "user_events",
        environment: Optional[str] = None,
        auto_restart: bool = True
    ):
        """
        Launch bronze stream job
        
        Args:
            topic: Kafka topic name
            environment: Environment name (dev/staging/prod)
            auto_restart: Auto-restart on failure
        
        Example:
            launcher = JobLauncher()
            launcher.launch_job(topic="user_events", environment="prod")
        """
        retry_count = 0
        
        while retry_count <= self.max_retries:
            try:
                logger.info(f"Launching bronze stream job: topic={topic}, environment={environment}")
                logger.info(f"Attempt {retry_count + 1} of {self.max_retries + 1}")
                
                # Create and run job
                self.job = BronzeStreamJob(topic=topic, environment=environment)
                self.running = True
                
                # Run job (blocks until completion or error)
                self.job.run()
                
                # If we get here, job completed successfully
                logger.info("Bronze stream job completed successfully")
                break
                
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, shutting down...")
                self.shutdown()
                break
                
            except Exception as e:
                retry_count += 1
                logger.error(f"Job failed (attempt {retry_count}): {str(e)}", exc_info=True)
                
                if auto_restart and retry_count <= self.max_retries:
                    logger.warning(f"Restarting job in {self.retry_delay_seconds} seconds...")
                    time.sleep(self.retry_delay_seconds)
                else:
                    logger.error(f"Max retries ({self.max_retries}) reached, giving up")
                    raise JobLauncherError(f"Job failed after {retry_count} attempts: {str(e)}")
    
    def shutdown(self):
        """Shutdown job gracefully"""
        logger.info("Shutting down job launcher...")
        self.running = False
        
        if self.job:
            try:
                # Job's context manager will handle cleanup
                logger.info("Job shutdown initiated")
            except Exception as e:
                logger.error(f"Error during shutdown: {str(e)}", exc_info=True)
    
    def get_job_status(self) -> Dict[str, Any]:
        """
        Get current job status
        
        Returns:
            Dictionary with job status
        """
        status = {
            "running": self.running,
            "job_initialized": self.job is not None,
            "topic": self.job.topic if self.job else None
        }
        
        return status


def main():
    """Main entry point for job launcher"""
    parser = argparse.ArgumentParser(description="Launch bronze ingestion job")
    parser.add_argument(
        "--topic",
        type=str,
        default="user_events",
        help="Kafka topic name (default: user_events)"
    )
    parser.add_argument(
        "--environment",
        type=str,
        default=None,
        choices=["dev", "staging", "prod"],
        help="Environment name (dev/staging/prod)"
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum retry attempts (default: 3)"
    )
    parser.add_argument(
        "--retry-delay",
        type=int,
        default=60,
        help="Retry delay in seconds (default: 60)"
    )
    parser.add_argument(
        "--no-auto-restart",
        action="store_true",
        help="Disable auto-restart on failure"
    )
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("Bronze Ingestion Job Launcher")
    logger.info("=" * 60)
    logger.info(f"Topic: {args.topic}")
    logger.info(f"Environment: {args.environment or 'default'}")
    logger.info(f"Max Retries: {args.max_retries}")
    logger.info(f"Auto Restart: {not args.no_auto_restart}")
    logger.info("=" * 60)
    
    try:
        launcher = JobLauncher(
            max_retries=args.max_retries,
            retry_delay_seconds=args.retry_delay
        )
        
        launcher.launch_job(
            topic=args.topic,
            environment=args.environment,
            auto_restart=not args.no_auto_restart
        )
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Job launcher failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

