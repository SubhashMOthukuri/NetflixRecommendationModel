"""
Gold Job Launcher - Gold Layer
Command-line launcher for gold layer jobs
Provides entry point for gold feature job and backfill job
"""
import argparse
import sys
import signal
from typing import Optional, List
from libs.logger import get_logger
from libs.exceptions import PipelineException

# Jobs
from data_streaming.gold_processing.jobs.gold_feature_job import GoldFeatureJob
from data_streaming.gold_processing.jobs.feature_backfill_job import FeatureBackfillJob

logger = get_logger(__name__)


class GoldJobLauncherError(PipelineException):
    """Raised when job launcher fails"""
    pass


class GoldJobLauncher:
    """
    Production-grade job launcher for gold layer
    
    Launches and manages gold layer jobs:
    - Gold feature job (regular feature computation)
    - Feature backfill job (historical features with time travel)
    
    Example:
        launcher = GoldJobLauncher()
        launcher.launch_feature_job(start_date="2024-01-15", end_date="2024-01-16")
    """
    
    def __init__(self):
        """Initialize job launcher"""
        self.running_jobs = []
        self._setup_signal_handlers()
        logger.info("Gold job launcher initialized")
    
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, shutting down gracefully...")
            self.shutdown()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def launch_feature_job(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        hours: Optional[List[int]] = None,
        feature_types: Optional[List[str]] = None,
        environment: Optional[str] = None,
        max_retries: int = 3
    ):
        """
        Launch gold feature job
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            hours: Optional list of hours (0-23)
            feature_types: Optional list of feature types to compute
            environment: Environment name (dev/staging/prod)
            max_retries: Maximum number of retries on failure
        
        Example:
            launcher.launch_feature_job(
                start_date="2024-01-15",
                end_date="2024-01-16",
                feature_types=["user", "item"]
            )
        """
        try:
            logger.info("Launching gold feature job...")
            
            retry_count = 0
            while retry_count <= max_retries:
                try:
                    job = GoldFeatureJob(environment=environment)
                    job.run(
                        start_date=start_date,
                        end_date=end_date,
                        hours=hours,
                        feature_types=feature_types
                    )
                    
                    logger.info("Gold feature job completed successfully")
                    return
                    
                except Exception as e:
                    retry_count += 1
                    if retry_count <= max_retries:
                        logger.warning(f"Job failed (attempt {retry_count}/{max_retries}): {str(e)}")
                        logger.info("Retrying...")
                    else:
                        logger.error(f"Job failed after {max_retries} retries: {str(e)}")
                        raise GoldJobLauncherError(f"Gold feature job failed: {str(e)}")
            
        except Exception as e:
            logger.error(f"Failed to launch gold feature job: {str(e)}", exc_info=True)
            raise GoldJobLauncherError(f"Cannot launch gold feature job: {str(e)}")
    
    def launch_backfill_job(
        self,
        start_date: str,
        end_date: str,
        entity_type: str = "user",
        feature_types: Optional[List[str]] = None,
        output_path: Optional[str] = None,
        environment: Optional[str] = None,
        max_retries: int = 3
    ):
        """
        Launch feature backfill job
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            entity_type: Type of entity ("user", "item", "session")
            feature_types: Optional list of feature types to compute
            output_path: Optional output path for backfilled features
            environment: Environment name (dev/staging/prod)
            max_retries: Maximum number of retries on failure
        
        Example:
            launcher.launch_backfill_job(
                start_date="2024-01-01",
                end_date="2024-01-31",
                entity_type="user",
                output_path="s3a://data-lake/training/features/"
            )
        """
        try:
            logger.info("Launching feature backfill job...")
            
            retry_count = 0
            while retry_count <= max_retries:
                try:
                    job = FeatureBackfillJob(environment=environment)
                    job.backfill_features(
                        start_date=start_date,
                        end_date=end_date,
                        entity_type=entity_type,
                        feature_types=feature_types,
                        output_path=output_path
                    )
                    
                    logger.info("Feature backfill job completed successfully")
                    return
                    
                except Exception as e:
                    retry_count += 1
                    if retry_count <= max_retries:
                        logger.warning(f"Backfill job failed (attempt {retry_count}/{max_retries}): {str(e)}")
                        logger.info("Retrying...")
                    else:
                        logger.error(f"Backfill job failed after {max_retries} retries: {str(e)}")
                        raise GoldJobLauncherError(f"Feature backfill job failed: {str(e)}")
            
        except Exception as e:
            logger.error(f"Failed to launch backfill job: {str(e)}", exc_info=True)
            raise GoldJobLauncherError(f"Cannot launch backfill job: {str(e)}")
    
    def shutdown(self):
        """Gracefully shutdown all running jobs"""
        try:
            logger.info("Shutting down jobs...")
            # In production, stop Spark sessions, close connections, etc.
            logger.info("Shutdown complete")
        except Exception as e:
            logger.error(f"Error during shutdown: {str(e)}")


def main():
    """Main entry point for command-line execution"""
    parser = argparse.ArgumentParser(description="Gold Layer Job Launcher")
    
    subparsers = parser.add_subparsers(dest="job_type", help="Job type to run")
    
    # Feature job parser
    feature_parser = subparsers.add_parser("feature", help="Run gold feature job")
    feature_parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    feature_parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")
    feature_parser.add_argument("--hours", type=int, nargs="+", help="Hours to process (0-23)")
    feature_parser.add_argument("--feature-types", type=str, nargs="+", 
                               help="Feature types to compute (user, item, session, statistical, temporal)")
    feature_parser.add_argument("--environment", type=str, default="dev", 
                               help="Environment (dev/staging/prod)")
    feature_parser.add_argument("--max-retries", type=int, default=3, 
                               help="Maximum retries on failure")
    
    # Backfill job parser
    backfill_parser = subparsers.add_parser("backfill", help="Run feature backfill job")
    backfill_parser.add_argument("--start-date", type=str, required=True, 
                                help="Start date (YYYY-MM-DD)")
    backfill_parser.add_argument("--end-date", type=str, required=True, 
                                help="End date (YYYY-MM-DD)")
    backfill_parser.add_argument("--entity-type", type=str, default="user",
                                choices=["user", "item", "session"],
                                help="Entity type")
    backfill_parser.add_argument("--feature-types", type=str, nargs="+",
                                help="Feature types to compute")
    backfill_parser.add_argument("--output-path", type=str,
                                help="Output path for backfilled features")
    backfill_parser.add_argument("--environment", type=str, default="dev",
                                help="Environment (dev/staging/prod)")
    backfill_parser.add_argument("--max-retries", type=int, default=3,
                                help="Maximum retries on failure")
    
    args = parser.parse_args()
    
    if not args.job_type:
        parser.print_help()
        sys.exit(1)
    
    try:
        launcher = GoldJobLauncher()
        
        if args.job_type == "feature":
            launcher.launch_feature_job(
                start_date=args.start_date,
                end_date=args.end_date,
                hours=args.hours,
                feature_types=args.feature_types,
                environment=args.environment,
                max_retries=args.max_retries
            )
        
        elif args.job_type == "backfill":
            launcher.launch_backfill_job(
                start_date=args.start_date,
                end_date=args.end_date,
                entity_type=args.entity_type,
                feature_types=args.feature_types,
                output_path=args.output_path,
                environment=args.environment,
                max_retries=args.max_retries
            )
        
        logger.info("Job completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Job failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

