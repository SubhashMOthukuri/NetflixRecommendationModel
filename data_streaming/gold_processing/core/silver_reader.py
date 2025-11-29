"""
Silver Reader - Gold Layer
Reads data from silver layer for feature engineering
"""
from typing import Optional, List
from pyspark.sql import DataFrame, SparkSession
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class SilverReaderError(PipelineException):
    """Raised when silver reading fails"""
    pass


class SilverReader:
    """
    Production-grade silver layer reader for gold processing
    
    Reads silver data with partition filtering for efficient processing.
    Optimizes I/O by using partition pruning to read only relevant partitions.
    
    Example:
        reader = SilverReader(spark)
        df = reader.read_silver(start_date="2024-01-15", end_date="2024-01-16")
        # Only reads partitions for those dates, not entire silver layer
    """
    
    def __init__(self, spark: SparkSession, silver_path: Optional[str] = None):
        """
        Initialize silver reader
        
        Args:
            spark: SparkSession instance
            silver_path: Path to silver layer (defaults to config value)
        """
        self.spark = spark
        
        # Load config for silver path
        try:
            from libs.config_loader import load_config
            config = load_config("config/spark_config.yaml")
            self.silver_path = silver_path or config.get("storage", {}).get("silver", {}).get("silver_path", "s3a://data-lake/silver/silver/")
        except Exception as e:
            logger.warning(f"Could not load config: {str(e)}, using default path")
            self.silver_path = silver_path or "s3a://data-lake/silver/silver/"
        
        logger.info(f"Silver reader initialized with path: {self.silver_path}")
    
    def read_silver(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        hours: Optional[List[int]] = None,
        partition_filter: Optional[str] = None,
        event_types: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Read data from silver layer with partition filtering
        
        Args:
            start_date: Start date (YYYY-MM-DD) - filters event_date partition
            end_date: End date (YYYY-MM-DD) - filters event_date partition
            hours: Optional list of hours (0-23) to process - filters event_hour partition
            partition_filter: Optional Spark SQL partition filter (additional filtering)
            event_types: Optional list of event types to filter
        
        Returns:
            DataFrame with silver data
        
        Example:
            # Read specific date range
            df = reader.read_silver(start_date="2024-01-15", end_date="2024-01-16")
            
            # Read specific hours only
            df = reader.read_silver(start_date="2024-01-15", hours=[18, 19, 20])
            
            # Read with event type filter
            df = reader.read_silver(start_date="2024-01-15", event_types=["video_play_start"])
        """
        try:
            logger.info(f"Reading silver data from {self.silver_path}")
            
            # Build partition filters for efficient partition pruning
            filters = []
            
            # Date range filtering (partition column: event_date)
            if start_date:
                filters.append(f"event_date >= '{start_date}'")
                logger.debug(f"Added date filter: event_date >= '{start_date}'")
            
            if end_date:
                filters.append(f"event_date <= '{end_date}'")
                logger.debug(f"Added date filter: event_date <= '{end_date}'")
            
            # Hour filtering (partition column: event_hour)
            if hours:
                if not all(0 <= h <= 23 for h in hours):
                    raise SilverReaderError("Hours must be between 0 and 23")
                hour_filter = " OR ".join([f"event_hour = {h}" for h in hours])
                filters.append(f"({hour_filter})")
                logger.debug(f"Added hour filter: {hour_filter}")
            
            # Event type filtering (if needed)
            if event_types:
                event_filter = " OR ".join([f"event_type = '{et}'" for et in event_types])
                filters.append(f"({event_filter})")
                logger.debug(f"Added event type filter: {event_filter}")
            
            # Additional partition filter (user-provided)
            if partition_filter:
                filters.append(partition_filter)
                logger.debug(f"Added custom partition filter: {partition_filter}")
            
            # Read with partition pruning (Spark automatically prunes partitions)
            logger.info("Loading silver data with partition pruning...")
            df = self.spark.read.format("parquet").load(self.silver_path)
            
            # Apply filters if any
            if filters:
                filter_expr = " AND ".join(filters)
                logger.info(f"Applying filters: {filter_expr}")
                df = df.filter(filter_expr)
            
            # Count records (lazy evaluation, but useful for logging)
            record_count = df.count()
            logger.info(f"Read {record_count:,} records from silver layer")
            
            # Log partition information
            if start_date or end_date:
                logger.info(f"Date range: {start_date or 'start'} to {end_date or 'end'}")
            if hours:
                logger.info(f"Hours filtered: {hours}")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to read silver data: {str(e)}", exc_info=True)
            raise SilverReaderError(f"Cannot read silver data: {str(e)}")
    
    def read_silver_streaming(
        self,
        checkpoint_location: str,
        start_date: Optional[str] = None,
        max_files_per_trigger: Optional[int] = None
    ) -> DataFrame:
        """
        Read silver data as streaming source (for real-time feature computation)
        
        Args:
            checkpoint_location: Path for Spark checkpointing
            start_date: Start date for initial load (YYYY-MM-DD)
            max_files_per_trigger: Max files to process per trigger (controls batch size)
        
        Returns:
            Streaming DataFrame
        
        Example:
            stream_df = reader.read_silver_streaming(
                checkpoint_location="s3a://checkpoints/gold_silver_reader",
                max_files_per_trigger=100
            )
        """
        try:
            logger.info("Reading silver data as streaming source...")
            
            stream_df = self.spark.readStream \
                .format("parquet") \
                .option("maxFilesPerTrigger", max_files_per_trigger or 100) \
                .load(self.silver_path)
            
            # Apply date filter if provided
            if start_date:
                stream_df = stream_df.filter(f"event_date >= '{start_date}'")
                logger.info(f"Applied date filter: event_date >= '{start_date}'")
            
            logger.info("Silver streaming source created")
            return stream_df
            
        except Exception as e:
            logger.error(f"Failed to create streaming source: {str(e)}", exc_info=True)
            raise SilverReaderError(f"Cannot create streaming source: {str(e)}")

