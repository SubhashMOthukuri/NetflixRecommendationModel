"""
Bronze Reader - Production ML Pipeline
Reads data from bronze layer (Parquet files) for silver processing
Handles partition filtering, incremental reads, and schema validation
"""
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from libs.config_loader import load_config, get_config_value
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class BronzeReaderError(PipelineException):
    """Raised when bronze reading fails"""
    pass


class BronzeReader:
    """
    Production-grade bronze data reader
    
    Reads from bronze layer with:
    - Partition filtering (read specific dates/hours)
    - Incremental reads (only new/unprocessed partitions)
    - Schema validation (ensures bronze schema matches)
    - Error handling and logging
    
    Example:
        reader = BronzeReader(spark)
        df = reader.read_bronze(start_date="2024-01-15", end_date="2024-01-16")
        # DataFrame with bronze data ready for silver processing
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config_path: str = "config/spark_config.yaml",
        bronze_path: Optional[str] = None
    ):
        """
        Initialize bronze reader
        
        Args:
            spark: SparkSession instance
            config_path: Path to Spark config file
            bronze_path: Optional bronze path (overrides config)
        """
        self.spark = spark
        
        # Load config
        try:
            self.config = load_config(config_path)
            storage_config = self.config.get("storage", {})
            bronze_config = storage_config.get("bronze", {})
            
            if bronze_path:
                self.bronze_path = bronze_path
            else:
                self.bronze_path = get_config_value(
                    bronze_config,
                    "validated_path",
                    "s3a://data-lake/bronze/validated/"
                )
            
            self.format = get_config_value(bronze_config, "format", "parquet")
            self.partition_columns = get_config_value(
                bronze_config,
                "partition_by",
                ["dt", "hr"]
            )
            
        except Exception as e:
            logger.warning(f"Could not load config: {str(e)}, using defaults")
            self.bronze_path = bronze_path or "s3a://data-lake/bronze/validated/"
            self.format = "parquet"
            self.partition_columns = ["dt", "hr"]
        
        logger.info(f"Bronze reader initialized (path: {self.bronze_path}, format: {self.format})")
    
    def read_bronze(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        hours: Optional[List[int]] = None,
        partition_filter: Optional[str] = None
    ) -> DataFrame:
        """
        Read data from bronze layer
        
        Args:
            start_date: Start date (YYYY-MM-DD) - filters by dt partition
            end_date: End date (YYYY-MM-DD) - filters by dt partition
            hours: Optional list of hours (0-23) to filter - filters by hr partition
            partition_filter: Optional Spark SQL partition filter (e.g., "dt='2024-01-15' AND hr=18")
        
        Returns:
            DataFrame with bronze data
        
        Example:
            # Read all data
            df = reader.read_bronze()
            
            # Read specific date range
            df = reader.read_bronze(start_date="2024-01-15", end_date="2024-01-16")
            
            # Read specific hours
            df = reader.read_bronze(start_date="2024-01-15", hours=[18, 19, 20])
            
            # Read with custom partition filter
            df = reader.read_bronze(partition_filter="dt='2024-01-15' AND hr>=18")
        """
        try:
            logger.info(f"Reading bronze data from {self.bronze_path}...")
            
            # Build partition filter
            filter_conditions = []
            
            if partition_filter:
                # Use custom partition filter
                logger.info(f"Using custom partition filter: {partition_filter}")
                df = self.spark.read \
                    .format(self.format) \
                    .option("basePath", self.bronze_path) \
                    .load(self.bronze_path) \
                    .filter(partition_filter)
            
            elif start_date or end_date or hours:
                # Build date/hour filters
                if start_date:
                    filter_conditions.append(f"dt >= '{start_date}'")
                if end_date:
                    filter_conditions.append(f"dt <= '{end_date}'")
                if hours:
                    hours_str = ",".join([str(h) for h in hours])
                    filter_conditions.append(f"hr IN ({hours_str})")
                
                partition_filter_str = " AND ".join(filter_conditions)
                logger.info(f"Using partition filter: {partition_filter_str}")
                
                df = self.spark.read \
                    .format(self.format) \
                    .option("basePath", self.bronze_path) \
                    .load(self.bronze_path) \
                    .filter(partition_filter_str)
            
            else:
                # Read all data (no filter)
                logger.info("Reading all bronze data (no partition filter)")
                df = self.spark.read \
                    .format(self.format) \
                    .load(self.bronze_path)
            
            # Get record count
            record_count = df.count()
            logger.info(f"Successfully read {record_count} records from bronze layer")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to read bronze data: {str(e)}", exc_info=True)
            raise BronzeReaderError(f"Cannot read bronze data: {str(e)}")
    
    def read_partition(
        self,
        date: str,
        hour: Optional[int] = None
    ) -> DataFrame:
        """
        Read specific partition from bronze layer
        
        Args:
            date: Date (YYYY-MM-DD)
            hour: Optional hour (0-23), if None reads all hours for date
        
        Returns:
            DataFrame with bronze data from partition
        
        Example:
            # Read specific date and hour
            df = reader.read_partition(date="2024-01-15", hour=18)
            
            # Read all hours for a date
            df = reader.read_partition(date="2024-01-15")
        """
        try:
            logger.info(f"Reading bronze partition: dt={date}, hr={hour}")
            
            if hour is not None:
                partition_filter = f"dt='{date}' AND hr={hour}"
            else:
                partition_filter = f"dt='{date}'"
            
            return self.read_bronze(partition_filter=partition_filter)
            
        except Exception as e:
            logger.error(f"Failed to read partition: {str(e)}", exc_info=True)
            raise BronzeReaderError(f"Cannot read partition: {str(e)}")
    
    def read_incremental(
        self,
        last_processed_date: Optional[str] = None,
        last_processed_hour: Optional[int] = None,
        lookback_days: int = 1
    ) -> DataFrame:
        """
        Read incremental data (only new/unprocessed partitions)
        
        Args:
            last_processed_date: Last processed date (YYYY-MM-DD) - reads data after this
            last_processed_hour: Last processed hour (0-23) - reads data after this hour
            lookback_days: Number of days to look back (default: 1 day)
        
        Returns:
            DataFrame with new bronze data
        
        Example:
            # Read data from last 24 hours
            df = reader.read_incremental()
            
            # Read data after last processed date/hour
            df = reader.read_incremental(
                last_processed_date="2024-01-15",
                last_processed_hour=18
            )
        """
        try:
            if last_processed_date:
                # Calculate start date (last processed + 1 hour or next day)
                if last_processed_hour is not None and last_processed_hour < 23:
                    # Same day, next hour
                    start_date = last_processed_date
                    start_hour = last_processed_hour + 1
                    hours = list(range(start_hour, 24))
                    logger.info(f"Reading incremental: from {start_date} hour {start_hour}+")
                    return self.read_bronze(start_date=start_date, hours=hours)
                else:
                    # Next day
                    last_date = datetime.strptime(last_processed_date, "%Y-%m-%d")
                    start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
                    logger.info(f"Reading incremental: from {start_date}")
                    return self.read_bronze(start_date=start_date)
            else:
                # Read last N days (lookback)
                end_date = datetime.now().strftime("%Y-%m-%d")
                start_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
                logger.info(f"Reading incremental: last {lookback_days} days ({start_date} to {end_date})")
                return self.read_bronze(start_date=start_date, end_date=end_date)
            
        except Exception as e:
            logger.error(f"Failed to read incremental data: {str(e)}", exc_info=True)
            raise BronzeReaderError(f"Cannot read incremental data: {str(e)}")
    
    def get_available_partitions(self) -> List[Dict[str, Any]]:
        """
        Get list of available partitions in bronze layer
        
        Returns:
            List of partition dictionaries with dt and hr
        
        Example:
            partitions = reader.get_available_partitions()
            # Returns: [{"dt": "2024-01-15", "hr": 18}, {"dt": "2024-01-15", "hr": 19}, ...]
        """
        try:
            logger.info("Discovering available partitions...")
            
            # Read partition columns only (efficient)
            df = self.spark.read \
                .format(self.format) \
                .load(self.bronze_path) \
                .select("dt", "hr") \
                .distinct()
            
            partitions = df.collect()
            partition_list = [
                {"dt": row["dt"], "hr": row["hr"]}
                for row in partitions
            ]
            
            logger.info(f"Found {len(partition_list)} partitions")
            return partition_list
            
        except Exception as e:
            logger.error(f"Failed to get partitions: {str(e)}", exc_info=True)
            return []
    
    def validate_bronze_schema(self, df: DataFrame) -> bool:
        """
        Validate DataFrame matches expected bronze schema
        
        Args:
            df: DataFrame to validate
        
        Returns:
            True if schema matches, False otherwise
        
        Note: This is a basic check - full validation happens in validation layer
        """
        try:
            # Check for required bronze columns
            required_columns = [
                "event_id",
                "event_type",
                "timestamp_ms",
                "processing_metadata"
            ]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                logger.warning(f"Missing required columns: {missing_columns}")
                return False
            
            logger.debug("Bronze schema validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Schema validation error: {str(e)}", exc_info=True)
            return False


# Convenience function
def read_from_bronze(
    spark: SparkSession,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    hours: Optional[List[int]] = None
) -> DataFrame:
    """
    Read from bronze layer (convenience function)
    
    Args:
        spark: SparkSession instance
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        hours: Optional list of hours (0-23)
    
    Returns:
        DataFrame with bronze data
    
    Example:
        df = read_from_bronze(spark, start_date="2024-01-15", end_date="2024-01-16")
    """
    reader = BronzeReader(spark)
    return reader.read_bronze(start_date=start_date, end_date=end_date, hours=hours)

