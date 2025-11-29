"""
Silver DLQ Handler - Production ML Pipeline
Handles failed records from silver processing and writes to Dead Letter Queue
Prevents data loss and enables investigation of failures
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, struct, current_timestamp
from libs.config_loader import load_config, get_config_value
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class SilverDLQError(PipelineException):
    """Raised when DLQ handling fails"""
    pass


class SilverDLQHandler:
    """
    Production-grade Dead Letter Queue handler for silver processing
    
    Handles failed records:
    - Records that fail validation
    - Records that fail contract enforcement
    - Records with processing errors
    - Writes to DLQ with error context
    
    Example:
        dlq_handler = SilverDLQHandler(spark)
        dlq_handler.write_to_dlq(failed_df, reason="validation_failed")
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config_path: str = "config/spark_config.yaml",
        dlq_path: Optional[str] = None
    ):
        """
        Initialize silver DLQ handler
        
        Args:
            spark: SparkSession instance
            config_path: Path to config file
            dlq_path: Optional DLQ path (overrides config)
        """
        self.spark = spark
        
        # Load config
        try:
            self.config = load_config(config_path)
            error_handling = self.config.get("error_handling", {})
            dlq_config = error_handling.get("dlq", {})
            
            if dlq_path:
                self.dlq_path = dlq_path
            else:
                self.dlq_path = get_config_value(
                    dlq_config,
                    "silver_path",
                    "s3a://data-lake/dlq/silver_processing/"
                )
            
            self.enabled = dlq_config.get("enabled", True)
            
        except Exception as e:
            logger.warning(f"Could not load config: {str(e)}, using defaults")
            self.dlq_path = dlq_path or "s3a://data-lake/dlq/silver_processing/"
            self.enabled = True
        
        logger.info(f"Silver DLQ handler initialized (path: {self.dlq_path}, enabled: {self.enabled})")
    
    def write_to_dlq(
        self,
        df: DataFrame,
        reason: str,
        error_message: Optional[str] = None,
        error_type: Optional[str] = None
    ) -> bool:
        """
        Write failed records to Dead Letter Queue
        
        Args:
            df: DataFrame with failed records
            reason: Reason for failure (e.g., "validation_failed", "contract_enforcement_failed")
            error_message: Optional error message
            error_type: Optional error type
        
        Returns:
            True if write successful, False otherwise
        
        Example:
            dlq_handler = SilverDLQHandler(spark)
            dlq_handler.write_to_dlq(failed_df, reason="validation_failed", error_message="Quality score too low")
        """
        if not self.enabled:
            logger.warning("DLQ is disabled, skipping write")
            return False
        
        try:
            if df.count() == 0:
                logger.debug("No records to write to DLQ")
                return True
            
            logger.info(f"Writing {df.count()} records to DLQ (reason: {reason})")
            
            # Add DLQ metadata
            dlq_df = self._add_dlq_metadata(df, reason, error_message, error_type)
            
            # Write to DLQ with partitioning by date and reason
            partition_date = datetime.now().strftime("%Y-%m-%d")
            partition_path = f"{self.dlq_path}dt={partition_date}/reason={reason}/"
            
            dlq_df.write \
                .format("parquet") \
                .mode("append") \
                .option("compression", "snappy") \
                .partitionBy("dt", "reason") \
                .save(partition_path)
            
            logger.info(f"Successfully wrote {df.count()} records to DLQ: {partition_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write to DLQ: {str(e)}", exc_info=True)
            return False
    
    def _add_dlq_metadata(
        self,
        df: DataFrame,
        reason: str,
        error_message: Optional[str],
        error_type: Optional[str]
    ) -> DataFrame:
        """
        Add DLQ metadata to DataFrame
        
        Args:
            df: Input DataFrame
            reason: Failure reason
            error_message: Optional error message
            error_type: Optional error type
        
        Returns:
            DataFrame with DLQ metadata added
        """
        try:
            # Add DLQ metadata columns
            dlq_df = df.withColumn("dlq_timestamp", current_timestamp()) \
                .withColumn("dlq_date", lit(datetime.now().strftime("%Y-%m-%d"))) \
                .withColumn("dlq_reason", lit(reason)) \
                .withColumn("dlq_error_message", lit(error_message or "")) \
                .withColumn("dlq_error_type", lit(error_type or "unknown"))
            
            # Add partition columns
            dlq_df = dlq_df.withColumn("dt", col("dlq_date")) \
                .withColumn("reason", col("dlq_reason"))
            
            return dlq_df
            
        except Exception as e:
            logger.error(f"Error adding DLQ metadata: {str(e)}", exc_info=True)
            return df
    
    def read_from_dlq(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        reason: Optional[str] = None
    ) -> DataFrame:
        """
        Read records from DLQ for investigation
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            reason: Optional failure reason filter
        
        Returns:
            DataFrame with DLQ records
        
        Example:
            dlq_handler = SilverDLQHandler(spark)
            failed_records = dlq_handler.read_from_dlq(start_date="2024-01-15", reason="validation_failed")
        """
        try:
            logger.info(f"Reading from DLQ: start_date={start_date}, end_date={end_date}, reason={reason}")
            
            # Build partition filter
            filter_conditions = []
            if start_date:
                filter_conditions.append(f"dt >= '{start_date}'")
            if end_date:
                filter_conditions.append(f"dt <= '{end_date}'")
            if reason:
                filter_conditions.append(f"reason = '{reason}'")
            
            if filter_conditions:
                partition_filter = " AND ".join(filter_conditions)
                df = self.spark.read \
                    .format("parquet") \
                    .load(self.dlq_path) \
                    .filter(partition_filter)
            else:
                df = self.spark.read \
                    .format("parquet") \
                    .load(self.dlq_path)
            
            logger.info(f"Read {df.count()} records from DLQ")
            return df
            
        except Exception as e:
            logger.error(f"Failed to read from DLQ: {str(e)}", exc_info=True)
            raise SilverDLQError(f"Cannot read from DLQ: {str(e)}")
    
    def get_dlq_stats(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get DLQ statistics
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
        
        Returns:
            Dictionary with DLQ statistics
        """
        try:
            df = self.read_from_dlq(start_date=start_date, end_date=end_date)
            
            total_count = df.count()
            
            # Count by reason
            if "dlq_reason" in df.columns:
                reason_counts = df.groupBy("dlq_reason").count().collect()
                reason_dict = {row["dlq_reason"]: row["count"] for row in reason_counts}
            else:
                reason_dict = {}
            
            stats = {
                "total_records": total_count,
                "by_reason": reason_dict,
                "start_date": start_date,
                "end_date": end_date
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting DLQ stats: {str(e)}", exc_info=True)
            return {"error": str(e)}


# Convenience function
def write_to_silver_dlq(
    spark: SparkSession,
    df: DataFrame,
    reason: str,
    error_message: Optional[str] = None
) -> bool:
    """
    Write to silver DLQ (convenience function)
    
    Args:
        spark: SparkSession instance
        df: DataFrame with failed records
        reason: Failure reason
        error_message: Optional error message
    
    Returns:
        True if write successful
    
    Example:
        write_to_silver_dlq(spark, failed_df, reason="validation_failed")
    """
    handler = SilverDLQHandler(spark)
    return handler.write_to_dlq(df, reason, error_message)

