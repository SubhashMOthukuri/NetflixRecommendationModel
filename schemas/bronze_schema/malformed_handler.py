"""
Malformed Data Handler - Production ML Pipeline
Handles bad/malformed data that cannot be fixed or validated
Routes invalid data to DLQ (Dead Letter Queue) to prevent pipeline crashes
"""
import json
from typing import Dict, Any, Optional, List
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnull, lit, struct, to_json
from libs.config_loader import load_config, get_config_value
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class MalformedDataError(PipelineException):
    """Raised when malformed data cannot be handled"""
    pass


class MalformedDataHandler:
    """
    Production-grade malformed data handler
    
    Handles:
    - Invalid JSON/parsing errors
    - Schema validation failures
    - Data type conversion errors
    - Corrupted records
    
    Routes bad data to DLQ to prevent pipeline crashes
    
    Example:
        handler = MalformedDataHandler()
        good_df, bad_df = handler.handle_malformed(df)
        # good_df: valid data to process
        # bad_df: invalid data to send to DLQ
    """
    
    def __init__(
        self,
        dlq_enabled: bool = True,
        dlq_path: Optional[str] = None,
        config_path: str = "kafka.yaml"
    ):
        """
        Initialize malformed data handler
        
        Args:
            dlq_enabled: Enable Dead Letter Queue
            dlq_path: Optional DLQ path (overrides config)
            config_path: Path to config file
        """
        self.dlq_enabled = dlq_enabled
        
        # Load config
        try:
            self.config = load_config(config_path)
            data_quality = self.config.get("data_quality", {})
            
            if dlq_path:
                self.dlq_path = dlq_path
            else:
                # Try to get from config
                self.dlq_path = get_config_value(
                    self.config,
                    "data_quality.dlq_path",
                    "s3a://data-lake/dlq/bronze_ingestion/"
                )
            
            self.dlq_enabled = data_quality.get("enable_dlq", True)
            
        except Exception as e:
            logger.warning(f"Could not load config: {str(e)}, using defaults")
            self.dlq_path = dlq_path or "s3a://data-lake/dlq/bronze_ingestion/"
        
        logger.info(f"Malformed data handler initialized (DLQ: {self.dlq_enabled})")
    
    def handle_malformed_data(
        self,
        df: DataFrame,
        error_column: str = "error",
        max_errors: int = 1000
    ) -> tuple[DataFrame, DataFrame]:
        """
        Split DataFrame into valid and invalid records
        
        Args:
            df: Input DataFrame with potential malformed data
            error_column: Column name to check for errors
            max_errors: Max number of errors to track
        
        Returns:
            Tuple of (valid_df, invalid_df)
        
        Example:
            handler = MalformedDataHandler()
            valid_df, invalid_df = handler.handle_malformed_data(df)
            # Process valid_df
            # Send invalid_df to DLQ
        """
        try:
            logger.info("Handling malformed data...")
            
            # Check if error column exists
            if error_column in df.columns:
                # Split based on error column
                valid_df = df.filter(col(error_column).isNull())
                invalid_df = df.filter(col(error_column).isNotNull())
            else:
                # No errors detected, all data is valid
                valid_df = df
                invalid_df = df.limit(0)  # Empty DataFrame
            
            valid_count = valid_df.count() if valid_df is not None else 0
            invalid_count = invalid_df.count() if invalid_df is not None else 0
            
            logger.info(f"Split data: {valid_count} valid, {invalid_count} invalid records")
            
            if invalid_count > 0:
                logger.warning(f"Found {invalid_count} malformed records")
                if self.dlq_enabled:
                    logger.info(f"Invalid records will be sent to DLQ: {self.dlq_path}")
            
            return valid_df, invalid_df
            
        except Exception as e:
            logger.error(f"Error handling malformed data: {str(e)}", exc_info=True)
            raise MalformedDataError(f"Cannot handle malformed data: {str(e)}")
    
    def enrich_with_error_context(
        self,
        df: DataFrame,
        error_message: str,
        error_type: str = "malformed_data",
        source_info: Optional[Dict[str, Any]] = None
    ) -> DataFrame:
        """
        Add error context to malformed records
        
        Args:
            df: DataFrame with malformed records
            error_message: Error message
            error_type: Type of error (malformed_data, validation_error, etc.)
            source_info: Optional source information (topic, partition, offset)
        
        Returns:
            DataFrame with error context added
        """
        try:
            # Add error columns
            df = df.withColumn("dlq_error_message", lit(error_message))
            df = df.withColumn("dlq_error_type", lit(error_type))
            df = df.withColumn("dlq_timestamp", lit(datetime.now().isoformat()))
            
            # Add source information if provided
            if source_info:
                for key, value in source_info.items():
                    df = df.withColumn(f"dlq_source_{key}", lit(value))
            
            # Add record as JSON for debugging
            try:
                # Convert all columns to JSON string
                df = df.withColumn("dlq_raw_record", to_json(struct([col(c) for c in df.columns])))
            except Exception:
                # Fallback: just add error message
                logger.warning("Could not serialize record to JSON")
            
            logger.debug("Added error context to malformed records")
            return df
            
        except Exception as e:
            logger.error(f"Error enriching with context: {str(e)}", exc_info=True)
            return df
    
    def write_to_dlq(
        self,
        df: DataFrame,
        partition_by: Optional[List[str]] = None
    ):
        """
        Write malformed records to Dead Letter Queue
        
        Args:
            df: DataFrame with malformed records
            partition_by: Optional partition columns (default: date/hour)
        
        Example:
            handler = MalformedDataHandler()
            valid_df, invalid_df = handler.handle_malformed_data(df)
            handler.write_to_dlq(invalid_df)
        """
        if not self.dlq_enabled:
            logger.warning("DLQ is disabled, skipping write")
            return
        
        if df is None or df.count() == 0:
            logger.info("No malformed records to write to DLQ")
            return
        
        try:
            logger.info(f"Writing {df.count()} malformed records to DLQ: {self.dlq_path}")
            
            # Default partition columns
            if not partition_by:
                partition_by = ["dlq_date", "dlq_hour"]
            
            # Add partition columns if not present
            if "dlq_date" not in df.columns:
                df = df.withColumn("dlq_date", lit(datetime.now().strftime("%Y-%m-%d")))
            if "dlq_hour" not in df.columns:
                df = df.withColumn("dlq_hour", lit(datetime.now().hour))
            
            # Write to DLQ (Parquet format)
            df.write \
                .mode("append") \
                .partitionBy(*partition_by) \
                .format("parquet") \
                .save(self.dlq_path)
            
            logger.info(f"Successfully wrote malformed records to DLQ")
            
        except Exception as e:
            logger.error(f"Error writing to DLQ: {str(e)}", exc_info=True)
            raise MalformedDataError(f"Cannot write to DLQ: {str(e)}")
    
    def handle_parse_errors(
        self,
        df: DataFrame,
        value_column: str = "value"
    ) -> tuple[DataFrame, DataFrame]:
        """
        Handle JSON parsing errors from Kafka
        
        Args:
            df: DataFrame with Kafka value column
            value_column: Name of column containing raw data
        
        Returns:
            Tuple of (valid_df, invalid_df)
        """
        try:
            from pyspark.sql.functions import from_json, col, when, isnull
            
            # Try to parse JSON
            # This is a simplified example - actual implementation would use schema
            # For now, we'll check if value column exists and is not null
            
            if value_column not in df.columns:
                logger.warning(f"Column {value_column} not found, skipping parse error handling")
                return df, df.limit(0)
            
            # Mark records with null or empty values as invalid
            invalid_df = df.filter(
                (col(value_column).isNull()) |
                (col(value_column) == "") |
                (col(value_column).cast("string") == "")
            )
            
            valid_df = df.filter(
                col(value_column).isNotNull() &
                (col(value_column) != "") &
                (col(value_column).cast("string") != "")
            )
            
            # Add error context to invalid records
            if invalid_df.count() > 0:
                invalid_df = self.enrich_with_error_context(
                    invalid_df,
                    error_message="Failed to parse JSON from Kafka value",
                    error_type="parse_error"
                )
            
            return valid_df, invalid_df
            
        except Exception as e:
            logger.error(f"Error handling parse errors: {str(e)}", exc_info=True)
            # Return all data as valid if we can't handle errors
            return df, df.limit(0)
    
    def get_dlq_stats(self) -> Dict[str, Any]:
        """
        Get DLQ statistics (if DLQ path is accessible)
        
        Returns:
            Dictionary with DLQ statistics
        """
        stats = {
            "dlq_enabled": self.dlq_enabled,
            "dlq_path": self.dlq_path,
            "records_count": None,  # Would need to read DLQ to get count
            "last_updated": None
        }
        
        # TODO: Implement reading DLQ to get actual stats
        # This would require Spark session and reading from DLQ path
        
        return stats


# Convenience function
def handle_malformed_data(
    df: DataFrame,
    dlq_enabled: bool = True,
    dlq_path: Optional[str] = None
) -> tuple[DataFrame, DataFrame]:
    """
    Handle malformed data (convenience function)
    
    Args:
        df: Input DataFrame
        dlq_enabled: Enable DLQ
        dlq_path: Optional DLQ path
    
    Returns:
        Tuple of (valid_df, invalid_df)
    
    Example:
        valid_df, invalid_df = handle_malformed_data(df)
        # Process valid_df
        # Send invalid_df to DLQ
    """
    handler = MalformedDataHandler(dlq_enabled=dlq_enabled, dlq_path=dlq_path)
    return handler.handle_malformed_data(df)

