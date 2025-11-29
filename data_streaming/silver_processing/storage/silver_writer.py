"""
Silver Writer - Production ML Pipeline
Writes processed, validated data to object store (S3/MinIO) in silver layer
Handles partitioning, compression, and format optimization for batch processing
"""
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, date_format, hour, to_date, from_unixtime
from libs.config_loader import load_config, get_config_value
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class SilverWriterError(PipelineException):
    """Raised when silver writing fails"""
    pass


class SilverWriter:
    """
    Production-grade silver data writer
    
    Writes to object store with:
    - Partitioning (by event_date/event_type per silver schema)
    - Compression (Snappy/Parquet)
    - Format optimization (Parquet)
    - Batch writes (silver is batch processing)
    
    Example:
        writer = SilverWriter()
        writer.write_batch(df, mode="append")
        # Data written to silver layer
    """
    
    def __init__(
        self,
        config_path: str = "config/spark_config.yaml",
        output_path: Optional[str] = None
    ):
        """
        Initialize silver writer
        
        Args:
            config_path: Path to Spark config file
            output_path: Optional output path (overrides config)
        """
        # Load config
        try:
            self.config = load_config(config_path)
            storage_config = self.config.get("storage", {})
            silver_config = storage_config.get("silver", {})
            
            if output_path:
                self.output_path = output_path
            else:
                self.output_path = get_config_value(
                    silver_config,
                    "silver_path",
                    "s3a://data-lake/silver/silver/"
                )
            
            self.format = get_config_value(silver_config, "format", "parquet")
            self.compression = get_config_value(silver_config, "compression", "snappy")
            self.partition_by = get_config_value(
                silver_config,
                "partition_by",
                ["event_date", "event_type"]
            )
            
        except Exception as e:
            logger.warning(f"Could not load config: {str(e)}, using defaults")
            self.output_path = output_path or "s3a://data-lake/silver/silver/"
            self.format = "parquet"
            self.compression = "snappy"
            self.partition_by = ["event_date", "event_type"]
        
        logger.info(f"Silver writer initialized (path: {self.output_path}, format: {self.format})")
    
    def write_batch(
        self,
        df: DataFrame,
        output_path: Optional[str] = None,
        partition_by: Optional[List[str]] = None,
        mode: str = "append"
    ):
        """
        Write batch DataFrame to silver layer
        
        Args:
            df: Batch DataFrame
            output_path: Optional output path
            partition_by: Optional partition columns
            mode: Write mode ("overwrite", "append", "ignore", "error")
        
        Example:
            writer = SilverWriter()
            writer.write_batch(df, mode="append")
        """
        try:
            logger.info("Writing batch data to silver layer...")
            
            output = output_path or self.output_path
            partition_cols = partition_by or self.partition_by
            
            # Ensure partition columns exist
            df = self._add_partition_columns(df, partition_cols)
            
            # Build write
            writer = df.write \
                .format(self.format) \
                .mode(mode)
            
            # Add compression
            if self.format == "parquet":
                writer = writer.option("compression", self.compression)
            
            # Add partitioning
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            # Write
            writer.save(output)
            
            record_count = df.count()
            logger.info(f"Successfully wrote {record_count} records to {output}")
            
        except Exception as e:
            logger.error(f"Failed to write batch data: {str(e)}", exc_info=True)
            raise SilverWriterError(f"Cannot write batch data: {str(e)}")
    
    def write_partition(
        self,
        df: DataFrame,
        partition_date: str,
        partition_event_type: Optional[str] = None,
        mode: str = "append"
    ):
        """
        Write specific partition to silver layer
        
        Args:
            df: Batch DataFrame
            partition_date: Partition date (YYYY-MM-DD)
            partition_event_type: Optional event type partition
            mode: Write mode
        
        Example:
            writer = SilverWriter()
            writer.write_partition(df, partition_date="2024-01-15", partition_event_type="video_play_start")
        """
        try:
            logger.info(f"Writing partition: date={partition_date}, event_type={partition_event_type}")
            
            # Filter DataFrame to partition
            df_filtered = df.filter(col("event_date") == partition_date)
            
            if partition_event_type:
                df_filtered = df_filtered.filter(col("event_type") == partition_event_type)
            
            # Write filtered data
            self.write_batch(df_filtered, mode=mode)
            
        except Exception as e:
            logger.error(f"Failed to write partition: {str(e)}", exc_info=True)
            raise SilverWriterError(f"Cannot write partition: {str(e)}")
    
    def _add_partition_columns(self, df: DataFrame, partition_cols: List[str]) -> DataFrame:
        """
        Add partition columns if they don't exist
        
        Args:
            df: Input DataFrame
            partition_cols: List of partition column names
        
        Returns:
            DataFrame with partition columns added
        """
        df_result = df
        
        for partition_col in partition_cols:
            if partition_col not in df_result.columns:
                if partition_col == "event_date":
                    # Extract date from event_timestamp
                    if "event_timestamp" in df_result.columns:
                        df_result = df_result.withColumn(
                            partition_col,
                            to_date(from_unixtime(col("event_timestamp") / 1000))
                        )
                    elif "event_date" not in df_result.columns:
                        # Use current date as fallback
                        df_result = df_result.withColumn(
                            partition_col,
                            to_date(lit("2024-01-01"))  # Fallback
                        )
                elif partition_col == "event_type":
                    # event_type should already exist, but if not, add as null
                    if "event_type" not in df_result.columns:
                        df_result = df_result.withColumn(partition_col, lit(None).cast("string"))
                elif partition_col == "event_hour":
                    # Extract hour from event_timestamp
                    if "event_timestamp" in df_result.columns:
                        df_result = df_result.withColumn(
                            partition_col,
                            hour(from_unixtime(col("event_timestamp") / 1000))
                        )
                    elif "event_hour" not in df_result.columns:
                        df_result = df_result.withColumn(partition_col, lit(0))
                else:
                    logger.warning(f"Could not add partition column {partition_col}, skipping")
        
        return df_result
    
    def get_write_stats(self, output_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Get write statistics (if output path is accessible)
        
        Args:
            output_path: Optional output path
        
        Returns:
            Dictionary with write statistics
        """
        output = output_path or self.output_path
        
        stats = {
            "output_path": output,
            "format": self.format,
            "compression": self.compression,
            "partition_by": self.partition_by,
            "files_count": None,  # Would need to read from storage
            "total_size": None     # Would need to read from storage
        }
        
        # TODO: Implement reading from storage to get actual stats
        # This would require Spark session and reading from output path
        
        return stats


# Convenience function
def write_to_silver(
    df: DataFrame,
    output_path: Optional[str] = None,
    mode: str = "append",
    partition_by: Optional[List[str]] = None
):
    """
    Write to silver layer (convenience function)
    
    Args:
        df: DataFrame to write
        output_path: Optional output path
        mode: Write mode ("overwrite", "append", etc.)
        partition_by: Optional partition columns
    
    Example:
        write_to_silver(df, mode="append")
    """
    writer = SilverWriter(output_path=output_path)
    writer.write_batch(df, output_path=output_path, partition_by=partition_by, mode=mode)

