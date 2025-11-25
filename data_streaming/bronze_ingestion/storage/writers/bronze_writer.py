"""
Bronze Writer - Production ML Pipeline
Writes validated, normalized data to object store (S3/MinIO) in bronze layer
Handles partitioning, compression, and format optimization
"""
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.functions import col, lit, date_format, hour
from libs.config_loader import load_config, get_config_value
from libs.logger import get_logger
from libs.exceptions import PipelineException
from schemas.bronze_schema.bronze_data_contract import get_partition_columns

logger = get_logger(__name__)


class BronzeWriterError(PipelineException):
    """Raised when bronze writing fails"""
    pass


class BronzeWriter:
    """
    Production-grade bronze data writer
    
    Writes to object store with:
    - Partitioning (by date/hour)
    - Compression (Snappy/Parquet)
    - Format optimization (Parquet)
    - Checkpointing (for streaming)
    
    Example:
        writer = BronzeWriter()
        query = writer.write_stream(df, output_path="s3://bronze/")
        query.awaitTermination()
    """
    
    def __init__(
        self,
        config_path: str = "spark_config.yaml",
        output_path: Optional[str] = None
    ):
        """
        Initialize bronze writer
        
        Args:
            config_path: Path to Spark config file
            output_path: Optional output path (overrides config)
        """
        # Load config
        try:
            self.config = load_config(config_path)
            storage_config = self.config.get("storage", {})
            bronze_config = storage_config.get("bronze", {})
            
            if output_path:
                self.output_path = output_path
            else:
                self.output_path = get_config_value(
                    bronze_config,
                    "validated_path",
                    "s3a://data-lake/bronze/validated/"
                )
            
            self.format = get_config_value(bronze_config, "format", "parquet")
            self.compression = get_config_value(bronze_config, "compression", "snappy")
            self.partition_by = get_config_value(
                bronze_config,
                "partition_by",
                ["dt", "hr"]
            )
            
        except Exception as e:
            logger.warning(f"Could not load config: {str(e)}, using defaults")
            self.output_path = output_path or "s3a://data-lake/bronze/validated/"
            self.format = "parquet"
            self.compression = "snappy"
            self.partition_by = ["dt", "hr"]
        
        logger.info(f"Bronze writer initialized (path: {self.output_path}, format: {self.format})")
    
    def write_stream(
        self,
        df: DataFrame,
        output_path: Optional[str] = None,
        checkpoint_location: Optional[str] = None,
        trigger_interval: Optional[str] = None,
        partition_by: Optional[List[str]] = None
    ) -> DataStreamWriter:
        """
        Write streaming DataFrame to bronze layer
        
        Args:
            df: Streaming DataFrame
            output_path: Optional output path (overrides instance path)
            checkpoint_location: Optional checkpoint location
            trigger_interval: Optional trigger interval (e.g., "30s")
            partition_by: Optional partition columns
        
        Returns:
            DataStreamWriter (call .start() to begin)
        
        Example:
            writer = BronzeWriter()
            query = writer.write_stream(df).start()
            query.awaitTermination()
        """
        try:
            logger.info("Setting up streaming write to bronze layer...")
            
            output = output_path or self.output_path
            
            # Get checkpoint location
            if not checkpoint_location:
                checkpoint_location = get_config_value(
                    self.config,
                    "streaming.checkpoint_location",
                    f"{output}/checkpoints/"
                )
            
            # Get trigger interval
            if not trigger_interval:
                trigger_interval = get_config_value(
                    self.config,
                    "streaming.trigger.interval",
                    "30s"
                )
            
            # Get partition columns
            partition_cols = partition_by or self.partition_by
            
            # Ensure partition columns exist
            df = self._add_partition_columns(df, partition_cols)
            
            # Build write stream
            writer = df.writeStream \
                .format(self.format) \
                .outputMode("append") \
                .option("path", output) \
                .option("checkpointLocation", checkpoint_location)
            
            # Add compression
            if self.format == "parquet":
                writer = writer.option("compression", self.compression)
            
            # Add partitioning
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            # Add trigger
            # Use string format for trigger (PySpark 3.5.0)
            # Format: "30 seconds" or "1 minute" (not "30s")
            if trigger_interval.endswith('s'):
                # Convert "30s" to "30 seconds"
                seconds = trigger_interval[:-1]
                trigger_interval = f"{seconds} seconds"
            writer = writer.trigger(processingTime=trigger_interval)
            
            logger.info(f"Streaming write configured: path={output}, format={self.format}, partition={partition_cols}")
            
            return writer
            
        except Exception as e:
            logger.error(f"Failed to setup streaming write: {str(e)}", exc_info=True)
            raise BronzeWriterError(f"Cannot setup streaming write: {str(e)}")
    
    def write_batch(
        self,
        df: DataFrame,
        output_path: Optional[str] = None,
        partition_by: Optional[List[str]] = None,
        mode: str = "overwrite"
    ):
        """
        Write batch DataFrame to bronze layer
        
        Args:
            df: Batch DataFrame
            output_path: Optional output path
            partition_by: Optional partition columns
            mode: Write mode ("overwrite", "append", "ignore", "error")
        
        Example:
            writer = BronzeWriter()
            writer.write_batch(df, mode="append")
        """
        try:
            logger.info("Writing batch data to bronze layer...")
            
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
            raise BronzeWriterError(f"Cannot write batch data: {str(e)}")
    
    def _add_partition_columns(self, df: DataFrame, partition_cols: List[str]) -> DataFrame:
        """
        Add partition columns if they don't exist
        
        Args:
            df: Input DataFrame
            partition_cols: List of partition column names
        
        Returns:
            DataFrame with partition columns added
        """
        from pyspark.sql.functions import current_timestamp, date_format as spark_date_format, hour as spark_hour
        
        # Map partition column names to their source
        partition_mapping = {
            "dt": "ingestion_date",
            "hr": "ingestion_hour",
            "date": "ingestion_date",
            "hour": "ingestion_hour"
        }
        
        for partition_col in partition_cols:
            if partition_col not in df.columns:
                # Try to get from mapping
                source_col = partition_mapping.get(partition_col, None)
                
                if source_col and source_col in df.columns:
                    # Use existing column
                    df = df.withColumn(partition_col, col(source_col))
                elif partition_col == "dt" or partition_col == "date":
                    # Add date partition (YYYY-MM-DD)
                    if "ingestion_date" in df.columns:
                        df = df.withColumn(partition_col, col("ingestion_date"))
                    else:
                        df = df.withColumn(
                            partition_col,
                            spark_date_format(current_timestamp(), "yyyy-MM-dd")
                        )
                elif partition_col == "hr" or partition_col == "hour":
                    # Add hour partition (0-23)
                    if "ingestion_hour" in df.columns:
                        df = df.withColumn(partition_col, col("ingestion_hour"))
                    else:
                        df = df.withColumn(partition_col, spark_hour(current_timestamp()))
                else:
                    logger.warning(f"Could not add partition column {partition_col}, skipping")
        
        return df
    
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
def write_to_bronze(
    df: DataFrame,
    output_path: Optional[str] = None,
    mode: str = "append",
    partition_by: Optional[List[str]] = None
):
    """
    Write to bronze layer (convenience function)
    
    Args:
        df: DataFrame to write
        output_path: Optional output path
        mode: Write mode ("overwrite", "append", etc.)
        partition_by: Optional partition columns
    
    Example:
        write_to_bronze(df, mode="append")
    """
    writer = BronzeWriter(output_path=output_path)
    writer.write_batch(df, output_path=output_path, partition_by=partition_by, mode=mode)

