"""
Bronze Normalizer - Production ML Pipeline
Normalizes data format: dates, types, formats, structures
Ensures consistent data format for bronze layer
"""
from typing import Dict, Any, List, Optional
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, isnull, to_timestamp, from_unixtime,
    date_format, unix_timestamp, lit, trim, lower, upper,
    regexp_replace, cast, struct, coalesce
)
from pyspark.sql.types import StringType, LongType, IntegerType, TimestampType
from libs.config_loader import load_config, get_config_value
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class NormalizationError(PipelineException):
    """Raised when normalization fails"""
    pass


class BronzeNormalizer:
    """
    Production-grade bronze data normalizer
    
    Normalizes:
    - Timestamps (convert to standard format)
    - Data types (cast to correct types)
    - String formats (trim, lowercase, etc.)
    - Nested structures (flatten or normalize)
    - Null handling (standardize null values)
    
    Example:
        normalizer = BronzeNormalizer()
        normalized_df = normalizer.normalize(df)
        # Data now in consistent format
    """
    
    def __init__(self, config_path: str = "spark_config.yaml"):
        """
        Initialize bronze normalizer
        
        Args:
            config_path: Path to config file
        """
        # Load config
        try:
            self.config = load_config(config_path)
            normalization_config = self.config.get("normalization", {})
            self.normalization_rules = normalization_config.get("rules", {})
        except Exception as e:
            logger.warning(f"Could not load config: {str(e)}, using defaults")
            self.normalization_rules = {}
        
        logger.info("Bronze normalizer initialized")
    
    def normalize(
        self,
        df: DataFrame,
        add_processing_metadata: bool = True
    ) -> DataFrame:
        """
        Normalize DataFrame to bronze format
        
        Args:
            df: Input DataFrame
            add_processing_metadata: Add processing metadata columns
        
        Returns:
            Normalized DataFrame
        
        Example:
            normalizer = BronzeNormalizer()
            normalized_df = normalizer.normalize(df)
        """
        try:
            logger.info("Starting data normalization...")
            
            # Normalize timestamps
            df = self._normalize_timestamps(df)
            
            # Normalize data types
            df = self._normalize_data_types(df)
            
            # Normalize string formats
            df = self._normalize_strings(df)
            
            # Normalize null values
            df = self._normalize_nulls(df)
            
            # Add processing metadata
            if add_processing_metadata:
                df = self._add_processing_metadata(df)
            
            logger.info("Data normalization completed")
            return df
            
        except Exception as e:
            logger.error(f"Normalization failed: {str(e)}", exc_info=True)
            raise NormalizationError(f"Cannot normalize data: {str(e)}")
    
    def _normalize_timestamps(self, df: DataFrame) -> DataFrame:
        """
        Normalize timestamps to standard format
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with normalized timestamps
        """
        # Normalize timestamp_ms (ensure it's long type)
        # Handle both "timestamp" and "timestamp_ms" column names
        if "timestamp_ms" in df.columns:
            df = df.withColumn(
                "timestamp_ms",
                col("timestamp_ms").cast("bigint")
            )
            logger.debug("Normalized timestamp_ms to long")
        elif "timestamp" in df.columns:
            # Rename timestamp to timestamp_ms for consistency
            df = df.withColumnRenamed("timestamp", "timestamp_ms")
            df = df.withColumn(
                "timestamp_ms",
                col("timestamp_ms").cast("bigint")
            )
            logger.debug("Normalized timestamp to timestamp_ms (long)")
        
        # Normalize timestamp_iso (ensure ISO 8601 format)
        if "timestamp_iso" in df.columns:
            # Try to parse and reformat to ISO 8601
            df = df.withColumn(
                "timestamp_iso",
                when(
                    col("timestamp_iso").isNotNull(),
                    # If already ISO format, keep it; otherwise try to convert
                    when(
                        col("timestamp_iso").rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"),
                        col("timestamp_iso")
                    ).otherwise(
                        # Try to convert from timestamp_ms
                        from_unixtime(col("timestamp_ms") / 1000, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                    )
                ).otherwise(None)
            )
            logger.debug("Normalized timestamp_iso to ISO 8601 format")
        
        # Add ingestion timestamp if not present
        if "processing_metadata.ingestion_timestamp_ms" not in df.columns:
            current_timestamp_ms = int(datetime.now().timestamp() * 1000)
            df = df.withColumn(
                "ingestion_timestamp_ms",
                lit(current_timestamp_ms)
            )
            logger.debug("Added ingestion_timestamp_ms")
        
        return df
    
    def _normalize_data_types(self, df: DataFrame) -> DataFrame:
        """
        Normalize data types to match schema
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with normalized data types
        """
        # Normalize event_id (ensure string)
        if "event_id" in df.columns:
            df = df.withColumn("event_id", col("event_id").cast("string"))
        
        # Normalize event_type (ensure string, lowercase)
        if "event_type" in df.columns:
            df = df.withColumn(
                "event_type",
                lower(trim(col("event_type").cast("string")))
            )
        
        # Normalize user_id (ensure string)
        if "user_id" in df.columns:
            df = df.withColumn("user_id", col("user_id").cast("string"))
        
        # Normalize session_id (ensure string)
        if "session_id" in df.columns:
            df = df.withColumn("session_id", col("session_id").cast("string"))
        
        logger.debug("Normalized data types")
        return df
    
    def _normalize_strings(self, df: DataFrame) -> DataFrame:
        """
        Normalize string formats (trim, lowercase, etc.)
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with normalized strings
        """
        # String fields to normalize
        string_fields = ["event_type", "user_id", "session_id"]
        
        for field in string_fields:
            if field in df.columns:
                # Trim whitespace
                df = df.withColumn(field, trim(col(field)))
                
                # Remove extra spaces
                df = df.withColumn(
                    field,
                    regexp_replace(col(field), r"\s+", " ")
                )
        
        # Normalize event_type to lowercase
        if "event_type" in df.columns:
            df = df.withColumn("event_type", lower(col("event_type")))
        
        logger.debug("Normalized string formats")
        return df
    
    def _normalize_nulls(self, df: DataFrame) -> DataFrame:
        """
        Normalize null values (standardize empty strings to null)
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with normalized nulls
        """
        # Convert empty strings to null for optional fields
        optional_string_fields = ["user_id", "session_id", "timestamp_iso"]
        
        for field in optional_string_fields:
            if field in df.columns:
                df = df.withColumn(
                    field,
                    when(
                        (col(field).isNotNull()) & (trim(col(field)) != ""),
                        col(field)
                    ).otherwise(None)
                )
        
        logger.debug("Normalized null values")
        return df
    
    def _add_processing_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add processing metadata (ingestion date, hour, etc.)
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with processing metadata
        """
        from pyspark.sql.functions import current_timestamp, date_format, hour
        
        # Get current timestamp
        current_ts = current_timestamp()
        
        # Add ingestion date (YYYY-MM-DD)
        if "processing_metadata.ingestion_date" not in df.columns:
            df = df.withColumn(
                "ingestion_date",
                date_format(current_ts, "yyyy-MM-dd")
            )
        
        # Add ingestion hour (0-23)
        if "processing_metadata.ingestion_hour" not in df.columns:
            df = df.withColumn(
                "ingestion_hour",
                hour(current_ts)
            )
        
        # Add ingestion timestamp if not present
        if "processing_metadata.ingestion_timestamp_ms" not in df.columns:
            current_timestamp_ms = int(datetime.now().timestamp() * 1000)
            df = df.withColumn(
                "ingestion_timestamp_ms",
                lit(current_timestamp_ms)
            )
        
        # Add source information if available from Kafka
        # (This would be added earlier in the pipeline from Kafka metadata)
        
        logger.debug("Added processing metadata")
        return df
    
    def normalize_event_type(self, df: DataFrame, event_type_column: str = "event_type") -> DataFrame:
        """
        Normalize event type (standardize format)
        
        Args:
            df: Input DataFrame
            event_type_column: Event type column name
        
        Returns:
            DataFrame with normalized event types
        """
        if event_type_column not in df.columns:
            logger.warning(f"Column {event_type_column} not found")
            return df
        
        # Normalize: lowercase, trim, replace underscores with hyphens (optional)
        df = df.withColumn(
            event_type_column,
            lower(trim(col(event_type_column)))
        )
        
        logger.debug(f"Normalized {event_type_column}")
        return df
    
    def normalize_timestamp_to_iso(self, df: DataFrame, timestamp_column: str = "timestamp_ms") -> DataFrame:
        """
        Convert timestamp_ms to ISO 8601 format
        
        Args:
            df: Input DataFrame
            timestamp_column: Timestamp column name
        
        Returns:
            DataFrame with ISO timestamp added
        """
        if timestamp_column not in df.columns:
            logger.warning(f"Column {timestamp_column} not found")
            return df
        
        # Convert milliseconds to ISO 8601
        df = df.withColumn(
            "timestamp_iso",
            from_unixtime(col(timestamp_column) / 1000, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        )
        
        logger.debug(f"Converted {timestamp_column} to ISO 8601")
        return df
    
    def get_normalization_stats(self, df: DataFrame) -> Dict[str, Any]:
        """
        Get normalization statistics
        
        Args:
            df: Normalized DataFrame
        
        Returns:
            Dictionary with normalization statistics
        """
        try:
            stats = {
                "total_records": df.count(),
                "normalized_fields": []
            }
            
            # Check which fields were normalized
            if "timestamp_ms" in df.columns:
                stats["normalized_fields"].append("timestamp_ms")
            if "timestamp_iso" in df.columns:
                stats["normalized_fields"].append("timestamp_iso")
            if "event_type" in df.columns:
                stats["normalized_fields"].append("event_type")
            if "ingestion_date" in df.columns:
                stats["normalized_fields"].append("ingestion_date")
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting normalization stats: {str(e)}", exc_info=True)
            return {"error": str(e)}


# Convenience function
def normalize_bronze_data(
    df: DataFrame,
    add_processing_metadata: bool = True
) -> DataFrame:
    """
    Normalize bronze data (convenience function)
    
    Args:
        df: Input DataFrame
        add_processing_metadata: Add processing metadata
    
    Returns:
        Normalized DataFrame
    
    Example:
        normalized_df = normalize_bronze_data(df)
    """
    normalizer = BronzeNormalizer()
    return normalizer.normalize(df, add_processing_metadata=add_processing_metadata)

