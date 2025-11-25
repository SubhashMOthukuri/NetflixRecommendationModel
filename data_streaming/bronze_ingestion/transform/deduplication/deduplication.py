"""
Deduplication - Production ML Pipeline
Removes duplicate records to prevent data duplication in bronze layer
Critical for data quality and accurate analytics
"""
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number, window, desc, max as spark_max, from_unixtime
from pyspark.sql.window import Window
from libs.config_loader import load_config, get_config_value
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class DeduplicationError(PipelineException):
    """Raised when deduplication fails"""
    pass


class Deduplication:
    """
    Production-grade deduplication handler
    
    Removes duplicates using:
    - Event ID (primary key)
    - Composite keys (event_id + timestamp)
    - Window functions (keep latest record)
    - Exact match (all fields identical)
    
    Example:
        dedupe = Deduplication()
        deduplicated_df = dedupe.remove_duplicates(df)
        # Duplicate records removed
    """
    
    def __init__(
        self,
        config_path: str = "kafka.yaml",
        deduplication_key: Optional[str] = None,
        strategy: str = "event_id"
    ):
        """
        Initialize deduplication handler
        
        Args:
            config_path: Path to config file
            deduplication_key: Optional custom deduplication key column
            strategy: Deduplication strategy ("event_id", "composite", "exact", "window")
        """
        self.strategy = strategy
        self.deduplication_key = deduplication_key
        
        # Load config
        try:
            self.config = load_config(config_path)
            dedupe_config = self.config.get("deduplication", {})
            self.strategy = dedupe_config.get("strategy", strategy)
            self.deduplication_key = dedupe_config.get("key", deduplication_key or "event_id")
        except Exception as e:
            logger.warning(f"Could not load config: {str(e)}, using defaults")
        
        # Default deduplication key
        if not self.deduplication_key:
            self.deduplication_key = "event_id"
        
        logger.info(f"Deduplication initialized (strategy: {strategy}, key: {self.deduplication_key})")
    
    def remove_duplicates(
        self,
        df: DataFrame,
        keep: str = "latest"
    ) -> DataFrame:
        """
        Remove duplicate records
        
        Args:
            df: Input DataFrame
            keep: Which record to keep ("latest", "earliest", "first")
        
        Returns:
            DataFrame with duplicates removed
        
        Example:
            dedupe = Deduplication()
            deduplicated_df = dedupe.remove_duplicates(df, keep="latest")
        """
        try:
            logger.info("Starting deduplication...")
            
            initial_count = df.count()
            
            if self.strategy == "event_id":
                df = self._dedupe_by_event_id(df, keep)
            elif self.strategy == "composite":
                df = self._dedupe_by_composite_key(df, keep)
            elif self.strategy == "exact":
                df = self._dedupe_by_exact_match(df, keep)
            elif self.strategy == "window":
                df = self._dedupe_by_window(df, keep)
            else:
                logger.warning(f"Unknown strategy: {self.strategy}, using event_id")
                df = self._dedupe_by_event_id(df, keep)
            
            final_count = df.count()
            duplicates_removed = initial_count - final_count
            
            if duplicates_removed > 0:
                logger.info(f"Removed {duplicates_removed} duplicate records ({duplicates_removed/initial_count*100:.2f}%)")
            else:
                logger.info("No duplicates found")
            
            return df
            
        except Exception as e:
            logger.error(f"Deduplication failed: {str(e)}", exc_info=True)
            raise DeduplicationError(f"Cannot remove duplicates: {str(e)}")
    
    def _dedupe_by_event_id(self, df: DataFrame, keep: str = "latest") -> DataFrame:
        """
        Remove duplicates by event_id (primary key)
        
        Args:
            df: Input DataFrame
            keep: Which record to keep
        
        Returns:
            DataFrame with duplicates removed
        """
        if self.deduplication_key not in df.columns:
            logger.warning(f"Deduplication key {self.deduplication_key} not found, skipping deduplication")
            return df
        
        # Create window for deduplication
        if keep == "latest":
            # Keep record with latest timestamp
            if "timestamp_ms" in df.columns:
                window_spec = Window.partitionBy(self.deduplication_key).orderBy(desc("timestamp_ms"))
            else:
                # If no timestamp, keep first occurrence
                window_spec = Window.partitionBy(self.deduplication_key).orderBy(col(self.deduplication_key))
        elif keep == "earliest":
            # Keep record with earliest timestamp
            if "timestamp_ms" in df.columns:
                window_spec = Window.partitionBy(self.deduplication_key).orderBy("timestamp_ms")
            else:
                window_spec = Window.partitionBy(self.deduplication_key).orderBy(col(self.deduplication_key))
        else:
            # Keep first occurrence
            window_spec = Window.partitionBy(self.deduplication_key).orderBy(col(self.deduplication_key))
        
        # Add row number
        df = df.withColumn("_row_num", row_number().over(window_spec))
        
        # Keep only first row per event_id
        df = df.filter(col("_row_num") == 1).drop("_row_num")
        
        logger.debug(f"Deduplicated by {self.deduplication_key}")
        return df
    
    def _dedupe_by_composite_key(self, df: DataFrame, keep: str = "latest") -> DataFrame:
        """
        Remove duplicates by composite key (event_id + timestamp)
        
        Args:
            df: Input DataFrame
            keep: Which record to keep
        
        Returns:
            DataFrame with duplicates removed
        """
        # Composite key: event_id + timestamp_ms (rounded to nearest second)
        if self.deduplication_key not in df.columns or "timestamp_ms" not in df.columns:
            logger.warning("Composite key columns not found, falling back to event_id")
            return self._dedupe_by_event_id(df, keep)
        
        # Round timestamp to nearest second (for near-duplicates)
        from pyspark.sql.functions import round as spark_round
        df = df.withColumn("_rounded_timestamp", spark_round(col("timestamp_ms") / 1000) * 1000)
        
        # Create composite key
        df = df.withColumn("_composite_key", col(self.deduplication_key) + "_" + col("_rounded_timestamp").cast("string"))
        
        # Deduplicate by composite key
        if keep == "latest":
            window_spec = Window.partitionBy("_composite_key").orderBy(desc("timestamp_ms"))
        else:
            window_spec = Window.partitionBy("_composite_key").orderBy("timestamp_ms")
        
        df = df.withColumn("_row_num", row_number().over(window_spec))
        df = df.filter(col("_row_num") == 1).drop("_row_num", "_rounded_timestamp", "_composite_key")
        
        logger.debug("Deduplicated by composite key")
        return df
    
    def _dedupe_by_exact_match(self, df: DataFrame, keep: str = "latest") -> DataFrame:
        """
        Remove duplicates by exact match (all fields identical)
        
        Args:
            df: Input DataFrame
            keep: Which record to keep
        
        Returns:
            DataFrame with duplicates removed
        """
        # Create hash of all columns for exact match
        from pyspark.sql.functions import hash, concat_ws
        
        # Get all column names
        all_columns = df.columns
        
        # Create hash of all columns
        df = df.withColumn("_hash", hash(concat_ws("|", *[col(c) for c in all_columns])))
        
        # Deduplicate by hash
        if keep == "latest" and "timestamp_ms" in df.columns:
            window_spec = Window.partitionBy("_hash").orderBy(desc("timestamp_ms"))
        else:
            window_spec = Window.partitionBy("_hash").orderBy(col("_hash"))
        
        df = df.withColumn("_row_num", row_number().over(window_spec))
        df = df.filter(col("_row_num") == 1).drop("_row_num", "_hash")
        
        logger.debug("Deduplicated by exact match")
        return df
    
    def _dedupe_by_window(self, df: DataFrame, keep: str = "latest") -> DataFrame:
        """
        Remove duplicates using time window (for streaming)
        
        Args:
            df: Input DataFrame
            keep: Which record to keep
        
        Returns:
            DataFrame with duplicates removed
        """
        if "timestamp_ms" not in df.columns:
            logger.warning("timestamp_ms not found, falling back to event_id")
            return self._dedupe_by_event_id(df, keep)
        
        # Define time window (e.g., 5 minutes)
        from pyspark.sql.functions import window
        
        # Create time window
        df = df.withColumn(
            "_time_window",
            window(from_unixtime(col("timestamp_ms") / 1000), "5 minutes")
        )
        
        # Deduplicate within window
        if self.deduplication_key in df.columns:
            window_spec = Window.partitionBy("_time_window", self.deduplication_key).orderBy(desc("timestamp_ms"))
        else:
            window_spec = Window.partitionBy("_time_window").orderBy(desc("timestamp_ms"))
        
        df = df.withColumn("_row_num", row_number().over(window_spec))
        df = df.filter(col("_row_num") == 1).drop("_row_num", "_time_window")
        
        logger.debug("Deduplicated by time window")
        return df
    
    def get_duplicate_stats(self, df: DataFrame) -> Dict[str, Any]:
        """
        Get duplicate statistics
        
        Args:
            df: Input DataFrame
        
        Returns:
            Dictionary with duplicate statistics
        """
        try:
            initial_count = df.count()
            
            # Count duplicates by deduplication key
            if self.deduplication_key in df.columns:
                duplicate_counts = df.groupBy(self.deduplication_key).count().filter(col("count") > 1)
                duplicate_groups = duplicate_counts.count()
                total_duplicates = duplicate_counts.agg(spark_max("count")).collect()[0][0] - 1 if duplicate_groups > 0 else 0
            else:
                duplicate_groups = 0
                total_duplicates = 0
            
            return {
                "total_records": initial_count,
                "duplicate_groups": duplicate_groups,
                "estimated_duplicates": total_duplicates,
                "deduplication_key": self.deduplication_key
            }
            
        except Exception as e:
            logger.error(f"Error getting duplicate stats: {str(e)}", exc_info=True)
            return {"error": str(e)}


# Convenience function
def remove_duplicates(
    df: DataFrame,
    strategy: str = "event_id",
    keep: str = "latest"
) -> DataFrame:
    """
    Remove duplicates (convenience function)
    
    Args:
        df: Input DataFrame
        strategy: Deduplication strategy
        keep: Which record to keep
    
    Returns:
        DataFrame with duplicates removed
    
    Example:
        deduplicated_df = remove_duplicates(df, strategy="event_id", keep="latest")
    """
    dedupe = Deduplication(strategy=strategy)
    return dedupe.remove_duplicates(df, keep=keep)

