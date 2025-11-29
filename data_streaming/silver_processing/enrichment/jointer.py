"""
Jointer - Production ML Pipeline
Enriches data by joining with lookup tables/reference data
Adds enriched fields like user_segment, video_category, country, region, timezone
"""
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, broadcast
from libs.config_loader import load_config
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class JointerError(PipelineException):
    """Raised when joining/enrichment fails"""
    pass


class Jointer:
    """
    Production-grade data jointer for enrichment
    
    Enriches data by joining with lookup tables:
    - user_id → user_segment (premium/standard/trial)
    - video_id → video_category (action/comedy/drama)
    - ip_address → country/region/timezone (geo lookup)
    
    Example:
        jointer = Jointer(spark)
        enriched_df = jointer.enrich(df)
        # Data enriched with lookup table joins
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config_path: str = "config/spark_config.yaml"
    ):
        """
        Initialize jointer
        
        Args:
            spark: SparkSession instance
            config_path: Path to config file
        """
        self.spark = spark
        self.lookup_tables = {}
        self.join_configs = {}
        
        # Load config
        try:
            self.config = load_config(config_path)
            self.join_configs = self._load_join_configs()
            logger.info("Jointer initialized")
        except Exception as e:
            logger.warning(f"Could not load config: {str(e)}, using default join configs")
            self.config = {}
            self.join_configs = self._get_default_join_configs()
        
        logger.info(f"Jointer initialized with {len(self.join_configs)} join configurations")
    
    def _load_join_configs(self) -> Dict[str, Any]:
        """
        Load join configurations from config
        
        Returns:
            Dictionary with join configurations
        """
        # Default join configs (can be overridden by config file)
        return self._get_default_join_configs()
    
    def _get_default_join_configs(self) -> Dict[str, Any]:
        """
        Get default join configurations
        
        Returns:
            Dictionary with default join configs
        """
        return {
            "user_segments": {
                "enabled": True,
                "lookup_path": "s3a://data-lake/reference/user_segments/",
                "join_key": "user_id",
                "join_type": "left",
                "columns_to_add": ["user_segment"],
                "broadcast": True  # Small lookup table, use broadcast join
            },
            "video_catalog": {
                "enabled": True,
                "lookup_path": "s3a://data-lake/reference/video_catalog/",
                "join_key": "video_id",
                "join_type": "left",
                "columns_to_add": ["video_category"],
                "broadcast": False  # Large catalog, regular join
            },
            "geo_ip": {
                "enabled": True,
                "lookup_path": "s3a://data-lake/reference/geo_ip/",
                "join_key": "ip_address",
                "join_type": "left",
                "columns_to_add": ["country", "region", "timezone"],
                "broadcast": True  # Small geo lookup, use broadcast join
            }
        }
    
    def enrich(self, df: DataFrame, enrichments: Optional[List[str]] = None) -> DataFrame:
        """
        Enrich DataFrame with lookup table joins
        
        Args:
            df: Input DataFrame
            enrichments: Optional list of enrichments to apply (if None, applies all enabled)
        
        Returns:
            DataFrame with enriched columns
        
        Example:
            jointer = Jointer(spark)
            enriched_df = jointer.enrich(df)
            # Enriched with user_segment, video_category, country, region, timezone
        """
        try:
            logger.info("Starting enrichment...")
            
            df_result = df
            
            # Apply enrichments
            if enrichments is None:
                # Apply all enabled enrichments
                enrichments = [
                    name for name, config in self.join_configs.items()
                    if config.get("enabled", True)
                ]
            
            for enrichment_name in enrichments:
                if enrichment_name in self.join_configs:
                    config = self.join_configs[enrichment_name]
                    if config.get("enabled", True):
                        df_result = self._apply_enrichment(df_result, enrichment_name, config)
            
            logger.info(f"Enrichment complete: applied {len(enrichments)} enrichments")
            return df_result
            
        except Exception as e:
            logger.error(f"Enrichment failed: {str(e)}", exc_info=True)
            raise JointerError(f"Cannot enrich data: {str(e)}")
    
    def _apply_enrichment(
        self,
        df: DataFrame,
        enrichment_name: str,
        config: Dict[str, Any]
    ) -> DataFrame:
        """
        Apply single enrichment (join with lookup table)
        
        Args:
            df: Input DataFrame
            enrichment_name: Name of enrichment (e.g., "user_segments")
            config: Join configuration
        
        Returns:
            DataFrame with enrichment applied
        """
        try:
            logger.info(f"Applying enrichment: {enrichment_name}")
            
            # Load lookup table
            lookup_df = self._load_lookup_table(
                config["lookup_path"],
                enrichment_name
            )
            
            if lookup_df is None:
                logger.warning(f"Lookup table not found for {enrichment_name}, skipping")
                return df
            
            # Perform join
            join_key = config["join_key"]
            join_type = config.get("join_type", "left")
            columns_to_add = config.get("columns_to_add", [])
            
            # Select only columns we need from lookup
            lookup_columns = [join_key] + columns_to_add
            lookup_df = lookup_df.select([col(c) for c in lookup_columns if c in lookup_df.columns])
            
            # Rename join key in lookup to avoid conflicts (if needed)
            lookup_df = lookup_df.withColumnRenamed(join_key, f"{join_key}_lookup")
            
            # Perform join
            if config.get("broadcast", False):
                # Broadcast join for small lookup tables
                lookup_df = broadcast(lookup_df)
                logger.debug(f"Using broadcast join for {enrichment_name}")
            
            df_result = df.join(
                lookup_df,
                df[join_key] == lookup_df[f"{join_key}_lookup"],
                how=join_type
            )
            
            # Drop the temporary lookup key column
            df_result = df_result.drop(f"{join_key}_lookup")
            
            # Select only the columns we want to add (avoid duplicates)
            # Keep all original columns + new enriched columns
            original_columns = df.columns
            new_columns = [c for c in columns_to_add if c in df_result.columns]
            
            # If columns already exist, don't add them again
            columns_to_select = original_columns + [c for c in new_columns if c not in original_columns]
            df_result = df_result.select([col(c) for c in columns_to_select])
            
            logger.info(f"Enrichment {enrichment_name} applied: added {len(new_columns)} columns")
            return df_result
            
        except Exception as e:
            logger.error(f"Error applying enrichment {enrichment_name}: {str(e)}", exc_info=True)
            raise JointerError(f"Cannot apply enrichment {enrichment_name}: {str(e)}")
    
    def _load_lookup_table(self, lookup_path: str, table_name: str) -> Optional[DataFrame]:
        """
        Load lookup table from storage
        
        Args:
            lookup_path: Path to lookup table (S3/MinIO)
            table_name: Name of lookup table (for caching)
        
        Returns:
            DataFrame with lookup data, or None if not found
        """
        try:
            # Check cache first
            if table_name in self.lookup_tables:
                logger.debug(f"Using cached lookup table: {table_name}")
                return self.lookup_tables[table_name]
            
            # Try to load from storage
            try:
                lookup_df = self.spark.read.format("parquet").load(lookup_path)
                logger.info(f"Loaded lookup table from {lookup_path}")
                
                # Cache for reuse
                self.lookup_tables[table_name] = lookup_df
                return lookup_df
                
            except Exception as e:
                logger.warning(f"Could not load lookup table from {lookup_path}: {str(e)}")
                # Return empty DataFrame with expected schema (graceful degradation)
                return None
                
        except Exception as e:
            logger.error(f"Error loading lookup table {table_name}: {str(e)}", exc_info=True)
            return None
    
    def enrich_user_segments(self, df: DataFrame) -> DataFrame:
        """
        Enrich with user segments (convenience method)
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with user_segment column added
        """
        return self.enrich(df, enrichments=["user_segments"])
    
    def enrich_video_catalog(self, df: DataFrame) -> DataFrame:
        """
        Enrich with video catalog (convenience method)
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with video_category column added
        """
        return self.enrich(df, enrichments=["video_catalog"])
    
    def enrich_geo(self, df: DataFrame) -> DataFrame:
        """
        Enrich with geo IP data (convenience method)
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with country, region, timezone columns added
        """
        return self.enrich(df, enrichments=["geo_ip"])
    
    def get_enrichment_stats(self, df_before: DataFrame, df_after: DataFrame) -> Dict[str, Any]:
        """
        Get enrichment statistics
        
        Args:
            df_before: DataFrame before enrichment
            df_after: DataFrame after enrichment
        
        Returns:
            Dictionary with enrichment statistics
        """
        try:
            stats = {
                "columns_before": len(df_before.columns),
                "columns_after": len(df_after.columns),
                "columns_added": len(df_after.columns) - len(df_before.columns),
                "enrichments_applied": len([c for c in df_after.columns if c not in df_before.columns])
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting enrichment stats: {str(e)}", exc_info=True)
            return {"error": str(e)}


# Convenience function
def enrich_dataframe(
    spark: SparkSession,
    df: DataFrame,
    enrichments: Optional[List[str]] = None
) -> DataFrame:
    """
    Enrich DataFrame (convenience function)
    
    Args:
        spark: SparkSession instance
        df: Input DataFrame
        enrichments: Optional list of enrichments to apply
    
    Returns:
        DataFrame with enriched columns
    
    Example:
        enriched_df = enrich_dataframe(spark, df)
    """
    jointer = Jointer(spark)
    return jointer.enrich(df, enrichments=enrichments)

