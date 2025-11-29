"""
Flattener - Production ML Pipeline
Flattens nested structures from bronze layer to match silver schema
Converts nested structs (video_data.video_id) to flat columns (video_id)
Handles JSON strings by parsing them first before flattening
"""
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnan, isnull, from_json, lit, struct, regexp_replace, lower, initcap, trim, coalesce
from pyspark.sql.types import StructType, StructField, StringType, LongType, MapType, IntegerType, BooleanType
from libs.config_loader import load_config
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class FlattenerError(PipelineException):
    """Raised when flattening fails"""
    pass


class Flattener:
    """
    Production-grade data flattener
    
    Flattens nested structures from bronze to silver:
    - video_data.video_id → video_id
    - device_info.device_type → device_type
    - event_data.action → action
    - Nested maps and arrays
    
    Example:
        flattener = Flattener()
        flat_df = flattener.flatten(bronze_df)
        # Nested structures flattened to match silver schema
    """
    
    def __init__(
        self,
        schema_path: str = "schemas/silver_schema/silver_schema.yaml"
    ):
        """
        Initialize flattener
        
        Args:
            schema_path: Path to silver schema YAML file
        """
        try:
            self.schema_config = load_config(schema_path)
            logger.info("Flattener initialized with silver schema")
        except Exception as e:
            logger.warning(f"Could not load schema: {str(e)}, using default flattening rules")
            self.schema_config = {}
        
        # Define flattening mappings (bronze nested → silver flat)
        # These map bronze nested fields to silver flat fields
        self.flattening_mappings = self._build_flattening_mappings()
        
        logger.info(f"Flattener initialized with {len(self.flattening_mappings)} flattening rules")
    
    def _build_flattening_mappings(self) -> Dict[str, str]:
        """
        Build flattening mappings from schema config
        
        Maps bronze nested paths to silver flat column names
        
        Returns:
            Dictionary mapping bronze_path -> silver_column
        """
        mappings = {}
        
        # Default mappings based on bronze → silver transformation
        # Bronze has: video_data.video_id, device_info.device_type, event_data.action
        # Silver has: video_id, device_type, action
        
        default_mappings = {
            # Video data flattening
            "video_data.video_id": "video_id",
            "video_data.title": "video_title",
            "video_data.duration_seconds": "video_duration_seconds",
            "video_data.play_position_seconds": "play_position_seconds",
            "video_data.quality": "video_quality",
            
            # Device info flattening
            "device_info.device_type": "device_type",
            "device_info.os": "os",
            "device_info.browser": "browser",
            "device_info.ip_address": "ip_address",
            
            # Event data flattening
            "event_data.action": "action",
            "event_data.target_id": "target_id",
            "event_data.target_type": "target_type",
        }
        
        # Try to read from schema config if available
        if self.schema_config:
            # Schema config might have field mappings
            # For now, use default mappings
            pass
        
        mappings.update(default_mappings)
        return mappings
    
    def flatten(self, df: DataFrame) -> DataFrame:
        """
        Flatten nested structures in DataFrame
        
        Args:
            df: Input DataFrame with nested structures (bronze format)
        
        Returns:
            DataFrame with flattened columns (silver format)
        
        Example:
            flattener = Flattener()
            flat_df = flattener.flatten(bronze_df)
        """
        try:
            logger.info("Starting flattening process...")
            
            initial_columns = df.columns
            df_result = df
            
            # Step 0: Check schema and parse JSON strings if needed
            df_result = self._parse_json_strings(df_result)
            
            # Step 1: Flatten struct columns
            df_result = self._flatten_structs(df_result)
            
            # Step 2: Flatten map columns (if any)
            df_result = self._flatten_maps(df_result)
            
            # Step 2.5: Drop original MAP columns after extraction (device, content, context)
            df_result = self._drop_original_map_columns(df_result)
            
            # Step 3: Rename columns to match silver schema
            df_result = self._rename_to_silver_schema(df_result)
            
            # Step 4: Add missing silver columns (as null)
            df_result = self._add_missing_silver_columns(df_result)
            
            final_columns = df_result.columns
            logger.info(f"Flattening complete: {len(initial_columns)} → {len(final_columns)} columns")
            
            return df_result
            
        except Exception as e:
            logger.error(f"Flattening failed: {str(e)}", exc_info=True)
            raise FlattenerError(f"Cannot flatten data: {str(e)}")
    
    def _parse_json_strings(self, df: DataFrame) -> DataFrame:
        """
        Parse JSON strings or extract from MAP types into structs before flattening
        
        Checks actual schema and:
        - Parses JSON strings if columns are StringType
        - Extracts from MAP types if columns are MapType
        - Skips if already StructType
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with data parsed/extracted into structs
        """
        try:
            df_result = df
            schema = df.schema
            schema_dict = {field.name: field.dataType for field in schema.fields}
            
            # Define expected struct schemas for JSON parsing
            video_data_schema = StructType([
                StructField("video_id", StringType(), True),
                StructField("title", StringType(), True),
                StructField("duration_seconds", LongType(), True),
                StructField("play_position_seconds", LongType(), True),
                StructField("quality", StringType(), True)
            ])
            
            device_info_schema = StructType([
                StructField("device_type", StringType(), True),
                StructField("os", StringType(), True),
                StructField("browser", StringType(), True),
                StructField("ip_address", StringType(), True)
            ])
            
            event_data_schema = StructType([
                StructField("action", StringType(), True),
                StructField("target_id", StringType(), True),
                StructField("target_type", StringType(), True),
                StructField("metadata", MapType(StringType(), StringType()), True)
            ])
            
            # Handle video_data: Extract from 'content' map (bronze uses content map, not video_data)
            if "content" in df.columns:
                # Extract video_data from content map
                logger.info("Extracting video_data from 'content' MAP...")
                # Also derive video_category from content_type for now (until enrichment lookup tables exist)
                df_result = df_result.withColumn(
                    "video_data",
                    struct(
                        col("content").getItem("content_id").alias("video_id"),
                        col("content").getItem("title").alias("title"),
                        col("content").getItem("duration_seconds").cast(LongType()).alias("duration_seconds"),
                        lit(0).cast(LongType()).alias("play_position_seconds"),  # Default to 0
                        lit(None).cast(StringType()).alias("quality")
                    )
                )
                # Derive video_category from content_type (temporary until enrichment lookup exists)
                if "video_category" not in df_result.columns:
                    df_result = df_result.withColumn(
                        "video_category",
                        when(col("content").getItem("content_type").isNotNull() & (trim(col("content").getItem("content_type")) != ""),
                             initcap(trim(col("content").getItem("content_type"))))
                        .otherwise(lit(None).cast(StringType()))
                    )
            elif "video_data" in df.columns:
                col_type = schema_dict.get("video_data")
                if isinstance(col_type, StringType):
                    # Check if it has non-null values before parsing
                    logger.info("Checking video_data JSON string...")
                    df_result = df_result.withColumn(
                        "video_data",
                        when(col("video_data").isNotNull() & (col("video_data") != ""), 
                             from_json(col("video_data"), video_data_schema))
                        .otherwise(lit(None).cast(video_data_schema))
                    )
                elif isinstance(col_type, StructType):
                    logger.debug("video_data is already a struct, skipping parsing")
                elif isinstance(col_type, MapType):
                    logger.info("Extracting video_data from MAP type...")
                    df_result = df_result.withColumn(
                        "video_data",
                        struct(
                            col("video_data").getItem("content_id").alias("video_id"),
                            col("video_data").getItem("title").alias("title"),
                            col("video_data").getItem("duration_seconds").cast(LongType()).alias("duration_seconds"),
                            lit(None).cast(LongType()).alias("play_position_seconds"),
                            lit(None).cast(StringType()).alias("quality")
                        )
                    )
            
            # Handle device_info: Extract from 'device' map (bronze uses device map, not device_info)
            if "device" in df.columns:
                # Extract device_info from device map
                logger.info("Extracting device_info from 'device' MAP...")
                # Handle empty strings for os and browser (convert empty to null)
                os_expr = when(
                    (col("device").getItem("os").isNotNull()) & (trim(col("device").getItem("os")) != ""),
                    trim(col("device").getItem("os"))
                ).otherwise(lit(None).cast(StringType()))
                
                browser_expr = when(
                    (col("device").getItem("browser").isNotNull()) & (trim(col("device").getItem("browser")) != ""),
                    trim(col("device").getItem("browser"))
                ).otherwise(lit(None).cast(StringType()))
                
                df_result = df_result.withColumn(
                    "device_info",
                    struct(
                        col("device").getItem("type").alias("device_type"),
                        os_expr.alias("os"),
                        browser_expr.alias("browser"),
                        lit(None).cast(StringType()).alias("ip_address")  # Not in bronze data, will be masked in silver
                    )
                )
            elif "device_info" in df.columns:
                col_type = schema_dict.get("device_info")
                if isinstance(col_type, StringType):
                    # Check if it has non-null values before parsing
                    logger.info("Checking device_info JSON string...")
                    df_result = df_result.withColumn(
                        "device_info",
                        when(col("device_info").isNotNull() & (col("device_info") != ""), 
                             from_json(col("device_info"), device_info_schema))
                        .otherwise(lit(None).cast(device_info_schema))
                    )
                elif isinstance(col_type, StructType):
                    logger.debug("device_info is already a struct, skipping parsing")
                elif isinstance(col_type, MapType):
                    logger.info("Extracting device_info from MAP type...")
                    df_result = df_result.withColumn(
                        "device_info",
                        struct(
                            col("device_info").getItem("type").alias("device_type"),
                            col("device_info").getItem("os").alias("os"),
                            col("device_info").getItem("browser").alias("browser"),
                            lit(None).cast(StringType()).alias("ip_address")
                        )
                    )
            
            # Handle event_data: Extract from 'context' and 'content' maps
            if "context" in df.columns and "content" in df.columns:
                # Extract event_data from context and content maps
                logger.info("Extracting event_data from 'context' and 'content' MAPs...")
                # Derive action from event_type (e.g., "video_play_start" -> "play", "video_pause" -> "pause")
                action_expr = when(col("event_type").isNotNull(),
                    regexp_replace(
                        regexp_replace(
                            lower(col("event_type")), 
                            "video_", ""
                        ),
                        "_start|_end|_pause|_resume|_stop", ""
                    )
                ).otherwise(lit(None).cast(StringType()))
                
                df_result = df_result.withColumn(
                    "event_data",
                    struct(
                        action_expr.alias("action"),  # Derive from event_type
                        col("content").getItem("content_id").alias("target_id"),  # Use content_id as target_id
                        col("content").getItem("content_type").alias("target_type"),  # Use content_type as target_type
                        col("context").alias("metadata")  # Use context map as metadata
                    )
                )
            elif "event_data" in df.columns:
                col_type = schema_dict.get("event_data")
                if isinstance(col_type, StringType):
                    # Check if it has non-null values before parsing
                    logger.info("Checking event_data JSON string...")
                    df_result = df_result.withColumn(
                        "event_data",
                        when(col("event_data").isNotNull() & (col("event_data") != ""), 
                             from_json(col("event_data"), event_data_schema))
                        .otherwise(lit(None).cast(event_data_schema))
                    )
                elif isinstance(col_type, StructType):
                    logger.debug("event_data is already a struct, skipping parsing")
            
            return df_result
            
        except Exception as e:
            logger.error(f"Error parsing JSON strings or extracting from maps: {str(e)}", exc_info=True)
            raise FlattenerError(f"Cannot parse JSON strings or extract from maps: {str(e)}")
    
    def _flatten_structs(self, df: DataFrame) -> DataFrame:
        """
        Flatten struct columns to individual columns
        
        Checks actual schema before attempting to flatten to avoid errors.
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with structs flattened
        """
        try:
            df_result = df
            schema = df_result.schema
            schema_dict = {field.name: field.dataType for field in schema.fields}
            
            # Flatten video_data struct (only if it's actually a struct)
            if "video_data" in df.columns:
                col_type = schema_dict.get("video_data")
                if isinstance(col_type, StructType):
                    logger.debug("Flattening video_data struct...")
                    df_result = df_result \
                        .withColumn("video_id", col("video_data.video_id")) \
                        .withColumn("video_title", col("video_data.title")) \
                        .withColumn("video_duration_seconds", col("video_data.duration_seconds")) \
                        .withColumn("play_position_seconds", 
                                   when(col("video_data.play_position_seconds").isNull(), lit(0).cast(LongType()))
                                   .otherwise(col("video_data.play_position_seconds"))) \
                        .withColumn("video_quality", col("video_data.quality"))
                    # Drop original struct column
                    df_result = df_result.drop("video_data")
                else:
                    logger.warning(f"video_data is not a struct (type: {col_type}), skipping flattening")
            
            # Flatten device_info struct (only if it's actually a struct)
            if "device_info" in df.columns:
                col_type = schema_dict.get("device_info")
                if isinstance(col_type, StructType):
                    logger.debug("Flattening device_info struct...")
                    df_result = df_result \
                        .withColumn("device_type", col("device_info.device_type")) \
                        .withColumn("os", 
                                   when((col("device_info.os").isNotNull()) & (trim(col("device_info.os")) != ""),
                                        trim(col("device_info.os")))
                                   .otherwise(lit(None).cast(StringType()))) \
                        .withColumn("browser", 
                                   when((col("device_info.browser").isNotNull()) & (trim(col("device_info.browser")) != ""),
                                        trim(col("device_info.browser")))
                                   .otherwise(lit(None).cast(StringType()))) \
                        .withColumn("ip_address", col("device_info.ip_address"))
                    # Drop original struct column
                    df_result = df_result.drop("device_info")
                else:
                    logger.warning(f"device_info is not a struct (type: {col_type}), skipping flattening")
            
            # Flatten event_data struct (only if it's actually a struct)
            if "event_data" in df.columns:
                col_type = schema_dict.get("event_data")
                if isinstance(col_type, StructType):
                    logger.debug("Flattening event_data struct...")
                    df_result = df_result \
                        .withColumn("action", 
                                   when(col("event_data.action").isNotNull() & (trim(col("event_data.action")) != ""),
                                        trim(col("event_data.action")))
                                   .otherwise(
                                       when(col("event_type").isNotNull(),
                                           regexp_replace(
                                               regexp_replace(
                                                   lower(col("event_type")), 
                                                   "video_", ""
                                               ),
                                               "_start|_end|_pause|_resume|_stop", ""
                                           )
                                       ).otherwise(lit(None).cast(StringType()))
                                   )) \
                        .withColumn("target_id", col("event_data.target_id")) \
                        .withColumn("target_type", col("event_data.target_type"))
                    # Drop original struct column
                    df_result = df_result.drop("event_data")
                else:
                    logger.warning(f"event_data is not a struct (type: {col_type}), skipping flattening")
            
            # Flatten processing_metadata struct (keep it but also extract some fields)
            # We keep processing_metadata for silver_processing_metadata transformation
            if "processing_metadata" in df.columns:
                logger.debug("Extracting fields from processing_metadata...")
                # Keep processing_metadata as-is, will be transformed later
                pass
            
            return df_result
            
        except Exception as e:
            logger.error(f"Error flattening structs: {str(e)}", exc_info=True)
            raise FlattenerError(f"Cannot flatten structs: {str(e)}")
    
    def _flatten_maps(self, df: DataFrame) -> DataFrame:
        """
        Flatten map columns (if any)
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with maps flattened
        """
        try:
            df_result = df
            
            # If there are map columns, extract common keys
            # For now, bronze doesn't have map columns that need flattening
            # But this method is here for future use
            
            # Example: If metadata was a map, we could do:
            # df_result = df_result.withColumn("metadata_key", col("metadata.key"))
            
            return df_result
            
        except Exception as e:
            logger.error(f"Error flattening maps: {str(e)}", exc_info=True)
            raise FlattenerError(f"Cannot flatten maps: {str(e)}")
    
    def _drop_original_map_columns(self, df: DataFrame) -> DataFrame:
        """
        Drop original MAP columns after extracting data from them
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with original MAP columns dropped
        """
        try:
            df_result = df
            columns_to_drop = []
            
            # Drop device, content, context maps if they exist (we've extracted their data)
            if "device" in df.columns:
                columns_to_drop.append("device")
            if "content" in df.columns:
                columns_to_drop.append("content")
            if "context" in df.columns:
                columns_to_drop.append("context")
            
            if columns_to_drop:
                logger.debug(f"Dropping original MAP columns: {columns_to_drop}")
                df_result = df_result.drop(*columns_to_drop)
            
            return df_result
            
        except Exception as e:
            logger.error(f"Error dropping MAP columns: {str(e)}", exc_info=True)
            raise FlattenerError(f"Cannot drop MAP columns: {str(e)}")
    
    def _rename_to_silver_schema(self, df: DataFrame) -> DataFrame:
        """
        Rename columns to match silver schema naming
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with renamed columns
        """
        try:
            df_result = df
            
            # Rename timestamp columns to match silver schema
            if "timestamp_ms" in df.columns:
                df_result = df_result.withColumnRenamed("timestamp_ms", "event_timestamp")
            
            # Keep partition column (it's a Kafka partition, useful for tracking)
            # Don't drop it - it's part of the silver schema
            
            # Rename timestamp_iso if exists (silver might not need it, but keep for now)
            
            # All other columns should already match after flattening
            
            return df_result
            
        except Exception as e:
            logger.error(f"Error renaming columns: {str(e)}", exc_info=True)
            raise FlattenerError(f"Cannot rename columns: {str(e)}")
    
    def _add_missing_silver_columns(self, df: DataFrame) -> DataFrame:
        """
        Add missing silver schema columns (as null)
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with all silver columns (missing ones as null)
        """
        try:
            df_result = df
            
            # Derive event_date and event_hour from bronze dt and hr columns
            # Always derive from dt/hr if they exist, even if event_date/event_hour already exist
            if "dt" in df_result.columns:
                logger.debug("Deriving event_date from dt column")
                df_result = df_result.withColumn("event_date", col("dt"))
            elif "event_date" not in df_result.columns:
                logger.debug("Adding missing column: event_date")
                df_result = df_result.withColumn("event_date", lit(None).cast("date"))
            
            if "hr" in df_result.columns:
                logger.debug("Deriving event_hour from hr column")
                df_result = df_result.withColumn("event_hour", col("hr"))
            elif "event_hour" not in df_result.columns:
                logger.debug("Adding missing column: event_hour")
                df_result = df_result.withColumn("event_hour", lit(None).cast("integer"))
            
            # Ensure action column exists and is derived from event_type if missing/null
            # Always derive action from event_type if event_type exists (more efficient than checking)
            if "action" not in df_result.columns:
                if "event_type" in df_result.columns:
                    df_result = df_result.withColumn(
                        "action",
                        when(col("event_type").isNotNull(),
                            regexp_replace(
                                regexp_replace(
                                    lower(col("event_type")), 
                                    "video_", ""
                                ),
                                "_start|_end|_pause|_resume|_stop", ""
                            )
                        ).otherwise(lit(None).cast(StringType()))
                    )
                else:
                    df_result = df_result.withColumn("action", lit(None).cast(StringType()))
            
            # Ensure play_position_seconds defaults to 0 if missing or null
            # Always use coalesce for efficiency (handles both missing and null cases)
            if "play_position_seconds" not in df_result.columns:
                df_result = df_result.withColumn("play_position_seconds", lit(0).cast(LongType()))
            else:
                # Replace nulls with 0 using coalesce (more efficient than when/otherwise)
                from pyspark.sql.functions import coalesce
                df_result = df_result.withColumn(
                    "play_position_seconds",
                    coalesce(col("play_position_seconds"), lit(0).cast(LongType()))
                )
            
            # Ensure video_category is derived from content_type if missing
            if "video_category" not in df_result.columns:
                if "content" in df_result.columns:
                    df_result = df_result.withColumn(
                        "video_category",
                        when(col("content").getItem("content_type").isNotNull() & (trim(col("content").getItem("content_type")) != ""),
                             initcap(trim(col("content").getItem("content_type"))))
                        .otherwise(lit(None).cast(StringType()))
                    )
                else:
                    df_result = df_result.withColumn("video_category", lit(None).cast(StringType()))
            
            # Silver schema required columns that might be missing
            silver_columns = [
                "event_id",
                "event_type",
                "event_timestamp",
                "user_id",
                "user_segment",  # Enriched field (will be null until lookup tables exist)
                "session_id",
                "target_id",
                "target_type",
                "video_id",
                "video_title",
                "video_duration_seconds",
                "video_quality",
                "device_type",
                "os",
                "browser",
                "ip_address",  # Not in bronze data, will be null
                "country",  # Enriched field (will be null until lookup tables exist)
                "region",  # Enriched field (will be null until lookup tables exist)
                "timezone",  # Enriched field (will be null until lookup tables exist)
            ]
            
            # Add missing columns as null (except those already handled above)
            # lit is already imported at the top
            for col_name in silver_columns:
                if col_name not in df_result.columns:
                    logger.debug(f"Adding missing column: {col_name}")
                    if col_name == "play_position_seconds":
                        df_result = df_result.withColumn(col_name, lit(0).cast(LongType()))
                    else:
                        df_result = df_result.withColumn(col_name, lit(None).cast("string"))
            
            return df_result
            
        except Exception as e:
            logger.error(f"Error adding missing columns: {str(e)}", exc_info=True)
            raise FlattenerError(f"Cannot add missing columns: {str(e)}")
    
    def get_flattening_stats(self, df_before: DataFrame, df_after: DataFrame) -> Dict[str, Any]:
        """
        Get flattening statistics
        
        Args:
            df_before: DataFrame before flattening
            df_after: DataFrame after flattening
        
        Returns:
            Dictionary with flattening statistics
        """
        try:
            stats = {
                "columns_before": len(df_before.columns),
                "columns_after": len(df_after.columns),
                "columns_added": len(df_after.columns) - len(df_before.columns),
                "nested_structs_flattened": 0,
                "mappings_applied": len(self.flattening_mappings)
            }
            
            # Count nested structs that were flattened
            nested_structs = ["video_data", "device_info", "event_data"]
            for struct_col in nested_structs:
                if struct_col in df_before.columns and struct_col not in df_after.columns:
                    stats["nested_structs_flattened"] += 1
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting flattening stats: {str(e)}", exc_info=True)
            return {"error": str(e)}


# Convenience function
def flatten_dataframe(df: DataFrame) -> DataFrame:
    """
    Flatten DataFrame (convenience function)
    
    Args:
        df: Input DataFrame with nested structures
    
    Returns:
        DataFrame with flattened columns
    
    Example:
        flat_df = flatten_dataframe(bronze_df)
    """
    flattener = Flattener()
    return flattener.flatten(df)

