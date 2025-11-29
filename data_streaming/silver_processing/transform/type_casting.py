"""
Type Casting - Production ML Pipeline
Casts DataFrame columns to correct types matching silver schema
Ensures data types match silver_schema.yaml definitions
"""
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnan, isnull, to_timestamp, to_date, unix_timestamp, from_unixtime
from pyspark.sql.types import StringType, LongType, IntegerType, DoubleType, BooleanType, DateType, TimestampType
from libs.config_loader import load_config
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class TypeCastingError(PipelineException):
    """Raised when type casting fails"""
    pass


class TypeCaster:
    """
    Production-grade type caster
    
    Casts columns to match silver schema types:
    - event_timestamp: string/long → timestamp
    - event_date: string → date
    - event_hour: string → integer
    - video_duration_seconds: string → long
    - data_quality_score: string → double
    
    Example:
        caster = TypeCaster()
        typed_df = caster.cast_types(df)
        # All columns now have correct types per silver schema
    """
    
    def __init__(
        self,
        schema_path: str = "schemas/silver_schema/silver_schema.yaml"
    ):
        """
        Initialize type caster
        
        Args:
            schema_path: Path to silver schema YAML file
        """
        try:
            self.schema_config = load_config(schema_path)
            self.type_mappings = self._build_type_mappings()
            logger.info("Type caster initialized with silver schema")
        except Exception as e:
            logger.warning(f"Could not load schema: {str(e)}, using default type mappings")
            self.schema_config = {}
            self.type_mappings = self._get_default_type_mappings()
        
        logger.info(f"Type caster initialized with {len(self.type_mappings)} type mappings")
    
    def _build_type_mappings(self) -> Dict[str, str]:
        """
        Build type mappings from schema config
        
        Returns:
            Dictionary mapping column_name -> target_type
        """
        mappings = {}
        
        # Read type mappings from schema config
        if self.schema_config and "type_mappings" in self.schema_config:
            type_mappings = self.schema_config["type_mappings"]
            
            # String fields
            for field in type_mappings.get("string_fields", []):
                mappings[field] = "string"
            
            # Long fields
            for field in type_mappings.get("long_fields", []):
                mappings[field] = "long"
            
            # Integer fields
            for field in type_mappings.get("integer_fields", []):
                mappings[field] = "integer"
            
            # Date fields
            for field in type_mappings.get("date_fields", []):
                mappings[field] = "date"
            
            # Timestamp fields
            for field in type_mappings.get("timestamp_fields", []):
                mappings[field] = "timestamp"
            
            # Double fields
            for field in type_mappings.get("double_fields", []):
                mappings[field] = "double"
            
            # Boolean fields
            for field in type_mappings.get("boolean_fields", []):
                mappings[field] = "boolean"
        
        # If no mappings from config, use defaults
        if not mappings:
            mappings = self._get_default_type_mappings()
        
        return mappings
    
    def _get_default_type_mappings(self) -> Dict[str, str]:
        """
        Get default type mappings (fallback)
        
        Returns:
            Dictionary with default type mappings
        """
        return {
            # String fields
            "event_id": "string",
            "event_type": "string",
            "user_id": "string",
            "session_id": "string",
            "action": "string",
            "target_id": "string",
            "target_type": "string",
            "video_id": "string",
            "video_title": "string",
            "video_quality": "string",
            "video_category": "string",
            "device_type": "string",
            "os": "string",
            "browser": "string",
            "ip_address": "string",
            "country": "string",
            "region": "string",
            "timezone": "string",
            "user_segment": "string",
            
            # Long fields
            "event_timestamp": "long",
            "video_duration_seconds": "long",
            "play_position_seconds": "long",
            
            # Integer fields
            "event_hour": "integer",
            
            # Date fields
            "event_date": "date",
            
            # Timestamp fields (also long, but can be cast to timestamp)
            "event_timestamp": "timestamp",
        }
    
    def cast_types(self, df: DataFrame, safe_mode: bool = True) -> DataFrame:
        """
        Cast all columns to correct types per silver schema
        
        Args:
            df: Input DataFrame
            safe_mode: If True, invalid casts return null instead of error
        
        Returns:
            DataFrame with all columns cast to correct types
        
        Example:
            caster = TypeCaster()
            typed_df = caster.cast_types(df)
        """
        try:
            logger.info("Starting type casting...")
            
            df_result = df
            
            # Cast each column according to type mappings
            for column_name, target_type in self.type_mappings.items():
                if column_name in df_result.columns:
                    df_result = self._cast_column(
                        df_result,
                        column_name,
                        target_type,
                        safe_mode
                    )
            
            logger.info("Type casting complete")
            return df_result
            
        except Exception as e:
            logger.error(f"Type casting failed: {str(e)}", exc_info=True)
            raise TypeCastingError(f"Cannot cast types: {str(e)}")
    
    def _cast_column(
        self,
        df: DataFrame,
        column_name: str,
        target_type: str,
        safe_mode: bool = True
    ) -> DataFrame:
        """
        Cast single column to target type
        
        Args:
            df: Input DataFrame
            column_name: Column to cast
            target_type: Target type (string, long, integer, date, timestamp, double, boolean)
            safe_mode: If True, invalid casts return null
        
        Returns:
            DataFrame with column cast
        """
        try:
            if target_type == "string":
                df = df.withColumn(column_name, col(column_name).cast(StringType()))
            
            elif target_type == "long":
                if safe_mode:
                    # Safe cast: invalid values become null
                    df = df.withColumn(
                        column_name,
                        when(
                            isnan(col(column_name)) | isnull(col(column_name)),
                            None
                        ).otherwise(col(column_name).cast(LongType()))
                    )
                else:
                    df = df.withColumn(column_name, col(column_name).cast(LongType()))
            
            elif target_type == "integer":
                if safe_mode:
                    df = df.withColumn(
                        column_name,
                        when(
                            isnan(col(column_name)) | isnull(col(column_name)),
                            None
                        ).otherwise(col(column_name).cast(IntegerType()))
                    )
                else:
                    df = df.withColumn(column_name, col(column_name).cast(IntegerType()))
            
            elif target_type == "double":
                if safe_mode:
                    df = df.withColumn(
                        column_name,
                        when(
                            isnan(col(column_name)) | isnull(col(column_name)),
                            None
                        ).otherwise(col(column_name).cast(DoubleType()))
                    )
                else:
                    df = df.withColumn(column_name, col(column_name).cast(DoubleType()))
            
            elif target_type == "boolean":
                if safe_mode:
                    df = df.withColumn(
                        column_name,
                        when(
                            isnan(col(column_name)) | isnull(col(column_name)),
                            None
                        ).otherwise(col(column_name).cast(BooleanType()))
                    )
                else:
                    df = df.withColumn(column_name, col(column_name).cast(BooleanType()))
            
            elif target_type == "date":
                # Cast to date (from string or timestamp)
                if safe_mode:
                    df = df.withColumn(
                        column_name,
                        when(
                            isnull(col(column_name)),
                            None
                        ).otherwise(to_date(col(column_name)))
                    )
                else:
                    df = df.withColumn(column_name, to_date(col(column_name)))
            
            elif target_type == "timestamp":
                # Cast to timestamp (from long milliseconds or string)
                if safe_mode:
                    df = df.withColumn(
                        column_name,
                        when(
                            isnull(col(column_name)),
                            None
                        ).otherwise(
                            when(
                                col(column_name).rlike("^\\d+$"),  # If numeric string
                                from_unixtime(col(column_name).cast(LongType()) / 1000)
                            ).otherwise(
                                to_timestamp(col(column_name))  # If string timestamp
                            )
                        )
                    )
                else:
                    # Try casting as milliseconds timestamp
                    df = df.withColumn(
                        column_name,
                        from_unixtime(col(column_name).cast(LongType()) / 1000)
                    )
            
            logger.debug(f"Casted {column_name} to {target_type}")
            return df
            
        except Exception as e:
            logger.error(f"Error casting {column_name} to {target_type}: {str(e)}")
            if safe_mode:
                # Return column as-is if cast fails in safe mode
                return df
            else:
                raise TypeCastingError(f"Cannot cast {column_name} to {target_type}: {str(e)}")
    
    def get_casting_stats(self, df_before: DataFrame, df_after: DataFrame) -> Dict[str, Any]:
        """
        Get type casting statistics
        
        Args:
            df_before: DataFrame before casting
            df_after: DataFrame after casting
        
        Returns:
            Dictionary with casting statistics
        """
        try:
            stats = {
                "columns_casted": len(self.type_mappings),
                "type_mappings_applied": len(self.type_mappings),
                "columns_processed": len([c for c in df_before.columns if c in self.type_mappings])
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting casting stats: {str(e)}", exc_info=True)
            return {"error": str(e)}


# Convenience function
def cast_dataframe_types(df: DataFrame, safe_mode: bool = True) -> DataFrame:
    """
    Cast DataFrame types (convenience function)
    
    Args:
        df: Input DataFrame
        safe_mode: If True, invalid casts return null
    
    Returns:
        DataFrame with correct types
    
    Example:
        typed_df = cast_dataframe_types(df)
    """
    caster = TypeCaster()
    return caster.cast_types(df, safe_mode=safe_mode)

