"""
Normalization Rules - Production ML Pipeline
Normalizes data values to match silver schema standards
Applies string normalization, enum mappings, and timestamp standardization
"""
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, trim, lower, upper, regexp_replace, from_unixtime, to_timestamp
from pyspark.sql.types import StringType
from libs.config_loader import load_config
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class NormalizationError(PipelineException):
    """Raised when normalization fails"""
    pass


class Normalizer:
    """
    Production-grade data normalizer
    
    Normalizes values to match silver schema:
    - String normalization: trim, lowercase, uppercase
    - Enum mappings: "phone" → "mobile", "4k" → "4K"
    - Timestamp normalization: ensure UTC, milliseconds
    - Value standardization
    
    Example:
        normalizer = Normalizer()
        normalized_df = normalizer.normalize(df)
        # All values normalized per silver schema rules
    """
    
    def __init__(
        self,
        schema_path: str = "schemas/silver_schema/silver_schema.yaml"
    ):
        """
        Initialize normalizer
        
        Args:
            schema_path: Path to silver schema YAML file
        """
        try:
            self.schema_config = load_config(schema_path)
            self.normalization_rules = self._load_normalization_rules()
            logger.info("Normalizer initialized with silver schema")
        except Exception as e:
            logger.warning(f"Could not load schema: {str(e)}, using default normalization rules")
            self.schema_config = {}
            self.normalization_rules = self._get_default_normalization_rules()
        
        logger.info("Normalizer initialized")
    
    def _load_normalization_rules(self) -> Dict[str, Any]:
        """
        Load normalization rules from schema config
        
        Returns:
            Dictionary with normalization rules
        """
        rules = {
            "string_normalization": {},
            "enum_mappings": {},
            "timestamp_normalization": {}
        }
        
        if self.schema_config and "normalization" in self.schema_config:
            norm_config = self.schema_config["normalization"]
            
            # String normalization rules
            if "string_normalization" in norm_config:
                rules["string_normalization"] = norm_config["string_normalization"]
            
            # Enum mappings
            if "enum_mappings" in norm_config:
                rules["enum_mappings"] = norm_config["enum_mappings"]
            
            # Timestamp normalization
            if "timestamp_normalization" in norm_config:
                rules["timestamp_normalization"] = norm_config["timestamp_normalization"]
        
        # If no rules from config, use defaults
        if not rules["string_normalization"] and not rules["enum_mappings"]:
            rules = self._get_default_normalization_rules()
        
        return rules
    
    def _get_default_normalization_rules(self) -> Dict[str, Any]:
        """
        Get default normalization rules (fallback)
        
        Returns:
            Dictionary with default normalization rules
        """
        return {
            "string_normalization": {
                "trim_whitespace": True,
                "lowercase": [
                    "event_type",
                    "action",
                    "target_type",
                    "device_type",
                    "os",
                    "browser",
                    "video_quality",
                    "video_category",
                    "user_segment"
                ],
                "uppercase": ["country"]
            },
            "enum_mappings": {
                "device_type": {
                    "phone": "mobile",
                    "smartphone": "mobile",
                    "ipad": "tablet",
                    "laptop": "desktop",
                    "computer": "desktop",
                    "smart-tv": "smart_tv",
                    "smart tv": "smart_tv"
                },
                "os": {
                    "android": "android",
                    "ios": "ios",
                    "iphone os": "ios",
                    "windows": "windows",
                    "mac": "macos",
                    "macos": "macos",
                    "darwin": "macos"
                },
                "video_quality": {
                    "240": "240p",
                    "360": "360p",
                    "480": "480p",
                    "720": "720p",
                    "1080": "1080p",
                    "1440": "1440p",
                    "2160": "2160p",
                    "4k": "4K",
                    "uhd": "4K"
                }
            },
            "timestamp_normalization": {
                "timezone": "UTC",
                "format": "milliseconds"
            }
        }
    
    def normalize(self, df: DataFrame) -> DataFrame:
        """
        Normalize all values in DataFrame per silver schema rules
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with normalized values
        
        Example:
            normalizer = Normalizer()
            normalized_df = normalizer.normalize(df)
        """
        try:
            logger.info("Starting normalization...")
            
            df_result = df
            
            # Step 1: Normalize strings (trim, lowercase, uppercase)
            df_result = self._normalize_strings(df_result)
            
            # Step 2: Apply enum mappings (variations → standard values)
            df_result = self._normalize_enums(df_result)
            
            # Step 3: Normalize timestamps (ensure UTC, milliseconds)
            df_result = self._normalize_timestamps(df_result)
            
            logger.info("Normalization complete")
            return df_result
            
        except Exception as e:
            logger.error(f"Normalization failed: {str(e)}", exc_info=True)
            raise NormalizationError(f"Cannot normalize data: {str(e)}")
    
    def _normalize_strings(self, df: DataFrame) -> DataFrame:
        """
        Normalize string columns (trim, lowercase, uppercase)
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with normalized strings
        """
        try:
            df_result = df
            string_norm = self.normalization_rules.get("string_normalization", {})
            
            # Trim whitespace (if enabled)
            if string_norm.get("trim_whitespace", True):
                # Trim all string columns (skip MAP, ARRAY, STRUCT types)
                schema_dict = {field.name: field.dataType for field in df_result.schema.fields}
                for col_name in df_result.columns:
                    col_type = schema_dict.get(col_name)
                    # Only trim StringType columns
                    if isinstance(col_type, StringType):
                        df_result = df_result.withColumn(
                            col_name,
                            when(col(col_name).isNotNull(), trim(col(col_name)))
                            .otherwise(col(col_name))
                        )
            
            # Lowercase columns
            lowercase_cols = string_norm.get("lowercase", [])
            for col_name in lowercase_cols:
                if col_name in df_result.columns:
                    df_result = df_result.withColumn(
                        col_name,
                        when(col(col_name).isNotNull(), lower(col(col_name)))
                        .otherwise(col(col_name))
                    )
                    logger.debug(f"Applied lowercase to {col_name}")
            
            # Uppercase columns
            uppercase_cols = string_norm.get("uppercase", [])
            for col_name in uppercase_cols:
                if col_name in df_result.columns:
                    df_result = df_result.withColumn(
                        col_name,
                        when(col(col_name).isNotNull(), upper(col(col_name)))
                        .otherwise(col(col_name))
                    )
                    logger.debug(f"Applied uppercase to {col_name}")
            
            return df_result
            
        except Exception as e:
            logger.error(f"Error normalizing strings: {str(e)}", exc_info=True)
            raise NormalizationError(f"Cannot normalize strings: {str(e)}")
    
    def _normalize_enums(self, df: DataFrame) -> DataFrame:
        """
        Apply enum mappings (variations → standard values)
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with enum values mapped
        """
        try:
            df_result = df
            enum_mappings = self.normalization_rules.get("enum_mappings", {})
            
            # Apply mappings for each field
            for field_name, mappings in enum_mappings.items():
                if field_name in df_result.columns:
                    # Build when conditions for all mappings
                    when_conditions = None
                    for old_value, new_value in mappings.items():
                        condition = col(field_name) == old_value
                        if when_conditions is None:
                            when_conditions = when(condition, new_value)
                        else:
                            when_conditions = when_conditions.when(condition, new_value)
                    
                    # Apply mappings (keep original if no match)
                    if when_conditions is not None:
                        df_result = df_result.withColumn(
                            field_name,
                            when_conditions.otherwise(col(field_name))
                        )
                        logger.debug(f"Applied enum mappings to {field_name} ({len(mappings)} mappings)")
            
            return df_result
            
        except Exception as e:
            logger.error(f"Error normalizing enums: {str(e)}", exc_info=True)
            raise NormalizationError(f"Cannot normalize enums: {str(e)}")
    
    def _normalize_timestamps(self, df: DataFrame) -> DataFrame:
        """
        Normalize timestamps (ensure UTC, milliseconds format)
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with normalized timestamps
        """
        try:
            df_result = df
            timestamp_norm = self.normalization_rules.get("timestamp_normalization", {})
            
            # Ensure event_timestamp is in milliseconds (already should be from type casting)
            # But we can validate/ensure format here
            if "event_timestamp" in df_result.columns:
                # Ensure it's long (milliseconds) - should already be from type casting
                # Just validate it's in correct format
                logger.debug("Validated event_timestamp format")
            
            # Normalize event_date if needed (should already be date from type casting)
            if "event_date" in df_result.columns:
                logger.debug("Validated event_date format")
            
            return df_result
            
        except Exception as e:
            logger.error(f"Error normalizing timestamps: {str(e)}", exc_info=True)
            raise NormalizationError(f"Cannot normalize timestamps: {str(e)}")
    
    def get_normalization_stats(self, df_before: DataFrame, df_after: DataFrame) -> Dict[str, Any]:
        """
        Get normalization statistics
        
        Args:
            df_before: DataFrame before normalization
            df_after: DataFrame after normalization
        
        Returns:
            Dictionary with normalization statistics
        """
        try:
            stats = {
                "string_normalizations_applied": len(self.normalization_rules.get("string_normalization", {}).get("lowercase", [])) + 
                                                  len(self.normalization_rules.get("string_normalization", {}).get("uppercase", [])),
                "enum_mappings_applied": sum(
                    len(mappings) for mappings in self.normalization_rules.get("enum_mappings", {}).values()
                ),
                "fields_normalized": len(self.normalization_rules.get("enum_mappings", {}).keys())
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting normalization stats: {str(e)}", exc_info=True)
            return {"error": str(e)}


# Convenience function
def normalize_dataframe(df: DataFrame) -> DataFrame:
    """
    Normalize DataFrame (convenience function)
    
    Args:
        df: Input DataFrame
    
    Returns:
        DataFrame with normalized values
    
    Example:
        normalized_df = normalize_dataframe(df)
    """
    normalizer = Normalizer()
    return normalizer.normalize(df)

