"""
Null Handler - Production ML Pipeline
Handles null values according to silver schema rules
Fills default values and validates required fields are not null
"""
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnull, lit, coalesce
from libs.config_loader import load_config
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class NullHandlerError(PipelineException):
    """Raised when null handling fails"""
    pass


class NullHandler:
    """
    Production-grade null value handler
    
    Handles null values per silver schema:
    - Fills default values for nulls (video_quality → "unknown")
    - Validates required fields are not null
    - Handles nullable vs required fields
    
    Example:
        handler = NullHandler()
        handled_df = handler.handle_nulls(df)
        # Nulls filled with defaults, required fields validated
    """
    
    def __init__(
        self,
        schema_path: str = "schemas/silver_schema/silver_schema.yaml"
    ):
        """
        Initialize null handler
        
        Args:
            schema_path: Path to silver schema YAML file
        """
        try:
            self.schema_config = load_config(schema_path)
            self.null_rules = self._load_null_rules()
            logger.info("Null handler initialized with silver schema")
        except Exception as e:
            logger.warning(f"Could not load schema: {str(e)}, using default null rules")
            self.schema_config = {}
            self.null_rules = self._get_default_null_rules()
        
        logger.info("Null handler initialized")
    
    def _load_null_rules(self) -> Dict[str, Any]:
        """
        Load null handling rules from schema config
        
        Returns:
            Dictionary with null handling rules
        """
        rules = {
            "nullable_fields": [],
            "default_values": {},
            "required_non_null_fields": []
        }
        
        if self.schema_config and "null_handling" in self.schema_config:
            null_config = self.schema_config["null_handling"]
            
            # Nullable fields
            if "nullable_fields" in null_config:
                rules["nullable_fields"] = null_config["nullable_fields"]
            
            # Default values
            if "default_values" in null_config:
                rules["default_values"] = null_config["default_values"]
            
            # Required non-null fields
            if "required_non_null_fields" in null_config:
                rules["required_non_null_fields"] = null_config["required_non_null_fields"]
        
        # If no rules from config, use defaults
        if not rules["nullable_fields"] and not rules["default_values"]:
            rules = self._get_default_null_rules()
        
        return rules
    
    def _get_default_null_rules(self) -> Dict[str, Any]:
        """
        Get default null handling rules (fallback)
        
        Returns:
            Dictionary with default null rules
        """
        return {
            "nullable_fields": [
                "user_id",
                "session_id",
                "action",
                "target_id",
                "target_type",
                "video_id",
                "video_title",
                "video_duration_seconds",
                "play_position_seconds",
                "video_quality",
                "video_category",
                "device_type",
                "os",
                "browser",
                "ip_address",
                "country",
                "region",
                "timezone",
                "user_segment"
            ],
            "default_values": {
                "action": None,
                "target_type": None,
                "video_quality": "unknown",
                "device_type": "unknown",
                "os": "unknown"
            },
            "required_non_null_fields": [
                "event_id",
                "event_type",
                "event_timestamp",
                "event_date",
                "event_hour"
            ]
        }
    
    def handle_nulls(
        self,
        df: DataFrame,
        fill_defaults: bool = True,
        validate_required: bool = True
    ) -> DataFrame:
        """
        Handle null values in DataFrame per silver schema rules
        
        Args:
            df: Input DataFrame
            fill_defaults: If True, fill nulls with default values
            validate_required: If True, validate required fields are not null
        
        Returns:
            DataFrame with nulls handled
        
        Example:
            handler = NullHandler()
            handled_df = handler.handle_nulls(df)
        """
        try:
            logger.info("Starting null handling...")
            
            df_result = df
            
            # Step 1: Fill default values for nulls
            if fill_defaults:
                df_result = self._fill_default_values(df_result)
            
            # Step 2: Validate required fields are not null
            if validate_required:
                self._validate_required_fields(df_result)
            
            logger.info("Null handling complete")
            return df_result
            
        except Exception as e:
            logger.error(f"Null handling failed: {str(e)}", exc_info=True)
            raise NullHandlerError(f"Cannot handle nulls: {str(e)}")
    
    def _fill_default_values(self, df: DataFrame) -> DataFrame:
        """
        Fill null values with default values from schema
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with defaults filled
        """
        try:
            df_result = df
            default_values = self.null_rules.get("default_values", {})
            
            for column_name, default_value in default_values.items():
                if column_name in df_result.columns:
                    if default_value is None:
                        # Keep null (no default)
                        continue
                    else:
                        # Fill null with default value
                        df_result = df_result.withColumn(
                            column_name,
                            coalesce(col(column_name), lit(default_value))
                        )
                        logger.debug(f"Filled nulls in {column_name} with default: {default_value}")
            
            return df_result
            
        except Exception as e:
            logger.error(f"Error filling default values: {str(e)}", exc_info=True)
            raise NullHandlerError(f"Cannot fill default values: {str(e)}")
    
    def _validate_required_fields(self, df: DataFrame) -> bool:
        """
        Validate required fields are not null
        
        Args:
            df: Input DataFrame
        
        Returns:
            True if all required fields are not null
        
        Raises:
            NullHandlerError: If required fields are null
        """
        try:
            required_fields = self.null_rules.get("required_non_null_fields", [])
            
            if not required_fields:
                logger.debug("No required fields to validate")
                return True
            
            # Check for nulls in required fields
            null_checks = []
            for field in required_fields:
                if field in df.columns:
                    null_count = df.filter(col(field).isNull()).count()
                    if null_count > 0:
                        null_checks.append({
                            "field": field,
                            "null_count": null_count
                        })
            
            if null_checks:
                error_msg = f"Required fields have null values: {null_checks}"
                logger.error(error_msg)
                raise NullHandlerError(error_msg)
            
            logger.debug("All required fields validated (no nulls)")
            return True
            
        except NullHandlerError:
            raise
        except Exception as e:
            logger.error(f"Error validating required fields: {str(e)}", exc_info=True)
            raise NullHandlerError(f"Cannot validate required fields: {str(e)}")
    
    def get_null_stats(self, df: DataFrame) -> Dict[str, Any]:
        """
        Get null value statistics
        
        Args:
            df: Input DataFrame
        
        Returns:
            Dictionary with null statistics
        """
        try:
            stats = {
                "total_rows": df.count(),
                "null_counts": {},
                "nullable_fields": self.null_rules.get("nullable_fields", []),
                "required_fields": self.null_rules.get("required_non_null_fields", [])
            }
            
            # Count nulls in each column
            for col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                if null_count > 0:
                    stats["null_counts"][col_name] = null_count
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting null stats: {str(e)}", exc_info=True)
            return {"error": str(e)}
    
    def fill_defaults_only(self, df: DataFrame) -> DataFrame:
        """
        Fill default values only (skip validation)
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with defaults filled
        
        Example:
            handler = NullHandler()
            df = handler.fill_defaults_only(df)
        """
        return self.handle_nulls(df, fill_defaults=True, validate_required=False)
    
    def validate_only(self, df: DataFrame) -> bool:
        """
        Validate required fields only (skip filling defaults)
        
        Args:
            df: Input DataFrame
        
        Returns:
            True if validation passes
        
        Example:
            handler = NullHandler()
            is_valid = handler.validate_only(df)
        """
        self.handle_nulls(df, fill_defaults=False, validate_required=True)
        return True


# Convenience function
def handle_null_values(df: DataFrame, fill_defaults: bool = True) -> DataFrame:
    """
    Handle null values (convenience function)
    
    Args:
        df: Input DataFrame
        fill_defaults: If True, fill nulls with default values
    
    Returns:
        DataFrame with nulls handled
    
    Example:
        handled_df = handle_null_values(df)
    """
    handler = NullHandler()
    return handler.handle_nulls(df, fill_defaults=fill_defaults, validate_required=True)

