"""
Silver Validation - Production ML Pipeline
Validates data quality per silver schema rules
Checks patterns, ranges, allowed values, and business rules
"""
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnull, regexp_extract, lit, current_timestamp, expr
from libs.config_loader import load_config
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class SilverValidationError(PipelineException):
    """Raised when validation fails"""
    pass


class SilverValidator:
    """
    Production-grade silver data validator
    
    Validates data per silver schema:
    - Pattern validation (event_id must be UUID)
    - Range validation (event_hour must be 0-23)
    - Allowed values (event_type must be from list)
    - Business rules (play_position ≤ video_duration)
    
    Example:
        validator = SilverValidator()
        validated_df = validator.validate(df)
        # Data validated with quality scores
    """
    
    def __init__(
        self,
        schema_path: str = "schemas/silver_schema/silver_schema.yaml"
    ):
        """
        Initialize silver validator
        
        Args:
            schema_path: Path to silver schema YAML file
        """
        try:
            self.schema_config = load_config(schema_path)
            self.validation_rules = self._load_validation_rules()
            logger.info("Silver validator initialized with schema")
        except Exception as e:
            logger.warning(f"Could not load schema: {str(e)}, using default validation rules")
            self.schema_config = {}
            self.validation_rules = self._get_default_validation_rules()
        
        logger.info("Silver validator initialized")
    
    def _load_validation_rules(self) -> Dict[str, Any]:
        """
        Load validation rules from schema config
        
        Returns:
            Dictionary with validation rules
        """
        rules = {
            "field_validations": {},
            "business_rules": [],
            "data_quality": {}
        }
        
        if self.schema_config:
            # Load field-level validations
            if "fields" in self.schema_config:
                for field in self.schema_config["fields"]:
                    field_name = field.get("name")
                    if field_name and "validation" in field:
                        rules["field_validations"][field_name] = field["validation"]
            
            # Load business rules
            if "data_quality" in self.schema_config:
                data_quality = self.schema_config["data_quality"]
                if "business_rules" in data_quality:
                    rules["business_rules"] = data_quality["business_rules"]
                if "min_quality_score" in data_quality:
                    rules["data_quality"]["min_quality_score"] = data_quality["min_quality_score"]
        
        # If no rules from config, use defaults
        if not rules["field_validations"]:
            rules = self._get_default_validation_rules()
        
        return rules
    
    def _get_default_validation_rules(self) -> Dict[str, Any]:
        """
        Get default validation rules (fallback)
        
        Returns:
            Dictionary with default validation rules
        """
        return {
            "field_validations": {
                "event_id": {
                    "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
                    "min_length": 36,
                    "max_length": 36
                },
                "event_type": {
                    "allowed_values": [
                        "video_play_start", "video_play_pause", "video_play_complete",
                        "video_seek", "post_like", "post_share", "add_to_cart",
                        "purchase", "search", "page_view"
                    ]
                },
                "event_hour": {
                    "min_value": 0,
                    "max_value": 23
                },
                "country": {
                    "pattern": "^[A-Z]{2}$"
                }
            },
            "business_rules": [
                {
                    "name": "play_position_less_than_duration",
                    "condition": "play_position_seconds <= video_duration_seconds",
                    "error_message": "Play position cannot exceed video duration"
                },
                {
                    "name": "event_timestamp_not_future",
                    "condition": "event_timestamp <= unix_timestamp(current_timestamp()) * 1000",
                    "error_message": "Event timestamp cannot be in the future"
                }
            ],
            "data_quality": {
                "min_quality_score": 0.7
            }
        }
    
    def validate(
        self,
        df: DataFrame,
        add_validation_columns: bool = True
    ) -> DataFrame:
        """
        Validate DataFrame per silver schema rules
        
        Args:
            df: Input DataFrame
            add_validation_columns: If True, adds validation_status and data_quality_score columns
        
        Returns:
            DataFrame with validation applied
        
        Example:
            validator = SilverValidator()
            validated_df = validator.validate(df)
        """
        try:
            logger.info("Starting validation...")
            
            df_result = df
            
            # Step 1: Validate field-level rules (patterns, ranges, allowed values)
            df_result = self._validate_fields(df_result)
            
            # Step 2: Validate business rules
            df_result = self._validate_business_rules(df_result)
            
            # Step 3: Calculate quality score and add validation columns
            if add_validation_columns:
                df_result = self._add_validation_columns(df_result)
            
            logger.info("Validation complete")
            return df_result
            
        except Exception as e:
            logger.error(f"Validation failed: {str(e)}", exc_info=True)
            raise SilverValidationError(f"Cannot validate data: {str(e)}")
    
    def _validate_fields(self, df: DataFrame) -> DataFrame:
        """
        Validate field-level rules (patterns, ranges, allowed values)
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with validation flags added
        """
        try:
            df_result = df
            field_validations = self.validation_rules.get("field_validations", {})
            
            # Add validation flags for each field
            for field_name, validation_rules in field_validations.items():
                if field_name in df_result.columns:
                    # Initialize validation flag (1 = valid, 0 = invalid)
                    df_result = df_result.withColumn(
                        f"_valid_{field_name}",
                        lit(1)
                    )
                    
                    # Pattern validation
                    if "pattern" in validation_rules:
                        pattern = validation_rules["pattern"]
                        df_result = df_result.withColumn(
                            f"_valid_{field_name}",
                            when(
                                isnull(col(field_name)),
                                lit(1)  # Null is valid for optional fields
                            ).when(
                                regexp_extract(col(field_name), pattern, 0) == col(field_name),
                                col(f"_valid_{field_name}")
                            ).otherwise(lit(0))
                        )
                    
                    # Range validation (min/max)
                    if "min_value" in validation_rules or "max_value" in validation_rules:
                        min_val = validation_rules.get("min_value")
                        max_val = validation_rules.get("max_value")
                        
                        condition = lit(True)
                        if min_val is not None:
                            condition = condition & (col(field_name) >= min_val)
                        if max_val is not None:
                            condition = condition & (col(field_name) <= max_val)
                        
                        df_result = df_result.withColumn(
                            f"_valid_{field_name}",
                            when(
                                isnull(col(field_name)),
                                lit(1)  # Null is valid for optional fields
                            ).when(
                                condition,
                                col(f"_valid_{field_name}")
                            ).otherwise(lit(0))
                        )
                    
                    # Allowed values validation
                    if "allowed_values" in validation_rules:
                        allowed_values = validation_rules["allowed_values"]
                        df_result = df_result.withColumn(
                            f"_valid_{field_name}",
                            when(
                                isnull(col(field_name)),
                                lit(1)  # Null is valid for optional fields
                            ).when(
                                col(field_name).isin(allowed_values),
                                col(f"_valid_{field_name}")
                            ).otherwise(lit(0))
                        )
                    
                    logger.debug(f"Applied validation rules to {field_name}")
            
            return df_result
            
        except Exception as e:
            logger.error(f"Error validating fields: {str(e)}", exc_info=True)
            raise SilverValidationError(f"Cannot validate fields: {str(e)}")
    
    def _validate_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Validate business rules
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with business rule validation flags
        """
        try:
            df_result = df
            business_rules = self.validation_rules.get("business_rules", [])
            
            # Add validation flag for business rules
            df_result = df_result.withColumn("_valid_business_rules", lit(1))
            
            for rule in business_rules:
                rule_name = rule.get("name", "unknown")
                condition = rule.get("condition", "")
                
                if condition:
                    try:
                        # Evaluate business rule condition
                        df_result = df_result.withColumn(
                            "_valid_business_rules",
                            when(
                                expr(condition),
                                col("_valid_business_rules")
                            ).otherwise(lit(0))
                        )
                        logger.debug(f"Applied business rule: {rule_name}")
                    except Exception as e:
                        logger.warning(f"Could not apply business rule {rule_name}: {str(e)}")
            
            return df_result
            
        except Exception as e:
            logger.error(f"Error validating business rules: {str(e)}", exc_info=True)
            raise SilverValidationError(f"Cannot validate business rules: {str(e)}")
    
    def _add_validation_columns(self, df: DataFrame) -> DataFrame:
        """
        Add validation status and quality score columns
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with validation columns added
        """
        try:
            df_result = df
            
            # Get all validation flag columns
            validation_flags = [c for c in df_result.columns if c.startswith("_valid_")]
            
            if not validation_flags:
                # No validation flags, set all as valid
                df_result = df_result.withColumn("validation_status", lit("valid"))
                df_result = df_result.withColumn("data_quality_score", lit(1.0))
            else:
                # Calculate quality score (percentage of valid fields)
                total_validations = len(validation_flags)
                valid_count = sum([col(flag) for flag in validation_flags])
                quality_score = valid_count / total_validations
                
                df_result = df_result.withColumn("data_quality_score", quality_score)
                
                # Set validation status
                min_score = self.validation_rules.get("data_quality", {}).get("min_quality_score", 0.7)
                df_result = df_result.withColumn(
                    "validation_status",
                    when(col("data_quality_score") >= min_score, lit("valid"))
                    .when(col("data_quality_score") >= 0.5, lit("warning"))
                    .otherwise(lit("invalid"))
                )
            
            # Drop temporary validation flag columns
            for flag_col in validation_flags:
                df_result = df_result.drop(flag_col)
            
            logger.debug("Added validation columns: validation_status, data_quality_score")
            return df_result
            
        except Exception as e:
            logger.error(f"Error adding validation columns: {str(e)}", exc_info=True)
            raise SilverValidationError(f"Cannot add validation columns: {str(e)}")
    
    def get_validation_stats(self, df: DataFrame) -> Dict[str, Any]:
        """
        Get validation statistics
        
        Args:
            df: Validated DataFrame
        
        Returns:
            Dictionary with validation statistics
        """
        try:
            if "validation_status" not in df.columns:
                return {"error": "DataFrame not validated (no validation_status column)"}
            
            # Count by validation status
            status_counts = df.groupBy("validation_status").count().collect()
            status_dict = {row["validation_status"]: row["count"] for row in status_counts}
            
            # Average quality score
            if "data_quality_score" in df.columns:
                avg_score = df.agg({"data_quality_score": "avg"}).collect()[0][0]
            else:
                avg_score = None
            
            stats = {
                "total_records": df.count(),
                "validation_status_counts": status_dict,
                "average_quality_score": avg_score,
                "valid_count": status_dict.get("valid", 0),
                "warning_count": status_dict.get("warning", 0),
                "invalid_count": status_dict.get("invalid", 0)
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting validation stats: {str(e)}", exc_info=True)
            return {"error": str(e)}


# Convenience function
def validate_silver_data(df: DataFrame, add_validation_columns: bool = True) -> DataFrame:
    """
    Validate silver data (convenience function)
    
    Args:
        df: Input DataFrame
        add_validation_columns: If True, adds validation columns
    
    Returns:
        DataFrame with validation applied
    
    Example:
        validated_df = validate_silver_data(df)
    """
    validator = SilverValidator()
    return validator.validate(df, add_validation_columns=add_validation_columns)

