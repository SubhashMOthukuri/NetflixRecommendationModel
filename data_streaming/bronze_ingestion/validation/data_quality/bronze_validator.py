"""
Bronze Data Validator - Production ML Pipeline
Validates data quality: nulls, ranges, formats, business rules
Ensures data meets quality standards before writing to bronze layer
"""
from typing import Dict, Any, List, Optional, Callable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnull, length, regexp_replace, trim, to_timestamp, lit
from libs.config_loader import load_config, get_config_value
from libs.logger import get_logger
from libs.exceptions import PipelineException, DataQualityError
from schemas.bronze_schema.bronze_data_contract import get_required_fields

logger = get_logger(__name__)


class BronzeValidationError(PipelineException):
    """Raised when bronze validation fails"""
    pass


class BronzeValidator:
    """
    Production-grade bronze data validator
    
    Validates:
    - Required fields (non-null)
    - Data ranges (e.g., timestamp > 0)
    - Format validation (e.g., UUID format)
    - Business rules (e.g., event_type in allowed list)
    - Data completeness
    
    Example:
        validator = BronzeValidator()
        validated_df, invalid_df = validator.validate(df)
        # validated_df: passes all quality checks
        # invalid_df: fails quality checks
    """
    
    def __init__(
        self,
        config_path: str = "kafka.yaml",
        strict_mode: bool = True
    ):
        """
        Initialize bronze validator
        
        Args:
            config_path: Path to config file
            strict_mode: If True, fail on validation errors
        """
        self.strict_mode = strict_mode
        
        # Load config
        try:
            self.config = load_config(config_path)
            data_quality = self.config.get("data_quality", {})
            self.validation_rules = data_quality.get("validation_rules", {})
        except Exception as e:
            logger.warning(f"Could not load config: {str(e)}, using defaults")
            self.validation_rules = {}
        
        # Get required fields
        self.required_fields = get_required_fields()
        
        logger.info(f"Bronze validator initialized (strict_mode: {strict_mode})")
    
    def validate(
        self,
        df: DataFrame,
        add_validation_columns: bool = True
    ) -> tuple[DataFrame, DataFrame]:
        """
        Validate DataFrame and split into valid/invalid records
        
        Args:
            df: Input DataFrame
            add_validation_columns: Add validation status columns
        
        Returns:
            Tuple of (valid_df, invalid_df)
        
        Example:
            validator = BronzeValidator()
            valid_df, invalid_df = validator.validate(df)
        """
        try:
            logger.info("Starting bronze data validation...")
            
            # Apply all validation rules
            df = self._validate_required_fields(df)
            df = self._validate_data_ranges(df)
            df = self._validate_formats(df)
            df = self._validate_business_rules(df)
            
            # Add validation status column
            if add_validation_columns:
                df = self._add_validation_status(df)
            
            # Split valid/invalid
            valid_df = df.filter(col("validation_status") == "valid")
            invalid_df = df.filter(col("validation_status") != "valid")
            
            valid_count = valid_df.count() if valid_df is not None else 0
            invalid_count = invalid_df.count() if invalid_df is not None else 0
            
            logger.info(f"Validation complete: {valid_count} valid, {invalid_count} invalid records")
            
            if invalid_count > 0:
                logger.warning(f"Found {invalid_count} records failing validation")
                if self.strict_mode:
                    raise BronzeValidationError(f"{invalid_count} records failed validation")
            
            return valid_df, invalid_df
            
        except Exception as e:
            logger.error(f"Validation failed: {str(e)}", exc_info=True)
            raise BronzeValidationError(f"Cannot validate data: {str(e)}")
    
    def _validate_required_fields(self, df: DataFrame) -> DataFrame:
        """
        Validate required fields are not null
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with validation column
        """
        # Check each required field
        validation_condition = None
        
        for field in self.required_fields:
            field_parts = field.split(".")
            field_name = field_parts[0]
            
            if field_name in df.columns:
                if validation_condition is None:
                    validation_condition = col(field_name).isNotNull()
                else:
                    validation_condition = validation_condition & col(field_name).isNotNull()
        
        if validation_condition is not None:
            df = df.withColumn(
                "required_fields_valid",
                when(validation_condition, True).otherwise(False)
            )
        else:
            df = df.withColumn("required_fields_valid", lit(True))
        
        logger.debug("Validated required fields")
        return df
    
    def _validate_data_ranges(self, df: DataFrame) -> DataFrame:
        """
        Validate data ranges (e.g., timestamp > 0)
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with validation column
        """
        # Validate timestamp_ms > 0
        if "timestamp_ms" in df.columns:
            df = df.withColumn(
                "timestamp_range_valid",
                when(
                    (col("timestamp_ms").isNotNull()) & (col("timestamp_ms") > 0),
                    True
                ).otherwise(False)
            )
        else:
            df = df.withColumn("timestamp_range_valid", lit(True))
        
        # Validate ingestion_timestamp_ms if present
        if "processing_metadata.ingestion_timestamp_ms" in df.columns:
            df = df.withColumn(
                "ingestion_timestamp_valid",
                when(
                    (col("processing_metadata.ingestion_timestamp_ms").isNotNull()) &
                    (col("processing_metadata.ingestion_timestamp_ms") > 0),
                    True
                ).otherwise(False)
            )
        else:
            df = df.withColumn("ingestion_timestamp_valid", lit(True))
        
        logger.debug("Validated data ranges")
        return df
    
    def _validate_formats(self, df: DataFrame) -> DataFrame:
        """
        Validate data formats (e.g., UUID, ISO timestamp)
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with validation column
        """
        # Validate event_id format (UUID-like, at least 32 chars)
        if "event_id" in df.columns:
            df = df.withColumn(
                "event_id_format_valid",
                when(
                    (col("event_id").isNotNull()) &
                    (length(col("event_id")) >= 32),
                    True
                ).otherwise(False)
            )
        else:
            df = df.withColumn("event_id_format_valid", lit(True))
        
        # Validate event_type is not empty
        if "event_type" in df.columns:
            df = df.withColumn(
                "event_type_format_valid",
                when(
                    (col("event_type").isNotNull()) &
                    (trim(col("event_type")) != ""),
                    True
                ).otherwise(False)
            )
        else:
            df = df.withColumn("event_type_format_valid", lit(True))
        
        # Validate timestamp_iso format if present
        if "timestamp_iso" in df.columns:
            # Basic ISO format check (contains 'T' and 'Z' or timezone)
            df = df.withColumn(
                "timestamp_iso_format_valid",
                when(
                    col("timestamp_iso").isNull(),
                    True  # Optional field, null is OK
                ).when(
                    (col("timestamp_iso").contains("T")) &
                    (length(col("timestamp_iso")) >= 19),  # Minimum ISO format length
                    True
                ).otherwise(False)
            )
        else:
            df = df.withColumn("timestamp_iso_format_valid", lit(True))
        
        logger.debug("Validated data formats")
        return df
    
    def _validate_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Validate business rules (e.g., event_type in allowed list)
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with validation column
        """
        # Allowed event types (from config or default)
        allowed_event_types = self.validation_rules.get(
            "allowed_event_types",
            ["video_play_start", "video_play_pause", "video_play_stop", "post_like", "add_to_cart", "page_view"]
        )
        
        if "event_type" in df.columns:
            df = df.withColumn(
                "business_rules_valid",
                when(
                    col("event_type").isin(allowed_event_types),
                    True
                ).otherwise(False)
            )
        else:
            df = df.withColumn("business_rules_valid", lit(True))
        
        # Validate timestamp is not too far in future (e.g., within 1 hour)
        if "timestamp_ms" in df.columns:
            from pyspark.sql.functions import current_timestamp, unix_timestamp
            
            # Current timestamp in milliseconds
            current_ms = unix_timestamp(current_timestamp()) * 1000
            
            # Allow up to 1 hour in future (3600000 ms)
            max_future_ms = 3600000
            
            df = df.withColumn(
                "timestamp_future_valid",
                when(
                    col("timestamp_ms").isNull(),
                    True
                ).when(
                    col("timestamp_ms") <= (current_ms + max_future_ms),
                    True
                ).otherwise(False)
            )
        else:
            df = df.withColumn("timestamp_future_valid", lit(True))
        
        logger.debug("Validated business rules")
        return df
    
    def _add_validation_status(self, df: DataFrame) -> DataFrame:
        """
        Add overall validation status column
        
        Args:
            df: Input DataFrame with individual validation columns
        
        Returns:
            DataFrame with validation_status column
        """
        # Combine all validation checks
        validation_columns = [
            "required_fields_valid",
            "timestamp_range_valid",
            "event_id_format_valid",
            "event_type_format_valid",
            "business_rules_valid",
            "timestamp_future_valid"
        ]
        
        # Check which columns exist
        existing_columns = [col_name for col_name in validation_columns if col_name in df.columns]
        
        if not existing_columns:
            # No validation columns, assume all valid
            df = df.withColumn("validation_status", lit("valid"))
            return df
        
        # Combine all validations (all must be True)
        validation_condition = col(existing_columns[0]) == True
        for col_name in existing_columns[1:]:
            validation_condition = validation_condition & (col(col_name) == True)
        
        # Set validation status
        df = df.withColumn(
            "validation_status",
            when(validation_condition, "valid")
            .when(col("required_fields_valid") == False, "invalid_required_fields")
            .when(col("timestamp_range_valid") == False, "invalid_timestamp_range")
            .when(col("event_id_format_valid") == False, "invalid_event_id_format")
            .when(col("event_type_format_valid") == False, "invalid_event_type_format")
            .when(col("business_rules_valid") == False, "invalid_business_rules")
            .when(col("timestamp_future_valid") == False, "invalid_timestamp_future")
            .otherwise("invalid_unknown")
        )
        
        # Add validation score (percentage of checks passed)
        total_checks = len(existing_columns)
        passed_checks = sum([when(col(c) == True, 1).otherwise(0) for c in existing_columns])
        df = df.withColumn(
            "validation_score",
            (passed_checks / total_checks) * 100
        )
        
        logger.debug("Added validation status")
        return df
    
    def get_validation_stats(self, df: DataFrame) -> Dict[str, Any]:
        """
        Get validation statistics
        
        Args:
            df: DataFrame with validation_status column
        
        Returns:
            Dictionary with validation statistics
        """
        try:
            if "validation_status" not in df.columns:
                logger.warning("validation_status column not found")
                return {"error": "validation_status column not found"}
            
            # Count by status
            stats = df.groupBy("validation_status").count().collect()
            
            result = {
                "total_records": df.count(),
                "by_status": {row["validation_status"]: row["count"] for row in stats},
                "valid_count": sum([row["count"] for row in stats if row["validation_status"] == "valid"]),
                "invalid_count": sum([row["count"] for row in stats if row["validation_status"] != "valid"])
            }
            
            if result["total_records"] > 0:
                result["valid_percentage"] = (result["valid_count"] / result["total_records"]) * 100
            else:
                result["valid_percentage"] = 0
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting validation stats: {str(e)}", exc_info=True)
            return {"error": str(e)}


# Convenience function
def validate_bronze_data(
    df: DataFrame,
    strict_mode: bool = True
) -> tuple[DataFrame, DataFrame]:
    """
    Validate bronze data (convenience function)
    
    Args:
        df: Input DataFrame
        strict_mode: If True, fail on validation errors
    
    Returns:
        Tuple of (valid_df, invalid_df)
    
    Example:
        valid_df, invalid_df = validate_bronze_data(df)
    """
    validator = BronzeValidator(strict_mode=strict_mode)
    return validator.validate(df)

