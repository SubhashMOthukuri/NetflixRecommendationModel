"""
Schema Enforcer - Production ML Pipeline
Enforces schema rules on data to ensure it matches bronze schema structure
Validates data types, required fields, and schema constraints
"""
from typing import Dict, Any, List, Optional, Union
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnan, isnull, lit
from libs.logger import get_logger
from libs.exceptions import PipelineException, SchemaValidationError
from schemas.bronze_schema.schema_loader import SchemaLoader, load_bronze_schema
from schemas.bronze_schema.bronze_data_contract import (
    get_required_fields,
    get_optional_fields
)

logger = get_logger(__name__)


class SchemaEnforcementError(PipelineException):
    """Raised when schema enforcement fails"""
    pass


class SchemaEnforcer:
    """
    Production-grade schema enforcer
    
    Enforces:
    - Required fields presence
    - Data type validation
    - Field constraints
    - Schema structure compliance
    
    Example:
        enforcer = SchemaEnforcer()
        validated_df = enforcer.enforce_schema(df)
        # DataFrame now matches bronze schema
    """
    
    def __init__(self, schema: Optional[Dict[str, Any]] = None):
        """
        Initialize schema enforcer
        
        Args:
            schema: Optional Avro schema dictionary (defaults to bronze schema)
        """
        if schema:
            self.schema = schema
        else:
            # Load bronze schema
            self.schema = load_bronze_schema()
        
        self.required_fields = get_required_fields()
        self.optional_fields = get_optional_fields()
        
        logger.info(f"Schema enforcer initialized with {len(self.required_fields)} required fields")
    
    def enforce_schema(
        self,
        df: DataFrame,
        strict: bool = True,
        add_missing_fields: bool = True,
        drop_extra_fields: bool = False
    ) -> DataFrame:
        """
        Enforce schema on Spark DataFrame
        
        Args:
            df: Input DataFrame
            strict: If True, fail on missing required fields
            add_missing_fields: If True, add missing optional fields as null
            drop_extra_fields: If True, drop fields not in schema
        
        Returns:
            DataFrame with enforced schema
        
        Raises:
            SchemaEnforcementError: If enforcement fails
        
        Example:
            enforcer = SchemaEnforcer()
            df = spark.readStream.format("kafka")...
            validated_df = enforcer.enforce_schema(df)
        """
        try:
            logger.info("Enforcing schema on DataFrame...")
            
            # Check required fields
            if strict:
                self._validate_required_fields(df)
            
            # Add missing optional fields
            if add_missing_fields:
                df = self._add_missing_fields(df)
            
            # Drop extra fields if requested
            if drop_extra_fields:
                df = self._drop_extra_fields(df)
            
            # Validate data types
            df = self._enforce_data_types(df)
            
            # Apply field constraints
            df = self._apply_constraints(df)
            
            logger.info("Schema enforcement completed successfully")
            return df
            
        except Exception as e:
            logger.error(f"Schema enforcement failed: {str(e)}", exc_info=True)
            raise SchemaEnforcementError(f"Cannot enforce schema: {str(e)}")
    
    def _validate_required_fields(self, df: DataFrame):
        """
        Validate that all required fields are present
        
        Args:
            df: DataFrame to validate
        
        Raises:
            SchemaEnforcementError: If required fields are missing
        """
        missing_fields = []
        
        for field in self.required_fields:
            # Handle nested fields (e.g., "processing_metadata.ingestion_timestamp_ms")
            field_parts = field.split(".")
            field_name = field_parts[0]
            
            if field_name not in df.columns:
                missing_fields.append(field)
        
        if missing_fields:
            error_msg = f"Missing required fields: {', '.join(missing_fields)}"
            logger.error(error_msg)
            raise SchemaEnforcementError(error_msg)
        
        logger.debug("All required fields present")
    
    def _add_missing_fields(self, df: DataFrame) -> DataFrame:
        """
        Add missing optional fields as null
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with missing fields added
        """
        for field in self.optional_fields:
            field_parts = field.split(".")
            field_name = field_parts[0]
            
            if field_name not in df.columns:
                logger.debug(f"Adding missing optional field: {field_name}")
                df = df.withColumn(field_name, lit(None).cast("string"))
        
        return df
    
    def _drop_extra_fields(self, df: DataFrame) -> DataFrame:
        """
        Drop fields not in schema
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with extra fields removed
        """
        # Get all fields from schema
        schema_fields = self._get_all_field_names(self.schema)
        
        # Get fields to keep (present in both DataFrame and schema)
        fields_to_keep = [col_name for col_name in df.columns if col_name in schema_fields]
        
        if len(fields_to_keep) < len(df.columns):
            dropped_fields = set(df.columns) - set(fields_to_keep)
            logger.info(f"Dropping extra fields: {dropped_fields}")
            df = df.select(*fields_to_keep)
        
        return df
    
    def _get_all_field_names(self, schema: Dict[str, Any], prefix: str = "") -> List[str]:
        """
        Recursively get all field names from schema
        
        Args:
            schema: Avro schema dictionary
            prefix: Field name prefix for nested fields
        
        Returns:
            List of field names
        """
        field_names = []
        
        if schema.get("type") == "record":
            for field in schema.get("fields", []):
                field_name = field["name"]
                full_name = f"{prefix}.{field_name}" if prefix else field_name
                field_names.append(full_name)
                
                # Handle nested records
                field_type = field.get("type")
                if isinstance(field_type, dict) and field_type.get("type") == "record":
                    nested_names = self._get_all_field_names(field_type, full_name)
                    field_names.extend(nested_names)
                elif isinstance(field_type, list):
                    # Union type - check for record
                    for union_type in field_type:
                        if isinstance(union_type, dict) and union_type.get("type") == "record":
                            nested_names = self._get_all_field_names(union_type, full_name)
                            field_names.extend(nested_names)
        
        return field_names
    
    def _enforce_data_types(self, df: DataFrame) -> DataFrame:
        """
        Enforce data types according to schema
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with enforced data types
        """
        # Map Avro types to Spark types
        type_mapping = {
            "string": "string",
            "long": "bigint",
            "int": "int",
            "double": "double",
            "float": "float",
            "boolean": "boolean",
            "null": "string"  # Nullable fields
        }
        
        # Get field types from schema
        field_types = self._get_field_types(self.schema)
        
        for field_name, expected_type in field_types.items():
            if field_name in df.columns:
                try:
                    # Cast to expected type
                    spark_type = type_mapping.get(expected_type, "string")
                    df = df.withColumn(field_name, col(field_name).cast(spark_type))
                    logger.debug(f"Casted {field_name} to {spark_type}")
                except Exception as e:
                    logger.warning(f"Could not cast {field_name} to {expected_type}: {str(e)}")
        
        return df
    
    def _get_field_types(self, schema: Dict[str, Any], prefix: str = "") -> Dict[str, str]:
        """
        Get field types from schema
        
        Args:
            schema: Avro schema dictionary
            prefix: Field name prefix
        
        Returns:
            Dictionary of field_name -> type
        """
        field_types = {}
        
        if schema.get("type") == "record":
            for field in schema.get("fields", []):
                field_name = field["name"]
                full_name = f"{prefix}.{field_name}" if prefix else field_name
                
                field_type = field.get("type")
                
                # Handle union types (nullable fields)
                if isinstance(field_type, list):
                    # Get non-null type
                    non_null_type = [t for t in field_type if t != "null"]
                    if non_null_type:
                        field_types[full_name] = non_null_type[0]
                else:
                    field_types[full_name] = field_type
                
                # Handle nested records
                if isinstance(field_type, dict) and field_type.get("type") == "record":
                    nested_types = self._get_field_types(field_type, full_name)
                    field_types.update(nested_types)
        
        return field_types
    
    def _apply_constraints(self, df: DataFrame) -> DataFrame:
        """
        Apply schema constraints (e.g., non-null, ranges)
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with constraints applied
        """
        # Example: Ensure event_id is not null
        if "event_id" in df.columns:
            df = df.filter(col("event_id").isNotNull())
        
        # Example: Ensure timestamp_ms is positive
        if "timestamp_ms" in df.columns:
            df = df.filter(col("timestamp_ms") > 0)
        
        # Example: Ensure event_type is not empty
        if "event_type" in df.columns:
            df = df.filter(col("event_type") != "")
        
        logger.debug("Applied schema constraints")
        return df
    
    def validate_record(self, record: Dict[str, Any]) -> tuple[bool, List[str]]:
        """
        Validate a single record against schema
        
        Args:
            record: Record dictionary
        
        Returns:
            Tuple of (is_valid, list_of_errors)
        
        Example:
            enforcer = SchemaEnforcer()
            is_valid, errors = enforcer.validate_record({"event_id": "123", ...})
        """
        errors = []
        
        # Check required fields
        for field in self.required_fields:
            field_parts = field.split(".")
            field_name = field_parts[0]
            
            if field_name not in record or record[field_name] is None:
                errors.append(f"Missing required field: {field_name}")
        
        # Check data types (basic validation)
        if "event_id" in record and not isinstance(record["event_id"], str):
            errors.append("event_id must be a string")
        
        if "timestamp_ms" in record:
            if not isinstance(record["timestamp_ms"], (int, float)):
                errors.append("timestamp_ms must be a number")
            elif record["timestamp_ms"] <= 0:
                errors.append("timestamp_ms must be positive")
        
        is_valid = len(errors) == 0
        return is_valid, errors


# Convenience function
def enforce_bronze_schema(
    df: DataFrame,
    strict: bool = True,
    add_missing_fields: bool = True
) -> DataFrame:
    """
    Enforce bronze schema on DataFrame (convenience function)
    
    Args:
        df: Input DataFrame
        strict: If True, fail on missing required fields
        add_missing_fields: If True, add missing optional fields
    
    Returns:
        DataFrame with enforced schema
    
    Example:
        df = spark.readStream.format("kafka")...
        validated_df = enforce_bronze_schema(df)
    """
    enforcer = SchemaEnforcer()
    return enforcer.enforce_schema(df, strict=strict, add_missing_fields=add_missing_fields)

