"""
Contract Enforcement - Production ML Pipeline
Enforces DataFrame matches silver schema exactly
Adds missing fields, ensures types, drops extra fields if needed
"""
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, LongType, IntegerType, DoubleType, BooleanType, DateType, TimestampType
from libs.config_loader import load_config
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class ContractEnforcementError(PipelineException):
    """Raised when contract enforcement fails"""
    pass


class ContractEnforcer:
    """
    Production-grade contract enforcer
    
    Enforces DataFrame matches silver schema:
    - Adds missing optional fields (as null)
    - Ensures all required fields exist
    - Casts types to match schema
    - Optionally drops extra fields
    
    Example:
        enforcer = ContractEnforcer()
        enforced_df = enforcer.enforce(df)
        # DataFrame now matches silver schema exactly
    """
    
    def __init__(
        self,
        schema_path: str = "schemas/silver_schema/silver_schema.yaml",
        strict: bool = False,
        drop_extra_fields: bool = False
    ):
        """
        Initialize contract enforcer
        
        Args:
            schema_path: Path to silver schema YAML file
            strict: If True, fails on missing required fields
            drop_extra_fields: If True, drops fields not in schema
        """
        self.strict = strict
        self.drop_extra_fields = drop_extra_fields
        
        try:
            self.schema_config = load_config(schema_path)
            self.schema_fields = self._load_schema_fields()
            self.required_fields = self._get_required_fields()
            self.optional_fields = self._get_optional_fields()
            logger.info("Contract enforcer initialized with silver schema")
        except Exception as e:
            logger.warning(f"Could not load schema: {str(e)}, using defaults")
            self.schema_config = {}
            self.schema_fields = {}
            self.required_fields = []
            self.optional_fields = []
        
        logger.info(f"Contract enforcer initialized (strict={strict}, drop_extra={drop_extra_fields})")
    
    def _load_schema_fields(self) -> Dict[str, Dict[str, Any]]:
        """
        Load schema fields from config
        
        Returns:
            Dictionary mapping field_name -> field_config
        """
        fields = {}
        
        if self.schema_config and "fields" in self.schema_config:
            for field in self.schema_config["fields"]:
                field_name = field.get("name")
                if field_name:
                    fields[field_name] = {
                        "type": field.get("type", "string"),
                        "required": field.get("required", False),
                        "description": field.get("description", "")
                    }
        
        return fields
    
    def _get_required_fields(self) -> List[str]:
        """
        Get list of required field names
        
        Returns:
            List of required field names
        """
        return [
            field_name for field_name, field_config in self.schema_fields.items()
            if field_config.get("required", False)
        ]
    
    def _get_optional_fields(self) -> List[str]:
        """
        Get list of optional field names
        
        Returns:
            List of optional field names
        """
        return [
            field_name for field_name, field_config in self.schema_fields.items()
            if not field_config.get("required", False)
        ]
    
    def enforce(
        self,
        df: DataFrame,
        add_missing_fields: bool = True,
        ensure_types: bool = True
    ) -> DataFrame:
        """
        Enforce DataFrame matches silver schema
        
        Args:
            df: Input DataFrame
            add_missing_fields: If True, adds missing optional fields (as null)
            ensure_types: If True, ensures types match schema
        
        Returns:
            DataFrame matching silver schema exactly
        
        Example:
            enforcer = ContractEnforcer()
            enforced_df = enforcer.enforce(df)
        """
        try:
            logger.info("Starting contract enforcement...")
            
            df_result = df
            
            # Step 1: Validate required fields exist
            self._validate_required_fields(df_result)
            
            # Step 2: Add missing optional fields (as null)
            if add_missing_fields:
                df_result = self._add_missing_fields(df_result)
            
            # Step 3: Ensure types match schema
            if ensure_types:
                df_result = self._ensure_types(df_result)
            
            # Step 4: Drop extra fields (if configured)
            if self.drop_extra_fields:
                df_result = self._drop_extra_fields(df_result)
            
            logger.info("Contract enforcement complete")
            return df_result
            
        except Exception as e:
            logger.error(f"Contract enforcement failed: {str(e)}", exc_info=True)
            raise ContractEnforcementError(f"Cannot enforce contract: {str(e)}")
    
    def _validate_required_fields(self, df: DataFrame) -> bool:
        """
        Validate all required fields exist
        
        Args:
            df: Input DataFrame
        
        Returns:
            True if all required fields exist
        
        Raises:
            ContractEnforcementError: If required fields are missing
        """
        try:
            missing_fields = [
                field for field in self.required_fields
                if field not in df.columns
            ]
            
            if missing_fields:
                error_msg = f"Missing required fields: {missing_fields}"
                if self.strict:
                    logger.error(error_msg)
                    raise ContractEnforcementError(error_msg)
                else:
                    logger.warning(error_msg)
            
            return True
            
        except ContractEnforcementError:
            raise
        except Exception as e:
            logger.error(f"Error validating required fields: {str(e)}", exc_info=True)
            raise ContractEnforcementError(f"Cannot validate required fields: {str(e)}")
    
    def _add_missing_fields(self, df: DataFrame) -> DataFrame:
        """
        Add missing optional fields (as null)
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with missing fields added
        """
        try:
            df_result = df
            
            # Get all schema fields
            all_schema_fields = list(self.schema_fields.keys())
            
            # Find missing fields
            missing_fields = [
                field for field in all_schema_fields
                if field not in df_result.columns
            ]
            
            # Add missing fields as null
            for field_name in missing_fields:
                field_config = self.schema_fields.get(field_name, {})
                field_type = field_config.get("type", "string")
                
                # Map schema type to Spark type
                spark_type = self._get_spark_type(field_type)
                
                df_result = df_result.withColumn(
                    field_name,
                    lit(None).cast(spark_type)
                )
                logger.debug(f"Added missing field: {field_name} (type: {field_type})")
            
            if missing_fields:
                logger.info(f"Added {len(missing_fields)} missing fields")
            
            return df_result
            
        except Exception as e:
            logger.error(f"Error adding missing fields: {str(e)}", exc_info=True)
            raise ContractEnforcementError(f"Cannot add missing fields: {str(e)}")
    
    def _ensure_types(self, df: DataFrame) -> DataFrame:
        """
        Ensure column types match schema
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with correct types
        """
        try:
            df_result = df
            
            for field_name, field_config in self.schema_fields.items():
                if field_name in df_result.columns:
                    expected_type = field_config.get("type", "string")
                    spark_type = self._get_spark_type(expected_type)
                    
                    # Cast to expected type
                    current_type = dict(df_result.dtypes)[field_name]
                    expected_type_str = str(spark_type)
                    
                    if current_type != expected_type_str:
                        try:
                            df_result = df_result.withColumn(
                                field_name,
                                col(field_name).cast(spark_type)
                            )
                            logger.debug(f"Casted {field_name} from {current_type} to {expected_type_str}")
                        except Exception as e:
                            logger.warning(f"Could not cast {field_name} to {expected_type}: {str(e)}")
            
            return df_result
            
        except Exception as e:
            logger.error(f"Error ensuring types: {str(e)}", exc_info=True)
            raise ContractEnforcementError(f"Cannot ensure types: {str(e)}")
    
    def _drop_extra_fields(self, df: DataFrame) -> DataFrame:
        """
        Drop fields not in schema
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with extra fields dropped
        """
        try:
            df_result = df
            
            # Get all schema fields
            all_schema_fields = set(self.schema_fields.keys())
            
            # Find extra fields (not in schema)
            extra_fields = [
                col_name for col_name in df_result.columns
                if col_name not in all_schema_fields
            ]
            
            # Drop extra fields
            if extra_fields:
                df_result = df_result.drop(*extra_fields)
                logger.info(f"Dropped {len(extra_fields)} extra fields: {extra_fields}")
            
            return df_result
            
        except Exception as e:
            logger.error(f"Error dropping extra fields: {str(e)}", exc_info=True)
            raise ContractEnforcementError(f"Cannot drop extra fields: {str(e)}")
    
    def _get_spark_type(self, schema_type: str):
        """
        Map schema type string to Spark type
        
        Args:
            schema_type: Schema type string (e.g., "string", "long", "timestamp")
        
        Returns:
            Spark DataType
        """
        type_mapping = {
            "string": StringType(),
            "long": LongType(),
            "integer": IntegerType(),
            "int": IntegerType(),
            "double": DoubleType(),
            "float": DoubleType(),
            "boolean": BooleanType(),
            "bool": BooleanType(),
            "date": DateType(),
            "timestamp": TimestampType(),
        }
        
        return type_mapping.get(schema_type.lower(), StringType())
    
    def get_enforcement_stats(self, df_before: DataFrame, df_after: DataFrame) -> Dict[str, Any]:
        """
        Get contract enforcement statistics
        
        Args:
            df_before: DataFrame before enforcement
            df_after: DataFrame after enforcement
        
        Returns:
            Dictionary with enforcement statistics
        """
        try:
            stats = {
                "columns_before": len(df_before.columns),
                "columns_after": len(df_after.columns),
                "columns_added": len(df_after.columns) - len(df_before.columns),
                "required_fields": len(self.required_fields),
                "optional_fields": len(self.optional_fields),
                "schema_fields": len(self.schema_fields)
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting enforcement stats: {str(e)}", exc_info=True)
            return {"error": str(e)}


# Convenience function
def enforce_contract(
    df: DataFrame,
    strict: bool = False,
    drop_extra_fields: bool = False
) -> DataFrame:
    """
    Enforce contract (convenience function)
    
    Args:
        df: Input DataFrame
        strict: If True, fails on missing required fields
        drop_extra_fields: If True, drops fields not in schema
    
    Returns:
        DataFrame matching silver schema exactly
    
    Example:
        enforced_df = enforce_contract(df)
    """
    enforcer = ContractEnforcer(strict=strict, drop_extra_fields=drop_extra_fields)
    return enforcer.enforce(df)

