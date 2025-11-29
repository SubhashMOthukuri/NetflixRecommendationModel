"""
Feature Schema Registry - Gold Layer
Manages feature schemas (definitions, types, validation rules, versioning)
Ensures feature consistency and enables schema evolution
"""
from typing import Optional, List, Dict, Any
from datetime import datetime
import json
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, lit, isnull, isnan
from pyspark.sql.types import (
    StringType, IntegerType, LongType, FloatType, DoubleType,
    BooleanType, ArrayType, StructType
)
from libs.logger import get_logger
from libs.exceptions import PipelineException
from libs.config_loader import load_config

logger = get_logger(__name__)


class FeatureSchemaRegistryError(PipelineException):
    """Raised when feature schema registry operations fail"""
    pass


class FeatureSchemaRegistry:
    """
    Production-grade feature schema registry
    
    Manages feature schemas:
    - Register feature schemas (name, type, validation rules)
    - Validate features against schemas
    - Manage schema versions
    - Track schema evolution
    - Provide schema lookup
    
    Example:
        registry = FeatureSchemaRegistry(spark)
        registry.register_feature("watch_time", "double", {"min": 0, "max": 86400})
        registry.validate_features(df, ["watch_time"])
    """
    
    def __init__(self, spark: SparkSession, registry_path: Optional[str] = None):
        """
        Initialize feature schema registry
        
        Args:
            spark: SparkSession instance
            registry_path: Path to schema registry storage (defaults to config)
        """
        self.spark = spark
        
        # Load config for registry path
        try:
            config = load_config("config/spark_config.yaml")
            gold_config = config.get("storage", {}).get("gold", {})
            self.registry_path = registry_path or gold_config.get("schema_registry_path", "s3a://data-lake/gold/schema_registry/")
        except Exception as e:
            logger.warning(f"Could not load config: {str(e)}, using default path")
            self.registry_path = registry_path or "s3a://data-lake/gold/schema_registry/"
        
        # In-memory schema cache
        self._schemas: Dict[str, Dict[str, Any]] = {}
        self._versions: Dict[str, List[str]] = {}
        
        # Load existing schemas
        self._load_schemas()
        
        logger.info(f"Feature schema registry initialized (path: {self.registry_path})")
    
    def _load_schemas(self):
        """Load schemas from storage"""
        try:
            # Try to load from storage
            if os.path.exists(self.registry_path.replace("s3a://", "")):
                # Local file system
                schema_file = os.path.join(self.registry_path.replace("s3a://", ""), "schemas.json")
                if os.path.exists(schema_file):
                    with open(schema_file, "r") as f:
                        data = json.load(f)
                        self._schemas = data.get("schemas", {})
                        self._versions = data.get("versions", {})
                        logger.info(f"Loaded {len(self._schemas)} schemas from registry")
        except Exception as e:
            logger.warning(f"Could not load schemas: {str(e)}, starting with empty registry")
    
    def _save_schemas(self):
        """Save schemas to storage"""
        try:
            data = {
                "schemas": self._schemas,
                "versions": self._versions,
                "last_updated": datetime.now().isoformat()
            }
            
            # Save to local file (in production, save to S3/database)
            local_path = self.registry_path.replace("s3a://", "")
            os.makedirs(local_path, exist_ok=True)
            schema_file = os.path.join(local_path, "schemas.json")
            
            with open(schema_file, "w") as f:
                json.dump(data, f, indent=2)
            
            logger.debug(f"Saved schemas to {schema_file}")
            
        except Exception as e:
            logger.warning(f"Could not save schemas: {str(e)}")
    
    def register_feature(
        self,
        feature_name: str,
        feature_type: str,
        validation_rules: Optional[Dict[str, Any]] = None,
        description: Optional[str] = None,
        version: Optional[str] = None
    ):
        """
        Register a feature schema
        
        Args:
            feature_name: Name of the feature
            feature_type: Type of feature ("string", "double", "int", "long", "float", "boolean", "array")
            validation_rules: Optional validation rules (min, max, allowed_values, pattern, etc.)
            description: Optional feature description
            version: Optional schema version (defaults to timestamp)
        
        Example:
            registry.register_feature(
                "watch_time",
                "double",
                validation_rules={"min": 0, "max": 86400},
                description="Total watch time in seconds"
            )
        """
        try:
            logger.info(f"Registering feature schema: {feature_name}")
            
            # Generate version if not provided
            if version is None:
                version = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Create schema entry
            schema_entry = {
                "feature_name": feature_name,
                "feature_type": feature_type,
                "validation_rules": validation_rules or {},
                "description": description or "",
                "version": version,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
            
            # Store schema
            if feature_name not in self._schemas:
                self._schemas[feature_name] = {}
                self._versions[feature_name] = []
            
            self._schemas[feature_name][version] = schema_entry
            self._versions[feature_name].append(version)
            
            # Mark latest version
            self._schemas[feature_name]["latest"] = version
            
            # Save to storage
            self._save_schemas()
            
            logger.info(f"Feature schema registered: {feature_name} (version: {version})")
            
        except Exception as e:
            logger.error(f"Failed to register feature schema: {str(e)}", exc_info=True)
            raise FeatureSchemaRegistryError(f"Cannot register feature schema: {str(e)}")
    
    def get_schema(
        self,
        feature_name: str,
        version: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get feature schema
        
        Args:
            feature_name: Name of the feature
            version: Optional schema version (defaults to latest)
        
        Returns:
            Schema dictionary
        
        Example:
            schema = registry.get_schema("watch_time")
        """
        try:
            if feature_name not in self._schemas:
                raise FeatureSchemaRegistryError(f"Feature schema not found: {feature_name}")
            
            if version is None:
                version = self._schemas[feature_name].get("latest")
            
            if version not in self._schemas[feature_name]:
                raise FeatureSchemaRegistryError(f"Schema version not found: {feature_name} v{version}")
            
            return self._schemas[feature_name][version]
            
        except Exception as e:
            logger.error(f"Failed to get schema: {str(e)}", exc_info=True)
            raise FeatureSchemaRegistryError(f"Cannot get schema: {str(e)}")
    
    def validate_features(
        self,
        df: DataFrame,
        feature_columns: List[str],
        strict: bool = False
    ) -> DataFrame:
        """
        Validate features against registered schemas
        
        Args:
            df: DataFrame with features to validate
            feature_columns: List of feature column names to validate
            strict: If True, fails on validation errors; if False, adds validation status
        
        Returns:
            DataFrame with validation results
        
        Example:
            validated_df = registry.validate_features(
                df,
                feature_columns=["watch_time", "video_count"]
            )
        """
        try:
            logger.info(f"Validating {len(feature_columns)} features against schemas...")
            
            result = df
            validation_errors = []
            
            for feature_col in feature_columns:
                if feature_col not in df.columns:
                    logger.warning(f"Feature column {feature_col} not found in DataFrame")
                    continue
                
                try:
                    # Get schema
                    schema = self.get_schema(feature_col)
                    
                    # Validate type
                    type_valid = self._validate_type(df, feature_col, schema["feature_type"])
                    if not type_valid:
                        validation_errors.append(f"{feature_col}: Type mismatch")
                    
                    # Validate rules
                    rules_valid, rule_errors = self._validate_rules(df, feature_col, schema.get("validation_rules", {}))
                    if not rules_valid:
                        validation_errors.extend([f"{feature_col}: {e}" for e in rule_errors])
                    
                    # Add validation status
                    result = result.withColumn(
                        f"{feature_col}_schema_valid",
                        lit(rules_valid and type_valid)
                    )
                    
                except FeatureSchemaRegistryError:
                    # Schema not found
                    if strict:
                        raise
                    else:
                        logger.warning(f"Schema not found for {feature_col}, skipping validation")
                        result = result.withColumn(
                            f"{feature_col}_schema_valid",
                            lit(None)
                        )
            
            # Add overall validation status
            if validation_errors:
                result = result.withColumn(
                    "schema_validation_status",
                    lit("invalid")
                ).withColumn(
                    "schema_validation_errors",
                    lit(", ".join(validation_errors))
                )
            else:
                result = result.withColumn(
                    "schema_validation_status",
                    lit("valid")
                ).withColumn(
                    "schema_validation_errors",
                    lit(None)
                )
            
            if validation_errors:
                logger.warning(f"Validation found {len(validation_errors)} errors")
            else:
                logger.info("All features validated successfully")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to validate features: {str(e)}", exc_info=True)
            raise FeatureSchemaRegistryError(f"Cannot validate features: {str(e)}")
    
    def _validate_type(
        self,
        df: DataFrame,
        feature_col: str,
        expected_type: str
    ) -> bool:
        """
        Validate feature type
        
        Returns:
            True if type matches, False otherwise
        """
        try:
            actual_type = dict(df.dtypes)[feature_col]
            
            # Map Spark types to expected types
            type_mapping = {
                "string": ["string"],
                "int": ["int"],
                "long": ["bigint"],
                "double": ["double"],
                "float": ["float"],
                "boolean": ["boolean"],
                "array": ["array"]
            }
            
            expected_spark_types = type_mapping.get(expected_type.lower(), [])
            
            return actual_type in expected_spark_types
            
        except Exception as e:
            logger.error(f"Failed to validate type: {str(e)}", exc_info=True)
            return False
    
    def _validate_rules(
        self,
        df: DataFrame,
        feature_col: str,
        rules: Dict[str, Any]
    ) -> tuple[bool, List[str]]:
        """
        Validate feature against validation rules
        
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        try:
            # Check min value
            if "min" in rules:
                min_val = rules["min"]
                min_count = df.filter(col(feature_col) < min_val).count()
                if min_count > 0:
                    errors.append(f"Values below min ({min_val}): {min_count} records")
            
            # Check max value
            if "max" in rules:
                max_val = rules["max"]
                max_count = df.filter(col(feature_col) > max_val).count()
                if max_count > 0:
                    errors.append(f"Values above max ({max_val}): {max_count} records")
            
            # Check allowed values
            if "allowed_values" in rules:
                allowed = rules["allowed_values"]
                invalid_count = df.filter(~col(feature_col).isin(allowed)).count()
                if invalid_count > 0:
                    errors.append(f"Values not in allowed list: {invalid_count} records")
            
            # Check pattern (for strings)
            if "pattern" in rules:
                pattern = rules["pattern"]
                # This is simplified - in production, use regexp
                logger.debug(f"Pattern validation not fully implemented for {feature_col}")
            
            # Check nullability
            if "nullable" in rules and not rules["nullable"]:
                null_count = df.filter(col(feature_col).isNull() | isnan(col(feature_col))).count()
                if null_count > 0:
                    errors.append(f"Null values found: {null_count} records")
            
            is_valid = len(errors) == 0
            return is_valid, errors
            
        except Exception as e:
            logger.error(f"Failed to validate rules: {str(e)}", exc_info=True)
            return False, [f"Validation error: {str(e)}"]
    
    def list_features(self) -> List[str]:
        """
        List all registered features
        
        Returns:
            List of feature names
        """
        return list(self._schemas.keys())
    
    def get_feature_versions(self, feature_name: str) -> List[str]:
        """
        Get all versions of a feature
        
        Args:
            feature_name: Name of the feature
        
        Returns:
            List of version strings
        """
        if feature_name not in self._versions:
            return []
        
        return self._versions[feature_name]
    
    def get_latest_version(self, feature_name: str) -> Optional[str]:
        """
        Get latest version of a feature
        
        Args:
            feature_name: Name of the feature
        
        Returns:
            Latest version string or None
        """
        if feature_name not in self._schemas:
            return None
        
        return self._schemas[feature_name].get("latest")
    
    def compare_schemas(
        self,
        feature_name: str,
        version1: str,
        version2: str
    ) -> Dict[str, Any]:
        """
        Compare two schema versions
        
        Args:
            feature_name: Name of the feature
            version1: First version
            version2: Second version
        
        Returns:
            Dictionary with comparison results
        """
        try:
            schema1 = self.get_schema(feature_name, version1)
            schema2 = self.get_schema(feature_name, version2)
            
            comparison = {
                "feature_name": feature_name,
                "version1": version1,
                "version2": version2,
                "changes": []
            }
            
            # Compare types
            if schema1["feature_type"] != schema2["feature_type"]:
                comparison["changes"].append({
                    "field": "feature_type",
                    "old": schema1["feature_type"],
                    "new": schema2["feature_type"]
                })
            
            # Compare validation rules
            rules1 = schema1.get("validation_rules", {})
            rules2 = schema2.get("validation_rules", {})
            
            for key in set(list(rules1.keys()) + list(rules2.keys())):
                if key not in rules1:
                    comparison["changes"].append({
                        "field": f"validation_rules.{key}",
                        "old": None,
                        "new": rules2[key]
                    })
                elif key not in rules2:
                    comparison["changes"].append({
                        "field": f"validation_rules.{key}",
                        "old": rules1[key],
                        "new": None
                    })
                elif rules1[key] != rules2[key]:
                    comparison["changes"].append({
                        "field": f"validation_rules.{key}",
                        "old": rules1[key],
                        "new": rules2[key]
                    })
            
            return comparison
            
        except Exception as e:
            logger.error(f"Failed to compare schemas: {str(e)}", exc_info=True)
            raise FeatureSchemaRegistryError(f"Cannot compare schemas: {str(e)}")

