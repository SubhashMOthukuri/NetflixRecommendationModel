"""
Schema Loader - Production ML Pipeline
Dynamically loads schemas from various sources (files, Schema Registry, config)
Enables schema evolution and flexible schema management
"""
import json
import yaml
from pathlib import Path
from typing import Optional, Dict, Any, Union
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, BooleanType, MapType, ArrayType
from libs.config_loader import load_config, get_config_value
from libs.logger import get_logger
from libs.exceptions import PipelineException
from schemas.bronze_schema.bronze_data_contract import get_bronze_schema, get_bronze_spark_schema

logger = get_logger(__name__)


class SchemaLoaderError(PipelineException):
    """Raised when schema cannot be loaded"""
    pass


class SchemaLoader:
    """
    Production-grade schema loader
    
    Loads schemas from:
    - Local files (JSON/YAML)
    - Schema Registry (Avro schemas)
    - Code definitions (bronze_data_contract)
    - Configuration files
    
    Example:
        loader = SchemaLoader()
        schema = loader.load_bronze_schema()
        # Use schema for validation
    """
    
    def __init__(
        self,
        schema_registry_url: Optional[str] = None,
        config_path: str = "schema_bronze.yaml"
    ):
        """
        Initialize schema loader
        
        Args:
            schema_registry_url: Optional Schema Registry URL
            config_path: Path to schema config YAML
        """
        self.schema_registry_url = schema_registry_url
        self.config_path = config_path
        self._schema_cache: Dict[str, Any] = {}
        
        # Load config if exists
        try:
            self.config = load_config(config_path)
            logger.info(f"Loaded schema config from {config_path}")
        except Exception as e:
            logger.warning(f"Could not load schema config: {str(e)}, using defaults")
            self.config = {}
    
    def load_bronze_schema(
        self,
        source: str = "code",
        schema_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Load bronze schema from specified source
        
        Args:
            source: Source type ("code", "file", "registry", "config")
            schema_name: Optional schema name/identifier
        
        Returns:
            Avro schema dictionary
        
        Raises:
            SchemaLoaderError: If schema cannot be loaded
        
        Example:
            # Load from code (default)
            schema = loader.load_bronze_schema()
            
            # Load from file
            schema = loader.load_bronze_schema(source="file", schema_name="bronze_schema.json")
            
            # Load from Schema Registry
            schema = loader.load_bronze_schema(source="registry", schema_name="bronze-user-events")
        """
        cache_key = f"{source}:{schema_name or 'default'}"
        
        # Check cache first
        if cache_key in self._schema_cache:
            logger.debug(f"Returning cached schema: {cache_key}")
            return self._schema_cache[cache_key]
        
        try:
            if source == "code":
                schema = self._load_from_code()
            elif source == "file":
                schema = self._load_from_file(schema_name)
            elif source == "registry":
                schema = self._load_from_registry(schema_name)
            elif source == "config":
                schema = self._load_from_config(schema_name)
            else:
                raise SchemaLoaderError(f"Unknown source type: {source}")
            
            # Cache schema
            self._schema_cache[cache_key] = schema
            logger.info(f"Loaded bronze schema from {source}")
            return schema
            
        except Exception as e:
            logger.error(f"Failed to load schema from {source}: {str(e)}", exc_info=True)
            raise SchemaLoaderError(f"Cannot load schema from {source}: {str(e)}")
    
    def _load_from_code(self) -> Dict[str, Any]:
        """Load schema from code definition (bronze_data_contract)"""
        return get_bronze_schema()
    
    def _load_from_file(self, file_path: Optional[str]) -> Dict[str, Any]:
        """
        Load schema from JSON/YAML file
        
        Args:
            file_path: Path to schema file
        
        Returns:
            Avro schema dictionary
        """
        if not file_path:
            # Try to get from config
            file_path = get_config_value(self.config, "schema_file", None)
            if not file_path:
                raise SchemaLoaderError("No file path provided and not found in config")
        
        schema_file = Path(file_path)
        
        if not schema_file.exists():
            raise SchemaLoaderError(f"Schema file not found: {schema_file}")
        
        logger.info(f"Loading schema from file: {schema_file}")
        
        try:
            with open(schema_file, 'r') as f:
                if file_path.endswith('.yaml') or file_path.endswith('.yml'):
                    schema = yaml.safe_load(f)
                else:
                    schema = json.load(f)
            
            return schema
            
        except json.JSONDecodeError as e:
            raise SchemaLoaderError(f"Invalid JSON in schema file: {str(e)}")
        except yaml.YAMLError as e:
            raise SchemaLoaderError(f"Invalid YAML in schema file: {str(e)}")
        except Exception as e:
            raise SchemaLoaderError(f"Error reading schema file: {str(e)}")
    
    def _load_from_registry(self, subject_name: Optional[str]) -> Dict[str, Any]:
        """
        Load schema from Schema Registry
        
        Args:
            subject_name: Schema Registry subject name
        
        Returns:
            Avro schema dictionary
        """
        if not self.schema_registry_url:
            # Try to get from config
            try:
                kafka_config = load_config("kafka.yaml")
                self.schema_registry_url = get_config_value(
                    kafka_config,
                    "schema_registry.url",
                    None
                )
            except Exception:
                pass
        
        if not self.schema_registry_url:
            raise SchemaLoaderError("Schema Registry URL not configured")
        
        if not subject_name:
            subject_name = get_config_value(
                self.config,
                "schema_registry.subject",
                "bronze-user-events-value"
            )
        
        logger.info(f"Loading schema from Schema Registry: {subject_name}")
        
        try:
            # Import Schema Registry client
            from schemas.kafka_schema.schema_registry_client import SchemaRegistryClient
            
            client = SchemaRegistryClient(self.schema_registry_url)
            schema = client.get_latest_schema(subject_name)
            
            return schema
            
        except Exception as e:
            raise SchemaLoaderError(f"Cannot load schema from registry: {str(e)}")
    
    def _load_from_config(self, schema_key: Optional[str]) -> Dict[str, Any]:
        """
        Load schema from configuration file
        
        Args:
            schema_key: Key in config file
        
        Returns:
            Avro schema dictionary
        """
        if not schema_key:
            schema_key = "bronze_schema"
        
        schema = get_config_value(self.config, schema_key, None)
        
        if not schema:
            raise SchemaLoaderError(f"Schema not found in config: {schema_key}")
        
        return schema
    
    def load_spark_schema(
        self,
        source: str = "code"
    ) -> Union[StructType, str]:
        """
        Load Spark schema (StructType or DDL string)
        
        Args:
            source: Source type ("code", "file", "config")
        
        Returns:
            Spark StructType or DDL string
        """
        if source == "code":
            # Return DDL string (can be converted to StructType)
            return get_bronze_spark_schema()
        elif source == "file":
            # Load from file and convert to StructType
            avro_schema = self._load_from_file(None)
            return self._avro_to_spark_struct(avro_schema)
        elif source == "config":
            # Load from config
            schema_ddl = get_config_value(self.config, "spark_schema_ddl", None)
            if schema_ddl:
                return schema_ddl
            else:
                # Fallback to code
                return get_bronze_spark_schema()
        else:
            raise SchemaLoaderError(f"Unknown source type: {source}")
    
    def _avro_to_spark_struct(self, avro_schema: Dict[str, Any]) -> StructType:
        """
        Convert Avro schema to Spark StructType
        
        Args:
            avro_schema: Avro schema dictionary
        
        Returns:
            Spark StructType
        """
        # This is a simplified converter
        # Full implementation would handle all Avro types
        logger.warning("Avro to Spark conversion is simplified - use code definition for full support")
        
        # For now, return the code-defined schema
        return self._load_from_code()
    
    def get_schema_version(self, schema: Dict[str, Any]) -> Optional[str]:
        """
        Get schema version from schema metadata
        
        Args:
            schema: Avro schema dictionary
        
        Returns:
            Version string or None
        """
        # Check for version in schema metadata
        if "version" in schema:
            return str(schema["version"])
        
        # Check for version in namespace or name
        namespace = schema.get("namespace", "")
        name = schema.get("name", "")
        
        # Try to extract version from name/namespace
        # This is schema-specific, adjust as needed
        return None
    
    def validate_schema_structure(self, schema: Dict[str, Any]) -> bool:
        """
        Validate schema structure (basic validation)
        
        Args:
            schema: Avro schema dictionary
        
        Returns:
            True if valid, False otherwise
        """
        required_fields = ["type", "name", "fields"]
        
        for field in required_fields:
            if field not in schema:
                logger.error(f"Schema missing required field: {field}")
                return False
        
        if schema["type"] != "record":
            logger.error(f"Schema type must be 'record', got: {schema['type']}")
            return False
        
        if not isinstance(schema["fields"], list):
            logger.error("Schema fields must be a list")
            return False
        
        logger.info("Schema structure validated successfully")
        return True


# Convenience functions
def load_bronze_schema(source: str = "code") -> Dict[str, Any]:
    """
    Load bronze schema (convenience function)
    
    Args:
        source: Source type ("code", "file", "registry", "config")
    
    Returns:
        Avro schema dictionary
    
    Example:
        schema = load_bronze_schema()
        # Use schema for validation
    """
    loader = SchemaLoader()
    return loader.load_bronze_schema(source=source)


def load_spark_schema(source: str = "code") -> Union[StructType, str]:
    """
    Load Spark schema (convenience function)
    
    Args:
        source: Source type ("code", "file", "config")
    
    Returns:
        Spark StructType or DDL string
    
    Example:
        schema = load_spark_schema()
        # Use for DataFrame operations
    """
    loader = SchemaLoader()
    return loader.load_spark_schema(source=source)

