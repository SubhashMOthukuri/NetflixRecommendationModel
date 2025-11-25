"""
Spark Session Manager - Production ML Pipeline
Creates and manages SparkSession with production-grade settings
Used by all Spark jobs for consistent, optimized Spark sessions
"""
import sys
from pathlib import Path
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
from libs.config_loader import load_config, get_config_value
from libs.logger import get_logger
from libs.exceptions import PipelineException

# Add project root to path for imports
project_root = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

logger = get_logger(__name__)


class SparkSessionError(PipelineException):
    """Raised when SparkSession cannot be created"""
    pass


class SparkSessionManager:
    """
    Production-grade SparkSession manager
    
    Creates SparkSession with:
    - Configuration from YAML files
    - Production optimizations (AQE, Kryo serialization)
    - Proper resource allocation
    - Error handling and logging
    
    Example:
        manager = SparkSessionManager()
        spark = manager.get_or_create_session()
        # Use spark for operations
        manager.stop_session()
    """
    
    def __init__(self, config_path: str = "spark_config.yaml", environment: Optional[str] = None):
        """
        Initialize SparkSession manager
        
        Args:
            config_path: Path to Spark config YAML file
            environment: Environment name (dev/staging/prod) for config overrides
        """
        self.config_path = config_path
        self.environment = environment
        self.spark: Optional[SparkSession] = None
        self.config: Dict[str, Any] = {}
        
        # Load configuration
        try:
            self.config = load_config(config_path, environment)
            logger.info(f"Loaded Spark config from {config_path}")
        except Exception as e:
            logger.error(f"Failed to load Spark config: {str(e)}")
            raise SparkSessionError(f"Cannot load Spark config: {str(e)}")
    
    def _build_spark_config(self) -> Dict[str, str]:
        """
        Build Spark configuration dictionary from YAML config
        
        Returns:
            Dictionary of Spark configuration options
        """
        spark_config = {}
        
        # Basic Spark settings
        spark_section = self.config.get("spark", {})
        
        # App name
        app_name = get_config_value(self.config, "spark.app_name", "bronze_ingestion_pipeline")
        spark_config["spark.app.name"] = app_name
        
        # Master URL
        master = get_config_value(self.config, "spark.master", "local[*]")
        spark_config["spark.master"] = master
        
        # Serialization (Kryo is faster than default Java serialization)
        spark_config["spark.serializer"] = "org.apache.spark.serializer.KryoSerializer"
        spark_config["spark.sql.adaptive.enabled"] = "true"
        spark_config["spark.sql.adaptive.coalescePartitions.enabled"] = "true"
        
        # Performance settings
        perf = self.config.get("performance", {})
        
        # Memory settings
        driver_memory = get_config_value(perf, "driver_memory", "2g")
        executor_memory = get_config_value(perf, "executor_memory", "4g")
        executor_cores = get_config_value(perf, "executor_cores", 2)
        
        spark_config["spark.driver.memory"] = driver_memory
        spark_config["spark.executor.memory"] = executor_memory
        spark_config["spark.executor.cores"] = str(executor_cores)
        
        # Dynamic allocation (auto-scale executors)
        dyn_alloc = perf.get("dynamic_allocation", {})
        if dyn_alloc.get("enabled", True):
            spark_config["spark.dynamicAllocation.enabled"] = "true"
            spark_config["spark.dynamicAllocation.minExecutors"] = str(dyn_alloc.get("min_executors", 1))
            spark_config["spark.dynamicAllocation.maxExecutors"] = str(dyn_alloc.get("max_executors", 10))
            spark_config["spark.dynamicAllocation.initialExecutors"] = str(dyn_alloc.get("initial_executors", 2))
        
        # SQL settings
        sql = spark_section.get("sql", {})
        
        # Shuffle partitions (parallelism)
        shuffle_partitions = get_config_value(sql, "shuffle_partitions", 200)
        spark_config["spark.sql.shuffle.partitions"] = str(shuffle_partitions)
        
        # Broadcast join threshold
        broadcast_threshold = get_config_value(sql, "auto_broadcast_join_threshold", 10485760)
        spark_config["spark.sql.autoBroadcastJoinThreshold"] = str(broadcast_threshold)
        
        # File settings
        files = sql.get("files", {})
        max_records_per_file = get_config_value(files, "max_records_per_file", 1000000)
        max_partition_bytes = get_config_value(files, "max_partition_bytes", 134217728)
        
        spark_config["spark.sql.files.maxRecordsPerFile"] = str(max_records_per_file)
        spark_config["spark.sql.files.maxPartitionBytes"] = str(max_partition_bytes)
        
        # Storage settings (S3/MinIO)
        storage = self.config.get("storage", {})
        s3 = storage.get("s3", {})
        
        if s3.get("endpoint"):
            # S3/MinIO configuration
            spark_config["spark.hadoop.fs.s3a.endpoint"] = s3["endpoint"]
            spark_config["spark.hadoop.fs.s3a.access.key"] = s3.get("access_key", "")
            spark_config["spark.hadoop.fs.s3a.secret.key"] = s3.get("secret_key", "")
            spark_config["spark.hadoop.fs.s3a.path.style.access"] = str(s3.get("path_style_access", True)).lower()
            spark_config["spark.hadoop.fs.s3a.impl"] = "org.apache.hadoop.fs.s3a.S3AFileSystem"
            spark_config["spark.hadoop.fs.s3a.connection.ssl.enabled"] = "false"  # For MinIO
        
        # Logging
        logging_config = self.config.get("logging", {})
        log_level = get_config_value(logging_config, "level", "INFO")
        spark_config["spark.logConf"] = "true"
        
        logger.info(f"Built Spark config with {len(spark_config)} settings")
        return spark_config
    
    def get_or_create_session(self, app_name_override: Optional[str] = None) -> SparkSession:
        """
        Get existing SparkSession or create new one with production settings
        
        Args:
            app_name_override: Optional custom app name (overrides config)
            
        Returns:
            SparkSession instance configured for production
            
        Raises:
            SparkSessionError: If session cannot be created
        """
        # Return existing session if available
        if self.spark is not None:
            logger.info("Returning existing SparkSession")
            return self.spark
        
        try:
            logger.info("Creating new SparkSession with production settings...")
            
            # Build Spark configuration
            spark_config = self._build_spark_config()
            
            # Start building SparkSession
            builder = SparkSession.builder
            
            # Apply all configuration
            for key, value in spark_config.items():
                builder = builder.config(key, value)
            
            # App name (override if provided)
            if app_name_override:
                builder = builder.appName(app_name_override)
            else:
                app_name = get_config_value(self.config, "spark.app_name", "bronze_ingestion_pipeline")
                builder = builder.appName(app_name)
            
            # Enable Hive support (for Delta/Iceberg tables)
            if self.config.get("spark", {}).get("session", {}).get("enable_hive_support", True):
                builder = builder.enableHiveSupport()
            
            # Add Kafka connector package (required for Kafka streaming)
            builder = builder.config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262"
            )
            
            # Create SparkSession
            self.spark = builder.getOrCreate()
            
            # Set log level
            log_level = get_config_value(self.config, "logging.level", "INFO")
            self.spark.sparkContext.setLogLevel(log_level)
            
            logger.info(f"SparkSession created successfully: {self.spark.version}")
            logger.info(f"Spark UI available at: http://localhost:{self.config.get('spark', {}).get('session', {}).get('ui_port', 4040)}")
            
            return self.spark
            
        except Exception as e:
            logger.error(f"Failed to create SparkSession: {str(e)}", exc_info=True)
            raise SparkSessionError(f"Cannot create SparkSession: {str(e)}")
    
    def stop_session(self):
        """
        Stop SparkSession and clean up resources
        
        Should be called when done with Spark operations
        """
        if self.spark is not None:
            try:
                logger.info("Stopping SparkSession...")
                self.spark.stop()
                self.spark = None
                logger.info("SparkSession stopped successfully")
            except Exception as e:
                logger.error(f"Error stopping SparkSession: {str(e)}", exc_info=True)
    
    def get_session(self) -> Optional[SparkSession]:
        """
        Get current SparkSession (without creating new one)
        
        Returns:
            SparkSession if exists, None otherwise
        """
        return self.spark


# Convenience function for easy usage
def get_spark_session(
    config_path: str = "spark_config.yaml",
    environment: Optional[str] = None,
    app_name: Optional[str] = None
) -> SparkSession:
    """
    Get or create SparkSession (convenience function)
    
    Args:
        config_path: Path to Spark config file
        environment: Environment name (dev/staging/prod)
        app_name: Optional app name override
        
    Returns:
        SparkSession instance
        
    Example:
        spark = get_spark_session()
        df = spark.readStream.format("kafka")...
    """
    manager = SparkSessionManager(config_path, environment)
    return manager.get_or_create_session(app_name)


# Global session manager instance (singleton pattern)
_global_manager: Optional[SparkSessionManager] = None


def get_global_spark_session() -> SparkSession:
    """
    Get global SparkSession (singleton pattern)
    
    Creates session once and reuses it across calls
    Useful for long-running applications
    
    Returns:
        Global SparkSession instance
    """
    global _global_manager
    
    if _global_manager is None:
        _global_manager = SparkSessionManager()
    
    return _global_manager.get_or_create_session()

