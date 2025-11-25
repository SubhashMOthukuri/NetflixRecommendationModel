"""
Kafka Stream Reader - Production ML Pipeline
Reads streaming data from Kafka topics using Spark Structured Streaming
Handles Kafka consumer configuration, offset management, and batch processing
"""
from typing import Optional, Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import DataStreamReader
from libs.config_loader import load_config, get_config_value
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class KafkaStreamReaderError(PipelineException):
    """Raised when Kafka stream reading fails"""
    pass


class KafkaStreamReader:
    """
    Production-grade Kafka stream reader
    
    Reads from Kafka topics with:
    - Configurable batch sizes
    - Offset management
    - Error handling
    - Performance optimizations
    
    Example:
        reader = KafkaStreamReader(spark)
        df = reader.read_stream("user_events")
        # df is a streaming DataFrame
    """
    
    def __init__(
        self,
        spark: SparkSession,
        kafka_config_path: str = "kafka.yaml",
        spark_config_path: str = "spark_config.yaml",
        environment: Optional[str] = None
    ):
        """
        Initialize Kafka stream reader
        
        Args:
            spark: SparkSession instance
            kafka_config_path: Path to Kafka config YAML
            spark_config_path: Path to Spark config YAML
            environment: Environment name (dev/staging/prod)
        """
        self.spark = spark
        self.environment = environment
        
        # Load configurations
        try:
            self.kafka_config = load_config(kafka_config_path, environment)
            self.spark_config = load_config(spark_config_path, environment)
            logger.info(f"Loaded Kafka and Spark configs")
        except Exception as e:
            logger.error(f"Failed to load configs: {str(e)}")
            raise KafkaStreamReaderError(f"Cannot load configurations: {str(e)}")
    
    def _get_kafka_bootstrap_servers(self) -> str:
        """
        Get Kafka bootstrap servers from config
        
        Returns:
            Bootstrap servers string (e.g., "localhost:9092")
        """
        bootstrap_servers = get_config_value(
            self.kafka_config,
            "broker.bootstrap_servers",
            "localhost:9092"
        )
        return bootstrap_servers
    
    def _get_consumer_config(self) -> Dict[str, Any]:
        """
        Get Kafka consumer configuration
        
        Returns:
            Dictionary of consumer settings
        """
        consumer_config = self.kafka_config.get("consumer", {})
        spark_kafka_config = self.spark_config.get("kafka", {}).get("consumer", {})
        
        # Merge configs (Spark config overrides Kafka config)
        # Note: Spark Structured Streaming manages offsets internally
        # Unsupported options: auto.offset.reset, enable.auto.commit
        # Use startingOffsets option instead (handled separately in read_stream)
        config = {
            "group.id": consumer_config.get("group_id", "bronze_ingestion_group"),
            # Only include options supported by Spark Structured Streaming
            "fetch.min.bytes": str(spark_kafka_config.get("fetch_min_bytes", 1024)),
            "fetch.max.wait.ms": str(spark_kafka_config.get("fetch_max_wait_ms", 500)),
            # Note: session.timeout.ms, heartbeat.interval.ms, isolation.level
            # are managed by Spark, but can be set if needed
        }
        
        return config
    
    def _get_streaming_config(self) -> Dict[str, Any]:
        """
        Get Spark streaming configuration
        
        Returns:
            Dictionary of streaming settings
        """
        streaming_config = self.spark_config.get("streaming", {})
        kafka_config = self.spark_config.get("kafka", {}).get("consumer", {})
        
        config = {
            "maxOffsetsPerTrigger": kafka_config.get("max_offsets_per_trigger", 10000),
            "startingOffsets": streaming_config.get("starting_offsets", "latest"),
            "failOnDataLoss": str(streaming_config.get("fail_on_data_loss", False)).lower(),
        }
        
        return config
    
    def read_stream(
        self,
        topic: str,
        topics: Optional[List[str]] = None,
        starting_offsets: Optional[str] = None,
        max_offsets_per_trigger: Optional[int] = None,
        additional_options: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        """
        Read streaming data from Kafka topic(s)
        
        Args:
            topic: Single topic name (or use 'topics' for multiple)
            topics: List of topic names (alternative to 'topic')
            starting_offsets: Starting offset ("earliest", "latest", or JSON)
            max_offsets_per_trigger: Max records per batch (None = use config)
            additional_options: Additional Kafka options
        
        Returns:
            Streaming DataFrame with Kafka columns:
            - key: Message key (bytes)
            - value: Message value (bytes)
            - topic: Topic name
            - partition: Partition number
            - offset: Offset number
            - timestamp: Message timestamp
            - timestampType: Timestamp type
        
        Raises:
            KafkaStreamReaderError: If stream cannot be read
            
        Example:
            reader = KafkaStreamReader(spark)
            df = reader.read_stream("user_events")
            # Process streaming DataFrame
        """
        try:
            logger.info(f"Reading stream from Kafka topic(s)...")
            
            # Get bootstrap servers
            bootstrap_servers = self._get_kafka_bootstrap_servers()
            logger.info(f"Kafka bootstrap servers: {bootstrap_servers}")
            
            # Build stream reader
            stream_reader = self.spark.readStream.format("kafka")
            
            # Set bootstrap servers
            stream_reader = stream_reader.option("kafka.bootstrap.servers", bootstrap_servers)
            
            # Set topic(s)
            if topics:
                # Multiple topics
                topics_str = ",".join(topics)
                stream_reader = stream_reader.option("subscribe", topics_str)
                logger.info(f"Subscribed to topics: {topics_str}")
            else:
                # Single topic
                stream_reader = stream_reader.option("subscribe", topic)
                logger.info(f"Subscribed to topic: {topic}")
            
            # Apply consumer config
            consumer_config = self._get_consumer_config()
            for key, value in consumer_config.items():
                stream_reader = stream_reader.option(f"kafka.{key}", value)
            
            # Apply streaming config
            streaming_config = self._get_streaming_config()
            
            # Override starting offsets if provided
            if starting_offsets:
                stream_reader = stream_reader.option("startingOffsets", starting_offsets)
            else:
                stream_reader = stream_reader.option("startingOffsets", streaming_config["startingOffsets"])
            
            # Override max offsets per trigger if provided
            if max_offsets_per_trigger:
                stream_reader = stream_reader.option("maxOffsetsPerTrigger", str(max_offsets_per_trigger))
            else:
                stream_reader = stream_reader.option("maxOffsetsPerTrigger", str(streaming_config["maxOffsetsPerTrigger"]))
            
            stream_reader = stream_reader.option("failOnDataLoss", streaming_config["failOnDataLoss"])
            
            # Apply additional options if provided
            if additional_options:
                for key, value in additional_options.items():
                    stream_reader = stream_reader.option(key, value)
            
            # Load streaming DataFrame
            df = stream_reader.load()
            
            logger.info(f"Successfully created streaming DataFrame from Kafka")
            logger.info(f"Streaming config: maxOffsetsPerTrigger={streaming_config['maxOffsetsPerTrigger']}, "
                       f"startingOffsets={streaming_config['startingOffsets']}")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to read Kafka stream: {str(e)}", exc_info=True)
            raise KafkaStreamReaderError(f"Cannot read Kafka stream: {str(e)}")
    
    def read_stream_from_config(self, topic_key: str = "user_events") -> DataFrame:
        """
        Read stream using topic name from Kafka config
        
        Args:
            topic_key: Key in config.topics (e.g., "user_events")
        
        Returns:
            Streaming DataFrame
        
        Example:
            reader = KafkaStreamReader(spark)
            df = reader.read_stream_from_config("user_events")
        """
        # Get topic name from config
        topics_config = self.kafka_config.get("topics", {})
        topic = topics_config.get(topic_key)
        
        if not topic:
            raise KafkaStreamReaderError(f"Topic '{topic_key}' not found in config")
        
        return self.read_stream(topic)
    
    def read_multiple_topics(self, topic_keys: List[str]) -> DataFrame:
        """
        Read from multiple Kafka topics
        
        Args:
            topic_keys: List of topic keys from config (e.g., ["user_events", "video_events"])
        
        Returns:
            Streaming DataFrame from all topics
        
        Example:
            reader = KafkaStreamReader(spark)
            df = reader.read_multiple_topics(["user_events", "video_events"])
        """
        topics_config = self.kafka_config.get("topics", {})
        topics = [topics_config.get(key) for key in topic_keys if topics_config.get(key)]
        
        if not topics:
            raise KafkaStreamReaderError(f"No valid topics found for keys: {topic_keys}")
        
        logger.info(f"Reading from multiple topics: {topics}")
        return self.read_stream(topics=topics)

