"""
Kafka Offset Manager - Production ML Pipeline
Manages Kafka offsets for tracking reading progress and enabling recovery
Critical for preventing data loss and enabling exactly-once processing
"""
import json
from typing import Optional, Dict, Any, List
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery
from libs.config_loader import load_config, get_config_value
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class OffsetManagerError(PipelineException):
    """Raised when offset management operations fail"""
    pass


class KafkaOffsetManager:
    """
    Production-grade Kafka offset manager
    
    Manages:
    - Offset tracking (where we left off reading)
    - Offset recovery (resume from last position)
    - Offset commits (save progress)
    - Offset validation (check for gaps)
    
    Example:
        manager = KafkaOffsetManager(spark)
        # Get last offset for topic
        offset = manager.get_last_offset("user_events", partition=0)
        # Start reading from specific offset
        df = reader.read_stream("user_events", starting_offsets=offset)
    """
    
    def __init__(
        self,
        spark: SparkSession,
        kafka_config_path: str = "kafka.yaml",
        checkpoint_location: Optional[str] = None,
        environment: Optional[str] = None
    ):
        """
        Initialize offset manager
        
        Args:
            spark: SparkSession instance
            kafka_config_path: Path to Kafka config YAML
            checkpoint_location: Optional checkpoint location (overrides config)
            environment: Environment name (dev/staging/prod)
        """
        self.spark = spark
        self.environment = environment
        
        # Load configuration
        try:
            self.kafka_config = load_config(kafka_config_path, environment)
            logger.info("Loaded Kafka config for offset management")
        except Exception as e:
            logger.error(f"Failed to load Kafka config: {str(e)}")
            raise OffsetManagerError(f"Cannot load Kafka config: {str(e)}")
        
        # Get checkpoint location
        if checkpoint_location:
            self.checkpoint_location = checkpoint_location
        else:
            # Try to get from Spark config
            try:
                spark_config = load_config("spark_config.yaml", environment)
                self.checkpoint_location = get_config_value(
                    spark_config,
                    "streaming.checkpoint_location",
                    "s3a://data-lake/checkpoints/bronze_ingestion/"
                )
            except Exception:
                self.checkpoint_location = "s3a://data-lake/checkpoints/bronze_ingestion/"
        
        logger.info(f"Checkpoint location: {self.checkpoint_location}")
    
    def _get_kafka_bootstrap_servers(self) -> str:
        """Get Kafka bootstrap servers"""
        return get_config_value(
            self.kafka_config,
            "broker.bootstrap_servers",
            "localhost:9092"
        )
    
    def get_last_offset_from_checkpoint(
        self,
        topic: str,
        checkpoint_path: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get last processed offset from Spark checkpoint
        
        Args:
            topic: Topic name
            checkpoint_path: Optional checkpoint path (defaults to instance checkpoint_location)
        
        Returns:
            Dictionary with offset information or None if not found
            Format: {"topic": {"partition": offset, ...}}
        
        Example:
            offset = manager.get_last_offset_from_checkpoint("user_events")
            # Returns: {"user_events": {"0": 12345, "1": 67890}}
        """
        try:
            checkpoint = checkpoint_path or self.checkpoint_location
            checkpoint_path_obj = Path(checkpoint) / "sources" / "0" / "commits"
            
            logger.info(f"Reading offset from checkpoint: {checkpoint_path_obj}")
            
            # Spark stores offsets in checkpoint directory
            # Format: checkpoint/sources/0/commits/{batch_id}
            if not checkpoint_path_obj.exists():
                logger.warning(f"Checkpoint path does not exist: {checkpoint_path_obj}")
                return None
            
            # Find latest commit file
            commit_files = sorted(checkpoint_path_obj.glob("*"), reverse=True)
            if not commit_files:
                logger.warning("No commit files found in checkpoint")
                return None
            
            latest_commit = commit_files[0]
            logger.info(f"Reading from latest commit: {latest_commit.name}")
            
            # Read commit file (JSON format)
            with open(latest_commit, 'r') as f:
                commit_data = json.load(f)
            
            # Extract offset information
            # Spark checkpoint format: {"sources": [{"source": "kafka", "startOffset": {...}, "endOffset": {...}}]}
            sources = commit_data.get("sources", [])
            for source in sources:
                if source.get("source") == "kafka":
                    end_offset = source.get("endOffset", {})
                    if topic in end_offset:
                        offset_info = {topic: end_offset[topic]}
                        logger.info(f"Found offset for topic {topic}: {offset_info}")
                        return offset_info
            
            logger.warning(f"No offset found for topic {topic} in checkpoint")
            return None
            
        except Exception as e:
            logger.error(f"Error reading offset from checkpoint: {str(e)}", exc_info=True)
            return None
    
    def format_offset_for_kafka(self, offset_dict: Dict[str, Any]) -> str:
        """
        Format offset dictionary as Kafka startingOffsets JSON string
        
        Args:
            offset_dict: Offset dictionary
                Format: {"topic": {"partition": offset, ...}}
        
        Returns:
            JSON string for Kafka startingOffsets option
        
        Example:
            offset_dict = {"user_events": {"0": 12345, "1": 67890}}
            json_str = manager.format_offset_for_kafka(offset_dict)
            # Returns: '{"user_events":{"0":12345,"1":67890}}'
        """
        try:
            json_str = json.dumps(offset_dict, separators=(',', ':'))
            logger.debug(f"Formatted offset JSON: {json_str}")
            return json_str
        except Exception as e:
            logger.error(f"Error formatting offset: {str(e)}")
            raise OffsetManagerError(f"Cannot format offset: {str(e)}")
    
    def get_starting_offset(
        self,
        topic: str,
        strategy: str = "checkpoint",
        fallback: str = "latest"
    ) -> str:
        """
        Get starting offset for reading from Kafka
        
        Args:
            topic: Topic name
            strategy: Strategy to get offset ("checkpoint", "earliest", "latest")
            fallback: Fallback if checkpoint not found ("earliest" or "latest")
        
        Returns:
            Starting offset string ("earliest", "latest", or JSON format)
        
        Example:
            # Try checkpoint first, fallback to latest
            offset = manager.get_starting_offset("user_events", strategy="checkpoint", fallback="latest")
            df = reader.read_stream("user_events", starting_offsets=offset)
        """
        if strategy == "checkpoint":
            # Try to get from checkpoint
            offset_dict = self.get_last_offset_from_checkpoint(topic)
            if offset_dict:
                # Format as JSON for Kafka
                return self.format_offset_for_kafka(offset_dict)
            else:
                # Fallback
                logger.info(f"No checkpoint found, using fallback: {fallback}")
                return fallback
        elif strategy == "earliest":
            return "earliest"
        elif strategy == "latest":
            return "latest"
        else:
            logger.warning(f"Unknown strategy {strategy}, using fallback: {fallback}")
            return fallback
    
    def validate_offset_range(
        self,
        topic: str,
        partition: int,
        start_offset: int,
        end_offset: int
    ) -> bool:
        """
        Validate offset range (check for gaps or invalid ranges)
        
        Args:
            topic: Topic name
            partition: Partition number
            start_offset: Start offset
            end_offset: End offset
        
        Returns:
            True if valid, False otherwise
        
        Note: This is a placeholder - full implementation would query Kafka
        """
        if end_offset < start_offset:
            logger.error(f"Invalid offset range: end ({end_offset}) < start ({start_offset})")
            return False
        
        if start_offset < 0 or end_offset < 0:
            logger.error(f"Invalid offset: negative values not allowed")
            return False
        
        logger.info(f"Offset range validated: partition {partition}, {start_offset} to {end_offset}")
        return True
    
    def get_consumer_group_offset(
        self,
        topic: str,
        consumer_group: Optional[str] = None
    ) -> Optional[Dict[str, int]]:
        """
        Get consumer group offset from Kafka (requires Kafka admin client)
        
        Args:
            topic: Topic name
            consumer_group: Consumer group ID (defaults to config)
        
        Returns:
            Dictionary of partition -> offset or None
        
        Note: This is a placeholder - full implementation would use KafkaAdminClient
        """
        if not consumer_group:
            consumer_group = get_config_value(
                self.kafka_config,
                "consumer.group_id",
                "bronze_ingestion_group"
            )
        
        logger.info(f"Getting consumer group offset for {consumer_group}, topic {topic}")
        logger.warning("get_consumer_group_offset not fully implemented - requires KafkaAdminClient")
        
        # TODO: Implement using KafkaAdminClient or kafka-python
        # This would query Kafka directly for consumer group offsets
        return None
    
    def reset_offset_to_earliest(
        self,
        topic: str,
        consumer_group: Optional[str] = None
    ) -> str:
        """
        Reset offset to earliest (replay all data)
        
        Args:
            topic: Topic name
            consumer_group: Consumer group ID
        
        Returns:
            "earliest" offset string
        """
        logger.info(f"Resetting offset to earliest for topic {topic}")
        return "earliest"
    
    def reset_offset_to_latest(
        self,
        topic: str,
        consumer_group: Optional[str] = None
    ) -> str:
        """
        Reset offset to latest (skip old data)
        
        Args:
            topic: Topic name
            consumer_group: Consumer group ID
        
        Returns:
            "latest" offset string
        """
        logger.info(f"Resetting offset to latest for topic {topic}")
        return "latest"
    
    def get_offset_info_from_query(self, query: StreamingQuery) -> Optional[Dict[str, Any]]:
        """
        Get offset information from active streaming query
        
        Args:
            query: StreamingQuery instance
        
        Returns:
            Offset information dictionary or None
        
        Note: Spark Structured Streaming manages offsets automatically via checkpoints
        This method provides visibility into current processing state
        """
        try:
            if not query.isActive:
                logger.warning("Query is not active")
                return None
            
            # Get query progress
            progress = query.lastProgress
            
            if not progress:
                logger.warning("No progress information available")
                return None
            
            # Extract offset information
            sources = progress.get("sources", [])
            offset_info = {}
            
            for source in sources:
                if source.get("description", "").startswith("Kafka"):
                    start_offset = source.get("startOffset", {})
                    end_offset = source.get("endOffset", {})
                    
                    offset_info = {
                        "start_offset": start_offset,
                        "end_offset": end_offset,
                        "num_input_rows": source.get("numInputRows", 0)
                    }
                    
                    logger.info(f"Query offset info: {offset_info}")
                    return offset_info
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting offset info from query: {str(e)}", exc_info=True)
            return None

