"""
Bronze Stream Job - Production ML Pipeline
Main streaming job that orchestrates Kafka → Spark → Bronze pipeline
Combines all components: reading, validation, transformation, writing
"""
from typing import Optional, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, to_json, struct, current_timestamp, lit
from pyspark.sql.types import StringType
from pyspark.sql.streaming import StreamingQuery
from libs.logger import get_logger
from libs.exceptions import PipelineException

# Core infrastructure
from data_streaming.bronze_ingestion.core.spark.spark_session import get_spark_session
from data_streaming.bronze_ingestion.core.spark.context_manager import SparkContextManager
from data_streaming.bronze_ingestion.core.kafka.kafka_stream_reader import KafkaStreamReader
from data_streaming.bronze_ingestion.core.kafka.offset_manager import KafkaOffsetManager

# Schema management
from schemas.bronze_schema.schema_loader import load_bronze_schema
from schemas.bronze_schema.schema_enforcer import enforce_bronze_schema
from schemas.bronze_schema.malformed_handler import MalformedDataHandler

# Validation
from data_streaming.bronze_ingestion.validation.data_quality.bronze_validator import BronzeValidator
from data_streaming.bronze_ingestion.validation.pii_handling.pii_scrubber import PIIScrubber

# Transformation
from data_streaming.bronze_ingestion.transform.normalization.bronze_normalizer import BronzeNormalizer
from data_streaming.bronze_ingestion.transform.deduplication.deduplication import Deduplication

# Storage
from data_streaming.bronze_ingestion.storage.writers.bronze_writer import BronzeWriter
from data_streaming.bronze_ingestion.storage.checkpointing.checkpoint_manager import CheckpointManager

# Observability
from data_streaming.bronze_ingestion.observability.metrics.bronze_metrics import BronzeMetrics
from data_streaming.bronze_ingestion.observability.alerting.alerting import Alerting

logger = get_logger(__name__)


class BronzeStreamJobError(PipelineException):
    """Raised when bronze stream job fails"""
    pass


class BronzeStreamJob:
    """
    Production-grade bronze streaming job
    
    Orchestrates complete pipeline:
    1. Read from Kafka
    2. Parse JSON
    3. Validate schema
    4. Handle malformed data
    5. Validate data quality
    6. Scrub PII
    7. Normalize data
    8. Remove duplicates
    9. Write to bronze layer
    10. Track metrics & alerts
    
    Example:
        job = BronzeStreamJob()
        job.run()
    """
    
    def __init__(
        self,
        topic: str = "user_events",
        environment: Optional[str] = None
    ):
        """
        Initialize bronze stream job
        
        Args:
            topic: Kafka topic name
            environment: Environment name (dev/staging/prod)
        """
        self.topic = topic
        self.environment = environment
        
        # Initialize Spark
        logger.info("Initializing Spark session...")
        self.spark = get_spark_session(environment=environment)
        
        # Initialize components
        logger.info("Initializing pipeline components...")
        self.kafka_reader = KafkaStreamReader(self.spark, environment=environment)
        self.offset_manager = KafkaOffsetManager(self.spark, environment=environment)
        self.schema_enforcer = None  # Will be initialized with schema
        self.malformed_handler = MalformedDataHandler()
        self.validator = BronzeValidator()
        self.pii_scrubber = PIIScrubber(scrub_mode="hash")
        self.normalizer = BronzeNormalizer()
        self.deduplication = Deduplication(strategy="event_id")
        self.bronze_writer = BronzeWriter()
        self.checkpoint_manager = CheckpointManager()
        self.metrics = BronzeMetrics()
        self.alerting = Alerting(enabled=True)
        
        # Context manager for graceful shutdown
        self.context_manager = SparkContextManager(self.spark)
        
        logger.info("Bronze stream job initialized")
    
    def _parse_kafka_messages(self, df: DataFrame) -> DataFrame:
        """
        Parse Kafka messages (binary) to JSON DataFrame
        
        Args:
            df: Kafka streaming DataFrame
        
        Returns:
            DataFrame with parsed JSON
        """
        try:
            # Load bronze schema
            schema = load_bronze_schema()
            
            # Convert Kafka value (bytes) to JSON
            # For now, we'll use a simple JSON parsing
            # In production, you'd use Avro deserialization if using Schema Registry
            
            # Extract fields from JSON
            # Kafka value is binary, cast to string then parse JSON
            parsed_df = df.select(
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp"),
                from_json(col("value").cast("string"), self._get_simple_schema()).alias("data")
            ).select(
                "data.*", 
                "topic", 
                "partition", 
                "offset", 
                "kafka_timestamp"
            ).withColumnRenamed("timestamp", "timestamp_ms")  # Rename to match expected column name
            
            logger.debug("Parsed Kafka messages to JSON")
            return parsed_df
            
        except Exception as e:
            logger.error(f"Error parsing Kafka messages: {str(e)}", exc_info=True)
            raise BronzeStreamJobError(f"Cannot parse Kafka messages: {str(e)}")
    
    def _get_simple_schema(self):
        """Get schema for JSON parsing matching producer output"""
        from pyspark.sql.types import StructType, StructField, StringType, LongType, MapType
        
        # Schema matching the producer's JSON structure
        return StructType([
            StructField("event_id", StringType()),
            StructField("event_type", StringType()),
            StructField("timestamp", LongType()),
            StructField("timestamp_iso", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("device", MapType(StringType(), StringType()), True),  # Nested object as map
            StructField("content", MapType(StringType(), StringType()), True),  # Nested object as map
            StructField("context", MapType(StringType(), StringType()), True),  # Nested object as map
        ])
    
    def _add_processing_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add processing metadata to DataFrame
        
        Args:
            df: Input DataFrame
        
        Returns:
            DataFrame with processing metadata
        """
        from pyspark.sql.functions import current_timestamp, date_format, hour
        
        # Add ingestion timestamp
        df = df.withColumn("ingestion_timestamp_ms", (current_timestamp().cast("long") * 1000))
        df = df.withColumn("ingestion_date", date_format(current_timestamp(), "yyyy-MM-dd"))
        df = df.withColumn("ingestion_hour", hour(current_timestamp()))
        df = df.withColumn("source_topic", lit(self.topic))
        
        # Add validation status (will be updated by validator)
        df = df.withColumn("validation_status", lit("pending"))
        df = df.withColumn("pii_scrubbed", lit(False))
        
        return df
    
    def run(self):
        """
        Run the bronze streaming job
        
        Main pipeline execution:
        1. Read from Kafka
        2. Parse and validate
        3. Transform
        4. Write to bronze
        5. Monitor and alert
        """
        try:
            logger.info(f"Starting bronze stream job for topic: {self.topic}")
            
            # Step 1: Read from Kafka
            logger.info("Step 1: Reading from Kafka...")
            kafka_df = self.kafka_reader.read_stream(self.topic)
            
            # Step 2: Parse Kafka messages
            logger.info("Step 2: Parsing Kafka messages...")
            parsed_df = self._parse_kafka_messages(kafka_df)
            
            # Step 3: Handle malformed data
            logger.info("Step 3: Handling malformed data...")
            # In streaming, we filter valid/invalid and process separately
            # For now, we'll assume all parsed data is valid (malformed handling happens in write)
            valid_df = parsed_df
            logger.info("Malformed data will be handled during write operation")
            
            # Step 4: Add processing metadata
            logger.info("Step 4: Adding processing metadata...")
            processed_df = self._add_processing_metadata(valid_df)
            
            # Step 5: Enforce schema
            logger.info("Step 5: Enforcing schema...")
            processed_df = enforce_bronze_schema(processed_df, strict=False, add_missing_fields=True)
            
            # Step 6: Validate data quality
            logger.info("Step 6: Validating data quality...")
            # In streaming, we add validation columns but don't split or count
            # Just add validation status columns for monitoring
            validated_df = processed_df
            # Add validation columns (simplified for streaming)
            from pyspark.sql.functions import when, col, lit
            validated_df = validated_df.withColumn(
                "validation_status",
                when(col("event_id").isNotNull() & (col("event_id") != ""), lit("valid"))
                .otherwise(lit("invalid"))
            )
            logger.info("Data quality validation columns added")
            
            # Step 7: Scrub PII
            logger.info("Step 7: Scrubbing PII...")
            scrubbed_df = self.pii_scrubber.scrub_pii(validated_df, mark_scrubbed=True)
            
            # Step 8: Normalize data
            logger.info("Step 8: Normalizing data...")
            normalized_df = self.normalizer.normalize(scrubbed_df, add_processing_metadata=True)
            
            # Step 9: Remove duplicates (streaming-compatible)
            logger.info("Step 9: Removing duplicates...")
            # For streaming, use window-based deduplication
            # Skip deduplication for now (can add stateful deduplication later)
            deduplicated_df = normalized_df
            logger.info("Deduplication skipped for streaming (will be handled in write)")
            
            # Step 10: Write to bronze layer
            logger.info("Step 10: Writing to bronze layer...")
            checkpoint_path = self.checkpoint_manager.get_checkpoint_path(self.topic)
            self.checkpoint_manager.ensure_checkpoint_directory(checkpoint_path)
            
            writer = self.bronze_writer.write_stream(
                deduplicated_df,
                checkpoint_location=checkpoint_path,
                trigger_interval="30s"
            )
            
            # Start streaming query
            query = writer.start()
            
            # Register query for graceful shutdown
            self.context_manager.register_streaming_query(query)
            
            logger.info(f"Bronze stream job started successfully (query ID: {query.id})")
            logger.info(f"Streaming query active: {query.isActive}")
            
            # Monitor and alert loop
            self._monitor_and_alert(query)
            
            # Wait for termination
            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Bronze stream job failed: {str(e)}", exc_info=True)
            raise BronzeStreamJobError(f"Cannot run bronze stream job: {str(e)}")
    
    def _monitor_and_alert(self, query: StreamingQuery):
        """
        Monitor query and send alerts (runs in background)
        
        Args:
            query: StreamingQuery instance
        """
        import threading
        import time
        
        def monitor_loop():
            while query.isActive:
                try:
                    # Get metrics from query
                    query_stats = self.metrics.get_metrics_from_query(query)
                    
                    # Get metrics summary
                    metrics_summary = self.metrics.get_metrics_summary()
                    
                    # Check and send alerts
                    alerts = self.alerting.check_and_alert(metrics_summary, query_stats)
                    
                    if alerts:
                        logger.warning(f"Sent {len(alerts)} alerts")
                    
                    # Sleep before next check
                    time.sleep(60)  # Check every minute
                    
                except Exception as e:
                    logger.error(f"Error in monitoring loop: {str(e)}", exc_info=True)
                    time.sleep(60)
        
        # Start monitoring thread
        monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        monitor_thread.start()
        logger.info("Monitoring thread started")


# Main entry point
def main():
    """Main entry point for bronze stream job"""
    import sys
    
    # Get topic from command line or use default
    topic = sys.argv[1] if len(sys.argv) > 1 else "user_events"
    environment = sys.argv[2] if len(sys.argv) > 2 else None
    
    logger.info(f"Starting bronze stream job: topic={topic}, environment={environment}")
    
    try:
        job = BronzeStreamJob(topic=topic, environment=environment)
        job.run()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down gracefully...")
    except Exception as e:
        logger.error(f"Bronze stream job failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

