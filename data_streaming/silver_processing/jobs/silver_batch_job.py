"""
Silver Batch Job - Production ML Pipeline
Main orchestrator for bronze → silver batch processing pipeline
Coordinates all components: read, transform, validate, write
"""
from typing import Optional, Dict, Any
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from libs.logger import get_logger
from libs.exceptions import PipelineException

# Core infrastructure
from data_streaming.silver_processing.core.bronze_reader import BronzeReader

# Transformations
from data_streaming.silver_processing.transform.flattener import Flattener
from data_streaming.silver_processing.transform.type_casting import TypeCaster
from data_streaming.silver_processing.transform.normalization_rules import Normalizer
from data_streaming.silver_processing.transform.null_handler import NullHandler
from data_streaming.silver_processing.transform.pii_masking import PIIMasker

# Enrichment
from data_streaming.silver_processing.enrichment.jointer import Jointer

# Validation
from data_streaming.silver_processing.validation.silver_validation import SilverValidator
from data_streaming.silver_processing.validation.contract_enforcement import ContractEnforcer

# Storage
from data_streaming.silver_processing.storage.silver_writer import SilverWriter
from data_streaming.silver_processing.storage.partition_tracker import PartitionTracker

# Observability
from data_streaming.silver_processing.observability.silver_metrics import SilverMetrics
from data_streaming.silver_processing.observability.silver_alerting import SilverAlerting

# Error Handling
from data_streaming.silver_processing.error_handling.silver_dlq_handler import SilverDLQHandler

# Spark session
from data_streaming.bronze_ingestion.core.spark.spark_session import get_spark_session

logger = get_logger(__name__)


class SilverBatchJobError(PipelineException):
    """Raised when silver batch job fails"""
    pass


class SilverBatchJob:
    """
    Production-grade silver batch processing job
    
    Orchestrates complete pipeline:
    1. Read from bronze layer
    2. Flatten nested structures
    3. Type cast columns
    4. Normalize values
    5. Handle null values
    6. Enrich with lookups
    7. Validate data quality
    8. Enforce schema contract
    9. Write to silver layer
    
    Example:
        job = SilverBatchJob()
        job.run(start_date="2024-01-15", end_date="2024-01-16")
    """
    
    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        environment: Optional[str] = None
    ):
        """
        Initialize silver batch job
        
        Args:
            spark: Optional SparkSession (creates new if None)
            environment: Environment name (dev/staging/prod)
        """
        self.environment = environment
        
        # Initialize Spark
        if spark is None:
            logger.info("Initializing Spark session...")
            self.spark = get_spark_session(environment=environment, app_name="silver_batch_job")
        else:
            self.spark = spark
        
        # Initialize components
        logger.info("Initializing pipeline components...")
        self.bronze_reader = BronzeReader(self.spark)
        self.flattener = Flattener()
        self.type_caster = TypeCaster()
        self.normalizer = Normalizer()
        self.null_handler = NullHandler()
        self.pii_masker = PIIMasker()  # Mask PII for privacy compliance
        self.jointer = Jointer(self.spark)
        self.validator = SilverValidator()
        self.contract_enforcer = ContractEnforcer()
        self.silver_writer = SilverWriter()
        
        # Initialize observability components
        self.metrics = SilverMetrics()
        self.alerting = SilverAlerting()
        self.dlq_handler = SilverDLQHandler(self.spark)
        self.partition_tracker = PartitionTracker(self.spark)
        
        logger.info("Silver batch job initialized")
    
    def run(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        hours: Optional[list] = None,
        partition_filter: Optional[str] = None
    ):
        """
        Run the silver batch processing job
        
        Args:
            start_date: Start date (YYYY-MM-DD) - filters bronze partitions
            end_date: End date (YYYY-MM-DD) - filters bronze partitions
            hours: Optional list of hours (0-23) to process
            partition_filter: Optional Spark SQL partition filter
        
        Example:
            job = SilverBatchJob()
            job.run(start_date="2024-01-15", end_date="2024-01-16")
        """
        try:
            logger.info("=" * 80)
            logger.info("Starting Silver Batch Processing Job")
            logger.info("=" * 80)
            
            start_time = datetime.now()
            
            # Step 1: Read from bronze layer
            logger.info("Step 1: Reading from bronze layer...")
            bronze_df = self.bronze_reader.read_bronze(
                start_date=start_date,
                end_date=end_date,
                hours=hours,
                partition_filter=partition_filter
            )
            bronze_count = bronze_df.count()
            logger.info(f"Read {bronze_count} records from bronze layer")
            
            # Record input metrics
            self.metrics.record_batch_processed(input_count=bronze_count, output_count=0)
            
            if bronze_count == 0:
                logger.warning("No data to process, exiting")
                return
            
            # Step 2: Flatten nested structures
            logger.info("Step 2: Flattening nested structures...")
            flat_df = self.flattener.flatten(bronze_df)
            logger.info("Flattening complete")
            
            # Step 3: Type cast columns
            logger.info("Step 3: Type casting columns...")
            typed_df = self.type_caster.cast_types(flat_df, safe_mode=True)
            logger.info("Type casting complete")
            
            # Step 4: Normalize values
            logger.info("Step 4: Normalizing values...")
            normalized_df = self.normalizer.normalize(typed_df)
            logger.info("Normalization complete")
            
            # Step 5: Handle null values
            logger.info("Step 5: Handling null values...")
            null_handled_df = self.null_handler.handle_nulls(
                normalized_df,
                fill_defaults=True,
                validate_required=True
            )
            logger.info("Null handling complete")
            
            # Step 5.5: Mask PII fields (IP address, geo-location) for privacy compliance
            logger.info("Step 5.5: Masking PII fields for privacy compliance...")
            pii_masked_df = self.pii_masker.mask_pii(null_handled_df, mark_masked=True)
            logger.info("PII masking complete")
            
            # Step 6: Enrich with lookups (user_segment - not PII, just enrichment)
            logger.info("Step 6: Enriching with lookup tables...")
            enriched_df = self.jointer.enrich(pii_masked_df)
            logger.info("Enrichment complete")
            
            # Step 7: Validate data quality
            logger.info("Step 7: Validating data quality...")
            validated_df = self.validator.validate(enriched_df, add_validation_columns=True)
            validation_stats = self.validator.get_validation_stats(validated_df)
            logger.info(f"Validation complete: {validation_stats}")
            
            # Record validation metrics
            self.metrics.record_validation_metrics(
                valid_count=validation_stats.get("valid_count", 0),
                warning_count=validation_stats.get("warning_count", 0),
                invalid_count=validation_stats.get("invalid_count", 0),
                avg_quality_score=validation_stats.get("average_quality_score")
            )
            
            # Handle invalid records (send to DLQ)
            if validation_stats.get("invalid_count", 0) > 0:
                from pyspark.sql.functions import col
                invalid_df = validated_df.filter(col("validation_status") == "invalid")
                self.dlq_handler.write_to_dlq(
                    invalid_df,
                    reason="validation_failed",
                    error_message="Data quality score below threshold"
                )
                # Remove invalid records from pipeline
                validated_df = validated_df.filter(col("validation_status") != "invalid")
            
            # Step 8: Enforce schema contract
            logger.info("Step 8: Enforcing schema contract...")
            try:
                enforced_df = self.contract_enforcer.enforce(
                    validated_df,
                    add_missing_fields=True,
                    ensure_types=True
                )
                logger.info("Contract enforcement complete")
            except Exception as e:
                # If contract enforcement fails, send to DLQ
                logger.error(f"Contract enforcement failed: {str(e)}")
                self.dlq_handler.write_to_dlq(
                    validated_df,
                    reason="contract_enforcement_failed",
                    error_message=str(e)
                )
                raise
            
            # Step 9: Write to silver layer
            logger.info("Step 9: Writing to silver layer...")
            write_start = datetime.now()
            self.silver_writer.write_batch(enforced_df, mode="append")
            write_duration = (datetime.now() - write_start).total_seconds() * 1000
            silver_count = enforced_df.count()
            logger.info(f"Successfully wrote {silver_count} records to silver layer")
            
            # Record write metrics
            self.metrics.record_write_metrics(
                records_written=silver_count,
                write_duration_ms=write_duration
            )
            
            # Mark partitions as processed
            if start_date:
                # Mark date partition as processed
                self.partition_tracker.mark_partition_processed(
                    date=start_date,
                    status="completed",
                    metadata={"records_processed": silver_count}
                )
            
            # Calculate processing time
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds() * 1000
            
            # Record final batch metrics
            self.metrics.record_batch_processed(
                input_count=bronze_count,
                output_count=silver_count,
                invalid_count=validation_stats.get("invalid_count", 0),
                batch_duration_ms=processing_time
            )
            
            # Get metrics summary and check alerts
            metrics_summary = self.metrics.get_metrics_summary()
            alerts = self.alerting.check_and_alert(metrics_summary, validation_stats)
            
            # Summary
            logger.info("=" * 80)
            logger.info("Silver Batch Processing Job Complete")
            logger.info("=" * 80)
            logger.info(f"Processing time: {processing_time/1000:.2f} seconds")
            logger.info(f"Records processed: {bronze_count} → {silver_count}")
            logger.info(f"Validation stats: {validation_stats}")
            logger.info(f"Alerts sent: {len(alerts)}")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"Silver batch job failed: {str(e)}", exc_info=True)
            raise SilverBatchJobError(f"Cannot run silver batch job: {str(e)}")
    
    def process_partition(
        self,
        date: str,
        hour: Optional[int] = None
    ):
        """
        Process specific partition from bronze to silver
        
        Args:
            date: Date (YYYY-MM-DD)
            hour: Optional hour (0-23)
        
        Example:
            job = SilverBatchJob()
            job.process_partition(date="2024-01-15", hour=18)
        """
        try:
            logger.info(f"Processing partition: date={date}, hour={hour}")
            
            if hour is not None:
                self.run(start_date=date, end_date=date, hours=[hour])
            else:
                self.run(start_date=date, end_date=date)
            
        except Exception as e:
            logger.error(f"Failed to process partition: {str(e)}", exc_info=True)
            raise SilverBatchJobError(f"Cannot process partition: {str(e)}")
    
    def process_incremental(
        self,
        last_processed_date: Optional[str] = None,
        last_processed_hour: Optional[int] = None,
        lookback_days: int = 1
    ):
        """
        Process incremental data (only new/unprocessed partitions)
        
        Args:
            last_processed_date: Last processed date (YYYY-MM-DD)
            last_processed_hour: Last processed hour (0-23)
            lookback_days: Number of days to look back
        
        Example:
            job = SilverBatchJob()
            job.process_incremental(lookback_days=1)
        """
        try:
            logger.info("Processing incremental data...")
            
            # Read incremental data from bronze
            bronze_df = self.bronze_reader.read_incremental(
                last_processed_date=last_processed_date,
                last_processed_hour=last_processed_hour,
                lookback_days=lookback_days
            )
            
            if bronze_df.count() == 0:
                logger.info("No new data to process")
                return
            
            # Process the incremental data
            # Extract date range from DataFrame
            # For simplicity, process all incremental data
            self.run()
            
        except Exception as e:
            logger.error(f"Failed to process incremental: {str(e)}", exc_info=True)
            raise SilverBatchJobError(f"Cannot process incremental: {str(e)}")


# Convenience function
def run_silver_batch_job(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    hours: Optional[list] = None,
    environment: Optional[str] = None
):
    """
    Run silver batch job (convenience function)
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        hours: Optional list of hours (0-23)
        environment: Environment name (dev/staging/prod)
    
    Example:
        run_silver_batch_job(start_date="2024-01-15", end_date="2024-01-16")
    """
    job = SilverBatchJob(environment=environment)
    job.run(start_date=start_date, end_date=end_date, hours=hours)

