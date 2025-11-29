"""
Gold Feature Job - Gold Layer
Main orchestrator for gold layer feature engineering pipeline
Coordinates all components: read, compute, validate, write
"""
from typing import Optional, List, Dict, Any
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from libs.logger import get_logger
from libs.exceptions import PipelineException

# Core infrastructure
from data_streaming.gold_processing.core.silver_reader import SilverReader
from data_streaming.gold_processing.core.feast_client import FeastClient

# Feature engineering
from data_streaming.gold_processing.feature_engineering.user_features import UserFeatures
from data_streaming.gold_processing.feature_engineering.item_features import ItemFeatures
from data_streaming.gold_processing.feature_engineering.session_features import SessionFeatures
from data_streaming.gold_processing.feature_engineering.statistical_features import StatisticalFeatures
from data_streaming.gold_processing.feature_engineering.temporal_features import TemporalFeatures
from data_streaming.gold_processing.feature_engineering.embedding_builder import EmbeddingBuilder

# Validation
from data_streaming.gold_processing.validation.feature_quality_checker import FeatureQualityChecker
from data_streaming.gold_processing.validation.feature_schema_registry import FeatureSchemaRegistry

# Storage
from data_streaming.gold_processing.storage.gold_writer import GoldWriter

# Monitoring
from data_streaming.gold_processing.monitoring.gold_metrics import GoldMetrics
from data_streaming.gold_processing.monitoring.feature_monitoring import FeatureMonitoring

# Spark session
from data_streaming.bronze_ingestion.core.spark.spark_session import get_spark_session

logger = get_logger(__name__)


class GoldFeatureJobError(PipelineException):
    """Raised when gold feature job fails"""
    pass


class GoldFeatureJob:
    """
    Production-grade gold feature engineering job
    
    Orchestrates complete pipeline:
    1. Read from silver layer
    2. Compute features (user, item, session, statistical, temporal, embeddings)
    3. Validate features (quality, schema)
    4. Write to gold layer and Feast
    5. Track metrics and monitor health
    
    Example:
        job = GoldFeatureJob()
        job.run(start_date="2024-01-15", end_date="2024-01-16")
    """
    
    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        environment: Optional[str] = None
    ):
        """
        Initialize gold feature job
        
        Args:
            spark: Optional SparkSession (creates new if None)
            environment: Environment name (dev/staging/prod)
        """
        self.environment = environment
        
        # Initialize Spark
        if spark is None:
            logger.info("Initializing Spark session...")
            self.spark = get_spark_session(environment=environment, app_name="gold_feature_job")
        else:
            self.spark = spark
        
        # Initialize components
        logger.info("Initializing pipeline components...")
        self.silver_reader = SilverReader(self.spark)
        self.feast_client = FeastClient()
        
        # Feature engineers
        self.user_features = UserFeatures(self.spark)
        self.item_features = ItemFeatures(self.spark)
        self.session_features = SessionFeatures(self.spark)
        self.statistical_features = StatisticalFeatures(self.spark)
        self.temporal_features = TemporalFeatures(self.spark)
        self.embedding_builder = EmbeddingBuilder(self.spark)
        
        # Validation
        self.quality_checker = FeatureQualityChecker(self.spark)
        self.schema_registry = FeatureSchemaRegistry(self.spark)
        
        # Storage
        self.gold_writer = GoldWriter(self.spark)
        
        # Monitoring
        self.metrics = GoldMetrics()
        self.feature_monitor = FeatureMonitoring(self.spark)
        
        logger.info("Gold feature job initialized")
    
    def run(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        hours: Optional[List[int]] = None,
        feature_types: Optional[List[str]] = None,
        point_in_time: Optional[str] = None
    ):
        """
        Run the gold feature engineering job
        
        Args:
            start_date: Start date (YYYY-MM-DD) - filters silver partitions
            end_date: End date (YYYY-MM-DD) - filters silver partitions
            hours: Optional list of hours (0-23) to process
            feature_types: Optional list of feature types to compute (defaults to all)
            point_in_time: Optional point-in-time timestamp for time travel
        
        Example:
            job = GoldFeatureJob()
            job.run(start_date="2024-01-15", end_date="2024-01-16")
        """
        try:
            logger.info("=" * 80)
            logger.info("Starting Gold Feature Engineering Job")
            logger.info("=" * 80)
            
            start_time = datetime.now()
            
            # Step 1: Read from silver layer
            logger.info("Step 1: Reading from silver layer...")
            read_start = datetime.now()
            silver_df = self.silver_reader.read_silver(
                start_date=start_date,
                end_date=end_date,
                hours=hours
            )
            silver_count = silver_df.count()
            read_duration = (datetime.now() - read_start).total_seconds() * 1000
            logger.info(f"Read {silver_count:,} records from silver layer in {read_duration:.2f}ms")
            
            self.metrics.record_batch_processed(input_count=silver_count, output_count=0)
            self.metrics.record_latency(read_duration, operation="silver_read")
            
            if silver_count == 0:
                logger.warning("No data to process, exiting")
                return
            
            # Default feature types if not specified
            if feature_types is None:
                feature_types = ["user", "item", "session", "statistical", "temporal"]
            
            # Step 2: Compute features
            logger.info("Step 2: Computing features...")
            features_dict = {}
            
            if "user" in feature_types:
                logger.info("Computing user features...")
                compute_start = datetime.now()
                user_features_df = self.user_features.compute_features(
                    silver_df,
                    point_in_time=point_in_time
                )
                compute_duration = (datetime.now() - compute_start).total_seconds() * 1000
                user_count = user_features_df.count()
                logger.info(f"Computed user features: {user_count:,} users in {compute_duration:.2f}ms")
                features_dict["user_features"] = user_features_df
                self.metrics.record_feature_computed("user_features", feature_count=50, entity_count=user_count)
                self.metrics.record_latency(compute_duration, operation="feature_computation", feature_type="user")
            
            if "item" in feature_types:
                logger.info("Computing item features...")
                compute_start = datetime.now()
                item_features_df = self.item_features.compute_features(
                    silver_df,
                    point_in_time=point_in_time
                )
                compute_duration = (datetime.now() - compute_start).total_seconds() * 1000
                item_count = item_features_df.count()
                logger.info(f"Computed item features: {item_count:,} items in {compute_duration:.2f}ms")
                features_dict["item_features"] = item_features_df
                self.metrics.record_feature_computed("item_features", feature_count=40, entity_count=item_count)
                self.metrics.record_latency(compute_duration, operation="feature_computation", feature_type="item")
            
            if "session" in feature_types:
                logger.info("Computing session features...")
                compute_start = datetime.now()
                session_features_df = self.session_features.compute_features(
                    silver_df,
                    point_in_time=point_in_time
                )
                compute_duration = (datetime.now() - compute_start).total_seconds() * 1000
                session_count = session_features_df.count()
                logger.info(f"Computed session features: {session_count:,} sessions in {compute_duration:.2f}ms")
                features_dict["session_features"] = session_features_df
                self.metrics.record_feature_computed("session_features", feature_count=30, entity_count=session_count)
                self.metrics.record_latency(compute_duration, operation="feature_computation", feature_type="session")
            
            if "statistical" in feature_types:
                logger.info("Computing statistical features...")
                compute_start = datetime.now()
                # Apply statistical features to user features if available
                if "user_features" in features_dict:
                    statistical_df = self.statistical_features.compute_features(
                        features_dict["user_features"],
                        numeric_columns=["watch_time_total", "video_count_total"],
                        group_by_column="user_id"
                    )
                    features_dict["user_features"] = statistical_df
                compute_duration = (datetime.now() - compute_start).total_seconds() * 1000
                logger.info(f"Computed statistical features in {compute_duration:.2f}ms")
                self.metrics.record_latency(compute_duration, operation="statistical_computation")
            
            if "temporal" in feature_types:
                logger.info("Computing temporal features...")
                compute_start = datetime.now()
                temporal_df = self.temporal_features.compute_features(
                    silver_df,
                    point_in_time=point_in_time
                )
                compute_duration = (datetime.now() - compute_start).total_seconds() * 1000
                logger.info(f"Computed temporal features in {compute_duration:.2f}ms")
                # Merge temporal features with user features if available
                if "user_features" in features_dict:
                    features_dict["user_features"] = features_dict["user_features"].join(
                        temporal_df.select("user_id", "days_since_last_event", "events_per_day"),
                        on="user_id",
                        how="left"
                    )
                self.metrics.record_latency(compute_duration, operation="temporal_computation")
            
            # Step 3: Validate features
            logger.info("Step 3: Validating features...")
            validated_features = {}
            
            for feature_type, feature_df in features_dict.items():
                logger.info(f"Validating {feature_type}...")
                validate_start = datetime.now()
                
                # Quality check
                quality_report = self.quality_checker.check_quality(
                    feature_df,
                    feature_columns=[c for c in feature_df.columns if c not in ["user_id", "video_id", "session_id"]],
                    quality_threshold=0.8
                )
                
                quality_score = quality_report.get("overall_score", 0.0)
                self.metrics.record_quality_score(feature_type, quality_score)
                
                # Schema validation
                validated_df = self.schema_registry.validate_features(
                    feature_df,
                    feature_columns=[c for c in feature_df.columns if c not in ["user_id", "video_id", "session_id"]],
                    strict=False
                )
                
                validated_features[feature_type] = validated_df
                
                validate_duration = (datetime.now() - validate_start).total_seconds() * 1000
                logger.info(f"Validated {feature_type}: quality_score={quality_score:.2f} in {validate_duration:.2f}ms")
                self.metrics.record_latency(validate_duration, operation="feature_validation", feature_type=feature_type)
            
            # Step 4: Monitor features
            logger.info("Step 4: Monitoring features...")
            for feature_type, feature_df in validated_features.items():
                monitoring_report = self.feature_monitor.monitor_features(
                    feature_df,
                    feature_columns=[c for c in feature_df.columns if c not in ["user_id", "video_id", "session_id"]]
                )
                
                overall_health = monitoring_report.get("overall_health", "unknown")
                logger.info(f"Feature monitoring for {feature_type}: {overall_health}")
                
                # Generate alerts
                alerts = self.feature_monitor.generate_alerts(monitoring_report)
                if alerts:
                    logger.warning(f"Generated {len(alerts)} alerts for {feature_type}")
                    for alert in alerts:
                        logger.warning(f"Alert: {alert['type']} - {alert['message']}")
            
            # Step 5: Write to gold layer
            logger.info("Step 5: Writing features to gold layer...")
            write_start = datetime.now()
            partition_date = start_date or datetime.now().strftime("%Y-%m-%d")
            
            write_results = self.gold_writer.write_batch(
                validated_features,
                mode="append",
                partition_date=partition_date
            )
            
            write_duration = (datetime.now() - write_start).total_seconds() * 1000
            logger.info(f"Features written to gold layer in {write_duration:.2f}ms")
            
            # Record write metrics
            for feature_type, feature_df in validated_features.items():
                record_count = feature_df.count()
                self.metrics.record_feature_store_operation(
                    "write",
                    feature_type,
                    record_count,
                    write_duration / len(validated_features)
                )
            
            # Step 6: Write to Feast (optional)
            logger.info("Step 6: Writing features to Feast...")
            try:
                for feature_type, feature_df in validated_features.items():
                    feast_start = datetime.now()
                    self.feast_client.write_features_to_offline_store(
                        feature_df,
                        feature_view_name=feature_type
                    )
                    feast_duration = (datetime.now() - feast_start).total_seconds() * 1000
                    logger.info(f"Features written to Feast: {feature_type} in {feast_duration:.2f}ms")
                    self.metrics.record_feature_store_operation(
                        "write",
                        feature_type,
                        feature_df.count(),
                        feast_duration
                    )
            except Exception as e:
                logger.warning(f"Failed to write to Feast: {str(e)}")
            
            # Final metrics
            total_duration = (datetime.now() - start_time).total_seconds() * 1000
            total_output = sum(df.count() for df in validated_features.values())
            
            self.metrics.record_batch_processed(input_count=silver_count, output_count=total_output)
            self.metrics.record_latency(total_duration, operation="gold_pipeline_total")
            
            logger.info("=" * 80)
            logger.info("Gold Feature Engineering Job Complete")
            logger.info(f"Total duration: {total_duration:.2f}ms")
            logger.info(f"Input: {silver_count:,} records → Output: {total_output:,} feature records")
            logger.info("=" * 80)
            
            # Export metrics summary
            metrics_summary = self.metrics.get_metrics_summary()
            logger.info(f"Metrics summary: {metrics_summary.get('overall_health', 'N/A')}")
            
        except Exception as e:
            logger.error(f"Gold feature job failed: {str(e)}", exc_info=True)
            self.metrics.record_error("gold_job_error", str(e))
            raise GoldFeatureJobError(f"Cannot run gold feature job: {str(e)}")

