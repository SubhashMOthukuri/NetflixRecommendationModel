"""
Silver Metrics - Production ML Pipeline
Tracks metrics for silver processing pipeline: records processed, latency, quality scores
Critical for monitoring pipeline health and performance
"""
from typing import Dict, Any, Optional
from datetime import datetime
from pyspark.sql import DataFrame
from libs.logger import get_logger
from libs.exceptions import PipelineException
from observability.metrics import MetricsCollector

logger = get_logger(__name__)


class SilverMetricsError(PipelineException):
    """Raised when metrics collection fails"""
    pass


class SilverMetrics:
    """
    Production-grade silver metrics collector
    
    Tracks:
    - Records processed (input from bronze, output to silver)
    - Processing latency (batch time, end-to-end)
    - Data quality metrics (validation scores, quality distribution)
    - Transformation statistics (flattening, enrichment, etc.)
    - Throughput (records per second)
    
    Example:
        metrics = SilverMetrics()
        metrics.record_batch_processed(1000, 950, 50)
        stats = metrics.get_metrics_summary()
    """
    
    def __init__(self, metrics_collector: Optional[MetricsCollector] = None):
        """
        Initialize silver metrics
        
        Args:
            metrics_collector: Optional MetricsCollector instance (creates new if None)
        """
        self.metrics_collector = metrics_collector or MetricsCollector()
        self.metrics_namespace = "silver_processing"
        
        logger.info("Silver metrics initialized")
    
    def record_batch_processed(
        self,
        input_count: int,
        output_count: int,
        invalid_count: int = 0,
        batch_duration_ms: Optional[float] = None
    ):
        """
        Record batch processing metrics
        
        Args:
            input_count: Number of records read from bronze
            output_count: Number of records written to silver
            invalid_count: Number of invalid records (sent to DLQ)
            batch_duration_ms: Batch processing duration in milliseconds
        """
        try:
            # Record counters
            self.metrics_collector.increment_counter(
                f"{self.metrics_namespace}.records.input",
                input_count
            )
            self.metrics_collector.increment_counter(
                f"{self.metrics_namespace}.records.output",
                output_count
            )
            self.metrics_collector.increment_counter(
                f"{self.metrics_namespace}.records.invalid",
                invalid_count
            )
            
            # Record latency if provided
            if batch_duration_ms:
                self.metrics_collector.record_timer(
                    f"{self.metrics_namespace}.batch.duration",
                    batch_duration_ms
                )
            
            # Calculate and record throughput
            if batch_duration_ms and batch_duration_ms > 0:
                throughput = (output_count / batch_duration_ms) * 1000  # records per second
                self.metrics_collector.set_gauge(
                    f"{self.metrics_namespace}.throughput.records_per_second",
                    throughput
                )
            
            # Calculate success rate
            if input_count > 0:
                success_rate = (output_count / input_count) * 100
                self.metrics_collector.set_gauge(
                    f"{self.metrics_namespace}.success_rate.percentage",
                    success_rate
                )
            
            logger.debug(f"Recorded batch metrics: input={input_count}, output={output_count}, invalid={invalid_count}")
            
        except Exception as e:
            logger.error(f"Error recording batch metrics: {str(e)}", exc_info=True)
    
    def record_validation_metrics(
        self,
        valid_count: int,
        warning_count: int,
        invalid_count: int,
        avg_quality_score: Optional[float] = None
    ):
        """
        Record validation metrics
        
        Args:
            valid_count: Number of valid records
            warning_count: Number of warning records
            invalid_count: Number of invalid records
            avg_quality_score: Average data quality score (0.0-1.0)
        """
        try:
            self.metrics_collector.increment_counter(
                f"{self.metrics_namespace}.validation.valid",
                valid_count
            )
            self.metrics_collector.increment_counter(
                f"{self.metrics_namespace}.validation.warning",
                warning_count
            )
            self.metrics_collector.increment_counter(
                f"{self.metrics_namespace}.validation.invalid",
                invalid_count
            )
            
            if avg_quality_score is not None:
                self.metrics_collector.set_gauge(
                    f"{self.metrics_namespace}.validation.avg_quality_score",
                    avg_quality_score
                )
            
            logger.debug(f"Recorded validation metrics: valid={valid_count}, warning={warning_count}, invalid={invalid_count}")
            
        except Exception as e:
            logger.error(f"Error recording validation metrics: {str(e)}", exc_info=True)
    
    def record_transformation_metrics(
        self,
        transformation_name: str,
        input_count: int,
        output_count: int,
        duration_ms: Optional[float] = None
    ):
        """
        Record transformation metrics
        
        Args:
            transformation_name: Name of transformation (e.g., "flatten", "enrich")
            input_count: Records before transformation
            output_count: Records after transformation
            duration_ms: Transformation duration in milliseconds
        """
        try:
            self.metrics_collector.increment_counter(
                f"{self.metrics_namespace}.transform.{transformation_name}.input",
                input_count
            )
            self.metrics_collector.increment_counter(
                f"{self.metrics_namespace}.transform.{transformation_name}.output",
                output_count
            )
            
            if duration_ms:
                self.metrics_collector.record_timer(
                    f"{self.metrics_namespace}.transform.{transformation_name}.duration",
                    duration_ms
                )
            
            logger.debug(f"Recorded transformation metrics: {transformation_name}")
            
        except Exception as e:
            logger.error(f"Error recording transformation metrics: {str(e)}", exc_info=True)
    
    def record_enrichment_metrics(
        self,
        enrichment_name: str,
        records_enriched: int,
        records_missing: int
    ):
        """
        Record enrichment metrics
        
        Args:
            enrichment_name: Name of enrichment (e.g., "user_segments", "geo_ip")
            records_enriched: Number of records successfully enriched
            records_missing: Number of records with missing lookup data
        """
        try:
            self.metrics_collector.increment_counter(
                f"{self.metrics_namespace}.enrichment.{enrichment_name}.enriched",
                records_enriched
            )
            self.metrics_collector.increment_counter(
                f"{self.metrics_namespace}.enrichment.{enrichment_name}.missing",
                records_missing
            )
            
            logger.debug(f"Recorded enrichment metrics: {enrichment_name}")
            
        except Exception as e:
            logger.error(f"Error recording enrichment metrics: {str(e)}", exc_info=True)
    
    def record_write_metrics(
        self,
        records_written: int,
        write_duration_ms: Optional[float] = None,
        files_written: int = 1
    ):
        """
        Record write metrics
        
        Args:
            records_written: Number of records written
            write_duration_ms: Write duration in milliseconds
            files_written: Number of files written
        """
        try:
            self.metrics_collector.increment_counter(
                f"{self.metrics_namespace}.write.records",
                records_written
            )
            self.metrics_collector.increment_counter(
                f"{self.metrics_namespace}.write.files",
                files_written
            )
            
            if write_duration_ms:
                self.metrics_collector.record_timer(
                    f"{self.metrics_namespace}.write.duration",
                    write_duration_ms
                )
            
            logger.debug(f"Recorded write metrics: records={records_written}, files={files_written}")
            
        except Exception as e:
            logger.error(f"Error recording write metrics: {str(e)}", exc_info=True)
    
    def record_error(
        self,
        error_type: str,
        error_count: int = 1,
        error_message: Optional[str] = None
    ):
        """
        Record error metrics
        
        Args:
            error_type: Type of error (e.g., "validation_error", "write_error")
            error_count: Number of errors
            error_message: Optional error message
        """
        try:
            self.metrics_collector.increment_counter(
                f"{self.metrics_namespace}.errors.{error_type}",
                error_count
            )
            
            logger.warning(f"Recorded error: {error_type} (count: {error_count})")
            
        except Exception as e:
            logger.error(f"Error recording error metrics: {str(e)}", exc_info=True)
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """
        Get summary of all silver metrics
        
        Returns:
            Dictionary with metrics summary
        """
        try:
            # Get all metrics from collector
            all_metrics = self.metrics_collector.get_all_metrics()
            
            # Filter silver metrics
            silver_counters = {
                k: v for k, v in all_metrics.get("counters", {}).items()
                if k.startswith(self.metrics_namespace)
            }
            silver_timers = {
                k: v for k, v in all_metrics.get("timers", {}).items()
                if k.startswith(self.metrics_namespace)
            }
            silver_gauges = {
                k: v for k, v in all_metrics.get("gauges", {}).items()
                if k.startswith(self.metrics_namespace)
            }
            
            summary = {
                "namespace": self.metrics_namespace,
                "counters": silver_counters,
                "timers": silver_timers,
                "gauges": silver_gauges,
                "timestamp": datetime.now().isoformat()
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting metrics summary: {str(e)}", exc_info=True)
            return {"error": str(e)}


# Convenience function
def get_silver_metrics() -> SilverMetrics:
    """
    Get silver metrics instance (convenience function)
    
    Returns:
        SilverMetrics instance
    
    Example:
        metrics = get_silver_metrics()
        metrics.record_batch_processed(1000, 950, 50)
    """
    return SilverMetrics()

