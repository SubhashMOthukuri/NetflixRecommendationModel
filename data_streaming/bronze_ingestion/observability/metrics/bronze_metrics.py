"""
Bronze Metrics - Production ML Pipeline
Tracks metrics for bronze ingestion pipeline: records processed, latency, errors
Critical for monitoring pipeline health and performance
"""
from typing import Dict, Any, Optional
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import col, count, sum as spark_sum, avg, max as spark_max, min as spark_min
from libs.logger import get_logger
from libs.exceptions import PipelineException
from observability.metrics import MetricsCollector

logger = get_logger(__name__)


class BronzeMetricsError(PipelineException):
    """Raised when metrics collection fails"""
    pass


class BronzeMetrics:
    """
    Production-grade bronze metrics collector
    
    Tracks:
    - Records processed (input, output, invalid)
    - Processing latency (batch time, end-to-end)
    - Error rates (validation failures, DLQ records)
    - Data quality metrics (validation scores)
    - Throughput (records per second)
    
    Example:
        metrics = BronzeMetrics()
        metrics.record_batch_processed(1000, 950, 50)
        stats = metrics.get_metrics_summary()
    """
    
    def __init__(self, metrics_collector: Optional[MetricsCollector] = None):
        """
        Initialize bronze metrics
        
        Args:
            metrics_collector: Optional MetricsCollector instance (creates new if None)
        """
        self.metrics_collector = metrics_collector or MetricsCollector()
        self.metrics_namespace = "bronze_ingestion"
        
        logger.info("Bronze metrics initialized")
    
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
            input_count: Number of records read from Kafka
            output_count: Number of records written to bronze
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
        invalid_count: int,
        validation_errors: Optional[Dict[str, int]] = None
    ):
        """
        Record validation metrics
        
        Args:
            valid_count: Number of valid records
            invalid_count: Number of invalid records
            validation_errors: Optional dictionary of error types -> counts
        """
        try:
            self.metrics_collector.increment_counter(
                f"{self.metrics_namespace}.validation.valid",
                valid_count
            )
            self.metrics_collector.increment_counter(
                f"{self.metrics_namespace}.validation.invalid",
                invalid_count
            )
            
            if validation_errors:
                for error_type, count in validation_errors.items():
                    self.metrics_collector.increment_counter(
                        f"{self.metrics_namespace}.validation.errors.{error_type}",
                        count
                    )
            
            logger.debug(f"Recorded validation metrics: valid={valid_count}, invalid={invalid_count}")
            
        except Exception as e:
            logger.error(f"Error recording validation metrics: {str(e)}", exc_info=True)
    
    def record_deduplication_metrics(
        self,
        input_count: int,
        output_count: int,
        duplicates_removed: int
    ):
        """
        Record deduplication metrics
        
        Args:
            input_count: Records before deduplication
            output_count: Records after deduplication
            duplicates_removed: Number of duplicates removed
        """
        try:
            self.metrics_collector.increment_counter(
                f"{self.metrics_namespace}.deduplication.input",
                input_count
            )
            self.metrics_collector.increment_counter(
                f"{self.metrics_namespace}.deduplication.output",
                output_count
            )
            self.metrics_collector.increment_counter(
                f"{self.metrics_namespace}.deduplication.removed",
                duplicates_removed
            )
            
            # Calculate duplicate rate
            if input_count > 0:
                duplicate_rate = (duplicates_removed / input_count) * 100
                self.metrics_collector.set_gauge(
                    f"{self.metrics_namespace}.deduplication.duplicate_rate.percentage",
                    duplicate_rate
                )
            
            logger.debug(f"Recorded deduplication metrics: removed={duplicates_removed}")
            
        except Exception as e:
            logger.error(f"Error recording deduplication metrics: {str(e)}", exc_info=True)
    
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
    
    def get_metrics_from_query(self, query: StreamingQuery) -> Dict[str, Any]:
        """
        Extract metrics from streaming query progress
        
        Args:
            query: StreamingQuery instance
        
        Returns:
            Dictionary with query metrics
        """
        try:
            if not query.isActive:
                return {"error": "Query is not active"}
            
            progress = query.lastProgress
            
            if not progress:
                return {"error": "No progress information available"}
            
            # Extract metrics
            metrics = {
                "query_id": query.id,
                "batch_id": progress.get("batchId"),
                "num_input_rows": progress.get("numInputRows", 0),
                "input_rows_per_second": progress.get("inputRowsPerSecond", 0),
                "processed_rows_per_second": progress.get("processedRowsPerSecond", 0),
                "batch_duration_ms": progress.get("batchDuration", {}).get("totalDurationMs", 0),
                "sources": progress.get("sources", []),
                "sink": progress.get("sink", {})
            }
            
            # Record metrics
            if metrics["num_input_rows"] > 0:
                self.record_batch_processed(
                    metrics["num_input_rows"],
                    metrics.get("sink", {}).get("numOutputRows", metrics["num_input_rows"]),
                    batch_duration_ms=metrics["batch_duration_ms"]
                )
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error getting metrics from query: {str(e)}", exc_info=True)
            return {"error": str(e)}
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """
        Get summary of all bronze metrics
        
        Returns:
            Dictionary with metrics summary
        """
        try:
            # Get all metrics from collector
            all_metrics = self.metrics_collector.get_all_metrics()
            
            # Filter bronze metrics
            bronze_counters = {
                k: v for k, v in all_metrics.get("counters", {}).items()
                if k.startswith(self.metrics_namespace)
            }
            bronze_timers = {
                k: v for k, v in all_metrics.get("timers", {}).items()
                if k.startswith(self.metrics_namespace)
            }
            bronze_gauges = {
                k: v for k, v in all_metrics.get("gauges", {}).items()
                if k.startswith(self.metrics_namespace)
            }
            
            summary = {
                "namespace": self.metrics_namespace,
                "counters": bronze_counters,
                "timers": bronze_timers,
                "gauges": bronze_gauges,
                "timestamp": datetime.now().isoformat()
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting metrics summary: {str(e)}", exc_info=True)
            return {"error": str(e)}


# Convenience function
def get_bronze_metrics() -> BronzeMetrics:
    """
    Get bronze metrics instance (convenience function)
    
    Returns:
        BronzeMetrics instance
    
    Example:
        metrics = get_bronze_metrics()
        metrics.record_batch_processed(1000, 950, 50)
    """
    return BronzeMetrics()

