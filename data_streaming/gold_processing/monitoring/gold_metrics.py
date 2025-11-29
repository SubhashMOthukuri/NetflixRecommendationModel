"""
Gold Metrics - Gold Layer
Tracks metrics for gold layer pipeline (records processed, latency, feature counts, quality scores)
Enables monitoring and alerting for production operations
"""
from typing import Optional, Dict, Any, List
from datetime import datetime
from collections import defaultdict
import time

from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class GoldMetricsError(PipelineException):
    """Raised when metrics operations fail"""
    pass


class GoldMetrics:
    """
    Production-grade metrics tracking for gold layer
    
    Tracks comprehensive metrics:
    - Records processed (input/output counts)
    - Processing latency (p95, p99)
    - Feature counts (features computed per type)
    - Quality scores (feature quality metrics)
    - Errors and failures
    
    Example:
        metrics = GoldMetrics()
        metrics.record_batch_processed(input_count=1000, output_count=950)
        metrics.record_latency(processing_time_ms=1500)
    """
    
    def __init__(self):
        """
        Initialize gold metrics tracker
        """
        # Metrics storage (in production, use Prometheus, CloudWatch, etc.)
        self._metrics: Dict[str, Any] = defaultdict(list)
        self._counters: Dict[str, int] = defaultdict(int)
        self._gauges: Dict[str, float] = {}
        
        logger.info("Gold metrics tracker initialized")
    
    def record_batch_processed(
        self,
        input_count: int,
        output_count: int,
        feature_type: Optional[str] = None
    ):
        """
        Record batch processing metrics
        
        Args:
            input_count: Number of input records
            output_count: Number of output records
            feature_type: Optional feature type (e.g., "user_features")
        
        Example:
            metrics.record_batch_processed(input_count=1000, output_count=950)
        """
        try:
            self._counters["gold_batch_input_total"] += input_count
            self._counters["gold_batch_output_total"] += output_count
            
            if feature_type:
                self._counters[f"gold_batch_input_{feature_type}"] += input_count
                self._counters[f"gold_batch_output_{feature_type}"] += output_count
            
            # Calculate success rate
            success_rate = output_count / input_count if input_count > 0 else 0.0
            self._gauges["gold_batch_success_rate"] = success_rate
            
            logger.debug(f"Batch processed: {input_count} input → {output_count} output (success_rate: {success_rate:.2%})")
            
        except Exception as e:
            logger.error(f"Failed to record batch metrics: {str(e)}", exc_info=True)
    
    def record_latency(
        self,
        processing_time_ms: float,
        operation: str = "feature_computation",
        feature_type: Optional[str] = None
    ):
        """
        Record processing latency
        
        Args:
            processing_time_ms: Processing time in milliseconds
            operation: Operation name (e.g., "feature_computation", "feature_write")
            feature_type: Optional feature type
        
        Example:
            metrics.record_latency(processing_time_ms=1500, operation="feature_computation")
        """
        try:
            self._metrics[f"gold_latency_{operation}"].append(processing_time_ms)
            
            if feature_type:
                self._metrics[f"gold_latency_{operation}_{feature_type}"].append(processing_time_ms)
            
            # Update gauge with latest latency
            self._gauges[f"gold_latency_{operation}_latest"] = processing_time_ms
            
            # Calculate percentiles if enough samples
            if len(self._metrics[f"gold_latency_{operation}"]) >= 10:
                sorted_latencies = sorted(self._metrics[f"gold_latency_{operation}"])
                p95_idx = int(len(sorted_latencies) * 0.95)
                p99_idx = int(len(sorted_latencies) * 0.99)
                
                self._gauges[f"gold_latency_{operation}_p95"] = sorted_latencies[p95_idx] if p95_idx < len(sorted_latencies) else sorted_latencies[-1]
                self._gauges[f"gold_latency_{operation}_p99"] = sorted_latencies[p99_idx] if p99_idx < len(sorted_latencies) else sorted_latencies[-1]
            
            logger.debug(f"Latency recorded: {operation} = {processing_time_ms:.2f}ms")
            
        except Exception as e:
            logger.error(f"Failed to record latency: {str(e)}", exc_info=True)
    
    def record_feature_computed(
        self,
        feature_type: str,
        feature_count: int,
        entity_count: int
    ):
        """
        Record feature computation metrics
        
        Args:
            feature_type: Type of features (e.g., "user_features", "item_features")
            feature_count: Number of features computed
            entity_count: Number of entities (users, items, etc.)
        
        Example:
            metrics.record_feature_computed("user_features", feature_count=50, entity_count=1000)
        """
        try:
            self._counters[f"gold_features_computed_{feature_type}"] += feature_count
            self._counters[f"gold_entities_processed_{feature_type}"] += entity_count
            
            # Calculate features per entity
            features_per_entity = feature_count / entity_count if entity_count > 0 else 0.0
            self._gauges[f"gold_features_per_entity_{feature_type}"] = features_per_entity
            
            logger.debug(f"Features computed: {feature_type} = {feature_count} features for {entity_count} entities")
            
        except Exception as e:
            logger.error(f"Failed to record feature metrics: {str(e)}", exc_info=True)
    
    def record_quality_score(
        self,
        feature_type: str,
        quality_score: float,
        validation_status: str = "valid"
    ):
        """
        Record feature quality metrics
        
        Args:
            feature_type: Type of features
            quality_score: Quality score (0.0-1.0)
            validation_status: Validation status ("valid", "warning", "invalid")
        
        Example:
            metrics.record_quality_score("user_features", quality_score=0.95, validation_status="valid")
        """
        try:
            self._metrics[f"gold_quality_score_{feature_type}"].append(quality_score)
            self._gauges[f"gold_quality_score_{feature_type}_latest"] = quality_score
            
            # Track validation status
            if validation_status == "valid":
                self._counters[f"gold_validation_valid_{feature_type}"] += 1
            elif validation_status == "warning":
                self._counters[f"gold_validation_warning_{feature_type}"] += 1
            elif validation_status == "invalid":
                self._counters[f"gold_validation_invalid_{feature_type}"] += 1
            
            logger.debug(f"Quality score recorded: {feature_type} = {quality_score:.2f} ({validation_status})")
            
        except Exception as e:
            logger.error(f"Failed to record quality metrics: {str(e)}", exc_info=True)
    
    def record_error(
        self,
        error_type: str,
        error_message: str,
        feature_type: Optional[str] = None
    ):
        """
        Record error metrics
        
        Args:
            error_type: Type of error (e.g., "feature_computation_error", "validation_error")
            error_message: Error message
            feature_type: Optional feature type
        
        Example:
            metrics.record_error("feature_computation_error", "Null pointer exception", "user_features")
        """
        try:
            self._counters["gold_errors_total"] += 1
            self._counters[f"gold_errors_{error_type}"] += 1
            
            if feature_type:
                self._counters[f"gold_errors_{error_type}_{feature_type}"] += 1
            
            # Store error details
            error_entry = {
                "timestamp": datetime.now().isoformat(),
                "error_type": error_type,
                "error_message": error_message,
                "feature_type": feature_type
            }
            self._metrics["gold_errors"].append(error_entry)
            
            # Keep only last 100 errors
            if len(self._metrics["gold_errors"]) > 100:
                self._metrics["gold_errors"] = self._metrics["gold_errors"][-100:]
            
            logger.warning(f"Error recorded: {error_type} - {error_message}")
            
        except Exception as e:
            logger.error(f"Failed to record error: {str(e)}", exc_info=True)
    
    def record_feature_store_operation(
        self,
        operation: str,
        feature_type: str,
        record_count: int,
        latency_ms: float
    ):
        """
        Record feature store operation metrics
        
        Args:
            operation: Operation type ("write", "read", "materialize")
            feature_type: Type of features
            record_count: Number of records processed
            latency_ms: Operation latency in milliseconds
        
        Example:
            metrics.record_feature_store_operation("write", "user_features", record_count=1000, latency_ms=500)
        """
        try:
            self._counters[f"gold_feature_store_{operation}_total"] += record_count
            self._counters[f"gold_feature_store_{operation}_{feature_type}"] += record_count
            
            self._metrics[f"gold_feature_store_{operation}_latency"].append(latency_ms)
            self._gauges[f"gold_feature_store_{operation}_latency_latest"] = latency_ms
            
            logger.debug(f"Feature store operation: {operation} {feature_type} = {record_count} records in {latency_ms:.2f}ms")
            
        except Exception as e:
            logger.error(f"Failed to record feature store metrics: {str(e)}", exc_info=True)
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """
        Get summary of all metrics
        
        Returns:
            Dictionary with metrics summary
        
        Example:
            summary = metrics.get_metrics_summary()
        """
        try:
            summary = {
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "timestamp": datetime.now().isoformat()
            }
            
            # Calculate averages for latency metrics
            latency_metrics = {}
            for key, values in self._metrics.items():
                if key.startswith("gold_latency_"):
                    if values:
                        latency_metrics[key] = {
                            "count": len(values),
                            "avg": sum(values) / len(values),
                            "min": min(values),
                            "max": max(values)
                        }
            
            summary["latency_metrics"] = latency_metrics
            
            # Calculate error rate
            total_errors = self._counters.get("gold_errors_total", 0)
            total_processed = self._counters.get("gold_batch_input_total", 0)
            error_rate = total_errors / total_processed if total_processed > 0 else 0.0
            summary["error_rate"] = error_rate
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to get metrics summary: {str(e)}", exc_info=True)
            return {}
    
    def reset_metrics(self):
        """
        Reset all metrics (useful for testing or periodic resets)
        """
        try:
            self._metrics.clear()
            self._counters.clear()
            self._gauges.clear()
            logger.info("Metrics reset")
            
        except Exception as e:
            logger.error(f"Failed to reset metrics: {str(e)}", exc_info=True)
    
    def export_metrics(self, format: str = "dict") -> Any:
        """
        Export metrics in specified format
        
        Args:
            format: Export format ("dict", "json", "prometheus")
        
        Returns:
            Exported metrics in specified format
        
        Example:
            metrics_dict = metrics.export_metrics(format="dict")
        """
        try:
            if format == "dict":
                return self.get_metrics_summary()
            
            elif format == "json":
                import json
                return json.dumps(self.get_metrics_summary(), indent=2)
            
            elif format == "prometheus":
                # Prometheus format
                lines = []
                
                # Counters
                for key, value in self._counters.items():
                    lines.append(f"{key} {value}")
                
                # Gauges
                for key, value in self._gauges.items():
                    lines.append(f"{key} {value}")
                
                return "\n".join(lines)
            
            else:
                raise GoldMetricsError(f"Unknown export format: {format}")
            
        except Exception as e:
            logger.error(f"Failed to export metrics: {str(e)}", exc_info=True)
            raise GoldMetricsError(f"Cannot export metrics: {str(e)}")

