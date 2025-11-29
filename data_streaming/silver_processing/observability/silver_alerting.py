"""
Silver Alerting - Production ML Pipeline
Sends alerts when silver processing issues occur: errors, low quality, failures
Critical for production monitoring and incident response
"""
from typing import Dict, Any, Optional, List
from datetime import datetime
from enum import Enum
from libs.config_loader import load_config, get_config_value
from libs.logger import get_logger
from libs.exceptions import PipelineException
from data_streaming.silver_processing.observability.silver_metrics import SilverMetrics

logger = get_logger(__name__)


class AlertLevel(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class SilverAlertingError(PipelineException):
    """Raised when alerting fails"""
    pass


class SilverAlerting:
    """
    Production-grade alerting system for silver processing
    
    Sends alerts for:
    - High error rates
    - Low data quality scores
    - Low throughput
    - High latency
    - Validation failures
    - Pipeline failures
    
    Example:
        alerting = SilverAlerting()
        alerting.check_and_alert(metrics_summary)
        # Alerts sent if thresholds exceeded
    """
    
    def __init__(
        self,
        config_path: str = "config/spark_config.yaml",
        enabled: bool = True
    ):
        """
        Initialize alerting system
        
        Args:
            config_path: Path to config file
            enabled: Enable/disable alerting
        """
        self.enabled = enabled
        
        # Load config
        try:
            self.config = load_config(config_path)
            monitoring_config = self.config.get("monitoring", {})
            alerting_config = monitoring_config.get("alerting", {})
            
            self.enabled = alerting_config.get("enabled", enabled)
            self.alert_thresholds = alerting_config.get("thresholds", {})
            self.alert_channels = alerting_config.get("channels", ["log"])
            
        except Exception as e:
            logger.warning(f"Could not load config: {str(e)}, using defaults")
            self.alert_thresholds = {}
            self.alert_channels = ["log"]
        
        # Default thresholds for silver processing
        if not self.alert_thresholds:
            self.alert_thresholds = {
                "error_rate_percentage": 5.0,  # Alert if > 5% errors
                "low_quality_score": 0.7,  # Alert if avg quality < 0.7
                "low_throughput_rps": 100,  # Alert if < 100 records/sec
                "high_latency_ms": 300000,  # Alert if > 5 minutes
                "validation_failure_rate": 10.0,  # Alert if > 10% validation failures
                "dlq_rate_percentage": 1.0  # Alert if > 1% to DLQ
            }
        
        logger.info(f"Silver alerting system initialized (enabled: {self.enabled})")
    
    def check_and_alert(
        self,
        metrics_summary: Dict[str, Any],
        validation_stats: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Check metrics and send alerts if thresholds exceeded
        
        Args:
            metrics_summary: Metrics summary from SilverMetrics
            validation_stats: Optional validation statistics
        
        Returns:
            List of alerts sent
        
        Example:
            alerting = SilverAlerting()
            alerts = alerting.check_and_alert(metrics_summary)
        """
        if not self.enabled:
            return []
        
        alerts_sent = []
        
        try:
            # Check error rate
            error_alert = self._check_error_rate(metrics_summary)
            if error_alert:
                alerts_sent.append(error_alert)
            
            # Check data quality score
            quality_alert = self._check_quality_score(metrics_summary, validation_stats)
            if quality_alert:
                alerts_sent.append(quality_alert)
            
            # Check throughput
            throughput_alert = self._check_throughput(metrics_summary)
            if throughput_alert:
                alerts_sent.append(throughput_alert)
            
            # Check latency
            latency_alert = self._check_latency(metrics_summary)
            if latency_alert:
                alerts_sent.append(latency_alert)
            
            # Check validation failures
            validation_alert = self._check_validation_failures(metrics_summary, validation_stats)
            if validation_alert:
                alerts_sent.append(validation_alert)
            
            # Check DLQ rate
            dlq_alert = self._check_dlq_rate(metrics_summary)
            if dlq_alert:
                alerts_sent.append(dlq_alert)
            
            # Send alerts
            for alert in alerts_sent:
                self._send_alert(alert)
            
            if alerts_sent:
                logger.warning(f"Sent {len(alerts_sent)} alerts")
            
            return alerts_sent
            
        except Exception as e:
            logger.error(f"Error checking and alerting: {str(e)}", exc_info=True)
            return []
    
    def _check_error_rate(self, metrics_summary: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Check if error rate exceeds threshold"""
        try:
            counters = metrics_summary.get("counters", {})
            error_count = sum([
                v for k, v in counters.items()
                if "error" in k.lower()
            ])
            
            total_count = counters.get("silver_processing.records.input", 0)
            
            if total_count > 0:
                error_rate = (error_count / total_count) * 100
                threshold = self.alert_thresholds.get("error_rate_percentage", 5.0)
                
                if error_rate > threshold:
                    return {
                        "level": AlertLevel.ERROR,
                        "title": "High Error Rate",
                        "message": f"Error rate {error_rate:.2f}% exceeds threshold {threshold}%",
                        "metric": "error_rate",
                        "value": error_rate,
                        "threshold": threshold
                    }
            
            return None
            
        except Exception as e:
            logger.error(f"Error checking error rate: {str(e)}")
            return None
    
    def _check_quality_score(
        self,
        metrics_summary: Dict[str, Any],
        validation_stats: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Check if data quality score is below threshold"""
        try:
            # Check from validation stats first
            if validation_stats:
                avg_score = validation_stats.get("average_quality_score")
                if avg_score is not None:
                    threshold = self.alert_thresholds.get("low_quality_score", 0.7)
                    if avg_score < threshold:
                        return {
                            "level": AlertLevel.WARNING,
                            "title": "Low Data Quality Score",
                            "message": f"Average quality score {avg_score:.2f} below threshold {threshold}",
                            "metric": "quality_score",
                            "value": avg_score,
                            "threshold": threshold
                        }
            
            # Check from metrics
            gauges = metrics_summary.get("gauges", {})
            quality_score = gauges.get("silver_processing.validation.avg_quality_score")
            if quality_score:
                threshold = self.alert_thresholds.get("low_quality_score", 0.7)
                if quality_score < threshold:
                    return {
                        "level": AlertLevel.WARNING,
                        "title": "Low Data Quality Score",
                        "message": f"Quality score {quality_score:.2f} below threshold {threshold}",
                        "metric": "quality_score",
                        "value": quality_score,
                        "threshold": threshold
                    }
            
            return None
            
        except Exception as e:
            logger.error(f"Error checking quality score: {str(e)}")
            return None
    
    def _check_throughput(self, metrics_summary: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Check if throughput is below threshold"""
        try:
            gauges = metrics_summary.get("gauges", {})
            throughput = gauges.get("silver_processing.throughput.records_per_second", 0)
            threshold = self.alert_thresholds.get("low_throughput_rps", 100)
            
            if throughput > 0 and throughput < threshold:
                return {
                    "level": AlertLevel.WARNING,
                    "title": "Low Throughput",
                    "message": f"Throughput {throughput:.2f} records/sec below threshold {threshold}",
                    "metric": "throughput",
                    "value": throughput,
                    "threshold": threshold
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error checking throughput: {str(e)}")
            return None
    
    def _check_latency(self, metrics_summary: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Check if latency exceeds threshold"""
        try:
            timers = metrics_summary.get("timers", {})
            batch_timer = timers.get("silver_processing.batch.duration", {})
            if batch_timer:
                avg_latency = batch_timer.get("avg", 0)
                threshold = self.alert_thresholds.get("high_latency_ms", 300000)  # 5 minutes
                
                if avg_latency > threshold:
                    return {
                        "level": AlertLevel.WARNING,
                        "title": "High Latency",
                        "message": f"Average latency {avg_latency:.2f}ms exceeds threshold {threshold}ms",
                        "metric": "latency",
                        "value": avg_latency,
                        "threshold": threshold
                    }
            
            return None
            
        except Exception as e:
            logger.error(f"Error checking latency: {str(e)}")
            return None
    
    def _check_validation_failures(
        self,
        metrics_summary: Dict[str, Any],
        validation_stats: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Check if validation failure rate exceeds threshold"""
        try:
            if validation_stats:
                invalid_count = validation_stats.get("invalid_count", 0)
                total = validation_stats.get("total_records", 0)
                
                if total > 0:
                    failure_rate = (invalid_count / total) * 100
                    threshold = self.alert_thresholds.get("validation_failure_rate", 10.0)
                    
                    if failure_rate > threshold:
                        return {
                            "level": AlertLevel.ERROR,
                            "title": "High Validation Failure Rate",
                            "message": f"Validation failure rate {failure_rate:.2f}% exceeds threshold {threshold}%",
                            "metric": "validation_failure_rate",
                            "value": failure_rate,
                            "threshold": threshold
                        }
            
            return None
            
        except Exception as e:
            logger.error(f"Error checking validation failures: {str(e)}")
            return None
    
    def _check_dlq_rate(self, metrics_summary: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Check if DLQ rate exceeds threshold"""
        try:
            counters = metrics_summary.get("counters", {})
            dlq_count = counters.get("silver_processing.records.invalid", 0)
            total_count = counters.get("silver_processing.records.input", 0)
            
            if total_count > 0:
                dlq_rate = (dlq_count / total_count) * 100
                threshold = self.alert_thresholds.get("dlq_rate_percentage", 1.0)
                
                if dlq_rate > threshold:
                    return {
                        "level": AlertLevel.WARNING,
                        "title": "High DLQ Rate",
                        "message": f"DLQ rate {dlq_rate:.2f}% exceeds threshold {threshold}%",
                        "metric": "dlq_rate",
                        "value": dlq_rate,
                        "threshold": threshold
                    }
            
            return None
            
        except Exception as e:
            logger.error(f"Error checking DLQ rate: {str(e)}")
            return None
    
    def _send_alert(self, alert: Dict[str, Any]):
        """
        Send alert through configured channels
        
        Args:
            alert: Alert dictionary
        """
        try:
            level = alert.get("level", AlertLevel.INFO)
            title = alert.get("title", "Alert")
            message = alert.get("message", "")
            
            # Send to configured channels
            for channel in self.alert_channels:
                if channel == "log":
                    self._send_log_alert(alert)
                elif channel == "email":
                    self._send_email_alert(alert)
                elif channel == "slack":
                    self._send_slack_alert(alert)
                elif channel == "pagerduty":
                    self._send_pagerduty_alert(alert)
                else:
                    logger.warning(f"Unknown alert channel: {channel}")
            
            logger.info(f"Alert sent: {title} - {message}")
            
        except Exception as e:
            logger.error(f"Error sending alert: {str(e)}", exc_info=True)
    
    def _send_log_alert(self, alert: Dict[str, Any]):
        """Send alert to logs"""
        level = alert.get("level", AlertLevel.INFO)
        title = alert.get("title", "Alert")
        message = alert.get("message", "")
        
        if level == AlertLevel.CRITICAL:
            logger.critical(f"ALERT [CRITICAL]: {title} - {message}")
        elif level == AlertLevel.ERROR:
            logger.error(f"ALERT [ERROR]: {title} - {message}")
        elif level == AlertLevel.WARNING:
            logger.warning(f"ALERT [WARNING]: {title} - {message}")
        else:
            logger.info(f"ALERT [INFO]: {title} - {message}")
    
    def _send_email_alert(self, alert: Dict[str, Any]):
        """Send alert via email (placeholder)"""
        logger.warning("Email alerting not implemented - requires email service configuration")
    
    def _send_slack_alert(self, alert: Dict[str, Any]):
        """Send alert to Slack (placeholder)"""
        logger.warning("Slack alerting not implemented - requires Slack webhook configuration")
    
    def _send_pagerduty_alert(self, alert: Dict[str, Any]):
        """Send alert to PagerDuty (placeholder)"""
        logger.warning("PagerDuty alerting not implemented - requires PagerDuty API configuration")


# Convenience function
def check_and_alert_silver(
    metrics_summary: Dict[str, Any],
    validation_stats: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Check metrics and send alerts (convenience function)
    
    Args:
        metrics_summary: Metrics summary
        validation_stats: Optional validation statistics
    
    Returns:
        List of alerts sent
    
    Example:
        alerts = check_and_alert_silver(metrics_summary)
    """
    alerting = SilverAlerting()
    return alerting.check_and_alert(metrics_summary, validation_stats)

