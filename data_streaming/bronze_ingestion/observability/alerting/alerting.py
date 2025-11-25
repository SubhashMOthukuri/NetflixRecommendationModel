"""
Alerting System - Production ML Pipeline
Sends alerts when pipeline issues occur: errors, high latency, low throughput
Critical for production monitoring and incident response
"""
from typing import Dict, Any, Optional, List
from datetime import datetime
from enum import Enum
from libs.config_loader import load_config, get_config_value
from libs.logger import get_logger
from libs.exceptions import PipelineException
from data_streaming.bronze_ingestion.observability.metrics.bronze_metrics import BronzeMetrics

logger = get_logger(__name__)


class AlertLevel(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertingError(PipelineException):
    """Raised when alerting fails"""
    pass


class Alerting:
    """
    Production-grade alerting system
    
    Sends alerts for:
    - High error rates
    - Low throughput
    - High latency
    - Validation failures
    - Pipeline failures
    
    Example:
        alerting = Alerting()
        alerting.check_and_alert(metrics_summary)
        # Alerts sent if thresholds exceeded
    """
    
    def __init__(
        self,
        config_path: str = "spark_config.yaml",
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
        
        # Default thresholds
        if not self.alert_thresholds:
            self.alert_thresholds = {
                "error_rate_percentage": 5.0,  # Alert if > 5% errors
                "low_throughput_rps": 100,  # Alert if < 100 records/sec
                "high_latency_ms": 60000,  # Alert if > 60 seconds
                "validation_failure_rate": 10.0,  # Alert if > 10% validation failures
                "dlq_rate_percentage": 1.0  # Alert if > 1% to DLQ
            }
        
        logger.info(f"Alerting system initialized (enabled: {self.enabled})")
    
    def check_and_alert(
        self,
        metrics_summary: Dict[str, Any],
        query_stats: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Check metrics and send alerts if thresholds exceeded
        
        Args:
            metrics_summary: Metrics summary from BronzeMetrics
            query_stats: Optional streaming query stats
        
        Returns:
            List of alerts sent
        
        Example:
            alerting = Alerting()
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
            
            # Check throughput
            throughput_alert = self._check_throughput(metrics_summary)
            if throughput_alert:
                alerts_sent.append(throughput_alert)
            
            # Check latency
            latency_alert = self._check_latency(metrics_summary, query_stats)
            if latency_alert:
                alerts_sent.append(latency_alert)
            
            # Check validation failures
            validation_alert = self._check_validation_failures(metrics_summary)
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
            
            total_count = counters.get("bronze_ingestion.records.input", 0)
            
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
    
    def _check_throughput(self, metrics_summary: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Check if throughput is below threshold"""
        try:
            gauges = metrics_summary.get("gauges", {})
            throughput = gauges.get("bronze_ingestion.throughput.records_per_second", 0)
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
    
    def _check_latency(
        self,
        metrics_summary: Dict[str, Any],
        query_stats: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Check if latency exceeds threshold"""
        try:
            # Check from query stats first
            if query_stats:
                batch_duration = query_stats.get("batch_duration_ms", 0)
                threshold = self.alert_thresholds.get("high_latency_ms", 60000)
                
                if batch_duration > threshold:
                    return {
                        "level": AlertLevel.WARNING,
                        "title": "High Latency",
                        "message": f"Batch duration {batch_duration}ms exceeds threshold {threshold}ms",
                        "metric": "latency",
                        "value": batch_duration,
                        "threshold": threshold
                    }
            
            # Check from metrics summary
            timers = metrics_summary.get("timers", {})
            batch_timer = timers.get("bronze_ingestion.batch.duration", {})
            if batch_timer:
                avg_latency = batch_timer.get("avg", 0)
                threshold = self.alert_thresholds.get("high_latency_ms", 60000)
                
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
    
    def _check_validation_failures(self, metrics_summary: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Check if validation failure rate exceeds threshold"""
        try:
            counters = metrics_summary.get("counters", {})
            invalid_count = counters.get("bronze_ingestion.validation.invalid", 0)
            valid_count = counters.get("bronze_ingestion.validation.valid", 0)
            total = invalid_count + valid_count
            
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
            dlq_count = counters.get("bronze_ingestion.records.invalid", 0)
            total_count = counters.get("bronze_ingestion.records.input", 0)
            
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
        # TODO: Implement email sending using SMTP or email service
    
    def _send_slack_alert(self, alert: Dict[str, Any]):
        """Send alert to Slack (placeholder)"""
        logger.warning("Slack alerting not implemented - requires Slack webhook configuration")
        # TODO: Implement Slack webhook posting
    
    def _send_pagerduty_alert(self, alert: Dict[str, Any]):
        """Send alert to PagerDuty (placeholder)"""
        logger.warning("PagerDuty alerting not implemented - requires PagerDuty API configuration")
        # TODO: Implement PagerDuty API integration


# Convenience function
def check_and_alert(
    metrics_summary: Dict[str, Any],
    query_stats: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Check metrics and send alerts (convenience function)
    
    Args:
        metrics_summary: Metrics summary
        query_stats: Optional query stats
    
    Returns:
        List of alerts sent
    
    Example:
        alerts = check_and_alert(metrics_summary)
    """
    alerting = Alerting()
    return alerting.check_and_alert(metrics_summary, query_stats)

