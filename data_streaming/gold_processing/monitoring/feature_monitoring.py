"""
Feature Monitoring - Gold Layer
Monitors feature health (drift detection, freshness, completeness, distribution monitoring)
Enables proactive detection of feature issues
"""
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, mean, stddev, count, isnull, isnan, min as spark_min, max as spark_max,
    lit, when, percentile_approx, abs as spark_abs
)
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class FeatureMonitoringError(PipelineException):
    """Raised when feature monitoring operations fail"""
    pass


class FeatureMonitoring:
    """
    Production-grade feature monitoring
    
    Monitors feature health:
    - Feature drift detection (distribution changes)
    - Feature freshness (last update time)
    - Feature completeness (null percentage)
    - Distribution tracking (mean, std changes)
    - Anomaly detection and alerting
    
    Example:
        monitor = FeatureMonitoring(spark)
        report = monitor.monitor_features(current_df, reference_df, feature_columns=["watch_time"])
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize feature monitor
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        self.drift_threshold = 0.1  # 10% change threshold
        self.freshness_threshold_hours = 24  # 24 hours freshness threshold
        logger.info("Feature monitor initialized")
    
    def monitor_features(
        self,
        current_df: DataFrame,
        feature_columns: List[str],
        reference_df: Optional[DataFrame] = None,
        timestamp_col: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Monitor features and generate health report
        
        Args:
            current_df: Current feature DataFrame
            feature_columns: List of feature column names to monitor
            reference_df: Optional reference DataFrame for drift detection
            timestamp_col: Optional timestamp column for freshness check
        
        Returns:
            Dictionary with monitoring report
        
        Example:
            report = monitor.monitor_features(
                current_df,
                feature_columns=["watch_time", "video_count"],
                reference_df=reference_df
            )
        """
        try:
            logger.info(f"Monitoring {len(feature_columns)} features...")
            
            monitoring_report = {
                "timestamp": datetime.now().isoformat(),
                "features": {},
                "overall_health": "healthy",
                "alerts": []
            }
            
            # Monitor each feature
            for feature_col in feature_columns:
                if feature_col not in current_df.columns:
                    logger.warning(f"Feature column {feature_col} not found")
                    continue
                
                logger.info(f"Monitoring feature: {feature_col}")
                
                feature_report = {
                    "feature_name": feature_col,
                    "health_status": "healthy",
                    "issues": []
                }
                
                # Check freshness
                if timestamp_col:
                    freshness = self._check_freshness(current_df, timestamp_col)
                    feature_report["freshness"] = freshness
                    if freshness["status"] != "fresh":
                        feature_report["health_status"] = "degraded"
                        feature_report["issues"].append(f"Feature is stale: {freshness['hours_since_update']:.1f} hours")
                
                # Check completeness
                completeness = self._check_completeness(current_df, feature_col)
                feature_report["completeness"] = completeness
                if completeness["null_percentage"] > 0.1:  # More than 10% nulls
                    feature_report["health_status"] = "degraded"
                    feature_report["issues"].append(f"High null percentage: {completeness['null_percentage']:.2%}")
                
                # Check drift (if reference provided)
                if reference_df is not None:
                    drift = self._check_drift(current_df, reference_df, feature_col)
                    feature_report["drift"] = drift
                    if drift["drift_detected"]:
                        feature_report["health_status"] = "degraded"
                        feature_report["issues"].append(f"Feature drift detected: {drift['drift_score']:.2f}")
                
                # Check distribution
                distribution = self._check_distribution(current_df, feature_col)
                feature_report["distribution"] = distribution
                
                # Check for anomalies
                anomalies = self._detect_anomalies(current_df, feature_col)
                feature_report["anomalies"] = anomalies
                if anomalies["anomaly_count"] > 0:
                    feature_report["health_status"] = "degraded"
                    feature_report["issues"].append(f"Anomalies detected: {anomalies['anomaly_count']} records")
                
                monitoring_report["features"][feature_col] = feature_report
                
                # Add to alerts if unhealthy
                if feature_report["health_status"] != "healthy":
                    monitoring_report["alerts"].append({
                        "feature": feature_col,
                        "status": feature_report["health_status"],
                        "issues": feature_report["issues"]
                    })
            
            # Determine overall health
            unhealthy_count = sum(
                1 for f in monitoring_report["features"].values()
                if f["health_status"] != "healthy"
            )
            
            if unhealthy_count == 0:
                monitoring_report["overall_health"] = "healthy"
            elif unhealthy_count < len(feature_columns) / 2:
                monitoring_report["overall_health"] = "degraded"
            else:
                monitoring_report["overall_health"] = "unhealthy"
            
            logger.info(f"Monitoring complete: overall_health = {monitoring_report['overall_health']}")
            
            return monitoring_report
            
        except Exception as e:
            logger.error(f"Failed to monitor features: {str(e)}", exc_info=True)
            raise FeatureMonitoringError(f"Cannot monitor features: {str(e)}")
    
    def _check_freshness(
        self,
        df: DataFrame,
        timestamp_col: str
    ) -> Dict[str, Any]:
        """
        Check feature freshness (last update time)
        
        Returns:
            Dictionary with freshness information
        """
        try:
            # Get latest timestamp
            latest_timestamp = df.agg(spark_max(col(timestamp_col))).collect()[0][0]
            
            if latest_timestamp is None:
                return {
                    "status": "unknown",
                    "latest_timestamp": None,
                    "hours_since_update": None
                }
            
            # Calculate hours since update
            if isinstance(latest_timestamp, (int, float)):
                # Unix timestamp (milliseconds)
                latest_dt = datetime.fromtimestamp(latest_timestamp / 1000)
            else:
                # Assume datetime object
                latest_dt = latest_timestamp
            
            hours_since_update = (datetime.now() - latest_dt).total_seconds() / 3600
            
            # Determine freshness status
            if hours_since_update <= self.freshness_threshold_hours:
                status = "fresh"
            elif hours_since_update <= self.freshness_threshold_hours * 2:
                status = "stale"
            else:
                status = "very_stale"
            
            return {
                "status": status,
                "latest_timestamp": latest_dt.isoformat() if hasattr(latest_dt, 'isoformat') else str(latest_dt),
                "hours_since_update": hours_since_update
            }
            
        except Exception as e:
            logger.error(f"Failed to check freshness: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "error": str(e)
            }
    
    def _check_completeness(
        self,
        df: DataFrame,
        feature_col: str
    ) -> Dict[str, Any]:
        """
        Check feature completeness (null percentage)
        
        Returns:
            Dictionary with completeness information
        """
        try:
            total_count = df.count()
            if total_count == 0:
                return {
                    "null_count": 0,
                    "null_percentage": 0.0,
                    "completeness_percentage": 0.0,
                    "status": "empty"
                }
            
            null_count = df.filter(col(feature_col).isNull() | isnan(col(feature_col))).count()
            null_percentage = null_count / total_count
            completeness_percentage = 1.0 - null_percentage
            
            # Determine completeness status
            if completeness_percentage >= 0.95:
                status = "complete"
            elif completeness_percentage >= 0.90:
                status = "mostly_complete"
            elif completeness_percentage >= 0.75:
                status = "incomplete"
            else:
                status = "very_incomplete"
            
            return {
                "null_count": null_count,
                "null_percentage": null_percentage,
                "completeness_percentage": completeness_percentage,
                "status": status
            }
            
        except Exception as e:
            logger.error(f"Failed to check completeness: {str(e)}", exc_info=True)
            return {
                "null_count": 0,
                "null_percentage": 0.0,
                "completeness_percentage": 0.0,
                "status": "error",
                "error": str(e)
            }
    
    def _check_drift(
        self,
        current_df: DataFrame,
        reference_df: DataFrame,
        feature_col: str
    ) -> Dict[str, Any]:
        """
        Check for feature drift (distribution changes)
        
        Returns:
            Dictionary with drift information
        """
        try:
            # Check if column is numeric
            col_type = dict(current_df.dtypes)[feature_col]
            if col_type not in ["int", "bigint", "double", "float"]:
                return {
                    "drift_detected": False,
                    "drift_score": 0.0,
                    "status": "non_numeric"
                }
            
            # Compute statistics for both datasets
            current_stats = current_df.agg(
                mean(col(feature_col)).alias("mean"),
                stddev(col(feature_col)).alias("std"),
                percentile_approx(col(feature_col), 0.5, 10000).alias("median")
            ).collect()[0]
            
            reference_stats = reference_df.agg(
                mean(col(feature_col)).alias("mean"),
                stddev(col(feature_col)).alias("std"),
                percentile_approx(col(feature_col), 0.5, 10000).alias("median")
            ).collect()[0]
            
            current_mean = current_stats["mean"]
            current_std = current_stats["std"]
            current_median = current_stats["median"]
            reference_mean = reference_stats["mean"]
            reference_std = reference_stats["std"]
            reference_median = reference_stats["median"]
            
            if current_mean is None or reference_mean is None:
                return {
                    "drift_detected": False,
                    "drift_score": 0.0,
                    "status": "insufficient_data"
                }
            
            # Calculate drift metrics
            mean_drift = abs(current_mean - reference_mean) / abs(reference_mean) if reference_mean != 0 else 0
            std_drift = abs(current_std - reference_std) / abs(reference_std) if reference_std and reference_std != 0 else 0
            median_drift = abs(current_median - reference_median) / abs(reference_median) if reference_median and reference_median != 0 else 0
            
            # Combined drift score
            drift_score = (mean_drift + std_drift + median_drift) / 3.0
            
            # Determine if drift detected
            drift_detected = drift_score > self.drift_threshold
            
            return {
                "drift_detected": drift_detected,
                "drift_score": drift_score,
                "mean_drift": mean_drift,
                "std_drift": std_drift,
                "median_drift": median_drift,
                "current_mean": current_mean,
                "reference_mean": reference_mean,
                "status": "drift_detected" if drift_detected else "no_drift"
            }
            
        except Exception as e:
            logger.error(f"Failed to check drift: {str(e)}", exc_info=True)
            return {
                "drift_detected": False,
                "drift_score": 0.0,
                "status": "error",
                "error": str(e)
            }
    
    def _check_distribution(
        self,
        df: DataFrame,
        feature_col: str
    ) -> Dict[str, Any]:
        """
        Check feature distribution (mean, std, percentiles)
        
        Returns:
            Dictionary with distribution information
        """
        try:
            # Check if column is numeric
            col_type = dict(df.dtypes)[feature_col]
            if col_type not in ["int", "bigint", "double", "float"]:
                return {
                    "status": "non_numeric"
                }
            
            # Compute distribution statistics
            stats = df.agg(
                mean(col(feature_col)).alias("mean"),
                stddev(col(feature_col)).alias("std"),
                spark_min(col(feature_col)).alias("min"),
                spark_max(col(feature_col)).alias("max"),
                percentile_approx(col(feature_col), 0.25, 10000).alias("q25"),
                percentile_approx(col(feature_col), 0.5, 10000).alias("median"),
                percentile_approx(col(feature_col), 0.75, 10000).alias("q75"),
                percentile_approx(col(feature_col), 0.95, 10000).alias("p95")
            ).collect()[0]
            
            return {
                "mean": stats["mean"],
                "std": stats["std"],
                "min": stats["min"],
                "max": stats["max"],
                "q25": stats["q25"],
                "median": stats["median"],
                "q75": stats["q75"],
                "p95": stats["p95"],
                "status": "ok"
            }
            
        except Exception as e:
            logger.error(f"Failed to check distribution: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "error": str(e)
            }
    
    def _detect_anomalies(
        self,
        df: DataFrame,
        feature_col: str,
        method: str = "iqr"
    ) -> Dict[str, Any]:
        """
        Detect anomalies in feature values
        
        Args:
            df: DataFrame with features
            feature_col: Feature column name
            method: Anomaly detection method ("iqr", "zscore")
        
        Returns:
            Dictionary with anomaly information
        """
        try:
            # Check if column is numeric
            col_type = dict(df.dtypes)[feature_col]
            if col_type not in ["int", "bigint", "double", "float"]:
                return {
                    "anomaly_count": 0,
                    "anomaly_percentage": 0.0,
                    "status": "non_numeric"
                }
            
            if method == "iqr":
                # IQR method
                q25 = df.agg(percentile_approx(col(feature_col), 0.25, 10000)).collect()[0][0]
                q75 = df.agg(percentile_approx(col(feature_col), 0.75, 10000)).collect()[0][0]
                
                if q25 is None or q75 is None:
                    return {
                        "anomaly_count": 0,
                        "anomaly_percentage": 0.0,
                        "status": "insufficient_data"
                    }
                
                iqr = q75 - q25
                lower_bound = q25 - 1.5 * iqr
                upper_bound = q75 + 1.5 * iqr
                
                total_count = df.filter(col(feature_col).isNotNull()).count()
                anomaly_count = df.filter(
                    (col(feature_col) < lower_bound) | (col(feature_col) > upper_bound)
                ).count()
                
                anomaly_percentage = anomaly_count / total_count if total_count > 0 else 0.0
                
                return {
                    "anomaly_count": anomaly_count,
                    "anomaly_percentage": anomaly_percentage,
                    "lower_bound": lower_bound,
                    "upper_bound": upper_bound,
                    "method": "iqr",
                    "status": "ok"
                }
            
            else:
                # Z-score method (simplified)
                mean_val = df.agg(mean(col(feature_col))).collect()[0][0]
                std_val = df.agg(stddev(col(feature_col))).collect()[0][0]
                
                if mean_val is None or std_val is None or std_val == 0:
                    return {
                        "anomaly_count": 0,
                        "anomaly_percentage": 0.0,
                        "status": "insufficient_data"
                    }
                
                # Anomalies: |z-score| > 3
                total_count = df.filter(col(feature_col).isNotNull()).count()
                anomaly_count = df.filter(
                    spark_abs((col(feature_col) - lit(mean_val)) / lit(std_val)) > 3
                ).count()
                
                anomaly_percentage = anomaly_count / total_count if total_count > 0 else 0.0
                
                return {
                    "anomaly_count": anomaly_count,
                    "anomaly_percentage": anomaly_percentage,
                    "method": "zscore",
                    "status": "ok"
                }
            
        except Exception as e:
            logger.error(f"Failed to detect anomalies: {str(e)}", exc_info=True)
            return {
                "anomaly_count": 0,
                "anomaly_percentage": 0.0,
                "status": "error",
                "error": str(e)
            }
    
    def generate_alerts(
        self,
        monitoring_report: Dict[str, Any],
        alert_thresholds: Optional[Dict[str, float]] = None
    ) -> List[Dict[str, Any]]:
        """
        Generate alerts based on monitoring report
        
        Args:
            monitoring_report: Monitoring report from monitor_features()
            alert_thresholds: Optional custom alert thresholds
        
        Returns:
            List of alert dictionaries
        """
        try:
            alerts = []
            
            default_thresholds = {
                "drift_threshold": 0.1,
                "null_threshold": 0.1,
                "freshness_threshold_hours": 24,
                "anomaly_threshold": 0.05
            }
            
            thresholds = alert_thresholds or default_thresholds
            
            for feature_name, feature_data in monitoring_report.get("features", {}).items():
                # Check drift
                if "drift" in feature_data:
                    drift = feature_data["drift"]
                    if drift.get("drift_detected") and drift.get("drift_score", 0) > thresholds["drift_threshold"]:
                        alerts.append({
                            "level": "warning",
                            "feature": feature_name,
                            "type": "drift",
                            "message": f"Feature drift detected: {drift['drift_score']:.2%} change",
                            "timestamp": datetime.now().isoformat()
                        })
                
                # Check completeness
                if "completeness" in feature_data:
                    completeness = feature_data["completeness"]
                    if completeness.get("null_percentage", 0) > thresholds["null_threshold"]:
                        alerts.append({
                            "level": "warning",
                            "feature": feature_name,
                            "type": "completeness",
                            "message": f"High null percentage: {completeness['null_percentage']:.2%}",
                            "timestamp": datetime.now().isoformat()
                        })
                
                # Check freshness
                if "freshness" in feature_data:
                    freshness = feature_data["freshness"]
                    hours = freshness.get("hours_since_update", 0)
                    if hours and hours > thresholds["freshness_threshold_hours"]:
                        alerts.append({
                            "level": "warning",
                            "feature": feature_name,
                            "type": "freshness",
                            "message": f"Feature is stale: {hours:.1f} hours since update",
                            "timestamp": datetime.now().isoformat()
                        })
                
                # Check anomalies
                if "anomalies" in feature_data:
                    anomalies = feature_data["anomalies"]
                    if anomalies.get("anomaly_percentage", 0) > thresholds["anomaly_threshold"]:
                        alerts.append({
                            "level": "warning",
                            "feature": feature_name,
                            "type": "anomaly",
                            "message": f"High anomaly rate: {anomalies['anomaly_percentage']:.2%}",
                            "timestamp": datetime.now().isoformat()
                        })
            
            return alerts
            
        except Exception as e:
            logger.error(f"Failed to generate alerts: {str(e)}", exc_info=True)
            return []

