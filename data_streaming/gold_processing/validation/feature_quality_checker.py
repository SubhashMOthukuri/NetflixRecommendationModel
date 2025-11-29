"""
Feature Quality Checker - Gold Layer
Validates feature quality (distributions, drift detection, nulls, outliers)
Ensures features meet quality standards before storing in feature store
"""
from typing import Optional, List, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, count, when, isnan, isnull, mean, stddev, min as spark_min, max as spark_max,
    lit, expr, percentile_approx, sum as spark_sum, abs as spark_abs
)
from pyspark.sql.types import DoubleType, FloatType, IntegerType, LongType
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class FeatureQualityCheckerError(PipelineException):
    """Raised when feature quality checking fails"""
    pass


class FeatureQualityChecker:
    """
    Production-grade feature quality checker
    
    Validates feature quality:
    - Feature distributions (mean, std, percentiles)
    - Feature drift detection (distribution changes over time)
    - Null and outlier detection
    - Feature range and type validation
    - Quality score generation
    
    Example:
        checker = FeatureQualityChecker(spark)
        quality_report = checker.check_quality(feature_df, feature_columns=["watch_time", "video_count"])
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize feature quality checker
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        logger.info("Feature quality checker initialized")
    
    def check_quality(
        self,
        df: DataFrame,
        feature_columns: List[str],
        reference_df: Optional[DataFrame] = None,
        quality_threshold: float = 0.8
    ) -> Dict[str, Any]:
        """
        Check quality of features and generate quality report
        
        Args:
            df: DataFrame with features to check
            feature_columns: List of feature column names to check
            reference_df: Optional reference DataFrame for drift detection
            quality_threshold: Minimum quality score threshold (0.0-1.0)
        
        Returns:
            Dictionary with quality report (scores, issues, recommendations)
        
        Example:
            report = checker.check_quality(
                feature_df,
                feature_columns=["watch_time", "video_count"],
                quality_threshold=0.8
            )
        """
        try:
            logger.info(f"Checking quality of {len(feature_columns)} features...")
            
            quality_report = {
                "feature_scores": {},
                "overall_score": 0.0,
                "issues": [],
                "recommendations": []
            }
            
            # Check each feature
            for feature_col in feature_columns:
                if feature_col not in df.columns:
                    logger.warning(f"Feature column {feature_col} not found in DataFrame")
                    continue
                
                logger.info(f"Checking feature: {feature_col}")
                
                # Check nulls
                null_score = self._check_nulls(df, feature_col)
                
                # Check outliers
                outlier_score = self._check_outliers(df, feature_col)
                
                # Check distribution
                distribution_score = self._check_distribution(df, feature_col)
                
                # Check drift (if reference provided)
                drift_score = 1.0
                if reference_df is not None:
                    drift_score = self._check_drift(df, reference_df, feature_col)
                
                # Calculate overall feature score
                feature_score = (null_score * 0.3 + outlier_score * 0.3 + 
                               distribution_score * 0.2 + drift_score * 0.2)
                
                quality_report["feature_scores"][feature_col] = {
                    "overall_score": feature_score,
                    "null_score": null_score,
                    "outlier_score": outlier_score,
                    "distribution_score": distribution_score,
                    "drift_score": drift_score
                }
                
                # Add issues if score is low
                if feature_score < quality_threshold:
                    quality_report["issues"].append({
                        "feature": feature_col,
                        "score": feature_score,
                        "reason": "Quality score below threshold"
                    })
            
            # Calculate overall quality score
            if quality_report["feature_scores"]:
                overall_score = sum(
                    s["overall_score"] for s in quality_report["feature_scores"].values()
                ) / len(quality_report["feature_scores"])
                quality_report["overall_score"] = overall_score
            else:
                quality_report["overall_score"] = 0.0
            
            # Generate recommendations
            quality_report["recommendations"] = self._generate_recommendations(quality_report)
            
            logger.info(f"Quality check complete: overall score = {quality_report['overall_score']:.2f}")
            
            return quality_report
            
        except Exception as e:
            logger.error(f"Failed to check feature quality: {str(e)}", exc_info=True)
            raise FeatureQualityCheckerError(f"Cannot check feature quality: {str(e)}")
    
    def _check_nulls(self, df: DataFrame, feature_col: str) -> float:
        """
        Check null percentage in feature
        
        Returns:
            Quality score (1.0 = no nulls, 0.0 = all nulls)
        """
        try:
            total_count = df.count()
            if total_count == 0:
                return 0.0
            
            null_count = df.filter(col(feature_col).isNull() | isnan(col(feature_col))).count()
            null_percentage = null_count / total_count
            
            # Score: 1.0 if no nulls, decreases linearly with null percentage
            score = max(0.0, 1.0 - null_percentage * 2.0)  # Penalize nulls heavily
            
            logger.debug(f"Null check for {feature_col}: {null_percentage:.2%} nulls, score: {score:.2f}")
            
            return score
            
        except Exception as e:
            logger.error(f"Failed to check nulls: {str(e)}", exc_info=True)
            return 0.0
    
    def _check_outliers(self, df: DataFrame, feature_col: str) -> float:
        """
        Check for outliers using IQR (Interquartile Range) method
        
        Returns:
            Quality score (1.0 = no outliers, 0.0 = many outliers)
        """
        try:
            # Check if column is numeric
            col_type = dict(df.dtypes)[feature_col]
            if col_type not in ["int", "bigint", "double", "float"]:
                return 1.0  # Non-numeric columns don't have outliers
            
            # Compute IQR
            q25 = df.agg(percentile_approx(col(feature_col), 0.25, 10000)).collect()[0][0]
            q75 = df.agg(percentile_approx(col(feature_col), 0.75, 10000)).collect()[0][0]
            
            if q25 is None or q75 is None:
                return 0.5  # Can't compute outliers
            
            iqr = q75 - q25
            lower_bound = q25 - 1.5 * iqr
            upper_bound = q75 + 1.5 * iqr
            
            # Count outliers
            total_count = df.filter(col(feature_col).isNotNull()).count()
            if total_count == 0:
                return 0.0
            
            outlier_count = df.filter(
                (col(feature_col) < lower_bound) | (col(feature_col) > upper_bound)
            ).count()
            
            outlier_percentage = outlier_count / total_count
            
            # Score: 1.0 if < 5% outliers, decreases with more outliers
            if outlier_percentage < 0.05:
                score = 1.0
            elif outlier_percentage < 0.10:
                score = 0.8
            elif outlier_percentage < 0.20:
                score = 0.5
            else:
                score = 0.2
            
            logger.debug(f"Outlier check for {feature_col}: {outlier_percentage:.2%} outliers, score: {score:.2f}")
            
            return score
            
        except Exception as e:
            logger.error(f"Failed to check outliers: {str(e)}", exc_info=True)
            return 0.5
    
    def _check_distribution(self, df: DataFrame, feature_col: str) -> float:
        """
        Check feature distribution (mean, std, skewness)
        
        Returns:
            Quality score based on distribution health
        """
        try:
            # Check if column is numeric
            col_type = dict(df.dtypes)[feature_col]
            if col_type not in ["int", "bigint", "double", "float"]:
                return 1.0  # Non-numeric columns don't have distribution issues
            
            # Compute statistics
            stats = df.agg(
                mean(col(feature_col)).alias("mean"),
                stddev(col(feature_col)).alias("std"),
                spark_min(col(feature_col)).alias("min"),
                spark_max(col(feature_col)).alias("max")
            ).collect()[0]
            
            mean_val = stats["mean"]
            std_val = stats["std"]
            min_val = stats["min"]
            max_val = stats["max"]
            
            if mean_val is None or std_val is None:
                return 0.5
            
            # Check for constant values (std = 0)
            if std_val == 0:
                logger.warning(f"Feature {feature_col} has zero variance (constant values)")
                return 0.3
            
            # Check for extreme skewness (mean far from median)
            median = df.agg(percentile_approx(col(feature_col), 0.5, 10000)).collect()[0][0]
            if median is None:
                return 0.5
            
            skewness_ratio = abs(mean_val - median) / std_val if std_val > 0 else 0
            
            # Score based on skewness
            if skewness_ratio < 0.1:
                score = 1.0  # Normal distribution
            elif skewness_ratio < 0.5:
                score = 0.8  # Slightly skewed
            elif skewness_ratio < 1.0:
                score = 0.6  # Moderately skewed
            else:
                score = 0.4  # Highly skewed
            
            logger.debug(f"Distribution check for {feature_col}: skewness_ratio={skewness_ratio:.2f}, score: {score:.2f}")
            
            return score
            
        except Exception as e:
            logger.error(f"Failed to check distribution: {str(e)}", exc_info=True)
            return 0.5
    
    def _check_drift(
        self,
        current_df: DataFrame,
        reference_df: DataFrame,
        feature_col: str
    ) -> float:
        """
        Check for feature drift (distribution changes between reference and current)
        
        Returns:
            Quality score (1.0 = no drift, 0.0 = significant drift)
        """
        try:
            # Check if column is numeric
            col_type = dict(current_df.dtypes)[feature_col]
            if col_type not in ["int", "bigint", "double", "float"]:
                return 1.0  # Non-numeric columns don't have drift
            
            # Compute statistics for both datasets
            current_stats = current_df.agg(
                mean(col(feature_col)).alias("mean"),
                stddev(col(feature_col)).alias("std")
            ).collect()[0]
            
            reference_stats = reference_df.agg(
                mean(col(feature_col)).alias("mean"),
                stddev(col(feature_col)).alias("std")
            ).collect()[0]
            
            current_mean = current_stats["mean"]
            current_std = current_stats["std"]
            reference_mean = reference_stats["mean"]
            reference_std = reference_stats["std"]
            
            if current_mean is None or current_std is None or reference_mean is None or reference_std is None:
                return 0.5
            
            # Calculate drift metrics
            mean_drift = abs(current_mean - reference_mean) / reference_mean if reference_mean != 0 else 0
            std_drift = abs(current_std - reference_std) / reference_std if reference_std != 0 else 0
            
            # Combined drift score
            drift_score = (mean_drift + std_drift) / 2.0
            
            # Convert to quality score (lower drift = higher score)
            if drift_score < 0.05:
                score = 1.0  # No significant drift
            elif drift_score < 0.10:
                score = 0.8  # Minor drift
            elif drift_score < 0.20:
                score = 0.6  # Moderate drift
            elif drift_score < 0.50:
                score = 0.4  # Significant drift
            else:
                score = 0.2  # Major drift
            
            logger.debug(f"Drift check for {feature_col}: drift_score={drift_score:.2f}, quality_score: {score:.2f}")
            
            return score
            
        except Exception as e:
            logger.error(f"Failed to check drift: {str(e)}", exc_info=True)
            return 0.5
    
    def _generate_recommendations(self, quality_report: Dict[str, Any]) -> List[str]:
        """
        Generate recommendations based on quality report
        
        Returns:
            List of recommendation strings
        """
        recommendations = []
        
        overall_score = quality_report.get("overall_score", 0.0)
        
        if overall_score < 0.7:
            recommendations.append("Overall feature quality is low - review feature engineering pipeline")
        
        # Check for specific issues
        for issue in quality_report.get("issues", []):
            feature = issue["feature"]
            score = issue["score"]
            
            if score < 0.5:
                recommendations.append(f"Feature {feature} has critical quality issues - investigate immediately")
            elif score < 0.7:
                recommendations.append(f"Feature {feature} quality is below threshold - consider improvements")
        
        # Check for drift issues
        for feature, scores in quality_report.get("feature_scores", {}).items():
            drift_score = scores.get("drift_score", 1.0)
            if drift_score < 0.6:
                recommendations.append(f"Feature {feature} shows significant drift - review data pipeline")
        
        if not recommendations:
            recommendations.append("All features meet quality standards")
        
        return recommendations
    
    def validate_features(
        self,
        df: DataFrame,
        feature_columns: List[str],
        quality_threshold: float = 0.8
    ) -> DataFrame:
        """
        Validate features and add quality scores to DataFrame
        
        Args:
            df: DataFrame with features
            feature_columns: List of feature column names
            quality_threshold: Minimum quality score threshold
        
        Returns:
            DataFrame with quality scores added
        
        Example:
            validated_df = checker.validate_features(
                feature_df,
                feature_columns=["watch_time", "video_count"]
            )
        """
        try:
            logger.info(f"Validating {len(feature_columns)} features...")
            
            # Check quality
            quality_report = self.check_quality(df, feature_columns, quality_threshold=quality_threshold)
            
            # Add overall quality score to DataFrame
            overall_score = quality_report["overall_score"]
            result = df.withColumn("feature_quality_score", lit(overall_score))
            
            # Add per-feature scores
            for feature_col, scores in quality_report["feature_scores"].items():
                feature_score = scores["overall_score"]
                result = result.withColumn(
                    f"{feature_col}_quality_score",
                    lit(feature_score)
                )
            
            logger.info(f"Features validated: overall score = {overall_score:.2f}")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to validate features: {str(e)}", exc_info=True)
            raise FeatureQualityCheckerError(f"Cannot validate features: {str(e)}")

