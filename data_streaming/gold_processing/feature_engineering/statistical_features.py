"""
Statistical Features - Gold Layer
Computes statistical features (mean, std, percentiles, distributions) from aggregated data
Used for normalization, scaling, and distribution-aware modeling
"""
from typing import Optional, List, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, mean, stddev, min as spark_min, max as spark_max,
    when, lit, expr, percentile_approx, count, sum as spark_sum,
    sqrt, pow as spark_pow
)
from pyspark.sql.types import DoubleType
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class StatisticalFeaturesError(PipelineException):
    """Raised when statistical feature computation fails"""
    pass


class StatisticalFeatures:
    """
    Production-grade statistical feature engineering
    
    Computes statistical aggregations from feature data:
    - Mean, std, min, max
    - Percentiles (25th, 50th, 75th, 95th)
    - Z-scores for normalization
    - Distribution features (skewness, kurtosis)
    
    Example:
        engineer = StatisticalFeatures(spark)
        stats_df = engineer.compute_features(feature_df, numeric_columns=["watch_time", "video_count"])
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize statistical feature engineer
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        logger.info("Statistical feature engineer initialized")
    
    def compute_features(
        self,
        df: DataFrame,
        numeric_columns: List[str],
        group_by_column: Optional[str] = None,
        prefix: str = "stat_"
    ) -> DataFrame:
        """
        Compute statistical features for numeric columns
        
        Args:
            df: DataFrame with numeric features
            numeric_columns: List of numeric column names to compute statistics for
            group_by_column: Optional column to group by (e.g., "user_id" for per-user stats)
            prefix: Prefix for statistical feature names
        
        Returns:
            DataFrame with statistical features added
        
        Example:
            # Global statistics
            stats_df = engineer.compute_features(
                df,
                numeric_columns=["watch_time", "video_count"]
            )
            
            # Per-user statistics
            stats_df = engineer.compute_features(
                df,
                numeric_columns=["watch_time", "video_count"],
                group_by_column="user_id"
            )
        """
        try:
            logger.info(f"Computing statistical features for {len(numeric_columns)} columns...")
            
            # Filter to only numeric columns that exist
            existing_numeric_cols = [c for c in numeric_columns if c in df.columns]
            if not existing_numeric_cols:
                logger.warning("No valid numeric columns found")
                return df
            
            logger.info(f"Computing statistics for: {existing_numeric_cols}")
            
            # Compute basic statistics
            logger.info("Computing basic statistics (mean, std, min, max)...")
            basic_stats = self._compute_basic_statistics(df, existing_numeric_cols, group_by_column, prefix)
            
            # Compute percentiles
            logger.info("Computing percentiles...")
            percentile_stats = self._compute_percentiles(df, existing_numeric_cols, group_by_column, prefix)
            
            # Compute z-scores
            logger.info("Computing z-scores...")
            zscore_features = self._compute_z_scores(df, existing_numeric_cols, group_by_column, prefix)
            
            # Compute distribution features
            logger.info("Computing distribution features...")
            distribution_features = self._compute_distribution_features(df, existing_numeric_cols, group_by_column, prefix)
            
            # Join all statistical features
            logger.info("Joining statistical features...")
            result = df
            
            if group_by_column:
                # Join on group_by_column
                for stats_df in [basic_stats, percentile_stats, zscore_features, distribution_features]:
                    if stats_df is not None:
                        result = result.join(stats_df, on=group_by_column, how="left")
            else:
                # Broadcast join for global statistics
                for stats_df in [basic_stats, percentile_stats, zscore_features, distribution_features]:
                    if stats_df is not None:
                        # Global stats - add as constants
                        stats_row = stats_df.collect()[0] if stats_df.count() > 0 else None
                        if stats_row:
                            for col_name in stats_df.columns:
                                if col_name != group_by_column:
                                    result = result.withColumn(col_name, lit(stats_row[col_name]))
            
            logger.info("Statistical features computed")
            return result
            
        except Exception as e:
            logger.error(f"Failed to compute statistical features: {str(e)}", exc_info=True)
            raise StatisticalFeaturesError(f"Cannot compute statistical features: {str(e)}")
    
    def _compute_basic_statistics(
        self,
        df: DataFrame,
        numeric_columns: List[str],
        group_by_column: Optional[str],
        prefix: str
    ) -> DataFrame:
        """
        Compute basic statistics (mean, std, min, max)
        
        Returns:
            DataFrame with mean, std, min, max for each column
        """
        try:
            agg_exprs = []
            
            for col_name in numeric_columns:
                agg_exprs.extend([
                    mean(col(col_name)).alias(f"{prefix}{col_name}_mean"),
                    stddev(col(col_name)).alias(f"{prefix}{col_name}_std"),
                    spark_min(col(col_name)).alias(f"{prefix}{col_name}_min"),
                    spark_max(col(col_name)).alias(f"{prefix}{col_name}_max")
                ])
            
            if group_by_column:
                result = df.groupBy(group_by_column).agg(*agg_exprs)
            else:
                result = df.agg(*agg_exprs)
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to compute basic statistics: {str(e)}", exc_info=True)
            raise StatisticalFeaturesError(f"Cannot compute basic statistics: {str(e)}")
    
    def _compute_percentiles(
        self,
        df: DataFrame,
        numeric_columns: List[str],
        group_by_column: Optional[str],
        prefix: str
    ) -> DataFrame:
        """
        Compute percentiles (25th, 50th, 75th, 95th)
        
        Returns:
            DataFrame with percentile features
        """
        try:
            agg_exprs = []
            percentiles = [0.25, 0.50, 0.75, 0.95]
            percentile_names = ["p25", "p50", "p75", "p95"]
            
            for col_name in numeric_columns:
                for p, p_name in zip(percentiles, percentile_names):
                    agg_exprs.append(
                        percentile_approx(col(col_name), p, 10000).alias(f"{prefix}{col_name}_{p_name}")
                    )
            
            if group_by_column:
                result = df.groupBy(group_by_column).agg(*agg_exprs)
            else:
                result = df.agg(*agg_exprs)
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to compute percentiles: {str(e)}", exc_info=True)
            raise StatisticalFeaturesError(f"Cannot compute percentiles: {str(e)}")
    
    def _compute_z_scores(
        self,
        df: DataFrame,
        numeric_columns: List[str],
        group_by_column: Optional[str],
        prefix: str
    ) -> DataFrame:
        """
        Compute z-scores for normalization (z = (x - mean) / std)
        
        Returns:
            DataFrame with z-score features added
        """
        try:
            result = df
            
            # First compute mean and std
            if group_by_column:
                stats = df.groupBy(group_by_column).agg(
                    *[mean(col(c)).alias(f"{c}_mean") for c in numeric_columns],
                    *[stddev(col(c)).alias(f"{c}_std") for c in numeric_columns]
                )
                
                result = result.join(stats, on=group_by_column, how="left")
                
                for col_name in numeric_columns:
                    result = result.withColumn(
                        f"{prefix}{col_name}_zscore",
                        when(
                            col(f"{col_name}_std") > 0,
                            (col(col_name) - col(f"{col_name}_mean")) / col(f"{col_name}_std")
                        ).otherwise(lit(0.0))
                    )
                
                # Drop intermediate columns
                drop_cols = [f"{c}_mean" for c in numeric_columns] + [f"{c}_std" for c in numeric_columns]
                result = result.drop(*drop_cols)
            else:
                # Global z-scores
                for col_name in numeric_columns:
                    mean_val = df.agg(mean(col(col_name))).collect()[0][0]
                    std_val = df.agg(stddev(col(col_name))).collect()[0][0]
                    
                    if std_val and std_val > 0:
                        result = result.withColumn(
                            f"{prefix}{col_name}_zscore",
                            (col(col_name) - lit(mean_val)) / lit(std_val)
                        )
                    else:
                        result = result.withColumn(
                            f"{prefix}{col_name}_zscore",
                            lit(0.0)
                        )
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to compute z-scores: {str(e)}", exc_info=True)
            raise StatisticalFeaturesError(f"Cannot compute z-scores: {str(e)}")
    
    def _compute_distribution_features(
        self,
        df: DataFrame,
        numeric_columns: List[str],
        group_by_column: Optional[str],
        prefix: str
    ) -> DataFrame:
        """
        Compute distribution features (skewness, kurtosis approximation)
        
        Note: Full skewness/kurtosis requires expensive calculations.
        This computes simplified versions.
        
        Returns:
            DataFrame with distribution features
        """
        try:
            # Simplified skewness: (mean - median) / std
            # Simplified kurtosis: (p95 - p5) / std (approximation)
            
            agg_exprs = []
            
            for col_name in numeric_columns:
                # Mean
                mean_col = mean(col(col_name)).alias(f"{col_name}_mean")
                # Median (p50)
                median_col = percentile_approx(col(col_name), 0.5, 10000).alias(f"{col_name}_median")
                # Std
                std_col = stddev(col(col_name)).alias(f"{col_name}_std")
                # P95 and P5 for kurtosis approximation
                p95_col = percentile_approx(col(col_name), 0.95, 10000).alias(f"{col_name}_p95")
                p5_col = percentile_approx(col(col_name), 0.05, 10000).alias(f"{col_name}_p5")
                
                agg_exprs.extend([mean_col, median_col, std_col, p95_col, p5_col])
            
            if group_by_column:
                stats = df.groupBy(group_by_column).agg(*agg_exprs)
            else:
                stats = df.agg(*agg_exprs)
            
            # Compute skewness and kurtosis
            result = stats
            
            for col_name in numeric_columns:
                # Skewness: (mean - median) / std
                result = result.withColumn(
                    f"{prefix}{col_name}_skewness",
                    when(
                        col(f"{col_name}_std") > 0,
                        (col(f"{col_name}_mean") - col(f"{col_name}_median")) / col(f"{col_name}_std")
                    ).otherwise(lit(0.0))
                )
                
                # Kurtosis approximation: (p95 - p5) / std
                result = result.withColumn(
                    f"{prefix}{col_name}_kurtosis",
                    when(
                        col(f"{col_name}_std") > 0,
                        (col(f"{col_name}_p95") - col(f"{col_name}_p5")) / col(f"{col_name}_std")
                    ).otherwise(lit(0.0))
                )
            
            # Drop intermediate columns
            drop_cols = []
            for col_name in numeric_columns:
                drop_cols.extend([
                    f"{col_name}_mean", f"{col_name}_median", f"{col_name}_std",
                    f"{col_name}_p95", f"{col_name}_p5"
                ])
            result = result.drop(*drop_cols)
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to compute distribution features: {str(e)}", exc_info=True)
            raise StatisticalFeaturesError(f"Cannot compute distribution features: {str(e)}")
    
    def normalize_features(
        self,
        df: DataFrame,
        numeric_columns: List[str],
        method: str = "zscore"
    ) -> DataFrame:
        """
        Normalize numeric features using specified method
        
        Args:
            df: DataFrame with features to normalize
            numeric_columns: List of numeric column names
            method: Normalization method ("zscore", "minmax", "robust")
        
        Returns:
            DataFrame with normalized features
        
        Example:
            normalized_df = engineer.normalize_features(
                df,
                numeric_columns=["watch_time", "video_count"],
                method="zscore"
            )
        """
        try:
            logger.info(f"Normalizing {len(numeric_columns)} columns using {method} method...")
            
            result = df
            
            if method == "zscore":
                # Z-score normalization: (x - mean) / std
                for col_name in numeric_columns:
                    if col_name in df.columns:
                        mean_val = df.agg(mean(col(col_name))).collect()[0][0]
                        std_val = df.agg(stddev(col(col_name))).collect()[0][0]
                        
                        if std_val and std_val > 0:
                            result = result.withColumn(
                                f"{col_name}_normalized",
                                (col(col_name) - lit(mean_val)) / lit(std_val)
                            )
                        else:
                            result = result.withColumn(
                                f"{col_name}_normalized",
                                lit(0.0)
                            )
            
            elif method == "minmax":
                # Min-max normalization: (x - min) / (max - min)
                for col_name in numeric_columns:
                    if col_name in df.columns:
                        min_val = df.agg(spark_min(col(col_name))).collect()[0][0]
                        max_val = df.agg(spark_max(col(col_name))).collect()[0][0]
                        
                        if max_val and max_val != min_val:
                            result = result.withColumn(
                                f"{col_name}_normalized",
                                (col(col_name) - lit(min_val)) / (lit(max_val) - lit(min_val))
                            )
                        else:
                            result = result.withColumn(
                                f"{col_name}_normalized",
                                lit(0.0)
                            )
            
            elif method == "robust":
                # Robust normalization: (x - median) / IQR
                for col_name in numeric_columns:
                    if col_name in df.columns:
                        median_val = df.agg(percentile_approx(col(col_name), 0.5, 10000)).collect()[0][0]
                        q75 = df.agg(percentile_approx(col(col_name), 0.75, 10000)).collect()[0][0]
                        q25 = df.agg(percentile_approx(col(col_name), 0.25, 10000)).collect()[0][0]
                        iqr = q75 - q25
                        
                        if iqr and iqr > 0:
                            result = result.withColumn(
                                f"{col_name}_normalized",
                                (col(col_name) - lit(median_val)) / lit(iqr)
                            )
                        else:
                            result = result.withColumn(
                                f"{col_name}_normalized",
                                lit(0.0)
                            )
            
            logger.info("Features normalized")
            return result
            
        except Exception as e:
            logger.error(f"Failed to normalize features: {str(e)}", exc_info=True)
            raise StatisticalFeaturesError(f"Cannot normalize features: {str(e)}")

