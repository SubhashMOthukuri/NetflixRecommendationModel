"""
Item Features - Gold Layer
Computes item-level (video) features from silver data
Aggregates item interactions over time windows (1d, 7d, 30d)
"""
from typing import Optional, List, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    when, lit, datediff, current_timestamp, to_timestamp, countDistinct,
    expr, first, last
)
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class ItemFeaturesError(PipelineException):
    """Raised when item feature computation fails"""
    pass


class ItemFeatures:
    """
    Production-grade item (video) feature engineering
    
    Computes item-level aggregations from silver events:
    - View count (1d, 7d, 30d, total)
    - Completion rate
    - Average watch time
    - Category performance
    - Popularity scores
    
    Example:
        engineer = ItemFeatures(spark)
        item_features_df = engineer.compute_features(silver_df, timestamp_col="event_timestamp")
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize item feature engineer
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        logger.info("Item feature engineer initialized")
    
    def compute_features(
        self,
        df: DataFrame,
        timestamp_col: str = "event_timestamp",
        item_id_col: str = "video_id",
        point_in_time: Optional[str] = None
    ) -> DataFrame:
        """
        Compute item-level features with point-in-time correctness
        
        Args:
            df: Silver DataFrame with item events
            timestamp_col: Timestamp column name
            item_id_col: Item ID column name (video_id)
            point_in_time: Point-in-time timestamp (YYYY-MM-DD HH:MM:SS) for time travel
        
        Returns:
            DataFrame with item features (one row per item)
        
        Example:
            # Current features
            features = engineer.compute_features(silver_df)
            
            # Point-in-time features (for training data)
            features = engineer.compute_features(
                silver_df,
                point_in_time="2024-01-15 10:00:00"
            )
        """
        try:
            logger.info("Computing item features...")
            
            # Filter to point-in-time if provided (time travel)
            if point_in_time:
                df = df.filter(col(timestamp_col) <= to_timestamp(lit(point_in_time)))
                logger.info(f"Filtered to point-in-time: {point_in_time}")
            
            # Ensure timestamp is timestamp type
            if timestamp_col in df.columns:
                df = df.withColumn(timestamp_col, to_timestamp(col(timestamp_col) / 1000))
            
            # Filter video events only
            video_events = df.filter(
                (col("event_type").like("%video%")) &
                (col(item_id_col).isNotNull())
            )
            
            # Compute all feature groups
            logger.info("Computing view count features...")
            view_count_features = self._compute_view_count_features(video_events, item_id_col, timestamp_col)
            
            logger.info("Computing completion features...")
            completion_features = self._compute_completion_features(video_events, item_id_col, timestamp_col)
            
            logger.info("Computing watch time features...")
            watch_time_features = self._compute_watch_time_features(video_events, item_id_col, timestamp_col)
            
            logger.info("Computing user engagement features...")
            user_engagement_features = self._compute_user_engagement_features(video_events, item_id_col, timestamp_col)
            
            logger.info("Computing popularity features...")
            popularity_features = self._compute_popularity_features(video_events, item_id_col, timestamp_col)
            
            # Join all feature groups
            logger.info("Joining feature groups...")
            result = view_count_features
            
            for feature_df in [completion_features, watch_time_features, user_engagement_features, popularity_features]:
                result = result.join(
                    feature_df,
                    on=item_id_col,
                    how="outer"
                )
            
            # Fill nulls with 0 for numeric features
            numeric_cols = [c for c in result.columns if c != item_id_col]
            for col_name in numeric_cols:
                result = result.withColumn(col_name, when(col(col_name).isNull(), lit(0)).otherwise(col(col_name)))
            
            logger.info(f"Item features computed: {result.count()} items")
            return result
            
        except Exception as e:
            logger.error(f"Failed to compute item features: {str(e)}", exc_info=True)
            raise ItemFeaturesError(f"Cannot compute item features: {str(e)}")
    
    def _compute_view_count_features(
        self,
        df: DataFrame,
        item_id_col: str,
        timestamp_col: str
    ) -> DataFrame:
        """
        Compute view count features (1d, 7d, 30d, total, unique users)
        
        Returns:
            DataFrame with view_count_1d, view_count_7d, view_count_30d, view_count_total, unique_users_total
        """
        try:
            current_ts = current_timestamp()
            
            # 1 day window
            views_1d = df.filter(
                datediff(current_ts, col(timestamp_col)) <= 1
            ).groupBy(item_id_col).agg(
                count("event_id").alias("view_count_1d")
            )
            
            # 7 day window
            views_7d = df.filter(
                datediff(current_ts, col(timestamp_col)) <= 7
            ).groupBy(item_id_col).agg(
                count("event_id").alias("view_count_7d")
            )
            
            # 30 day window
            views_30d = df.filter(
                datediff(current_ts, col(timestamp_col)) <= 30
            ).groupBy(item_id_col).agg(
                count("event_id").alias("view_count_30d")
            )
            
            # Total views and unique users
            views_total = df.groupBy(item_id_col).agg(
                count("event_id").alias("view_count_total"),
                countDistinct("user_id").alias("unique_users_total")
            )
            
            # Join all windows
            result = views_total
            for df_window in [views_30d, views_7d, views_1d]:
                result = result.join(df_window, on=item_id_col, how="outer")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to compute view count features: {str(e)}", exc_info=True)
            raise ItemFeaturesError(f"Cannot compute view count features: {str(e)}")
    
    def _compute_completion_features(
        self,
        df: DataFrame,
        item_id_col: str,
        timestamp_col: str
    ) -> DataFrame:
        """
        Compute completion features (completion rate, completion count)
        
        Returns:
            DataFrame with completion_rate, completion_count, start_count
        """
        try:
            # Count starts
            starts = df.filter(
                col("event_type").like("%start%")
            ).groupBy(item_id_col).agg(
                count("event_id").alias("start_count")
            )
            
            # Count completions
            completions = df.filter(
                col("event_type").like("%complete%")
            ).groupBy(item_id_col).agg(
                count("event_id").alias("completion_count")
            )
            
            # Calculate completion rate
            result = starts.join(completions, on=item_id_col, how="outer").withColumn(
                "completion_rate",
                when(col("start_count") > 0, col("completion_count") / col("start_count"))
                .otherwise(lit(0.0))
            )
            
            return result.select(item_id_col, "completion_rate", "completion_count", "start_count")
            
        except Exception as e:
            logger.error(f"Failed to compute completion features: {str(e)}", exc_info=True)
            raise ItemFeaturesError(f"Cannot compute completion features: {str(e)}")
    
    def _compute_watch_time_features(
        self,
        df: DataFrame,
        item_id_col: str,
        timestamp_col: str
    ) -> DataFrame:
        """
        Compute watch time features (avg watch time, total watch time, watch time by window)
        
        Returns:
            DataFrame with avg_watch_time, total_watch_time, avg_watch_time_7d, avg_watch_time_30d
        """
        try:
            # Filter events with play position
            watch_events = df.filter(col("play_position_seconds").isNotNull())
            
            current_ts = current_timestamp()
            
            # Average watch time (overall)
            avg_watch = watch_events.groupBy(item_id_col).agg(
                avg("play_position_seconds").alias("avg_watch_time"),
                spark_sum("play_position_seconds").alias("total_watch_time")
            )
            
            # Average watch time (7d window)
            avg_watch_7d = watch_events.filter(
                datediff(current_ts, col(timestamp_col)) <= 7
            ).groupBy(item_id_col).agg(
                avg("play_position_seconds").alias("avg_watch_time_7d")
            )
            
            # Average watch time (30d window)
            avg_watch_30d = watch_events.filter(
                datediff(current_ts, col(timestamp_col)) <= 30
            ).groupBy(item_id_col).agg(
                avg("play_position_seconds").alias("avg_watch_time_30d")
            )
            
            # Join
            result = avg_watch
            for df_window in [avg_watch_30d, avg_watch_7d]:
                result = result.join(df_window, on=item_id_col, how="outer")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to compute watch time features: {str(e)}", exc_info=True)
            raise ItemFeaturesError(f"Cannot compute watch time features: {str(e)}")
    
    def _compute_user_engagement_features(
        self,
        df: DataFrame,
        item_id_col: str,
        timestamp_col: str
    ) -> DataFrame:
        """
        Compute user engagement features (avg sessions per user, return rate)
        
        Returns:
            DataFrame with avg_sessions_per_user, return_user_rate
        """
        try:
            # Average sessions per user (users who watched this video)
            sessions_per_user = df.filter(
                col("session_id").isNotNull()
            ).groupBy(item_id_col, "user_id").agg(
                countDistinct("session_id").alias("sessions_count")
            ).groupBy(item_id_col).agg(
                avg("sessions_count").alias("avg_sessions_per_user")
            )
            
            # Return user rate (users who watched video more than once)
            total_users = df.groupBy(item_id_col).agg(
                countDistinct("user_id").alias("total_users")
            )
            
            return_users = df.groupBy(item_id_col, "user_id").agg(
                count("event_id").alias("interaction_count")
            ).filter(col("interaction_count") > 1).groupBy(item_id_col).agg(
                countDistinct("user_id").alias("return_users")
            )
            
            return_rate = total_users.join(return_users, on=item_id_col, how="outer").withColumn(
                "return_user_rate",
                when(col("total_users") > 0, col("return_users") / col("total_users"))
                .otherwise(lit(0.0))
            ).select(item_id_col, "return_user_rate")
            
            # Join
            result = sessions_per_user.join(return_rate, on=item_id_col, how="outer")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to compute user engagement features: {str(e)}", exc_info=True)
            raise ItemFeaturesError(f"Cannot compute user engagement features: {str(e)}")
    
    def _compute_popularity_features(
        self,
        df: DataFrame,
        item_id_col: str,
        timestamp_col: str
    ) -> DataFrame:
        """
        Compute popularity features (trending score, growth rate)
        
        Returns:
            DataFrame with popularity_score, growth_rate_7d, trending_score
        """
        try:
            current_ts = current_timestamp()
            
            # Views in last 7 days vs previous 7 days (growth rate)
            views_last_7d = df.filter(
                (datediff(current_ts, col(timestamp_col)) <= 7) &
                (datediff(current_ts, col(timestamp_col)) > 0)
            ).groupBy(item_id_col).agg(
                count("event_id").alias("views_last_7d")
            )
            
            views_prev_7d = df.filter(
                (datediff(current_ts, col(timestamp_col)) > 7) &
                (datediff(current_ts, col(timestamp_col)) <= 14)
            ).groupBy(item_id_col).agg(
                count("event_id").alias("views_prev_7d")
            )
            
            growth_rate = views_last_7d.join(views_prev_7d, on=item_id_col, how="outer").withColumn(
                "growth_rate_7d",
                when(col("views_prev_7d") > 0, (col("views_last_7d") - col("views_prev_7d")) / col("views_prev_7d"))
                .otherwise(lit(0.0))
            )
            
            # Popularity score (weighted combination of views, completion rate, watch time)
            # Get completion rate and watch time
            completion = df.filter(
                col("event_type").like("%complete%")
            ).groupBy(item_id_col).agg(
                count("event_id").alias("completions")
            )
            
            total_views = df.groupBy(item_id_col).agg(
                count("event_id").alias("total_views")
            )
            
            avg_watch = df.filter(
                col("play_position_seconds").isNotNull()
            ).groupBy(item_id_col).agg(
                avg("play_position_seconds").alias("avg_watch")
            )
            
            # Calculate popularity score (normalized combination)
            popularity = total_views.join(completion, on=item_id_col, how="outer").join(
                avg_watch, on=item_id_col, how="outer"
            ).withColumn(
                "completion_ratio",
                when(col("total_views") > 0, col("completions") / col("total_views"))
                .otherwise(lit(0.0))
            ).withColumn(
                "popularity_score",
                (col("total_views") / 1000.0) * 0.4 +  # Views weight
                (col("completion_ratio")) * 0.4 +  # Completion weight
                (col("avg_watch") / 3600.0) * 0.2  # Watch time weight (normalized to hours)
            ).select(item_id_col, "popularity_score")
            
            # Trending score (recent views vs historical)
            views_recent = df.filter(
                datediff(current_ts, col(timestamp_col)) <= 1
            ).groupBy(item_id_col).agg(
                count("event_id").alias("views_recent")
            )
            
            views_historical = df.filter(
                datediff(current_ts, col(timestamp_col)) > 1
            ).groupBy(item_id_col).agg(
                count("event_id").alias("views_historical")
            )
            
            trending = views_recent.join(views_historical, on=item_id_col, how="outer").withColumn(
                "trending_score",
                when(col("views_historical") > 0, col("views_recent") / col("views_historical"))
                .otherwise(col("views_recent"))
            ).select(item_id_col, "trending_score")
            
            # Join all
            result = growth_rate.join(popularity, on=item_id_col, how="outer")
            result = result.join(trending, on=item_id_col, how="outer")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to compute popularity features: {str(e)}", exc_info=True)
            raise ItemFeaturesError(f"Cannot compute popularity features: {str(e)}")

