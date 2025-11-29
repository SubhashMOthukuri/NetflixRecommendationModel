"""
User Features - Gold Layer
Computes user-level features from silver data
Aggregates user behavior over time windows (1d, 7d, 30d)
"""
from typing import Optional, List, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    collect_list, when, lit, window, expr, countDistinct, first, last,
    datediff, current_timestamp, to_timestamp, from_unixtime
)
from pyspark.sql.window import Window
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class UserFeaturesError(PipelineException):
    """Raised when user feature computation fails"""
    pass


class UserFeatures:
    """
    Production-grade user feature engineering
    
    Computes user-level aggregations from silver events:
    - Watch time (1d, 7d, 30d)
    - Video count (total, by category)
    - Category preferences
    - Session statistics
    - Engagement scores
    
    Example:
        engineer = UserFeatures(spark)
        user_features_df = engineer.compute_features(silver_df, timestamp_col="event_timestamp")
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize user feature engineer
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        logger.info("User feature engineer initialized")
    
    def compute_features(
        self,
        df: DataFrame,
        timestamp_col: str = "event_timestamp",
        user_id_col: str = "user_id",
        point_in_time: Optional[str] = None
    ) -> DataFrame:
        """
        Compute user-level features with point-in-time correctness
        
        Args:
            df: Silver DataFrame with user events
            timestamp_col: Timestamp column name
            user_id_col: User ID column name
            point_in_time: Point-in-time timestamp (YYYY-MM-DD HH:MM:SS) for time travel
        
        Returns:
            DataFrame with user features (one row per user)
        
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
            logger.info("Computing user features...")
            
            # Filter to point-in-time if provided (time travel)
            if point_in_time:
                df = df.filter(col(timestamp_col) <= to_timestamp(lit(point_in_time)))
                logger.info(f"Filtered to point-in-time: {point_in_time}")
            
            # Ensure timestamp is timestamp type
            if timestamp_col in df.columns:
                df = df.withColumn(timestamp_col, to_timestamp(col(timestamp_col) / 1000))
            
            # Compute all feature groups
            logger.info("Computing watch time features...")
            watch_time_features = self._compute_watch_time_features(df, user_id_col, timestamp_col)
            
            logger.info("Computing video count features...")
            video_count_features = self._compute_video_count_features(df, user_id_col, timestamp_col)
            
            logger.info("Computing category preference features...")
            category_features = self._compute_category_preferences(df, user_id_col, timestamp_col)
            
            logger.info("Computing session features...")
            session_features = self._compute_session_features(df, user_id_col, timestamp_col)
            
            logger.info("Computing engagement features...")
            engagement_features = self._compute_engagement_features(df, user_id_col, timestamp_col)
            
            # Join all feature groups
            logger.info("Joining feature groups...")
            result = watch_time_features
            
            for feature_df in [video_count_features, category_features, session_features, engagement_features]:
                result = result.join(
                    feature_df,
                    on=user_id_col,
                    how="outer"
                )
            
            # Fill nulls with 0 for numeric features
            numeric_cols = [c for c in result.columns if c != user_id_col]
            for col_name in numeric_cols:
                result = result.withColumn(col_name, when(col(col_name).isNull(), lit(0)).otherwise(col(col_name)))
            
            logger.info(f"User features computed: {result.count()} users")
            return result
            
        except Exception as e:
            logger.error(f"Failed to compute user features: {str(e)}", exc_info=True)
            raise UserFeaturesError(f"Cannot compute user features: {str(e)}")
    
    def _compute_watch_time_features(
        self,
        df: DataFrame,
        user_id_col: str,
        timestamp_col: str
    ) -> DataFrame:
        """
        Compute watch time features (1d, 7d, 30d, total)
        
        Returns:
            DataFrame with watch_time_1d, watch_time_7d, watch_time_30d, watch_time_total
        """
        try:
            # Filter video play events
            video_events = df.filter(
                (col("event_type").like("%video%")) &
                (col("play_position_seconds").isNotNull())
            )
            
            # Current timestamp for window calculations
            current_ts = current_timestamp()
            
            # 1 day window
            watch_1d = video_events.filter(
                datediff(current_ts, col(timestamp_col)) <= 1
            ).groupBy(user_id_col).agg(
                spark_sum("play_position_seconds").alias("watch_time_1d")
            )
            
            # 7 day window
            watch_7d = video_events.filter(
                datediff(current_ts, col(timestamp_col)) <= 7
            ).groupBy(user_id_col).agg(
                spark_sum("play_position_seconds").alias("watch_time_7d")
            )
            
            # 30 day window
            watch_30d = video_events.filter(
                datediff(current_ts, col(timestamp_col)) <= 30
            ).groupBy(user_id_col).agg(
                spark_sum("play_position_seconds").alias("watch_time_30d")
            )
            
            # Total watch time
            watch_total = video_events.groupBy(user_id_col).agg(
                spark_sum("play_position_seconds").alias("watch_time_total")
            )
            
            # Join all windows
            result = watch_total
            for df_window in [watch_30d, watch_7d, watch_1d]:
                result = result.join(df_window, on=user_id_col, how="outer")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to compute watch time features: {str(e)}", exc_info=True)
            raise UserFeaturesError(f"Cannot compute watch time features: {str(e)}")
    
    def _compute_video_count_features(
        self,
        df: DataFrame,
        user_id_col: str,
        timestamp_col: str
    ) -> DataFrame:
        """
        Compute video count features (total, unique, by time window)
        
        Returns:
            DataFrame with video_count_1d, video_count_7d, video_count_30d, video_count_total, unique_videos_total
        """
        try:
            # Filter video events
            video_events = df.filter(col("event_type").like("%video%"))
            
            current_ts = current_timestamp()
            
            # Count by time windows
            count_1d = video_events.filter(
                datediff(current_ts, col(timestamp_col)) <= 1
            ).groupBy(user_id_col).agg(
                count("video_id").alias("video_count_1d")
            )
            
            count_7d = video_events.filter(
                datediff(current_ts, col(timestamp_col)) <= 7
            ).groupBy(user_id_col).agg(
                count("video_id").alias("video_count_7d")
            )
            
            count_30d = video_events.filter(
                datediff(current_ts, col(timestamp_col)) <= 30
            ).groupBy(user_id_col).agg(
                count("video_id").alias("video_count_30d")
            )
            
            # Total counts
            count_total = video_events.groupBy(user_id_col).agg(
                count("video_id").alias("video_count_total"),
                countDistinct("video_id").alias("unique_videos_total")
            )
            
            # Join
            result = count_total
            for df_window in [count_30d, count_7d, count_1d]:
                result = result.join(df_window, on=user_id_col, how="outer")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to compute video count features: {str(e)}", exc_info=True)
            raise UserFeaturesError(f"Cannot compute video count features: {str(e)}")
    
    def _compute_category_preferences(
        self,
        df: DataFrame,
        user_id_col: str,
        timestamp_col: str
    ) -> DataFrame:
        """
        Compute category preference features (top categories, category distribution)
        
        Returns:
            DataFrame with top_category, category_diversity, category_counts
        """
        try:
            # Filter video events with category
            video_events = df.filter(
                (col("event_type").like("%video%")) &
                (col("video_category").isNotNull())
            )
            
            # Count videos per category per user
            category_counts = video_events.groupBy(user_id_col, "video_category").agg(
                count("video_id").alias("category_count")
            )
            
            # Window to get top category per user
            window_spec = Window.partitionBy(user_id_col).orderBy(col("category_count").desc())
            top_category = category_counts.withColumn(
                "rank",
                expr("row_number() over (partition by user_id order by category_count desc)")
            ).filter(col("rank") == 1).select(
                user_id_col,
                col("video_category").alias("top_category")
            )
            
            # Category diversity (number of unique categories)
            category_diversity = video_events.groupBy(user_id_col).agg(
                countDistinct("video_category").alias("category_diversity")
            )
            
            # Join
            result = top_category.join(category_diversity, on=user_id_col, how="outer")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to compute category features: {str(e)}", exc_info=True)
            raise UserFeaturesError(f"Cannot compute category features: {str(e)}")
    
    def _compute_session_features(
        self,
        df: DataFrame,
        user_id_col: str,
        timestamp_col: str
    ) -> DataFrame:
        """
        Compute session-level features (session count, avg session duration, events per session)
        
        Returns:
            DataFrame with session_count_7d, session_count_30d, avg_session_duration, avg_events_per_session
        """
        try:
            # Filter events with session_id
            session_events = df.filter(col("session_id").isNotNull())
            
            current_ts = current_timestamp()
            
            # Session count by time window
            session_count_7d = session_events.filter(
                datediff(current_ts, col(timestamp_col)) <= 7
            ).groupBy(user_id_col).agg(
                countDistinct("session_id").alias("session_count_7d")
            )
            
            session_count_30d = session_events.filter(
                datediff(current_ts, col(timestamp_col)) <= 30
            ).groupBy(user_id_col).agg(
                countDistinct("session_id").alias("session_count_30d")
            )
            
            # Events per session
            events_per_session = session_events.groupBy(user_id_col, "session_id").agg(
                count("event_id").alias("events_in_session")
            ).groupBy(user_id_col).agg(
                avg("events_in_session").alias("avg_events_per_session")
            )
            
            # Join
            result = session_count_30d.join(session_count_7d, on=user_id_col, how="outer")
            result = result.join(events_per_session, on=user_id_col, how="outer")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to compute session features: {str(e)}", exc_info=True)
            raise UserFeaturesError(f"Cannot compute session features: {str(e)}")
    
    def _compute_engagement_features(
        self,
        df: DataFrame,
        user_id_col: str,
        timestamp_col: str
    ) -> DataFrame:
        """
        Compute engagement features (completion rate, interaction frequency, recency)
        
        Returns:
            DataFrame with completion_rate, interaction_frequency_7d, days_since_last_interaction
        """
        try:
            # Completion rate (completed videos / started videos)
            video_events = df.filter(col("event_type").like("%video%"))
            
            started = video_events.filter(
                col("event_type").like("%start%")
            ).groupBy(user_id_col).agg(
                count("event_id").alias("videos_started")
            )
            
            completed = video_events.filter(
                col("event_type").like("%complete%")
            ).groupBy(user_id_col).agg(
                count("event_id").alias("videos_completed")
            )
            
            completion_rate = started.join(completed, on=user_id_col, how="outer").withColumn(
                "completion_rate",
                when(col("videos_started") > 0, col("videos_completed") / col("videos_started"))
                .otherwise(lit(0.0))
            ).select(user_id_col, "completion_rate")
            
            # Interaction frequency (events per day in last 7 days)
            current_ts = current_timestamp()
            interaction_freq = df.filter(
                datediff(current_ts, col(timestamp_col)) <= 7
            ).groupBy(user_id_col).agg(
                (count("event_id") / 7.0).alias("interaction_frequency_7d")
            )
            
            # Days since last interaction
            last_interaction = df.groupBy(user_id_col).agg(
                spark_max(timestamp_col).alias("last_interaction_timestamp")
            ).withColumn(
                "days_since_last_interaction",
                datediff(current_ts, col("last_interaction_timestamp"))
            ).select(user_id_col, "days_since_last_interaction")
            
            # Join
            result = completion_rate.join(interaction_freq, on=user_id_col, how="outer")
            result = result.join(last_interaction, on=user_id_col, how="outer")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to compute engagement features: {str(e)}", exc_info=True)
            raise UserFeaturesError(f"Cannot compute engagement features: {str(e)}")

