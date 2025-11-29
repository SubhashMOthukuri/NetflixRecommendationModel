"""
Session Features - Gold Layer
Computes session-level features from silver data
Aggregates events within sessions to create session behavior features
"""
from typing import Optional, List, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    when, lit, datediff, current_timestamp, to_timestamp, countDistinct,
    expr, first, last, unix_timestamp
)
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class SessionFeaturesError(PipelineException):
    """Raised when session feature computation fails"""
    pass


class SessionFeatures:
    """
    Production-grade session feature engineering
    
    Computes session-level aggregations from silver events:
    - Session duration
    - Events per session
    - Session engagement score
    - Session completion rate
    - Session quality metrics
    
    Example:
        engineer = SessionFeatures(spark)
        session_features_df = engineer.compute_features(silver_df, timestamp_col="event_timestamp")
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize session feature engineer
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        logger.info("Session feature engineer initialized")
    
    def compute_features(
        self,
        df: DataFrame,
        timestamp_col: str = "event_timestamp",
        session_id_col: str = "session_id",
        user_id_col: str = "user_id",
        point_in_time: Optional[str] = None
    ) -> DataFrame:
        """
        Compute session-level features with point-in-time correctness
        
        Args:
            df: Silver DataFrame with session events
            timestamp_col: Timestamp column name
            session_id_col: Session ID column name
            user_id_col: User ID column name
            point_in_time: Point-in-time timestamp (YYYY-MM-DD HH:MM:SS) for time travel
        
        Returns:
            DataFrame with session features (one row per session)
        
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
            logger.info("Computing session features...")
            
            # Filter to point-in-time if provided (time travel)
            if point_in_time:
                df = df.filter(col(timestamp_col) <= to_timestamp(lit(point_in_time)))
                logger.info(f"Filtered to point-in-time: {point_in_time}")
            
            # Ensure timestamp is timestamp type
            if timestamp_col in df.columns:
                df = df.withColumn(timestamp_col, to_timestamp(col(timestamp_col) / 1000))
            
            # Filter events with session_id
            session_events = df.filter(col(session_id_col).isNotNull())
            
            if session_events.count() == 0:
                logger.warning("No session events found")
                return self.spark.createDataFrame([], schema=f"{session_id_col} STRING")
            
            # Compute all feature groups
            logger.info("Computing duration features...")
            duration_features = self._compute_duration_features(session_events, session_id_col, timestamp_col)
            
            logger.info("Computing event count features...")
            event_count_features = self._compute_event_count_features(session_events, session_id_col)
            
            logger.info("Computing engagement features...")
            engagement_features = self._compute_engagement_features(session_events, session_id_col)
            
            logger.info("Computing completion features...")
            completion_features = self._compute_completion_features(session_events, session_id_col)
            
            logger.info("Computing quality features...")
            quality_features = self._compute_quality_features(session_events, session_id_col, user_id_col)
            
            # Join all feature groups
            logger.info("Joining feature groups...")
            result = duration_features
            
            for feature_df in [event_count_features, engagement_features, completion_features, quality_features]:
                result = result.join(
                    feature_df,
                    on=session_id_col,
                    how="outer"
                )
            
            # Fill nulls with 0 for numeric features
            numeric_cols = [c for c in result.columns if c != session_id_col and c != user_id_col]
            for col_name in numeric_cols:
                result = result.withColumn(col_name, when(col(col_name).isNull(), lit(0)).otherwise(col(col_name)))
            
            logger.info(f"Session features computed: {result.count()} sessions")
            return result
            
        except Exception as e:
            logger.error(f"Failed to compute session features: {str(e)}", exc_info=True)
            raise SessionFeaturesError(f"Cannot compute session features: {str(e)}")
    
    def _compute_duration_features(
        self,
        df: DataFrame,
        session_id_col: str,
        timestamp_col: str
    ) -> DataFrame:
        """
        Compute session duration features (total duration, avg time between events)
        
        Returns:
            DataFrame with session_duration_seconds, avg_time_between_events
        """
        try:
            # Get session start and end timestamps
            session_times = df.groupBy(session_id_col).agg(
                min(timestamp_col).alias("session_start"),
                max(timestamp_col).alias("session_end"),
                count("event_id").alias("event_count")
            )
            
            # Calculate duration in seconds
            duration = session_times.withColumn(
                "session_duration_seconds",
                unix_timestamp(col("session_end")) - unix_timestamp(col("session_start"))
            )
            
            # Average time between events (duration / (events - 1))
            avg_time = duration.withColumn(
                "avg_time_between_events",
                when(col("event_count") > 1, col("session_duration_seconds") / (col("event_count") - 1))
                .otherwise(lit(0.0))
            )
            
            # Get user_id for each session (first user_id in session)
            user_id = df.groupBy(session_id_col).agg(
                first("user_id").alias("user_id")
            )
            
            result = avg_time.join(user_id, on=session_id_col, how="left")
            
            return result.select(session_id_col, "user_id", "session_duration_seconds", "avg_time_between_events")
            
        except Exception as e:
            logger.error(f"Failed to compute duration features: {str(e)}", exc_info=True)
            raise SessionFeaturesError(f"Cannot compute duration features: {str(e)}")
    
    def _compute_event_count_features(
        self,
        df: DataFrame,
        session_id_col: str
    ) -> DataFrame:
        """
        Compute event count features (total events, unique event types, video events)
        
        Returns:
            DataFrame with event_count, unique_event_types, video_event_count
        """
        try:
            # Total event count
            total_events = df.groupBy(session_id_col).agg(
                count("event_id").alias("event_count"),
                countDistinct("event_type").alias("unique_event_types")
            )
            
            # Video event count
            video_events = df.filter(
                col("event_type").like("%video%")
            ).groupBy(session_id_col).agg(
                count("event_id").alias("video_event_count")
            )
            
            # Join
            result = total_events.join(video_events, on=session_id_col, how="outer")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to compute event count features: {str(e)}", exc_info=True)
            raise SessionFeaturesError(f"Cannot compute event count features: {str(e)}")
    
    def _compute_engagement_features(
        self,
        df: DataFrame,
        session_id_col: str
    ) -> DataFrame:
        """
        Compute engagement features (watch time, interaction rate, engagement score)
        
        Returns:
            DataFrame with total_watch_time, interaction_rate, engagement_score
        """
        try:
            # Total watch time in session
            watch_time = df.filter(
                (col("event_type").like("%video%")) &
                (col("play_position_seconds").isNotNull())
            ).groupBy(session_id_col).agg(
                spark_sum("play_position_seconds").alias("total_watch_time")
            )
            
            # Interaction rate (video events / total events)
            total_events = df.groupBy(session_id_col).agg(
                count("event_id").alias("total_events")
            )
            
            video_events = df.filter(
                col("event_type").like("%video%")
            ).groupBy(session_id_col).agg(
                count("event_id").alias("video_events")
            )
            
            interaction_rate = total_events.join(video_events, on=session_id_col, how="outer").withColumn(
                "interaction_rate",
                when(col("total_events") > 0, col("video_events") / col("total_events"))
                .otherwise(lit(0.0))
            )
            
            # Engagement score (weighted combination)
            engagement = watch_time.join(interaction_rate, on=session_id_col, how="outer").withColumn(
                "engagement_score",
                (col("total_watch_time") / 3600.0) * 0.6 +  # Watch time weight (normalized to hours)
                (col("interaction_rate")) * 0.4  # Interaction rate weight
            )
            
            return engagement.select(session_id_col, "total_watch_time", "interaction_rate", "engagement_score")
            
        except Exception as e:
            logger.error(f"Failed to compute engagement features: {str(e)}", exc_info=True)
            raise SessionFeaturesError(f"Cannot compute engagement features: {str(e)}")
    
    def _compute_completion_features(
        self,
        df: DataFrame,
        session_id_col: str
    ) -> DataFrame:
        """
        Compute completion features (videos completed, completion rate)
        
        Returns:
            DataFrame with videos_completed, completion_rate
        """
        try:
            # Videos started in session
            videos_started = df.filter(
                col("event_type").like("%start%")
            ).groupBy(session_id_col).agg(
                count("event_id").alias("videos_started")
            )
            
            # Videos completed in session
            videos_completed = df.filter(
                col("event_type").like("%complete%")
            ).groupBy(session_id_col).agg(
                count("event_id").alias("videos_completed")
            )
            
            # Completion rate
            result = videos_started.join(videos_completed, on=session_id_col, how="outer").withColumn(
                "completion_rate",
                when(col("videos_started") > 0, col("videos_completed") / col("videos_started"))
                .otherwise(lit(0.0))
            )
            
            return result.select(session_id_col, "videos_completed", "completion_rate")
            
        except Exception as e:
            logger.error(f"Failed to compute completion features: {str(e)}", exc_info=True)
            raise SessionFeaturesError(f"Cannot compute completion features: {str(e)}")
    
    def _compute_quality_features(
        self,
        df: DataFrame,
        session_id_col: str,
        user_id_col: str
    ) -> DataFrame:
        """
        Compute session quality features (unique videos, categories, session type)
        
        Returns:
            DataFrame with unique_videos, unique_categories, session_type
        """
        try:
            # Unique videos watched
            unique_videos = df.filter(
                (col("event_type").like("%video%")) &
                (col("video_id").isNotNull())
            ).groupBy(session_id_col).agg(
                countDistinct("video_id").alias("unique_videos")
            )
            
            # Unique categories
            unique_categories = df.filter(
                (col("event_type").like("%video%")) &
                (col("video_category").isNotNull())
            ).groupBy(session_id_col).agg(
                countDistinct("video_category").alias("unique_categories")
            )
            
            # Session type (browsing vs watching)
            # Browsing: many videos, short watch time
            # Watching: few videos, long watch time
            watch_time = df.filter(
                (col("event_type").like("%video%")) &
                (col("play_position_seconds").isNotNull())
            ).groupBy(session_id_col).agg(
                spark_sum("play_position_seconds").alias("total_watch_time")
            )
            
            video_count = df.filter(
                col("event_type").like("%video%")
            ).groupBy(session_id_col).agg(
                countDistinct("video_id").alias("video_count")
            )
            
            session_type = watch_time.join(video_count, on=session_id_col, how="outer").withColumn(
                "session_type",
                when(
                    (col("total_watch_time") > 1800) & (col("video_count") <= 3),
                    lit("watching")
                ).when(
                    (col("total_watch_time") < 600) & (col("video_count") > 5),
                    lit("browsing")
                ).otherwise(lit("mixed"))
            )
            
            # Join all
            result = unique_videos.join(unique_categories, on=session_id_col, how="outer")
            result = result.join(session_type, on=session_id_col, how="outer")
            
            return result.select(session_id_col, "unique_videos", "unique_categories", "session_type")
            
        except Exception as e:
            logger.error(f"Failed to compute quality features: {str(e)}", exc_info=True)
            raise SessionFeaturesError(f"Cannot compute quality features: {str(e)}")

