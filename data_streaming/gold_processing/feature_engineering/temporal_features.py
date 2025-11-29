"""
Temporal Features - Gold Layer
Computes time-based features (recency, frequency, time-of-day patterns, seasonality)
Captures temporal behavior patterns for ML models
"""
from typing import Optional, List, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, count, datediff, current_timestamp, to_timestamp,
    hour, dayofweek, dayofmonth, month, year, weekofyear,
    when, lit, countDistinct, max as spark_max, min as spark_min,
    avg, sum as spark_sum, expr, date_format
)
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class TemporalFeaturesError(PipelineException):
    """Raised when temporal feature computation fails"""
    pass


class TemporalFeatures:
    """
    Production-grade temporal feature engineering
    
    Computes time-based features from silver events:
    - Time components (hour, day of week, month)
    - Recency features (days since last event)
    - Frequency features (events per day/week)
    - Time-of-day patterns (morning, afternoon, evening, night)
    - Seasonality features
    
    Example:
        engineer = TemporalFeatures(spark)
        temporal_df = engineer.compute_features(silver_df, timestamp_col="event_timestamp")
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize temporal feature engineer
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        logger.info("Temporal feature engineer initialized")
    
    def compute_features(
        self,
        df: DataFrame,
        timestamp_col: str = "event_timestamp",
        entity_id_col: str = "user_id",
        point_in_time: Optional[str] = None
    ) -> DataFrame:
        """
        Compute temporal features with point-in-time correctness
        
        Args:
            df: Silver DataFrame with events
            timestamp_col: Timestamp column name
            entity_id_col: Entity ID column (user_id, video_id, etc.)
            point_in_time: Point-in-time timestamp (YYYY-MM-DD HH:MM:SS) for time travel
        
        Returns:
            DataFrame with temporal features added
        
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
            logger.info("Computing temporal features...")
            
            # Filter to point-in-time if provided (time travel)
            if point_in_time:
                df = df.filter(col(timestamp_col) <= to_timestamp(lit(point_in_time)))
                logger.info(f"Filtered to point-in-time: {point_in_time}")
            
            # Ensure timestamp is timestamp type
            if timestamp_col in df.columns:
                df = df.withColumn(timestamp_col, to_timestamp(col(timestamp_col) / 1000))
            
            # Compute all temporal feature groups
            logger.info("Extracting time components...")
            time_components = self._extract_time_components(df, timestamp_col)
            
            logger.info("Computing recency features...")
            recency_features = self._compute_recency_features(df, timestamp_col, entity_id_col)
            
            logger.info("Computing frequency features...")
            frequency_features = self._compute_frequency_features(df, timestamp_col, entity_id_col)
            
            logger.info("Computing time-of-day patterns...")
            time_patterns = self._compute_time_of_day_patterns(df, timestamp_col, entity_id_col)
            
            logger.info("Computing seasonality features...")
            seasonality_features = self._compute_seasonality_features(df, timestamp_col, entity_id_col)
            
            # Join all temporal features
            logger.info("Joining temporal features...")
            result = time_components
            
            for feature_df in [recency_features, frequency_features, time_patterns, seasonality_features]:
                if feature_df is not None:
                    result = result.join(
                        feature_df,
                        on=entity_id_col,
                        how="outer"
                    )
            
            logger.info("Temporal features computed")
            return result
            
        except Exception as e:
            logger.error(f"Failed to compute temporal features: {str(e)}", exc_info=True)
            raise TemporalFeaturesError(f"Cannot compute temporal features: {str(e)}")
    
    def _extract_time_components(
        self,
        df: DataFrame,
        timestamp_col: str
    ) -> DataFrame:
        """
        Extract time components (hour, day of week, month, etc.)
        
        Returns:
            DataFrame with time component columns added
        """
        try:
            result = df.withColumn("event_hour", hour(col(timestamp_col))) \
                .withColumn("event_day_of_week", dayofweek(col(timestamp_col))) \
                .withColumn("event_day_of_month", dayofmonth(col(timestamp_col))) \
                .withColumn("event_month", month(col(timestamp_col))) \
                .withColumn("event_year", year(col(timestamp_col))) \
                .withColumn("event_week_of_year", weekofyear(col(timestamp_col))) \
                .withColumn("event_date", date_format(col(timestamp_col), "yyyy-MM-dd"))
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to extract time components: {str(e)}", exc_info=True)
            raise TemporalFeaturesError(f"Cannot extract time components: {str(e)}")
    
    def _compute_recency_features(
        self,
        df: DataFrame,
        timestamp_col: str,
        entity_id_col: str
    ) -> DataFrame:
        """
        Compute recency features (days since last event, days since first event)
        
        Returns:
            DataFrame with recency features per entity
        """
        try:
            current_ts = current_timestamp()
            
            # Last event timestamp per entity
            last_event = df.groupBy(entity_id_col).agg(
                spark_max(timestamp_col).alias("last_event_timestamp")
            )
            
            # First event timestamp per entity
            first_event = df.groupBy(entity_id_col).agg(
                spark_min(timestamp_col).alias("first_event_timestamp")
            )
            
            # Compute recency features
            result = last_event.join(first_event, on=entity_id_col, how="outer").withColumn(
                "days_since_last_event",
                datediff(current_ts, col("last_event_timestamp"))
            ).withColumn(
                "days_since_first_event",
                datediff(current_ts, col("first_event_timestamp"))
            ).withColumn(
                "user_age_days",
                datediff(col("last_event_timestamp"), col("first_event_timestamp"))
            )
            
            return result.select(
                entity_id_col,
                "days_since_last_event",
                "days_since_first_event",
                "user_age_days"
            )
            
        except Exception as e:
            logger.error(f"Failed to compute recency features: {str(e)}", exc_info=True)
            raise TemporalFeaturesError(f"Cannot compute recency features: {str(e)}")
    
    def _compute_frequency_features(
        self,
        df: DataFrame,
        timestamp_col: str,
        entity_id_col: str
    ) -> DataFrame:
        """
        Compute frequency features (events per day, events per week, active days)
        
        Returns:
            DataFrame with frequency features per entity
        """
        try:
            current_ts = current_timestamp()
            
            # Total events per entity
            total_events = df.groupBy(entity_id_col).agg(
                count("event_id").alias("total_events")
            )
            
            # Active days (unique dates with events)
            active_days = df.withColumn("event_date", date_format(col(timestamp_col), "yyyy-MM-dd")) \
                .groupBy(entity_id_col).agg(
                    countDistinct("event_date").alias("active_days")
                )
            
            # Events in last 7 days
            events_7d = df.filter(
                datediff(current_ts, col(timestamp_col)) <= 7
            ).groupBy(entity_id_col).agg(
                count("event_id").alias("events_7d")
            )
            
            # Events in last 30 days
            events_30d = df.filter(
                datediff(current_ts, col(timestamp_col)) <= 30
            ).groupBy(entity_id_col).agg(
                count("event_id").alias("events_30d")
            )
            
            # Join and compute frequency metrics
            result = total_events.join(active_days, on=entity_id_col, how="outer")
            result = result.join(events_7d, on=entity_id_col, how="outer")
            result = result.join(events_30d, on=entity_id_col, how="outer")
            
            # Compute frequency rates
            result = result.withColumn(
                "events_per_day",
                when(col("active_days") > 0, col("total_events") / col("active_days"))
                .otherwise(lit(0.0))
            ).withColumn(
                "events_per_day_7d",
                when(col("events_7d").isNotNull(), col("events_7d") / 7.0)
                .otherwise(lit(0.0))
            ).withColumn(
                "events_per_day_30d",
                when(col("events_30d").isNotNull(), col("events_30d") / 30.0)
                .otherwise(lit(0.0))
            )
            
            return result.select(
                entity_id_col,
                "total_events",
                "active_days",
                "events_7d",
                "events_30d",
                "events_per_day",
                "events_per_day_7d",
                "events_per_day_30d"
            )
            
        except Exception as e:
            logger.error(f"Failed to compute frequency features: {str(e)}", exc_info=True)
            raise TemporalFeaturesError(f"Cannot compute frequency features: {str(e)}")
    
    def _compute_time_of_day_patterns(
        self,
        df: DataFrame,
        timestamp_col: str,
        entity_id_col: str
    ) -> DataFrame:
        """
        Compute time-of-day patterns (morning, afternoon, evening, night activity)
        
        Returns:
            DataFrame with time-of-day pattern features per entity
        """
        try:
            # Extract hour
            df_with_hour = df.withColumn("event_hour", hour(col(timestamp_col)))
            
            # Define time periods
            # Morning: 6-12, Afternoon: 12-18, Evening: 18-22, Night: 22-6
            df_with_period = df_with_hour.withColumn(
                "time_period",
                when((col("event_hour") >= 6) & (col("event_hour") < 12), "morning")
                .when((col("event_hour") >= 12) & (col("event_hour") < 18), "afternoon")
                .when((col("event_hour") >= 18) & (col("event_hour") < 22), "evening")
                .otherwise("night")
            )
            
            # Count events per time period per entity
            period_counts = df_with_period.groupBy(entity_id_col, "time_period").agg(
                count("event_id").alias("period_count")
            )
            
            # Pivot to get counts per period
            period_pivot = period_counts.groupBy(entity_id_col).pivot("time_period").agg(
                spark_sum("period_count")
            )
            
            # Total events for normalization
            total_events = df.groupBy(entity_id_col).agg(
                count("event_id").alias("total_events")
            )
            
            # Compute percentages
            result = period_pivot.join(total_events, on=entity_id_col, how="outer")
            
            for period in ["morning", "afternoon", "evening", "night"]:
                col_name = period
                pct_col_name = f"{period}_activity_pct"
                result = result.withColumn(
                    pct_col_name,
                    when(col("total_events") > 0, col(col_name) / col("total_events"))
                    .otherwise(lit(0.0))
                )
            
            # Preferred time period (most active)
            df_with_period_counts = df_with_period.groupBy(entity_id_col, "time_period").agg(
                count("event_id").alias("period_count")
            )
            
            # Get max period per entity
            window_spec = expr("row_number() over (partition by user_id order by period_count desc)")
            preferred_period = df_with_period_counts.withColumn("rank", window_spec) \
                .filter(col("rank") == 1) \
                .select(entity_id_col, col("time_period").alias("preferred_time_period"))
            
            result = result.join(preferred_period, on=entity_id_col, how="left")
            
            return result.select(
                entity_id_col,
                "morning", "afternoon", "evening", "night",
                "morning_activity_pct", "afternoon_activity_pct",
                "evening_activity_pct", "night_activity_pct",
                "preferred_time_period"
            )
            
        except Exception as e:
            logger.error(f"Failed to compute time-of-day patterns: {str(e)}", exc_info=True)
            raise TemporalFeaturesError(f"Cannot compute time-of-day patterns: {str(e)}")
    
    def _compute_seasonality_features(
        self,
        df: DataFrame,
        timestamp_col: str,
        entity_id_col: str
    ) -> DataFrame:
        """
        Compute seasonality features (day of week patterns, monthly patterns)
        
        Returns:
            DataFrame with seasonality features per entity
        """
        try:
            # Extract day of week and month
            df_with_season = df.withColumn("day_of_week", dayofweek(col(timestamp_col))) \
                .withColumn("month", month(col(timestamp_col)))
            
            # Day of week activity
            dow_counts = df_with_season.groupBy(entity_id_col, "day_of_week").agg(
                count("event_id").alias("dow_count")
            )
            
            # Pivot day of week
            dow_pivot = dow_counts.groupBy(entity_id_col).pivot("day_of_week").agg(
                spark_sum("dow_count")
            )
            
            # Rename columns (1=Sunday, 7=Saturday)
            dow_renamed = dow_pivot.select(
                entity_id_col,
                col("1").alias("sunday_events"),
                col("2").alias("monday_events"),
                col("3").alias("tuesday_events"),
                col("4").alias("wednesday_events"),
                col("5").alias("thursday_events"),
                col("6").alias("friday_events"),
                col("7").alias("saturday_events")
            )
            
            # Total events for normalization
            total_events = df.groupBy(entity_id_col).agg(
                count("event_id").alias("total_events")
            )
            
            # Compute day of week percentages
            result = dow_renamed.join(total_events, on=entity_id_col, how="outer")
            
            for day in ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"]:
                col_name = f"{day}_events"
                pct_col_name = f"{day}_activity_pct"
                result = result.withColumn(
                    pct_col_name,
                    when(col("total_events") > 0, col(col_name) / col("total_events"))
                    .otherwise(lit(0.0))
                )
            
            # Preferred day of week
            preferred_dow = df_with_season.groupBy(entity_id_col, "day_of_week").agg(
                count("event_id").alias("dow_count")
            )
            
            window_spec = expr("row_number() over (partition by user_id order by dow_count desc)")
            preferred = preferred_dow.withColumn("rank", window_spec) \
                .filter(col("rank") == 1) \
                .select(entity_id_col, col("day_of_week").alias("preferred_day_of_week"))
            
            result = result.join(preferred, on=entity_id_col, how="left")
            
            # Weekend vs weekday activity
            weekend_events = df_with_season.filter(
                (col("day_of_week") == 1) | (col("day_of_week") == 7)
            ).groupBy(entity_id_col).agg(
                count("event_id").alias("weekend_events")
            )
            
            weekday_events = df_with_season.filter(
                (col("day_of_week") >= 2) & (col("day_of_week") <= 6)
            ).groupBy(entity_id_col).agg(
                count("event_id").alias("weekday_events")
            )
            
            weekend_ratio = weekend_events.join(weekday_events, on=entity_id_col, how="outer").withColumn(
                "weekend_activity_ratio",
                when(col("weekday_events") > 0, col("weekend_events") / col("weekday_events"))
                .otherwise(lit(0.0))
            )
            
            result = result.join(weekend_ratio, on=entity_id_col, how="outer")
            
            return result.select(
                entity_id_col,
                "sunday_events", "monday_events", "tuesday_events", "wednesday_events",
                "thursday_events", "friday_events", "saturday_events",
                "sunday_activity_pct", "monday_activity_pct", "tuesday_activity_pct",
                "wednesday_activity_pct", "thursday_activity_pct", "friday_activity_pct",
                "saturday_activity_pct",
                "preferred_day_of_week",
                "weekend_activity_ratio"
            )
            
        except Exception as e:
            logger.error(f"Failed to compute seasonality features: {str(e)}", exc_info=True)
            raise TemporalFeaturesError(f"Cannot compute seasonality features: {str(e)}")

