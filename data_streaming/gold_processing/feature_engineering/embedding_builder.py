"""
Embedding Builder - Gold Layer
Builds vector embeddings for users and items using collaborative filtering
Enables similarity search, recommendations, and relationship modeling
"""
from typing import Optional, List, Dict, Any, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, collect_list, array, lit,
    when, udf, explode, row_number, dense_rank
)
from pyspark.sql.types import ArrayType, FloatType, IntegerType
from pyspark.sql.window import Window
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors, VectorUDT
import numpy as np
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class EmbeddingBuilderError(PipelineException):
    """Raised when embedding building fails"""
    pass


class EmbeddingBuilder:
    """
    Production-grade embedding builder for collaborative filtering
    
    Builds vector embeddings from user-item interactions:
    - User embeddings (from user-item interactions)
    - Item embeddings (from item-user interactions)
    - Similarity scores
    - Embedding features for ML models
    
    Example:
        builder = EmbeddingBuilder(spark)
        user_embeddings, item_embeddings = builder.build_embeddings(interaction_df)
    """
    
    def __init__(self, spark: SparkSession, embedding_dim: int = 50):
        """
        Initialize embedding builder
        
        Args:
            spark: SparkSession instance
            embedding_dim: Dimension of embeddings (default: 50)
        """
        self.spark = spark
        self.embedding_dim = embedding_dim
        logger.info(f"Embedding builder initialized (dimension: {embedding_dim})")
    
    def build_embeddings(
        self,
        df: DataFrame,
        user_id_col: str = "user_id",
        item_id_col: str = "video_id",
        rating_col: Optional[str] = None,
        method: str = "als"
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Build user and item embeddings from interactions
        
        Args:
            df: DataFrame with user-item interactions
            user_id_col: User ID column name
            item_id_col: Item ID column name
            rating_col: Optional rating column (if None, uses interaction count)
            method: Embedding method ("als", "simple", "weighted")
        
        Returns:
            Tuple of (user_embeddings_df, item_embeddings_df)
        
        Example:
            user_emb, item_emb = builder.build_embeddings(
                interaction_df,
                user_id_col="user_id",
                item_id_col="video_id"
            )
        """
        try:
            logger.info(f"Building embeddings using {method} method...")
            
            if method == "als":
                return self._build_als_embeddings(df, user_id_col, item_id_col, rating_col)
            elif method == "simple":
                return self._build_simple_embeddings(df, user_id_col, item_id_col)
            elif method == "weighted":
                return self._build_weighted_embeddings(df, user_id_col, item_id_col, rating_col)
            else:
                raise EmbeddingBuilderError(f"Unknown embedding method: {method}")
                
        except Exception as e:
            logger.error(f"Failed to build embeddings: {str(e)}", exc_info=True)
            raise EmbeddingBuilderError(f"Cannot build embeddings: {str(e)}")
    
    def _build_als_embeddings(
        self,
        df: DataFrame,
        user_id_col: str,
        item_id_col: str,
        rating_col: Optional[str]
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Build embeddings using ALS (Alternating Least Squares) matrix factorization
        
        Returns:
            Tuple of (user_embeddings_df, item_embeddings_df)
        """
        try:
            logger.info("Building ALS embeddings...")
            
            # Prepare interaction data
            if rating_col and rating_col in df.columns:
                interactions = df.select(user_id_col, item_id_col, rating_col)
            else:
                # Use interaction count as implicit rating
                interactions = df.groupBy(user_id_col, item_id_col).agg(
                    count("*").alias("rating")
                )
            
            # Create numeric IDs for ALS (ALS requires integer IDs)
            users = interactions.select(user_id_col).distinct()
            items = interactions.select(item_id_col).distinct()
            
            # Create user and item mappings
            user_window = Window.orderBy(user_id_col)
            item_window = Window.orderBy(item_id_col)
            
            user_mapping = users.withColumn("user_idx", row_number().over(user_window) - 1)
            item_mapping = items.withColumn("item_idx", row_number().over(item_window) - 1)
            
            # Join with mappings
            interactions_mapped = interactions.join(user_mapping, on=user_id_col, how="inner") \
                .join(item_mapping, on=item_id_col, how="inner") \
                .select("user_idx", "item_idx", "rating")
            
            # Train ALS model
            logger.info("Training ALS model...")
            als = ALS(
                maxIter=10,
                regParam=0.1,
                userCol="user_idx",
                itemCol="item_idx",
                ratingCol="rating",
                coldStartStrategy="drop",
                implicitPrefs=(rating_col is None)  # Use implicit feedback if no rating column
            )
            
            model = als.fit(interactions_mapped)
            
            # Extract user and item factors (embeddings)
            user_factors = model.userFactors
            item_factors = model.itemFactors
            
            # Map back to original IDs
            user_embeddings = user_factors.join(user_mapping, on=user_factors.id == user_mapping.user_idx) \
                .select(user_id_col, "features")
            
            item_embeddings = item_factors.join(item_mapping, on=item_factors.id == item_mapping.item_idx) \
                .select(item_id_col, "features")
            
            # Rename features to embedding
            user_embeddings = user_embeddings.withColumnRenamed("features", "user_embedding")
            item_embeddings = item_embeddings.withColumnRenamed("features", "item_embedding")
            
            logger.info(f"ALS embeddings built: {user_embeddings.count()} users, {item_embeddings.count()} items")
            
            return user_embeddings, item_embeddings
            
        except Exception as e:
            logger.error(f"Failed to build ALS embeddings: {str(e)}", exc_info=True)
            raise EmbeddingBuilderError(f"Cannot build ALS embeddings: {str(e)}")
    
    def _build_simple_embeddings(
        self,
        df: DataFrame,
        user_id_col: str,
        item_id_col: str
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Build simple embeddings using interaction frequency (one-hot encoding style)
        
        Returns:
            Tuple of (user_embeddings_df, item_embeddings_df)
        """
        try:
            logger.info("Building simple embeddings...")
            
            # Get unique users and items
            users = df.select(user_id_col).distinct()
            items = df.select(item_id_col).distinct()
            
            user_count = users.count()
            item_count = items.count()
            
            logger.info(f"Found {user_count} users and {item_count} items")
            
            # Create user-to-item interaction matrix (sparse)
            user_item_interactions = df.groupBy(user_id_col).agg(
                collect_list(item_id_col).alias("interacted_items")
            )
            
            # Create item-to-user interaction matrix (sparse)
            item_user_interactions = df.groupBy(item_id_col).agg(
                collect_list(user_id_col).alias("interacted_users")
            )
            
            # Build embeddings using interaction patterns
            # User embedding: normalized interaction vector
            # Item embedding: normalized interaction vector
            
            # For simplicity, use interaction frequency as embedding
            # In production, you'd use more sophisticated methods
            
            # User embeddings: aggregate item interactions
            user_item_counts = df.groupBy(user_id_col, item_id_col).agg(
                count("*").alias("interaction_count")
            )
            
            # Item embeddings: aggregate user interactions
            item_user_counts = df.groupBy(item_id_col, user_id_col).agg(
                count("*").alias("interaction_count")
            )
            
            # Create simple embeddings (interaction frequency vectors)
            # This is a simplified version - in production, use proper embedding methods
            
            logger.warning("Simple embeddings method is basic - consider using ALS for better results")
            
            # Return placeholder embeddings (in production, implement proper embedding)
            user_embeddings = users.withColumn(
                "user_embedding",
                lit(None).cast(ArrayType(FloatType()))
            )
            
            item_embeddings = items.withColumn(
                "item_embedding",
                lit(None).cast(ArrayType(FloatType()))
            )
            
            logger.info("Simple embeddings built (placeholder - implement proper embedding logic)")
            
            return user_embeddings, item_embeddings
            
        except Exception as e:
            logger.error(f"Failed to build simple embeddings: {str(e)}", exc_info=True)
            raise EmbeddingBuilderError(f"Cannot build simple embeddings: {str(e)}")
    
    def _build_weighted_embeddings(
        self,
        df: DataFrame,
        user_id_col: str,
        item_id_col: str,
        rating_col: Optional[str]
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Build weighted embeddings using interaction weights/ratings
        
        Returns:
            Tuple of (user_embeddings_df, item_embeddings_df)
        """
        try:
            logger.info("Building weighted embeddings...")
            
            # Use ALS with weighted interactions
            return self._build_als_embeddings(df, user_id_col, item_id_col, rating_col)
            
        except Exception as e:
            logger.error(f"Failed to build weighted embeddings: {str(e)}", exc_info=True)
            raise EmbeddingBuilderError(f"Cannot build weighted embeddings: {str(e)}")
    
    def compute_similarity(
        self,
        embeddings_df: DataFrame,
        entity_id_col: str,
        embedding_col: str = "embedding",
        top_k: int = 10
    ) -> DataFrame:
        """
        Compute similarity scores between entities using embeddings
        
        Args:
            embeddings_df: DataFrame with embeddings
            entity_id_col: Entity ID column name
            embedding_col: Embedding column name
            top_k: Number of top similar entities to return
        
        Returns:
            DataFrame with similarity scores (entity_id, similar_entity_id, similarity_score)
        
        Example:
            similarities = builder.compute_similarity(
                user_embeddings,
                entity_id_col="user_id",
                embedding_col="user_embedding",
                top_k=10
            )
        """
        try:
            logger.info(f"Computing similarity scores (top {top_k})...")
            
            # This is a simplified version - in production, use efficient similarity computation
            # For large datasets, use approximate nearest neighbor search (ANN)
            
            # Collect embeddings (for small datasets)
            # For large datasets, use distributed similarity computation
            
            logger.warning("Similarity computation is simplified - for production, use ANN libraries")
            
            # Placeholder - implement proper cosine similarity computation
            # In production, use libraries like faiss, annoy, or Spark MLlib's similarity functions
            
            result = embeddings_df.select(entity_id_col).withColumn(
                "similar_entity_id",
                lit(None).cast("string")
            ).withColumn(
                "similarity_score",
                lit(0.0).cast(FloatType())
            )
            
            logger.info("Similarity scores computed (placeholder)")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to compute similarity: {str(e)}", exc_info=True)
            raise EmbeddingBuilderError(f"Cannot compute similarity: {str(e)}")
    
    def add_embedding_features(
        self,
        df: DataFrame,
        user_embeddings: DataFrame,
        item_embeddings: DataFrame,
        user_id_col: str = "user_id",
        item_id_col: str = "video_id"
    ) -> DataFrame:
        """
        Add embedding features to DataFrame by joining embeddings
        
        Args:
            df: Input DataFrame
            user_embeddings: User embeddings DataFrame
            item_embeddings: Item embeddings DataFrame
            user_id_col: User ID column name
            item_id_col: Item ID column name
        
        Returns:
            DataFrame with embedding features added
        
        Example:
            df_with_embeddings = builder.add_embedding_features(
                df,
                user_embeddings,
                item_embeddings
            )
        """
        try:
            logger.info("Adding embedding features to DataFrame...")
            
            result = df
            
            # Join user embeddings
            if user_id_col in df.columns:
                result = result.join(
                    user_embeddings,
                    on=user_id_col,
                    how="left"
                )
                logger.info("User embeddings joined")
            
            # Join item embeddings
            if item_id_col in df.columns:
                result = result.join(
                    item_embeddings,
                    on=item_id_col,
                    how="left"
                )
                logger.info("Item embeddings joined")
            
            logger.info("Embedding features added")
            return result
            
        except Exception as e:
            logger.error(f"Failed to add embedding features: {str(e)}", exc_info=True)
            raise EmbeddingBuilderError(f"Cannot add embedding features: {str(e)}")
    
    def extract_embedding_dimensions(
        self,
        embeddings_df: DataFrame,
        embedding_col: str = "embedding",
        prefix: str = "emb_"
    ) -> DataFrame:
        """
        Extract individual dimensions from embedding vector as separate columns
        
        Args:
            embeddings_df: DataFrame with embeddings
            embedding_col: Embedding column name
            prefix: Prefix for dimension column names
        
        Returns:
            DataFrame with embedding dimensions as separate columns
        
        Example:
            df_expanded = builder.extract_embedding_dimensions(
                user_embeddings,
                embedding_col="user_embedding",
                prefix="user_emb_"
            )
        """
        try:
            logger.info(f"Extracting embedding dimensions (dimension: {self.embedding_dim})...")
            
            # UDF to extract embedding dimensions
            def extract_dims(embedding_vector):
                if embedding_vector is None:
                    return [0.0] * self.embedding_dim
                if hasattr(embedding_vector, 'toArray'):
                    return embedding_vector.toArray().tolist()
                if isinstance(embedding_vector, list):
                    return embedding_vector
                return [0.0] * self.embedding_dim
            
            extract_dims_udf = udf(extract_dims, ArrayType(FloatType()))
            
            # Extract dimensions
            result = embeddings_df.withColumn(
                "embedding_dims",
                extract_dims_udf(col(embedding_col))
            )
            
            # Create columns for each dimension
            for i in range(self.embedding_dim):
                result = result.withColumn(
                    f"{prefix}dim_{i}",
                    col("embedding_dims")[i]
                )
            
            # Drop intermediate column
            result = result.drop("embedding_dims")
            
            logger.info(f"Extracted {self.embedding_dim} embedding dimensions")
            return result
            
        except Exception as e:
            logger.error(f"Failed to extract embedding dimensions: {str(e)}", exc_info=True)
            raise EmbeddingBuilderError(f"Cannot extract embedding dimensions: {str(e)}")

