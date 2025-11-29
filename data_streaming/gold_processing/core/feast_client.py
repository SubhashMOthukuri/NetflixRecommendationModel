"""
Feast Client - Gold Layer
Production-grade client for Feast feature store operations
Handles feature store connections, writes, and retrievals
"""
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import os
from pathlib import Path

from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)

try:
    from feast import FeatureStore, RepoConfig
    from feast.data_source import FileSource
    from feast.infra.offline_stores.file import FileOfflineStoreConfig
    from feast.infra.online_stores.redis import RedisOnlineStoreConfig
    from feast.repo_config import RegistryConfig
    FEAST_AVAILABLE = True
except ImportError:
    FEAST_AVAILABLE = False
    logger.warning("Feast not installed. Install with: pip install feast[redis]")


class FeastClientError(PipelineException):
    """Raised when Feast operations fail"""
    pass


class FeastClient:
    """
    Production-grade Feast feature store client
    
    Manages connections to Feast registry, offline store (Parquet/S3), and online store (Redis).
    Handles feature writes, retrievals, and feature definition management.
    
    Example:
        client = FeastClient(repo_path="feature_repo")
        client.write_features(feature_df, entity_df)
        features = client.get_offline_features(entity_df, feature_list)
    """
    
    def __init__(
        self,
        repo_path: Optional[str] = None,
        registry_path: Optional[str] = None,
        offline_store_path: Optional[str] = None,
        online_store_type: str = "redis",
        online_store_host: Optional[str] = None,
        online_store_port: int = 6379
    ):
        """
        Initialize Feast client
        
        Args:
            repo_path: Path to Feast feature repository
            registry_path: Path to Feast registry (defaults to repo_path/registry.db)
            offline_store_path: Path to offline store (Parquet/S3) - defaults to config
            online_store_type: Type of online store ("redis", "sqlite", etc.)
            online_store_host: Online store host (defaults to localhost for Redis)
            online_store_port: Online store port (defaults to 6379 for Redis)
        """
        if not FEAST_AVAILABLE:
            raise FeastClientError("Feast is not installed. Install with: pip install feast[redis]")
        
        # Load config for paths
        try:
            from libs.config_loader import load_config
            config = load_config("config/spark_config.yaml")
            gold_config = config.get("storage", {}).get("gold", {})
            
            self.repo_path = repo_path or gold_config.get("feast_repo_path", "feature_repo")
            self.registry_path = registry_path or gold_config.get("feast_registry_path", f"{self.repo_path}/registry.db")
            self.offline_store_path = offline_store_path or gold_config.get("offline_store_path", "s3a://data-lake/gold/features/")
            
            # Online store config
            online_config = gold_config.get("online_store", {})
            self.online_store_type = online_store_type or online_config.get("type", "redis")
            self.online_store_host = online_store_host or online_config.get("host", "localhost")
            self.online_store_port = online_store_port or online_config.get("port", 6379)
            
        except Exception as e:
            logger.warning(f"Could not load config: {str(e)}, using defaults")
            self.repo_path = repo_path or "feature_repo"
            self.registry_path = registry_path or f"{self.repo_path}/registry.db"
            self.offline_store_path = offline_store_path or "s3a://data-lake/gold/features/"
            self.online_store_type = online_store_type
            self.online_store_host = online_store_host or "localhost"
            self.online_store_port = online_store_port or 6379
        
        # Initialize Feast store
        self._store: Optional[FeatureStore] = None
        self._initialized = False
        
        logger.info(f"Feast client initialized (repo: {self.repo_path}, online: {self.online_store_type})")
    
    def _get_feature_store(self) -> FeatureStore:
        """
        Get or create Feast feature store instance
        
        Returns:
            FeatureStore instance
        """
        if self._store is None:
            try:
                # Check if repo exists
                if not os.path.exists(self.repo_path):
                    logger.warning(f"Feast repo not found at {self.repo_path}, creating...")
                    self._create_feast_repo()
                
                # Initialize feature store
                self._store = FeatureStore(repo_path=self.repo_path)
                self._initialized = True
                logger.info("Feast feature store connected")
                
            except Exception as e:
                logger.error(f"Failed to connect to Feast: {str(e)}", exc_info=True)
                raise FeastClientError(f"Cannot connect to Feast feature store: {str(e)}")
        
        return self._store
    
    def _create_feast_repo(self):
        """Create Feast repository structure if it doesn't exist"""
        try:
            os.makedirs(self.repo_path, exist_ok=True)
            
            # Create feature_store.yaml
            config_content = f"""project: gold_features
registry: {self.registry_path}
provider: local
online_store:
    type: redis
    connection_string: {self.online_store_host}:{self.online_store_port}
offline_store:
    type: file
"""
            config_path = os.path.join(self.repo_path, "feature_store.yaml")
            with open(config_path, "w") as f:
                f.write(config_content)
            
            # Create features directory
            os.makedirs(os.path.join(self.repo_path, "features"), exist_ok=True)
            
            logger.info(f"Created Feast repo at {self.repo_path}")
            
        except Exception as e:
            logger.error(f"Failed to create Feast repo: {str(e)}", exc_info=True)
            raise FeastClientError(f"Cannot create Feast repo: {str(e)}")
    
    def write_features_to_offline_store(
        self,
        feature_df,
        feature_view_name: str,
        timestamp_column: str = "event_timestamp",
        entity_columns: Optional[List[str]] = None
    ):
        """
        Write features to Feast offline store (Parquet/S3)
        
        Args:
            feature_df: Spark DataFrame with features
            feature_view_name: Name of feature view
            timestamp_column: Timestamp column name
            entity_columns: List of entity column names (e.g., ["user_id", "video_id"])
        
        Example:
            client.write_features_to_offline_store(
                feature_df=df,
                feature_view_name="user_features",
                entity_columns=["user_id"]
            )
        """
        try:
            logger.info(f"Writing features to offline store: {feature_view_name}")
            
            # Write to offline store path
            output_path = f"{self.offline_store_path}{feature_view_name}/"
            
            # Write as Parquet
            feature_df.write \
                .mode("overwrite") \
                .partitionBy(timestamp_column.split("_")[0] if "_" in timestamp_column else "date") \
                .format("parquet") \
                .option("compression", "snappy") \
                .save(output_path)
            
            logger.info(f"Features written to {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to write features to offline store: {str(e)}", exc_info=True)
            raise FeastClientError(f"Cannot write features to offline store: {str(e)}")
    
    def materialize_features_to_online_store(
        self,
        feature_view_name: str,
        start_date: str,
        end_date: str
    ):
        """
        Materialize features from offline store to online store (for real-time serving)
        
        Args:
            feature_view_name: Name of feature view to materialize
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
        
        Example:
            client.materialize_features_to_online_store(
                feature_view_name="user_features",
                start_date="2024-01-15",
                end_date="2024-01-16"
            )
        """
        try:
            logger.info(f"Materializing features to online store: {feature_view_name}")
            
            store = self._get_feature_store()
            
            # Materialize features
            store.materialize(
                start_date=datetime.fromisoformat(start_date),
                end_date=datetime.fromisoformat(end_date)
            )
            
            logger.info(f"Features materialized: {start_date} to {end_date}")
            
        except Exception as e:
            logger.error(f"Failed to materialize features: {str(e)}", exc_info=True)
            raise FeastClientError(f"Cannot materialize features: {str(e)}")
    
    def get_offline_features(
        self,
        entity_df,
        feature_list: List[str],
        timestamp_column: str = "event_timestamp"
    ):
        """
        Get features from offline store (for training data generation)
        
        Args:
            entity_df: DataFrame with entity IDs and timestamps
            feature_list: List of feature names to retrieve
            timestamp_column: Timestamp column name
        
        Returns:
            DataFrame with features joined to entities
        
        Example:
            features = client.get_offline_features(
                entity_df=training_entities,
                feature_list=["user_features:watch_time_7d", "user_features:video_count_7d"]
            )
        """
        try:
            logger.info(f"Retrieving offline features: {feature_list}")
            
            store = self._get_feature_store()
            
            # Get features (point-in-time correct)
            feature_df = store.get_historical_features(
                entity_df=entity_df,
                features=feature_list
            )
            
            logger.info(f"Retrieved {len(feature_list)} features")
            return feature_df
            
        except Exception as e:
            logger.error(f"Failed to retrieve offline features: {str(e)}", exc_info=True)
            raise FeastClientError(f"Cannot retrieve offline features: {str(e)}")
    
    def get_online_features(
        self,
        entity_rows: List[Dict[str, Any]],
        feature_list: List[str]
    ) -> Dict[str, Any]:
        """
        Get features from online store (for real-time serving)
        
        Args:
            entity_rows: List of entity dictionaries (e.g., [{"user_id": "123"}])
            feature_list: List of feature names to retrieve
        
        Returns:
            Dictionary with features
        
        Example:
            features = client.get_online_features(
                entity_rows=[{"user_id": "123"}],
                feature_list=["user_features:watch_time_7d"]
            )
        """
        try:
            logger.debug(f"Retrieving online features for {len(entity_rows)} entities")
            
            store = self._get_feature_store()
            
            # Get features from online store
            feature_vector = store.get_online_features(
                entity_rows=entity_rows,
                features=feature_list
            )
            
            logger.debug(f"Retrieved {len(feature_list)} features from online store")
            return feature_vector.to_dict()
            
        except Exception as e:
            logger.error(f"Failed to retrieve online features: {str(e)}", exc_info=True)
            raise FeastClientError(f"Cannot retrieve online features: {str(e)}")
    
    def apply_feature_store(self, feature_df, entity_df, feature_view_name: str):
        """
        Apply (write) features to Feast feature store (both offline and online)
        
        Args:
            feature_df: Spark DataFrame with features
            entity_df: DataFrame with entity IDs
            feature_view_name: Name of feature view
        
        Example:
            client.apply_feature_store(
                feature_df=computed_features,
                entity_df=entities,
                feature_view_name="user_features"
            )
        """
        try:
            logger.info(f"Applying features to Feast: {feature_view_name}")
            
            # Write to offline store
            self.write_features_to_offline_store(
                feature_df=feature_df,
                feature_view_name=feature_view_name
            )
            
            # Materialize to online store (for recent data)
            today = datetime.now().strftime("%Y-%m-%d")
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            
            self.materialize_features_to_online_store(
                feature_view_name=feature_view_name,
                start_date=yesterday,
                end_date=today
            )
            
            logger.info(f"Features applied to Feast: {feature_view_name}")
            
        except Exception as e:
            logger.error(f"Failed to apply features: {str(e)}", exc_info=True)
            raise FeastClientError(f"Cannot apply features: {str(e)}")

