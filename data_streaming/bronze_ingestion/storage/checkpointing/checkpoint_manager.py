"""
Checkpoint Manager - Production ML Pipeline
Manages Spark checkpoints for streaming job recovery and exactly-once processing
Critical for preventing data loss and enabling job recovery
"""
import json
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from pyspark.sql.streaming import StreamingQuery
from libs.config_loader import load_config, get_config_value
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class CheckpointManagerError(PipelineException):
    """Raised when checkpoint management fails"""
    pass


class CheckpointManager:
    """
    Production-grade checkpoint manager
    
    Manages:
    - Checkpoint creation and validation
    - Checkpoint cleanup (retention)
    - Checkpoint recovery
    - Checkpoint statistics
    
    Example:
        manager = CheckpointManager()
        checkpoint_path = manager.get_checkpoint_path("user_events")
        # Use checkpoint_path in streaming write
    """
    
    def __init__(
        self,
        config_path: str = "spark_config.yaml",
        checkpoint_base_path: Optional[str] = None
    ):
        """
        Initialize checkpoint manager
        
        Args:
            config_path: Path to Spark config file
            checkpoint_base_path: Optional base path for checkpoints
        """
        # Load config
        try:
            self.config = load_config(config_path)
            
            if checkpoint_base_path:
                self.checkpoint_base_path = checkpoint_base_path
            else:
                self.checkpoint_base_path = get_config_value(
                    self.config,
                    "streaming.checkpoint_location",
                    "s3a://data-lake/checkpoints/bronze_ingestion/"
                )
            
            # Get retention settings
            checkpoint_config = self.config.get("storage", {}).get("checkpoint", {})
            self.retention_period = checkpoint_config.get("retention_period", "7d")
            self.enabled = checkpoint_config.get("enabled", True)
            
        except Exception as e:
            logger.warning(f"Could not load config: {str(e)}, using defaults")
            self.checkpoint_base_path = checkpoint_base_path or "s3a://data-lake/checkpoints/bronze_ingestion/"
            self.retention_period = "7d"
            self.enabled = True
        
        logger.info(f"Checkpoint manager initialized (path: {self.checkpoint_base_path})")
    
    def get_checkpoint_path(self, job_name: str) -> str:
        """
        Get checkpoint path for a job
        
        Args:
            job_name: Job name (e.g., "user_events", "bronze_ingestion")
        
        Returns:
            Full checkpoint path
        
        Example:
            manager = CheckpointManager()
            path = manager.get_checkpoint_path("user_events")
            # Returns: "s3a://data-lake/checkpoints/bronze_ingestion/user_events/"
        """
        checkpoint_path = f"{self.checkpoint_base_path.rstrip('/')}/{job_name}/"
        logger.debug(f"Checkpoint path for {job_name}: {checkpoint_path}")
        return checkpoint_path
    
    def validate_checkpoint(self, checkpoint_path: str) -> bool:
        """
        Validate checkpoint exists and is valid
        
        Args:
            checkpoint_path: Checkpoint path to validate
        
        Returns:
            True if checkpoint is valid, False otherwise
        """
        try:
            # Check if checkpoint directory exists
            # In production, this would check S3/MinIO
            # For now, we'll assume it's valid if path is provided
            logger.debug(f"Validating checkpoint: {checkpoint_path}")
            
            # TODO: Implement actual validation by checking checkpoint directory structure
            # Checkpoint should have: sources/, sinks/, state/ directories
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating checkpoint: {str(e)}", exc_info=True)
            return False
    
    def cleanup_old_checkpoints(
        self,
        checkpoint_path: str,
        retention_days: Optional[int] = None
    ) -> int:
        """
        Cleanup old checkpoints (beyond retention period)
        
        Args:
            checkpoint_path: Checkpoint path
            retention_days: Optional retention days (overrides config)
        
        Returns:
            Number of checkpoints cleaned up
        
        Note: This is a placeholder - full implementation would delete old checkpoint files
        """
        try:
            if not retention_days:
                # Parse retention period (e.g., "7d" -> 7 days)
                if self.retention_period.endswith("d"):
                    retention_days = int(self.retention_period[:-1])
                else:
                    retention_days = 7  # Default
            
            logger.info(f"Cleaning up checkpoints older than {retention_days} days in {checkpoint_path}")
            
            # TODO: Implement actual cleanup by:
            # 1. Listing checkpoint files
            # 2. Checking modification dates
            # 3. Deleting files older than retention_days
            
            logger.warning("Checkpoint cleanup not fully implemented - requires file system access")
            return 0
            
        except Exception as e:
            logger.error(f"Error cleaning up checkpoints: {str(e)}", exc_info=True)
            return 0
    
    def get_checkpoint_info(self, checkpoint_path: str) -> Dict[str, Any]:
        """
        Get checkpoint information
        
        Args:
            checkpoint_path: Checkpoint path
        
        Returns:
            Dictionary with checkpoint information
        """
        try:
            info = {
                "checkpoint_path": checkpoint_path,
                "exists": self.validate_checkpoint(checkpoint_path),
                "last_updated": None,  # Would need to read from file system
                "size": None,  # Would need to calculate
                "files_count": None  # Would need to count
            }
            
            # TODO: Implement reading actual checkpoint metadata
            # This would require file system access to read checkpoint files
            
            return info
            
        except Exception as e:
            logger.error(f"Error getting checkpoint info: {str(e)}", exc_info=True)
            return {"error": str(e)}
    
    def reset_checkpoint(self, checkpoint_path: str) -> bool:
        """
        Reset checkpoint (delete and start fresh)
        
        Args:
            checkpoint_path: Checkpoint path to reset
        
        Returns:
            True if reset successful, False otherwise
        
        Warning: This will cause data reprocessing from beginning!
        """
        try:
            logger.warning(f"Resetting checkpoint: {checkpoint_path}")
            logger.warning("This will cause data reprocessing from beginning!")
            
            # TODO: Implement actual checkpoint deletion
            # This would require file system access to delete checkpoint directory
            
            logger.warning("Checkpoint reset not fully implemented - requires file system access")
            return False
            
        except Exception as e:
            logger.error(f"Error resetting checkpoint: {str(e)}", exc_info=True)
            return False
    
    def get_checkpoint_stats(self, query: StreamingQuery) -> Dict[str, Any]:
        """
        Get checkpoint statistics from active streaming query
        
        Args:
            query: StreamingQuery instance
        
        Returns:
            Dictionary with checkpoint statistics
        """
        try:
            if not query.isActive:
                return {"error": "Query is not active"}
            
            # Get query progress
            progress = query.lastProgress
            
            if not progress:
                return {"error": "No progress information available"}
            
            # Extract checkpoint information
            checkpoint_info = {
                "query_id": query.id,
                "query_name": query.name if hasattr(query, 'name') else None,
                "is_active": query.isActive,
                "run_id": progress.get("runId"),
                "batch_id": progress.get("batchId"),
                "num_input_rows": progress.get("numInputRows", 0),
                "input_rows_per_second": progress.get("inputRowsPerSecond", 0),
                "processed_rows_per_second": progress.get("processedRowsPerSecond", 0),
                "sources": progress.get("sources", []),
                "sink": progress.get("sink", {})
            }
            
            logger.debug(f"Checkpoint stats for query {query.id}: {checkpoint_info}")
            return checkpoint_info
            
        except Exception as e:
            logger.error(f"Error getting checkpoint stats: {str(e)}", exc_info=True)
            return {"error": str(e)}
    
    def ensure_checkpoint_directory(self, checkpoint_path: str) -> bool:
        """
        Ensure checkpoint directory exists (create if needed)
        
        Args:
            checkpoint_path: Checkpoint path
        
        Returns:
            True if directory exists or created, False otherwise
        """
        try:
            # In production, this would create S3/MinIO directory
            # For local development, create local directory
            if checkpoint_path.startswith("file://") or not checkpoint_path.startswith("s3"):
                # Local file system
                path = Path(checkpoint_path.replace("file://", ""))
                path.mkdir(parents=True, exist_ok=True)
                logger.debug(f"Created checkpoint directory: {path}")
            else:
                # S3/MinIO - directory created automatically on first write
                logger.debug(f"Checkpoint directory will be created on first write: {checkpoint_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error ensuring checkpoint directory: {str(e)}", exc_info=True)
            return False


# Convenience function
def get_checkpoint_path(job_name: str, config_path: str = "spark_config.yaml") -> str:
    """
    Get checkpoint path for a job (convenience function)
    
    Args:
        job_name: Job name
        config_path: Path to config file
    
    Returns:
        Checkpoint path
    
    Example:
        path = get_checkpoint_path("user_events")
        # Returns: "s3a://data-lake/checkpoints/bronze_ingestion/user_events/"
    """
    manager = CheckpointManager(config_path=config_path)
    return manager.get_checkpoint_path(job_name)

