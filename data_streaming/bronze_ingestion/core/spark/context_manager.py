"""
Spark Context Manager - Production ML Pipeline
Manages Spark lifecycle: graceful shutdown, resource cleanup, signal handling
Critical for production jobs to prevent data loss and resource leaks
"""
import atexit
import signal
import sys
from typing import Optional, List, Callable
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery
from libs.logger import get_logger
from libs.exceptions import PipelineException

logger = get_logger(__name__)


class SparkContextError(PipelineException):
    """Raised when Spark context operations fail"""
    pass


class SparkContextManager:
    """
    Production-grade Spark context manager
    
    Handles:
    - Graceful shutdown on SIGTERM/SIGINT
    - Streaming query cleanup
    - Resource cleanup
    - Exit handlers
    
    Example:
        manager = SparkContextManager(spark)
        query = spark.readStream...start()
        manager.register_streaming_query(query)
        # On shutdown, queries are stopped gracefully
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize context manager
        
        Args:
            spark: SparkSession instance to manage
        """
        self.spark = spark
        self.streaming_queries: List[StreamingQuery] = []
        self.cleanup_handlers: List[Callable] = []
        self._shutdown_registered = False
        
        # Register shutdown handlers
        self._register_shutdown_handlers()
    
    def _register_shutdown_handlers(self):
        """Register signal handlers and exit handlers for graceful shutdown"""
        if self._shutdown_registered:
            return
        
        # Register exit handler (called on normal exit)
        atexit.register(self.cleanup)
        
        # Register signal handlers (called on SIGTERM/SIGINT)
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        self._shutdown_registered = True
        logger.info("Shutdown handlers registered")
    
    def _signal_handler(self, signum, frame):
        """
        Handle shutdown signals (SIGTERM, SIGINT)
        
        Args:
            signum: Signal number
            frame: Current stack frame
        """
        signal_name = signal.Signals(signum).name
        logger.warning(f"Received {signal_name} signal, initiating graceful shutdown...")
        self.cleanup()
        sys.exit(0)
    
    def register_streaming_query(self, query: StreamingQuery):
        """
        Register a streaming query for cleanup on shutdown
        
        Args:
            query: StreamingQuery instance to manage
            
        Example:
            query = df.writeStream.start()
            manager.register_streaming_query(query)
        """
        if query not in self.streaming_queries:
            self.streaming_queries.append(query)
            logger.info(f"Registered streaming query: {query.id}")
        else:
            logger.warning(f"Streaming query {query.id} already registered")
    
    def register_cleanup_handler(self, handler: Callable):
        """
        Register a custom cleanup function to call on shutdown
        
        Args:
            handler: Function to call during cleanup
            
        Example:
            def cleanup_custom_resource():
                # Close connections, files, etc.
                pass
            manager.register_cleanup_handler(cleanup_custom_resource)
        """
        if handler not in self.cleanup_handlers:
            self.cleanup_handlers.append(handler)
            logger.info("Registered custom cleanup handler")
    
    def stop_all_streaming_queries(self, timeout_seconds: int = 60):
        """
        Stop all registered streaming queries gracefully
        
        Args:
            timeout_seconds: Max time to wait for query to stop
            
        Raises:
            SparkContextError: If query cannot be stopped
        """
        if not self.streaming_queries:
            logger.info("No streaming queries to stop")
            return
        
        logger.info(f"Stopping {len(self.streaming_queries)} streaming queries...")
        
        for query in self.streaming_queries:
            try:
                query_id = query.id
                query_name = query.name if hasattr(query, 'name') else f"query-{query_id}"
                
                logger.info(f"Stopping streaming query: {query_name} (id: {query_id})")
                
                # Check if query is active
                if query.isActive:
                    # Stop query gracefully
                    query.stop()
                    
                    # Wait for query to stop (with timeout)
                    try:
                        query.awaitTermination(timeout=timeout_seconds)
                        logger.info(f"Streaming query {query_name} stopped successfully")
                    except Exception as e:
                        logger.warning(f"Query {query_name} did not stop within timeout: {str(e)}")
                else:
                    logger.info(f"Streaming query {query_name} was already stopped")
                    
            except Exception as e:
                logger.error(f"Error stopping streaming query {query.id}: {str(e)}", exc_info=True)
                raise SparkContextError(f"Failed to stop streaming query: {str(e)}")
        
        # Clear the list
        self.streaming_queries.clear()
        logger.info("All streaming queries stopped")
    
    def cleanup(self):
        """
        Cleanup all resources: stop queries, call handlers, stop Spark
        
        This is called automatically on:
        - Normal program exit
        - SIGTERM signal (Kubernetes pod termination)
        - SIGINT signal (Ctrl+C)
        """
        logger.info("Starting cleanup process...")
        
        try:
            # Stop all streaming queries first (most important)
            self.stop_all_streaming_queries()
            
            # Call custom cleanup handlers
            for handler in self.cleanup_handlers:
                try:
                    logger.info("Calling custom cleanup handler...")
                    handler()
                except Exception as e:
                    logger.error(f"Error in cleanup handler: {str(e)}", exc_info=True)
            
            # Stop SparkSession (if not already stopped)
            if self.spark is not None:
                try:
                    logger.info("Stopping SparkSession...")
                    self.spark.stop()
                    logger.info("SparkSession stopped successfully")
                except Exception as e:
                    logger.error(f"Error stopping SparkSession: {str(e)}", exc_info=True)
            
            logger.info("Cleanup completed successfully")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}", exc_info=True)
            raise SparkContextError(f"Cleanup failed: {str(e)}")
    
    def wait_for_queries(self, timeout_seconds: Optional[int] = None):
        """
        Wait for all streaming queries to terminate
        
        Args:
            timeout_seconds: Optional timeout (None = wait forever)
            
        Example:
            # Start queries
            query1 = df1.writeStream.start()
            query2 = df2.writeStream.start()
            manager.register_streaming_query(query1)
            manager.register_streaming_query(query2)
            
            # Wait for queries (blocks until stopped)
            manager.wait_for_queries()
        """
        if not self.streaming_queries:
            logger.warning("No streaming queries registered")
            return
        
        logger.info(f"Waiting for {len(self.streaming_queries)} streaming queries to terminate...")
        
        for query in self.streaming_queries:
            try:
                if query.isActive:
                    if timeout_seconds:
                        query.awaitTermination(timeout=timeout_seconds)
                    else:
                        query.awaitTermination()  # Wait forever
            except Exception as e:
                logger.error(f"Error waiting for query {query.id}: {str(e)}", exc_info=True)


# Context manager for Python 'with' statement
class SparkContext:
    """
    Context manager for Spark operations (Python 'with' statement)
    
    Automatically handles cleanup when exiting 'with' block
    
    Example:
        with SparkContext(spark) as ctx:
            query = df.writeStream.start()
            ctx.register_streaming_query(query)
            # Query automatically stopped when exiting 'with' block
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize context manager
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        self.manager: Optional[SparkContextManager] = None
    
    def __enter__(self) -> SparkContextManager:
        """Enter context manager"""
        self.manager = SparkContextManager(self.spark)
        return self.manager
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager - cleanup automatically"""
        if self.manager:
            self.manager.cleanup()
        
        # Return False to propagate exceptions, True to suppress
        return False


# Convenience function
def create_context_manager(spark: SparkSession) -> SparkContextManager:
    """
    Create SparkContextManager (convenience function)
    
    Args:
        spark: SparkSession instance
        
    Returns:
        SparkContextManager instance
        
    Example:
        spark = get_spark_session()
        manager = create_context_manager(spark)
        query = df.writeStream.start()
        manager.register_streaming_query(query)
    """
    return SparkContextManager(spark)

