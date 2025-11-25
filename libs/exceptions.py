"""
Custom Exception Classes - Netflix Production Standards
Specific error types for better error handling and debugging
"""
from typing import Optional, Dict, Any


class PipelineException(Exception):
    """
    Base exception for all pipeline errors
    Netflix standard: All custom exceptions inherit from this
    """
    def __init__(self, message: str, context: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.context = context or {}
    
    def __str__(self):
        if self.context:
            context_str = ", ".join([f"{k}={v}" for k, v in self.context.items()])
            return f"{self.message} | Context: {context_str}"
        return self.message


class SchemaValidationError(PipelineException):
    """
    Raised when schema validation fails
    Used in Bronze layer when validating incoming data
    """
    pass


class DataQualityError(PipelineException):
    """
    Raised when data quality checks fail
    Used when data doesn't meet quality standards
    """
    pass


class KafkaConnectionError(PipelineException):
    """
    Raised when Kafka connection fails
    Used when producer/consumer cannot connect to Kafka
    """
    pass


class KafkaProduceError(PipelineException):
    """
    Raised when Kafka produce operation fails
    Used when message cannot be sent to Kafka
    """
    pass


class S3WriteError(PipelineException):
    """
    Raised when S3 write operation fails
    Used when data cannot be written to S3/MinIO
    """
    pass


class ConfigLoadError(PipelineException):
    """
    Raised when configuration cannot be loaded
    Used when config files are missing or invalid
    """
    pass


class SparkJobError(PipelineException):
    """
    Raised when Spark job fails
    Used when Spark streaming job encounters errors
    """
    pass

