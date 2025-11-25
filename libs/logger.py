"""
Production Logging Module - Netflix Standards
First file ML engineers create - enables visibility into pipeline operations
"""
import logging
import os
import sys
from pathlib import Path
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Optional


def setup_logger(
    name: str,
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    format_string: Optional[str] = None,
    max_bytes: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5
) -> logging.Logger:
    """
    Setup production-ready logger (Netflix standards)
    
    Args:
        name: Logger name (usually __name__)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path to write logs
        format_string: Custom log format
        max_bytes: Max log file size before rotation (default: 10MB)
        backup_count: Number of backup log files to keep
        
    Returns:
        Configured logger instance
        
    Example:
        logger = setup_logger(__name__)
        logger.info("Pipeline started")
        logger.error("Something failed", exc_info=True)
    """
    # Create logger
    logger = logging.getLogger(name)
    
    # Set log level
    valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    if log_level.upper() not in valid_levels:
        raise ValueError(f"Invalid log level: {log_level}. Must be one of {valid_levels}")
    
    level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(level)
    
    # Prevent duplicate logs (if logger already configured)
    if logger.handlers:
        return logger
    
    # Default format: timestamp, name, level, file:line, message
    if format_string is None:
        format_string = (
            '%(asctime)s - %(name)s - %(levelname)s - '
            '[%(filename)s:%(lineno)d] - %(message)s'
        )
    
    formatter = logging.Formatter(format_string)
    
    # Console handler (always log to console - Netflix standard)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler with rotation (production standard)
    if log_file:
        # Create logs directory if it doesn't exist
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Use RotatingFileHandler to prevent disk space issues
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str = None, log_file: Optional[str] = None) -> logging.Logger:
    """
    Get existing logger or create new one with default settings
    
    Args:
        name: Logger name (default: root logger)
        log_file: Optional log file path
        
    Returns:
        Logger instance
        
    Example:
        logger = get_logger(__name__)
        logger.info("Message")
    """
    if name is None:
        name = __name__
    
    logger = logging.getLogger(name)
    
    # If logger not configured, setup with defaults
    if not logger.handlers:
        # Get log level from environment or default to INFO
        log_level = os.getenv("LOG_LEVEL", "INFO")
        
        # Default log file location (in project logs/ directory)
        if log_file is None:
            project_root = Path(__file__).parent.parent
            log_file = str(project_root / "logs" / f"{name.replace('.', '_')}.log")
        
        logger = setup_logger(name, log_level=log_level, log_file=log_file)
    
    return logger


def log_performance(logger: logging.Logger, operation: str, duration_ms: float):
    """
    Log performance metrics (Netflix standard)
    
    Args:
        logger: Logger instance
        operation: Operation name
        duration_ms: Duration in milliseconds
        
    Example:
        log_performance(logger, "kafka_produce", 45.2)
    """
    logger.info(f"PERF: {operation} took {duration_ms:.2f}ms")


def log_error_with_context(
    logger: logging.Logger,
    error: Exception,
    context: dict,
    operation: str = "operation"
):
    """
    Log error with context (Netflix standard for debugging)
    
    Args:
        logger: Logger instance
        error: Exception that occurred
        context: Additional context dictionary
        operation: Operation name
        
    Example:
        log_error_with_context(
            logger, 
            exception, 
            {"user_id": "123", "event_id": "evt_456"},
            "kafka_produce"
        )
    """
    context_str = ", ".join([f"{k}={v}" for k, v in context.items()])
    logger.error(
        f"ERROR in {operation}: {str(error)} | Context: {context_str}",
        exc_info=True
    )

