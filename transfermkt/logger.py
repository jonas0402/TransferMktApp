"""
Logging utilities for TransferMkt data pipeline.

This module provides centralized logging configuration and utilities.
"""

import logging
import time
from functools import wraps
from typing import Callable, Any


def setup_logging(level: int = logging.INFO, format_string: str = None) -> logging.Logger:
    """
    Set up logging configuration for the application.
    
    Args:
        level: Logging level (default: INFO)
        format_string: Custom format string for log messages
        
    Returns:
        Configured logger instance
    """
    if format_string is None:
        format_string = '%(asctime)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=level,
        format=format_string,
        force=True  # Override any existing configuration
    )
    
    return logging.getLogger(__name__)


def log_execution_time(func: Callable) -> Callable:
    """
    Decorator to log the execution time of a function.
    
    Args:
        func: Function to decorate
        
    Returns:
        Wrapped function with execution time logging
    """
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logging.info(f"Function '{func.__name__}' took {end_time - start_time:.2f} seconds")
        return result
    return wrapper


def get_logger(name: str = None) -> logging.Logger:
    """
    Get a logger instance with optional name.
    
    Args:
        name: Logger name (defaults to calling module)
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name or __name__)