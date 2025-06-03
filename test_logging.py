#!/usr/bin/env python3
"""
Test script to verify logging configuration is working correctly.
Run this script to see sample log output.
"""

import sys
import os

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.logging_config import setup_logging, get_logger

def test_logging():
    """Test the logging configuration."""
    setup_logging()
    
    # Test loggers from different modules
    app_logger = get_logger("app")
    service_logger = get_logger("services.ingestion")
    connector_logger = get_logger("connectors.alphavantage")
    repo_logger = get_logger("repositories.data_repository")
    
    print("Testing logging configuration...")
    print("=" * 50)
    
    # Test different log levels
    app_logger.debug("This is a DEBUG message from app")
    app_logger.info("This is an INFO message from app")
    app_logger.warning("This is a WARNING message from app")
    app_logger.error("This is an ERROR message from app")
    
    service_logger.info("Testing ingestion service logger")
    connector_logger.info("Testing alphavantage connector logger")
    repo_logger.info("Testing data repository logger")
    
    print("=" * 50)
    print("Logging test completed!")
    print("Check the 'ingestion_service.log' file for detailed logs.")

if __name__ == "__main__":
    test_logging()
