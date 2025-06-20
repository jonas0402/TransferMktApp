"""
TransferMkt Players Data Extraction Script

This script serves as a thin wrapper that orchestrates player data extraction
using the modular transfermkt package. It maintains compatibility with existing
Airflow DAG while leveraging the refactored modular architecture.
"""

import time
import logging

from transfermkt.logger import setup_logging, log_execution_time
from transfermkt.config import Config
from transfermkt.player_logic import PlayerDataManager


@log_execution_time
def main():
    """
    Main function to orchestrate player data extraction and processing.
    
    This function serves as the entry point for the Airflow DAG task,
    maintaining the same interface while using the modular architecture.
    """
    # Set up logging
    setup_logging()
    
    # Validate AWS credentials
    if not Config.validate_aws_credentials():
        raise ValueError("AWS credentials not found in environment variables")
    
    # Initialize the player data manager
    player_manager = PlayerDataManager()
    
    # Extract all player data using the modular approach
    try:
        all_data = player_manager.extract_all_player_data()
        logging.info("Player data extraction completed successfully")
        
        # Log summary statistics
        logging.info("Data extraction summary:")
        for data_type, data in all_data.items():
            if isinstance(data, dict) and 'data' in data:
                count = len(data['data'])
                logging.info(f"  {data_type}: {count} records")
            elif isinstance(data, list):
                logging.info(f"  {data_type}: {len(data)} records")
                
    except Exception as e:
        logging.error(f"Player data extraction failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    logging.info(f"Total script execution time: {end_time - start_time:.2f} seconds")
