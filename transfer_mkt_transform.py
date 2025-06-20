"""
TransferMkt Data Transformation Script

This script serves as a thin wrapper that orchestrates data transformation
using the modular transfermkt package. It maintains compatibility with existing
Airflow DAG while leveraging the refactored modular architecture.
"""

import time
import logging
from datetime import datetime

from transfermkt.logger import setup_logging, log_execution_time
from transfermkt.config import Config
from transfermkt.io_utils import S3Client
from transfermkt.transform_utils import (
    process_club_profiles,
    process_players_profile,
    process_player_stats,
    process_players_achievements,
    process_players_data,
    process_players_injuries,
    process_players_market_value,
    process_players_transfers,
    process_league_data
)


@log_execution_time
def load_all_data_from_s3(s3_client: S3Client) -> dict:
    """
    Load all required data from S3.
    
    Args:
        s3_client: S3 client instance
        
    Returns:
        Dictionary containing all loaded data
    """
    data_sources = {
        'club_profiles_data': 'raw_data/club_profiles_data/',
        'players_profile_data': 'raw_data/players_profile_data/',
        'player_stats_data': 'raw_data/player_stats_data/',
        'players_achievements_data': 'raw_data/players_achievements_data/',
        'players_data': 'raw_data/players_data/',
        'players_injuries_data': 'raw_data/players_injuries_data/',
        'players_market_value_data': 'raw_data/players_market_value_data/',
        'players_transfers_data': 'raw_data/players_transfers_data/',
        'leagues_table_data': 'raw_data/league_data/'
    }
    
    loaded_data = {}
    missing_data = []
    
    for data_type, s3_path in data_sources.items():
        logging.info(f"Loading {data_type} from S3...")
        data = s3_client.read_json_from_s3(s3_path)
        if data is None:
            missing_data.append(data_type)
            logging.error(f"{data_type} is missing from S3")
        else:
            loaded_data[data_type] = data
            logging.info(f"Successfully loaded {data_type}")
    
    if missing_data:
        raise ValueError(f"Missing required data: {', '.join(missing_data)}")
    
    return loaded_data


@log_execution_time
def transform_all_data(loaded_data: dict, output_prefix: str, current_date: str) -> dict:
    """
    Transform all loaded data using the modular transformation functions.
    
    Args:
        loaded_data: Dictionary containing all raw data
        output_prefix: Output path prefix for transformed data
        current_date: Current date string
        
    Returns:
        Dictionary containing all transformed DataFrames
    """
    transformed_data = {}
    
    # Define transformation mappings
    transformations = [
        ('club_profiles', process_club_profiles, 'club_profiles_data'),
        ('players_profile', process_players_profile, 'players_profile_data'),
        ('player_stats', process_player_stats, 'player_stats_data'),
        ('players_achievements', process_players_achievements, 'players_achievements_data'),
        ('players_data', process_players_data, 'players_data'),
        ('players_injuries', process_players_injuries, 'players_injuries_data'),
        ('players_market_value', process_players_market_value, 'players_market_value_data'),
        ('players_transfers', process_players_transfers, 'players_transfers_data'),
        ('league_data', process_league_data, 'leagues_table_data')
    ]
    
    for result_key, transform_func, data_key in transformations:
        try:
            logging.info(f"Transforming {result_key}...")
            df = transform_func(loaded_data[data_key], output_prefix, current_date)
            transformed_data[result_key] = df
            logging.info(f"Successfully transformed {result_key}: {len(df)} records")
        except Exception as e:
            logging.error(f"Failed to transform {result_key}: {e}", exc_info=True)
            raise
    
    return transformed_data


@log_execution_time
def cleanup_old_transformed_files(s3_client: S3Client) -> None:
    """
    Clean up old transformed files in S3, keeping only the most recent ones.
    
    Args:
        s3_client: S3 client instance
    """
    folders_to_cleanup = [
        'transformed_data/club_profiles_data',
        'transformed_data/player_profile_data',
        'transformed_data/player_stats_data',
        'transformed_data/player_achievements_data',
        'transformed_data/player_injuries_data',
        'transformed_data/player_transfers_data',
        'transformed_data/players_data',
        'transformed_data/player_market_value_data',
        'transformed_data/league_data'
    ]
    
    for folder in folders_to_cleanup:
        s3_client.delete_old_files(folder, Config.FILES_TO_KEEP)


@log_execution_time
def main():
    """
    Main function to orchestrate data transformation process.
    
    This function serves as the entry point for the Airflow DAG task,
    maintaining the same interface while using the modular architecture.
    """
    # Set up logging
    setup_logging()
    
    # Validate AWS credentials
    if not Config.validate_aws_credentials():
        raise ValueError("AWS credentials not found in environment variables")
    
    current_date = str(datetime.now().date())
    output_prefix = Config.TRANSFORMED_DATA_PREFIX
    
    # Initialize S3 client
    s3_client = S3Client()
    
    try:
        # Step 1: Load all data from S3
        logging.info("Starting data transformation process...")
        loaded_data = load_all_data_from_s3(s3_client)
        
        # Step 2: Transform all data
        transformed_data = transform_all_data(loaded_data, output_prefix, current_date)
        
        # Step 3: Clean up old files
        cleanup_old_transformed_files(s3_client)
        
        logging.info("Data transformation completed successfully")
        
        # Log summary statistics
        logging.info("Transformation summary:")
        for data_type, df in transformed_data.items():
            logging.info(f"  {data_type}: {len(df)} records, {len(df.columns)} columns")
            
    except Exception as e:
        logging.error("Data transformation failed. Aborting process.", exc_info=True)
        raise


if __name__ == '__main__':
    start_time = time.time()
    main()
    end_time = time.time()
    logging.info(f"Total script execution time: {end_time - start_time:.2f} seconds")

