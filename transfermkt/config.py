"""
Configuration module for TransferMkt data pipeline.

This module centralizes all configuration settings including API endpoints,
AWS credentials, S3 bucket names, and data paths.
"""

import os
from typing import Dict, List


class Config:
    """Configuration class for TransferMkt data pipeline."""
    
    # API Configuration
    BASE_URL = "https://transfermarkt-api.fly.dev/"
    
    # AWS Configuration
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_REGION = "us-east-1"
    
    # S3 Configuration
    S3_BUCKET_NAME = "transfermkt-data"
    
    # Data paths on S3
    RAW_DATA_PATHS = {
        'club_profiles': 'raw_data/club_profiles_data',
        'players': 'raw_data/players_data',
        'player_profile': 'raw_data/players_profile_data',
        'player_jersey_numbers': 'raw_data/players_jersey_numbers_data',
        'player_market_value': 'raw_data/players_market_value_data',
        'player_stats': 'raw_data/player_stats_data',
        'player_injuries': 'raw_data/players_injuries_data',
        'player_achievements': 'raw_data/players_achievements_data',
        'player_transfers': 'raw_data/players_transfers_data',
        'league_table': 'raw_data/league_data'
    }
    
    TRANSFORMED_DATA_PREFIX = "transformed_data"
    
    # Competition Configuration
    DEFAULT_COMPETITION_CODE = 'MLS1'
    DEFAULT_LEAGUE_NAME = 'major league soccer'
    
    # Glue Configuration
    GLUE_DATABASE = 'transfermarket_analytics'
    CRAWLER_NAMES = [
        'club_profile_crawler',
        'league_data_crawler',
        'players_data_crawler',
        'player_achievements_crawler',
        'player_injuries_crawler',
        'player_market_value_crawler',
        'player_profile_crawler',
        'player_stats_crawler',
        'player_transfers_crawler'
    ]
    
    # Processing Configuration
    MAX_WORKERS = 100
    FILES_TO_KEEP = 1
    
    @classmethod
    def validate_aws_credentials(cls) -> bool:
        """Validate that AWS credentials are available."""
        return bool(cls.AWS_ACCESS_KEY_ID and cls.AWS_SECRET_ACCESS_KEY)
    
    @classmethod
    def get_s3_path(cls, data_type: str) -> str:
        """Get S3 path for a specific data type."""
        return cls.RAW_DATA_PATHS.get(data_type, f"raw_data/{data_type}_data")