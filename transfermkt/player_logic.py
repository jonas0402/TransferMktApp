"""
Player-specific business logic for TransferMkt data pipeline.

This module contains all player-related business logic including
data extraction, processing, and aggregation functions.
"""

from typing import Dict, Any, List, Optional
import logging
from concurrent.futures import ThreadPoolExecutor

from .config import Config
from .io_utils import APIClient, S3Client
from .logger import log_execution_time


class PlayerDataManager:
    """Manages player data extraction and processing operations."""
    
    def __init__(self):
        """Initialize the PlayerDataManager with required clients."""
        self.api_client = APIClient()
        self.s3_client = S3Client()
        self.player_ids = []
        self.club_ids = []
    
    @log_execution_time
    def get_club_ids(self, competition_id: str) -> Dict[str, Any]:
        """
        Retrieve club IDs for a given competition.
        
        Args:
            competition_id: Competition identifier
            
        Returns:
            Dictionary containing club data
        """
        endpoint = f"competitions/{competition_id}/clubs"
        data_dict = {}
        
        try:
            response_data = self.api_client.make_request(endpoint)
            if not response_data:
                raise Exception("Failed to get club data from API")
            
            data_dict["data"] = response_data
            self.club_ids.extend([club['id'] for club in response_data['clubs']])
            logging.info(f"Fetched {len(self.club_ids)} club IDs for competition ID: {competition_id}")
            return data_dict
        except Exception as e:
            logging.error(f"Error fetching club IDs: {e}")
            return None
    
    @log_execution_time
    def get_club_players(self, club_ids: List[str]) -> Dict[str, Any]:
        """
        Retrieve players for each club.
        
        Args:
            club_ids: List of club IDs
            
        Returns:
            Dictionary containing club players data
        """
        data_dict = {"data": []}
        
        for club_id in club_ids:
            endpoint = f"clubs/{club_id}/players"
            try:
                response_data = self.api_client.make_request(endpoint)
                if not response_data:
                    logging.warning(f"No data returned for club ID: {club_id}")
                    continue
                
                club_player_data = {
                    "club_id": club_id,
                    "players": response_data
                }
                data_dict["data"].append(club_player_data)
                self.player_ids.extend([player['id'] for player in response_data['players']])
                logging.info(f"Fetched players for club ID: {club_id}")
            except Exception as e:
                logging.error(f"Error fetching players for club ID {club_id}: {e}")
        
        return data_dict
    
    @log_execution_time
    def get_player_data(self, endpoint_template: str, player_ids: List[str]) -> Dict[str, Any]:
        """
        Generic function to fetch player data for multiple players with improved error handling.
        
        Args:
            endpoint_template: API endpoint template with {} placeholder for player ID
            player_ids: List of player IDs
            
        Returns:
            Dictionary containing player data
        """
        data_dict = {"data": []}
        successful_requests = 0
        failed_requests = 0
        
        for player_id in player_ids:
            try:
                endpoint = endpoint_template.format(player_id)
                response_data = self.api_client.make_request(endpoint)
                
                if response_data is not None:
                    player_data = {
                        "player_id": player_id,
                        "players": response_data
                    }
                    data_dict["data"].append(player_data)
                    successful_requests += 1
                    logging.info(f"Successfully fetched data for player ID: {player_id}")
                else:
                    failed_requests += 1
                    logging.warning(f"Failed to fetch data for player ID: {player_id} after all retries")
                    
            except Exception as e:
                failed_requests += 1
                logging.error(f"Unexpected error fetching player data for ID {player_id}: {e}")
        
        logging.info(f"Player data fetch summary: {successful_requests} successful, {failed_requests} failed")
        
        # Return data even if some requests failed
        return data_dict
    
    @log_execution_time
    def get_player_data_concurrent(self, endpoint_templates: Dict[str, str], 
                                 player_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Fetch multiple types of player data concurrently.
        
        Args:
            endpoint_templates: Dictionary mapping data types to endpoint templates
            player_ids: List of player IDs
            
        Returns:
            Dictionary mapping data types to their respective data
        """
        results = {}
        
        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            futures = {
                data_type: executor.submit(self.get_player_data, template, player_ids)
                for data_type, template in endpoint_templates.items()
            }
            
            for data_type, future in futures.items():
                try:
                    results[data_type] = future.result()
                except Exception as e:
                    logging.error(f"Error fetching {data_type} data: {e}")
                    results[data_type] = {"data": []}
        
        return results
    
    def get_league_table_data(self, comp_name: str) -> List[Dict[str, Any]]:
        """
        Get league table data by scraping transfermarkt website.
        
        Args:
            comp_name: Competition name
            
        Returns:
            List of league table records
        """
        return self.api_client.scrape_transfermarkt_table(comp_name)
    
    @log_execution_time
    def upload_all_data_to_s3(self, data_dict: Dict[str, Any]) -> None:
        """
        Upload all collected data to S3.
        
        Args:
            data_dict: Dictionary containing all data to upload
        """
        # Define the mapping between data types and S3 paths
        s3_mappings = {
            'club_profiles': ('club_profile_data', Config.RAW_DATA_PATHS['club_profiles']),
            'club_players': ('club_players_data', Config.RAW_DATA_PATHS['players']),
            'players_profile': ('players_profile_data', Config.RAW_DATA_PATHS['player_profile']),
            'player_stats': ('player_stats_data', Config.RAW_DATA_PATHS['player_stats']),
            'players_market_value': ('players_market_value_data', Config.RAW_DATA_PATHS['player_market_value']),
            'players_achievements': ('players_achievements_data', Config.RAW_DATA_PATHS['player_achievements']),
            'players_injuries': ('players_injuries_data', Config.RAW_DATA_PATHS['player_injuries']),
            'players_transfers': ('players_transfers_data', Config.RAW_DATA_PATHS['player_transfers']),
            'league_table': ('league_table_data', Config.RAW_DATA_PATHS['league_table'])
        }
        
        for data_type, data in data_dict.items():
            if data_type in s3_mappings:
                file_name, folder_path = s3_mappings[data_type]
                self.s3_client.upload_json(data, file_name, folder_path)
            else:
                logging.warning(f"Unknown data type for S3 upload: {data_type}")
    
    @log_execution_time
    def cleanup_old_files(self) -> None:
        """Clean up old files in S3, keeping only the most recent ones."""
        for folder_path in Config.RAW_DATA_PATHS.values():
            self.s3_client.delete_old_files(folder_path, Config.FILES_TO_KEEP)
    
    def extract_all_player_data(self, competition_code: str = None, 
                              league_name: str = None) -> Dict[str, Any]:
        """
        Main orchestration method to extract all player-related data.
        
        Args:
            competition_code: Competition code (defaults to MLS1)
            league_name: League name for table scraping (defaults to 'major league soccer')
            
        Returns:
            Dictionary containing all extracted data
        """
        competition_code = competition_code or Config.DEFAULT_COMPETITION_CODE
        league_name = league_name or Config.DEFAULT_LEAGUE_NAME
        
        # Step 1: Get club data
        logging.info("Starting data extraction process...")
        club_profile_data = self.get_club_ids(competition_code)
        if not club_profile_data:
            raise Exception("Failed to get club profile data")
        
        # Step 2: Get players from clubs
        club_players_data = self.get_club_players(self.club_ids)
        if not club_players_data['data']:
            raise Exception("Failed to get club players data")
        
        # Step 3: Get league table data
        league_table_data = self.get_league_table_data(league_name)
        
        # Step 4: Get detailed player data concurrently
        endpoint_templates = {
            'players_profile': 'players/{}/profile',
            'player_stats': 'players/{}/stats',
            'players_market_value': 'players/{}/market_value',
            'players_achievements': 'players/{}/achievements',
            'players_injuries': 'players/{}/injuries',
            'players_transfers': 'players/{}/transfers',
        }
        
        player_data_results = self.get_player_data_concurrent(endpoint_templates, self.player_ids)
        
        # Combine all data
        all_data = {
            'club_profiles': club_profile_data,
            'club_players': club_players_data,
            'league_table': league_table_data,
            **player_data_results
        }
        
        # Step 5: Upload to S3
        self.upload_all_data_to_s3(all_data)
        
        # Step 6: Cleanup old files
        self.cleanup_old_files()
        
        logging.info("Data extraction process completed successfully")
        return all_data