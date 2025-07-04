"""
Watermark and control table utilities for TransferMkt data pipeline.

This module manages data completeness tracking and helps identify missing data
to optimize API calls and ensure data quality.
"""

import pandas as pd
import boto3
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Tuple
import logging
from dataclasses import dataclass

from .config import Config
from .io_utils import S3Client


@dataclass
class DataSourceConfig:
    """Configuration for each data source"""
    name: str
    s3_key_pattern: str  # Pattern like "raw_data/{source}_data/{source}_data_{date}.json"
    required_for_teams: bool = True  # Whether this data source is required for each team
    frequency: str = "daily"  # daily, weekly, monthly
    depends_on: List[str] = None  # Other data sources this depends on


class WatermarkManager:
    """Manages watermark/control table for tracking data completeness"""
    
    def __init__(self):
        self.s3_client = S3Client()
        self.config = Config()
        
        # Define your data sources
        self.data_sources = {
            'club_profiles': DataSourceConfig(
                name='club_profiles',
                s3_key_pattern='raw_data/club_profiles_data/club_profile_data_{date}.json',
                required_for_teams=True
            ),
            'players_profile': DataSourceConfig(
                name='players_profile', 
                s3_key_pattern='raw_data/players_profile_data/players_profile_data_{date}.json',
                required_for_teams=True
            ),
            'player_stats': DataSourceConfig(
                name='player_stats',
                s3_key_pattern='raw_data/player_stats_data/player_stats_data_{date}.json',
                required_for_teams=True
            ),
            'players_achievements': DataSourceConfig(
                name='players_achievements',
                s3_key_pattern='raw_data/players_achievements_data/players_achievements_data_{date}.json',
                required_for_teams=True
            ),
            'players_data': DataSourceConfig(
                name='players_data',
                s3_key_pattern='raw_data/players_data/club_players_data_{date}.json',
                required_for_teams=True
            ),
            'players_injuries': DataSourceConfig(
                name='players_injuries',
                s3_key_pattern='raw_data/players_injuries_data/players_injuries_data_{date}.json',
                required_for_teams=True
            ),
            'players_market_value': DataSourceConfig(
                name='players_market_value',
                s3_key_pattern='raw_data/players_market_value_data/players_market_value_data_{date}.json',
                required_for_teams=True
            ),
            'players_transfers': DataSourceConfig(
                name='players_transfers',
                s3_key_pattern='raw_data/players_transfers_data/players_transfers_data_{date}.json',
                required_for_teams=True
            ),
            'leagues_table': DataSourceConfig(
                name='leagues_table',
                s3_key_pattern='raw_data/league_data/league_table_data_{date}.json',
                required_for_teams=False  # League data is not team-specific
            )
        }
    
    def create_watermark_table(self, date: str, force_refresh: bool = False) -> pd.DataFrame:
        """Create watermark table for tracking data completeness"""
        try:
            # Get list of teams from config or existing data
            teams = self._get_team_list(date)
            
            watermark_data = []
            
            for team_id in teams:
                for source_name, source_config in self.data_sources.items():
                    if not source_config.required_for_teams and source_name != 'leagues_table':
                        continue
                        
                    # Check if data exists for this team/source/date
                    if source_config.required_for_teams:
                        # For team-specific data, check if team actually has data in the file
                        data_exists, record_count = self._check_team_data_exists(source_config, date, team_id)
                    else:
                        # For non-team specific data, just check file existence
                        data_exists = self._check_data_exists(source_config, date, None)
                        record_count = 0
                    
                    watermark_data.append({
                        'date': date,
                        'team_id': team_id if source_config.required_for_teams else 'ALL',
                        'data_source': source_name,
                        'data_exists': data_exists,
                        'last_checked': datetime.now().isoformat(),
                        'file_size_bytes': self._get_file_size(source_config, date) if data_exists else 0,
                        'record_count': record_count,
                        'data_quality_score': None,  # Will be populated after validation
                        'needs_refresh': not data_exists or force_refresh
                    })
            
            # Add league table entry (not team-specific)
            league_config = self.data_sources['leagues_table']
            league_exists = self._check_data_exists(league_config, date, None)
            watermark_data.append({
                'date': date,
                'team_id': 'ALL',
                'data_source': 'leagues_table',
                'data_exists': league_exists,
                'last_checked': datetime.now().isoformat(),
                'file_size_bytes': self._get_file_size(league_config, date) if league_exists else 0,
                'record_count': None,
                'data_quality_score': None,
                'needs_refresh': not league_exists or force_refresh
            })
            
            df = pd.DataFrame(watermark_data)
            
            # Save watermark table
            watermark_key = f"control_data/watermark_table_{date}.csv"
            self.s3_client.upload_dataframe(df, watermark_key)
            
            total_missing = len(df[df['needs_refresh'] == True])
            logging.info(f"Created watermark table with {len(df)} entries for {date}")
            logging.info(f"Found {total_missing} missing/incomplete data sources")
            return df
            
        except Exception as e:
            logging.error(f"Error creating watermark table: {e}", exc_info=True)
            raise
    
    def get_missing_data_sources(self, date: str) -> Dict[str, List[str]]:
        """Get list of missing data sources by team"""
        try:
            # Load or create watermark table
            watermark_df = self._load_watermark_table(date)
            if watermark_df is None:
                watermark_df = self.create_watermark_table(date)
            
            # Find missing data
            missing_data = {}
            
            missing_records = watermark_df[watermark_df['needs_refresh'] == True]
            
            for _, row in missing_records.iterrows():
                team_id = row['team_id']
                data_source = row['data_source']
                
                if team_id not in missing_data:
                    missing_data[team_id] = []
                missing_data[team_id].append(data_source)
            
            logging.info(f"Found missing data for {len(missing_data)} teams on {date}")
            return missing_data
            
        except Exception as e:
            logging.error(f"Error getting missing data sources: {e}", exc_info=True)
            return {}
    
    def update_data_status(self, date: str, team_id: str, data_source: str, 
                          success: bool, record_count: int = None, 
                          data_quality_score: float = None):
        """Update the watermark table after data fetch/processing"""
        try:
            watermark_df = self._load_watermark_table(date)
            if watermark_df is None:
                logging.warning(f"No watermark table found for {date}, creating new one")
                watermark_df = self.create_watermark_table(date)
            
            # Update the specific record
            mask = (watermark_df['team_id'] == team_id) & (watermark_df['data_source'] == data_source)
            
            if mask.any():
                watermark_df.loc[mask, 'data_exists'] = success
                watermark_df.loc[mask, 'needs_refresh'] = not success
                watermark_df.loc[mask, 'last_checked'] = datetime.now().isoformat()
                
                if record_count is not None:
                    watermark_df.loc[mask, 'record_count'] = record_count
                
                if data_quality_score is not None:
                    watermark_df.loc[mask, 'data_quality_score'] = data_quality_score
                
                # Save updated watermark table
                watermark_key = f"control_data/watermark_table_{date}.csv"
                self.s3_client.upload_dataframe(watermark_df, watermark_key)
                
                logging.info(f"Updated watermark for {team_id}/{data_source}: success={success}")
            else:
                logging.warning(f"No watermark record found for {team_id}/{data_source}")
                
        except Exception as e:
            logging.error(f"Error updating data status: {e}", exc_info=True)
    
    def get_data_completeness_report(self, date: str) -> Dict:
        """Generate a completeness report for the given date"""
        try:
            watermark_df = self._load_watermark_table(date)
            if watermark_df is None:
                return {"error": "No watermark table found"}
            
            total_expected = len(watermark_df)
            total_complete = len(watermark_df[watermark_df['data_exists'] == True])
            
            completeness_by_source = watermark_df.groupby('data_source').agg({
                'data_exists': ['count', 'sum'],
                'record_count': 'sum',
                'file_size_bytes': 'sum'
            }).round(2)
            
            completeness_by_team = watermark_df[watermark_df['team_id'] != 'ALL'].groupby('team_id').agg({
                'data_exists': ['count', 'sum']
            }).round(2)
            
            return {
                'date': date,
                'overall_completeness': round(total_complete / total_expected * 100, 2),
                'total_expected_files': total_expected,
                'total_complete_files': total_complete,
                'missing_files': total_expected - total_complete,
                'completeness_by_source': completeness_by_source.to_dict(),
                'completeness_by_team': completeness_by_team.to_dict(),
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            logging.error(f"Error generating completeness report: {e}", exc_info=True)
            return {"error": str(e)}
    
    def _get_team_list(self, date: str) -> List[str]:
        """Get list of team IDs from config or existing data"""
        try:
            # Try to get from config first
            if hasattr(self.config, 'TEAM_IDS') and self.config.TEAM_IDS:
                logging.info(f"Using {len(self.config.TEAM_IDS)} teams from config: {self.config.TEAM_IDS}")
                return self.config.TEAM_IDS
            
            # Fallback: try to extract from existing club profiles data
            try:
                club_data = self.s3_client.load_json_from_s3(
                    f"raw_data/club_profiles_data/club_profile_data_{date}.json"
                )
                if club_data and 'data' in club_data:
                    team_ids = []
                    for item in club_data['data']:
                        if 'clubs' in item:
                            for club in item['clubs']:
                                if 'id' in club:
                                    team_ids.append(str(club['id']))
                    if team_ids:
                        unique_teams = list(set(team_ids))  # Remove duplicates
                        logging.info(f"Extracted {len(unique_teams)} teams from club profiles data: {unique_teams}")
                        return unique_teams
            except Exception as e:
                logging.warning(f"Could not extract teams from club profiles: {e}")
            
            # Try to extract from any existing players data
            try:
                players_data = self.s3_client.load_json_from_s3(
                    f"raw_data/players_data/club_players_data_{date}.json"
                )
                if players_data and 'data' in players_data:
                    team_ids = []
                    for item in players_data['data']:
                        if 'players' in item and 'players' in item['players']:
                            for player in item['players']['players']:
                                if 'club' in player and 'id' in player['club']:
                                    team_ids.append(str(player['club']['id']))
                    if team_ids:
                        unique_teams = list(set(team_ids))  # Remove duplicates
                        logging.info(f"Extracted {len(unique_teams)} teams from players data: {unique_teams}")
                        return unique_teams
            except Exception as e:
                logging.warning(f"Could not extract teams from players data: {e}")
            
            # Final fallback: use a default MLS team list if nothing else works
            default_mls_teams = [
                "583",   # LA Galaxy
                "6977",  # LAFC  
                "1031",  # New York City FC
                "1769",  # New York Red Bulls
                "1706",  # Seattle Sounders
                "2089",  # Portland Timbers
                "1020",  # Atlanta United
                "3449",  # Inter Miami
                "1037",  # Orlando City
                "1708",  # FC Dallas
                "1707",  # Sporting Kansas City
                "1709",  # Colorado Rapids
                "1710",  # Real Salt Lake
                "1712",  # San Jose Earthquakes
                "1711",  # Vancouver Whitecaps
                "1033",  # Toronto FC
                "1034",  # Montreal Impact
                "1036",  # New England Revolution
                "1035",  # Columbus Crew
                "1038",  # Chicago Fire
                "3560",  # Minnesota United
                "3562",  # FC Cincinnati
                "3559",  # Nashville SC
                "3561",  # Austin FC
                "3558",  # Charlotte FC
                "1713",  # Houston Dynamo
                "3563",  # St. Louis City SC
            ]
            logging.warning(f"Using default MLS team list with {len(default_mls_teams)} teams")
            return default_mls_teams
            
        except Exception as e:
            logging.error(f"Error getting team list: {e}")
            return []
    
    def _check_data_exists(self, source_config: DataSourceConfig, date: str, team_id: str = None) -> bool:
        """Check if data file exists in S3"""
        try:
            s3_key = source_config.s3_key_pattern.format(date=date)
            return self.s3_client.file_exists(s3_key)
        except Exception:
            return False
    
    def _get_file_size(self, source_config: DataSourceConfig, date: str) -> int:
        """Get file size in bytes"""
        try:
            s3_key = source_config.s3_key_pattern.format(date=date)
            return self.s3_client.get_file_size(s3_key)
        except Exception:
            return 0
    
    def _load_watermark_table(self, date: str) -> Optional[pd.DataFrame]:
        """Load existing watermark table"""
        try:
            watermark_key = f"control_data/watermark_table_{date}.csv"
            if self.s3_client.file_exists(watermark_key):
                return self.s3_client.load_dataframe_from_s3(watermark_key)
            return None
        except Exception as e:
            logging.warning(f"Could not load watermark table for {date}: {e}")
            return None
    
    def _check_team_data_exists(self, source_config: DataSourceConfig, date: str, team_id: str) -> Tuple[bool, int]:
        """Check if team-specific data exists and get record count"""
        try:
            s3_key = source_config.s3_key_pattern.format(date=date)
            if not self.s3_client.file_exists(s3_key):
                return False, 0
            
            # Load the data file and check if the specific team has data
            data = self.s3_client.load_json_from_s3(s3_key)
            if not data or 'data' not in data:
                return False, 0
            
            record_count = 0
            found_team = False
            
            # Debug: Log the data structure to understand format
            logging.debug(f"Checking data structure for {source_config.name}: {type(data.get('data', []))}")
            
            # Check different data structures based on source type
            if source_config.name == 'club_profiles':
                # Handle club profiles structure: data['data'] contains club info directly
                data_content = data.get('data', {})
                if 'clubs' in data_content:
                    for club in data_content['clubs']:
                        if isinstance(club, dict) and 'id' in club and str(club['id']) == team_id:
                            found_team = True
                            record_count += 1
                else:
                    # Alternative structure: data['data'] might be the club data itself
                    if isinstance(data_content, dict) and 'id' in data_content and str(data_content['id']) == team_id:
                        found_team = True
                        record_count = 1
                                
            elif source_config.name in ['players_profile', 'player_stats', 'players_achievements', 
                                       'players_injuries', 'players_market_value', 'players_transfers']:
                for item in data['data']:
                    if isinstance(item, dict) and 'players' in item:
                        # Check if this player belongs to the team
                        player_data = item['players']
                        
                        # Handle different player data structures
                        if isinstance(player_data, dict):
                            if 'club' in player_data and isinstance(player_data['club'], dict) and 'id' in player_data['club']:
                                if str(player_data['club']['id']) == team_id:
                                    found_team = True
                                    record_count += 1
                            elif 'players' in player_data:  # For nested players structure
                                players_list = player_data.get('players', [])
                                if isinstance(players_list, list):
                                    for player in players_list:
                                        if isinstance(player, dict) and 'club' in player and isinstance(player['club'], dict) and 'id' in player['club']:
                                            if str(player['club']['id']) == team_id:
                                                found_team = True
                                                record_count += 1
                                        
            elif source_config.name == 'players_data':
                for item in data['data']:
                    if isinstance(item, dict) and 'players' in item and isinstance(item['players'], dict):
                        players_data = item['players']
                        if 'players' in players_data and isinstance(players_data['players'], list):
                            for player in players_data['players']:
                                if isinstance(player, dict) and 'club' in player and isinstance(player['club'], dict) and 'id' in player['club']:
                                    if str(player['club']['id']) == team_id:
                                        found_team = True
                                        record_count += 1
            
            logging.debug(f"Team {team_id} in {source_config.name}: found={found_team}, records={record_count}")
            return found_team, record_count
            
        except Exception as e:
            logging.warning(f"Error checking team data for {team_id} in {source_config.name}: {e}")
            return False, 0