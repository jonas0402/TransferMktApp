"""
Data transformation utilities for TransferMkt data pipeline.

This module contains all data transformation and processing functions
including type inference, data cleaning, and business logic transformations.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List
import logging
from datetime import datetime

from .config import Config
from .logger import log_execution_time


def parse_market_value(value: str) -> float:
    """
    Parse a market value string with units (k/m) into a float.
    
    Args:
        value: Market value string (e.g., '€10.5m', '€500k')
        
    Returns:
        Parsed market value as float
    """
    if not value:
        return float('nan')
    
    value = value.replace('€', '').strip().lower()
    try:
        if 'k' in value:
            return float(value.replace('k', '')) * 1000
        elif 'm' in value:
            return float(value.replace('m', '')) * 1000000
        else:
            return float(value)
    except Exception as e:
        logging.warning(f"Failed to parse market value '{value}': {e}")
        return float('nan')


def infer_glue_type(column: str, series: pd.Series) -> str:
    """
    Infer the Glue column data type based on column name and data.
    
    Args:
        column: Column name
        series: Pandas series with data
        
    Returns:
        Glue data type string
    """
    col_lower = column.lower()
    
    # Handle ID columns
    if 'clubid' in col_lower or '_id' in col_lower:
        return 'string'
    
    # Handle timestamp columns
    if 'updatedat' in col_lower:
        try:
            parsed = pd.to_datetime(series, errors='coerce')
            if parsed.notna().sum() > 0:
                return 'timestamp'
        except Exception:
            pass

    # Handle date columns
    if 'date' in col_lower:
        try:
            parsed = pd.to_datetime(series, errors='coerce')
            if parsed.notna().sum() > 0:
                return 'date'
        except Exception:
            pass

    # Infer from pandas dtype
    if pd.api.types.is_integer_dtype(series):
        return 'int'
    elif pd.api.types.is_float_dtype(series):
        return 'double'
    elif pd.api.types.is_bool_dtype(series):
        return 'boolean'
    elif pd.api.types.is_datetime64_any_dtype(series):
        return 'timestamp'
    else:
        return 'string'


@log_execution_time
def process_club_profiles(data: Dict[str, Any], output_prefix: str, current_date: str) -> pd.DataFrame:
    """
    Process club profiles data.
    
    Args:
        data: Raw club profiles data
        output_prefix: Output path prefix
        current_date: Current date string
        
    Returns:
        Processed DataFrame
    """
    try:
        df = pd.json_normalize(
            data['data'],
            'clubs',
            ['seasonId', 'updatedAt'],
            meta_prefix='club_',
            record_prefix='club_'
        )
        df['club_updatedAt'] = pd.to_datetime(df['club_updatedAt'], errors='raise')
        
        from .io_utils import S3Client
        s3_client = S3Client()
        s3_client.upload_dataframe(
            df, 
            f'{output_prefix}/club_profiles_data/club_profile_data_transformed_{current_date}.csv'
        )
        return df
    except Exception as e:
        logging.error(f"Error processing club profiles: {e}", exc_info=True)
        raise


@log_execution_time
def process_players_profile(data: Dict[str, Any], output_prefix: str, current_date: str) -> pd.DataFrame:
    """
    Process players profile data.
    
    Args:
        data: Raw players profile data
        output_prefix: Output path prefix
        current_date: Current date string
        
    Returns:
        Processed DataFrame
    """
    try:
        df = pd.json_normalize(data['data'], sep='_')
        df.columns = df.columns.str.replace('players', 'player')
        
        # Date transformations
        df['player_dateOfBirth'] = pd.to_datetime(df['player_dateOfBirth'], format='%Y-%m-%d', errors='raise')
        df['player_club_joined'] = pd.to_datetime(df['player_club_joined'], format='%Y-%m-%d', errors='raise')
        df['player_club_contractExpires'] = pd.to_datetime(df['player_club_contractExpires'], format='%Y-%m-%d', errors='raise')
        df['player_updatedAt'] = pd.to_datetime(df['player_updatedAt'], errors='raise')
        
        # Numeric transformations
        df['player_age'] = pd.to_numeric(df['player_age'], downcast='integer', errors='raise')
        
        # Height processing
        df['player_height'] = (
            df['player_height']
            .fillna('')
            .astype(str)
            .str.replace('m', '')
            .str.replace(',', '.')
        )
        df['player_height'] = pd.to_numeric(df['player_height'], errors='raise')
        
        # Shirt number processing
        df['player_shirtNumber'] = df['player_shirtNumber'].str.replace('#', '')
        
        # Market value processing
        df['player_marketValue'] = (
            df['player_marketValue']
            .fillna('')
            .astype(str)
            .str.replace('€', '')
            .map(parse_market_value)
        )
        
        # Expand citizenship and position arrays
        citizenship_df = df['player_citizenship'].apply(pd.Series)
        citizenship_df.columns = [f'player_citizenship_{i+1}' for i in range(citizenship_df.shape[1])]
        df = pd.concat([df, citizenship_df], axis=1)
        
        position_df = df['player_position_other'].apply(pd.Series)
        position_df.columns = [f'player_position_other_{i+1}' for i in range(position_df.shape[1])]
        df = pd.concat([df, position_df], axis=1)
        
        # Remove duplicate columns
        df = df.loc[:, ~df.columns.duplicated()]
        
        from .io_utils import S3Client
        s3_client = S3Client()
        s3_client.upload_dataframe(
            df, 
            f'{output_prefix}/player_profile_data/player_profile_data_transformed_{current_date}.csv'
        )
        return df
    except Exception as e:
        logging.error(f"Error processing players profile: {e}", exc_info=True)
        raise


@log_execution_time
def process_player_stats(data: Dict[str, Any], output_prefix: str, current_date: str) -> pd.DataFrame:
    """
    Process player statistics data.
    
    Args:
        data: Raw player stats data
        output_prefix: Output path prefix
        current_date: Current date string
        
    Returns:
        Processed DataFrame
    """
    try:
        stats_dfs = []
        for row in data['data']:
            if 'stats' in row['players']:
                normalized = pd.json_normalize(
                    row['players'],
                    'stats',
                    ['id', 'updatedAt'],
                    sep='_',
                    meta_prefix='player_',
                    record_prefix='player_',
                    errors='ignore'
                )
                stats_dfs.append(normalized)
        
        if not stats_dfs:
            raise ValueError("No player stats data available")
        
        df = pd.concat(stats_dfs, ignore_index=True)
        
        # Process minutes played
        df['player_minutesPlayed'] = df['player_minutesPlayed'].astype(str).str.replace("'", "", regex=False)
        df['player_minutesPlayed'] = pd.to_numeric(df['player_minutesPlayed'], errors='coerce')
        
        # Process numeric columns
        numeric_columns = [
            'player_appearances', 'player_goalsConceded', 'player_cleanSheets',
            'player_yellowCards', 'player_redCards', 'player_goals',
            'player_secondYellowCards', 'player_assists'
        ]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='raise')
        
        df['player_updatedAt'] = pd.to_datetime(df['player_updatedAt'], errors='raise')
        
        from .io_utils import S3Client
        s3_client = S3Client()
        s3_client.upload_dataframe(
            df, 
            f'{output_prefix}/player_stats_data/player_stats_data_transformed_{current_date}.csv'
        )
        return df
    except Exception as e:
        logging.error(f"Error processing player stats: {e}", exc_info=True)
        raise


@log_execution_time
def process_players_achievements(data: Dict[str, Any], output_prefix: str, current_date: str) -> pd.DataFrame:
    """
    Process player achievements data.
    
    Args:
        data: Raw player achievements data
        output_prefix: Output path prefix
        current_date: Current date string
        
    Returns:
        Processed DataFrame
    """
    try:
        achievements_dfs = []
        for player in data['data']:
            if 'achievements' in player['players']:
                normalized = pd.json_normalize(
                    player['players'],
                    ['achievements', ['details']],
                    ['id', ['achievements', 'title'], ['achievements', 'count'], 'updatedAt'],
                    sep='_',
                    meta_prefix='player_',
                    record_prefix='player_',
                    errors='raise'
                )
                achievements_dfs.append(normalized)
        
        if not achievements_dfs:
            raise ValueError("No player achievements data available")
        
        df = pd.concat(achievements_dfs, ignore_index=True)
        df['player_updatedAt'] = pd.to_datetime(df['player_updatedAt'], errors='raise')
        
        from .io_utils import S3Client
        s3_client = S3Client()
        s3_client.upload_dataframe(
            df, 
            f'{output_prefix}/player_achievements_data/player_achievements_data_transformed_{current_date}.csv'
        )
        return df
    except Exception as e:
        logging.error(f"Error processing player achievements: {e}", exc_info=True)
        raise


@log_execution_time
def process_players_data(data: Dict[str, Any], output_prefix: str, current_date: str) -> pd.DataFrame:
    """
    Process players data (club players).
    
    Args:
        data: Raw players data
        output_prefix: Output path prefix
        current_date: Current date string
        
    Returns:
        Processed DataFrame
    """
    try:
        players_dfs = []
        for player in data['data']:
            normalized = pd.json_normalize(
                player['players'],
                ['players'],
                ['updatedAt'],
                sep='_',
                meta_prefix='player_',
                record_prefix='player_',
                errors='raise'
            )
            players_dfs.append(normalized)
        
        if not players_dfs:
            raise ValueError("No players data available")
        
        df = pd.concat(players_dfs, ignore_index=True)
        
        # Date transformations
        df['player_dateOfBirth'] = pd.to_datetime(df['player_dateOfBirth'], format='%Y-%m-%d', errors='raise')
        df['player_joinedOn'] = pd.to_datetime(df['player_joinedOn'], format='%Y-%m-%d', errors='raise')
        df['player_contract'] = pd.to_datetime(df['player_contract'], format='%Y-%m-%d', errors='raise')
        df['player_updatedAt'] = pd.to_datetime(df['player_updatedAt'], errors='raise')
        
        # Numeric transformations
        df['player_age'] = pd.to_numeric(df['player_age'], downcast='integer', errors='raise')
        
        # Height processing
        df['player_height'] = (
            df['player_height']
            .fillna('')
            .astype(str)
            .str.replace('m', '')
            .str.replace(',', '.')
        )
        df['player_height'] = pd.to_numeric(df['player_height'], errors='raise')
        
        # Market value processing
        df['player_marketValue'] = (
            df['player_marketValue']
            .fillna('')
            .astype(str)
            .str.replace('€', '')
            .map(parse_market_value)
        )
        
        # Expand nationality array
        citizenship_df = df['player_nationality'].apply(pd.Series)
        citizenship_df.columns = [f'player_nationality_{i+1}' for i in range(citizenship_df.shape[1])]
        df = pd.concat([df, citizenship_df], axis=1)
        
        from .io_utils import S3Client
        s3_client = S3Client()
        s3_client.upload_dataframe(
            df, 
            f'{output_prefix}/players_data/club_players_data_transformed_{current_date}.csv'
        )
        return df
    except Exception as e:
        logging.error(f"Error processing players data: {e}", exc_info=True)
        raise


@log_execution_time
def process_players_injuries(data: Dict[str, Any], output_prefix: str, current_date: str) -> pd.DataFrame:
    """
    Process player injuries data.
    
    Args:
        data: Raw player injuries data
        output_prefix: Output path prefix
        current_date: Current date string
        
    Returns:
        Processed DataFrame
    """
    try:
        injuries_dfs = []
        for player in data['data']:
            if 'injuries' in player['players']:
                normalized = pd.json_normalize(
                    player['players'],
                    ['injuries'],
                    ['updatedAt', 'id'],
                    sep='_',
                    meta_prefix='player_',
                    record_prefix='player_',
                    errors='raise'
                )
                injuries_dfs.append(normalized)
        
        if not injuries_dfs:
            raise ValueError("No player injuries data available")
        
        df = pd.concat(injuries_dfs, ignore_index=True)
        
        # Rename columns
        df = df.rename(columns={
            'player_fromDate': 'player_from',
            'player_untilDate': 'player_until',
            'player_days': 'player_days',
            'player_gamesMissed': 'player_gamesMissed',
            'player_gamesMissedClubs': 'player_gamesMissedClubs'
        })
        
        # Validate required columns
        required_columns = ['player_from', 'player_until', 'player_days', 'player_gamesMissed', 'player_gamesMissedClubs']
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns in injuries data: {missing}")
        
        # Date transformations
        df['player_from'] = pd.to_datetime(df['player_from'], format='%Y-%m-%d', errors='raise')
        df['player_until'] = pd.to_datetime(df['player_until'], format='%Y-%m-%d', errors='raise')
        df['player_updatedAt'] = pd.to_datetime(df['player_updatedAt'], errors='raise')
        
        # Numeric transformations
        df['player_days'] = df['player_days'].apply(lambda x: str(x).replace(' days', ''))
        df['player_days'] = pd.to_numeric(df['player_days'], errors='raise', downcast='integer')
        df['player_gamesMissed'] = pd.to_numeric(df['player_gamesMissed'], errors='raise', downcast='integer')
        
        # Expand games missed clubs array
        games_missed_df = df['player_gamesMissedClubs'].apply(pd.Series)
        games_missed_df.columns = [f'player_gamesMissedClubs_{i+1}' for i in range(games_missed_df.shape[1])]
        df = pd.concat([df, games_missed_df], axis=1)
        
        from .io_utils import S3Client
        s3_client = S3Client()
        s3_client.upload_dataframe(
            df, 
            f'{output_prefix}/player_injuries_data/player_injuries_data_transformed_{current_date}.csv'
        )
        return df
    except Exception as e:
        logging.error(f"Error processing player injuries: {e}", exc_info=True)
        raise


@log_execution_time
def process_players_market_value(data: Dict[str, Any], output_prefix: str, current_date: str) -> pd.DataFrame:
    """
    Process player market value data.
    
    Args:
        data: Raw player market value data
        output_prefix: Output path prefix
        current_date: Current date string
        
    Returns:
        Processed DataFrame
    """
    try:
        df = pd.json_normalize(data['data'], sep='_', errors='raise')
        market_value_history = []
        
        for history, player_id in zip(df.get('players_marketValueHistory', []), df.get('player_id', [])):
            if isinstance(history, list):
                normalized = pd.json_normalize(history, errors='raise')
                normalized['id'] = player_id
                # Rename historical market value column to avoid collision
                if 'marketValue' in normalized.columns:
                    normalized.rename(columns={'marketValue': 'historical_marketValue'}, inplace=True)
                normalized.columns = [f'player_{col}' for col in normalized.columns]
                market_value_history.append(normalized)
        
        if market_value_history:
            history_df = pd.concat(market_value_history, ignore_index=True)
            merged_df = df.merge(history_df, on='player_id', how='left')
            merged_df.drop(columns=['players_id', 'players_marketValueHistory'], inplace=True, errors='ignore')
        else:
            merged_df = df
        
        # Clean column names
        merged_df.columns = [col.replace('players', 'player') for col in merged_df.columns]
        merged_df.columns = [col.replace(' ', '_').replace('-', '_').replace(',', '').replace('.', '').lower()
                             for col in merged_df.columns]
        
        # Numeric transformations
        merged_df['player_age'] = pd.to_numeric(merged_df['player_age'], downcast='integer', errors='raise')
        
        for col in merged_df.columns:
            if 'ranking' in col and 'worldwide' not in col:
                merged_df[col] = pd.to_numeric(merged_df[col], downcast='integer', errors='raise')
            elif 'ranking_worldwide' in col:
                merged_df[col] = pd.to_numeric(merged_df[col], downcast='float', errors='raise')
        
        # Date transformations
        if 'player_date' in merged_df.columns:
            merged_df['player_date'] = pd.to_datetime(merged_df['player_date'], format='%Y-%m-%d', errors='raise')
        
        if 'player_updatedat' in merged_df.columns:
            merged_df['player_updatedat'] = pd.to_datetime(merged_df['player_updatedat'], errors='raise')
        
        # Market value transformations
        for col_name in ['player_marketvalue', 'player_value']:
            if col_name in merged_df.columns:
                col_data = merged_df.get(col_name)
                if isinstance(col_data, pd.DataFrame):
                    col_data = col_data.iloc[:, 0]
                merged_df[col_name] = (
                    col_data.fillna('')
                    .astype(str)
                    .str.replace('€', '')
                    .map(parse_market_value)
                )
        
        from .io_utils import S3Client
        s3_client = S3Client()
        s3_client.upload_dataframe(
            merged_df, 
            f'{output_prefix}/player_market_value_data/player_market_value_data_transformed_{current_date}.csv'
        )
        return merged_df
    except Exception as e:
        logging.error(f"Error processing player market value data: {e}", exc_info=True)
        raise


@log_execution_time
def process_players_transfers(data: Dict[str, Any], output_prefix: str, current_date: str) -> pd.DataFrame:
    """
    Process player transfers data.
    
    Args:
        data: Raw player transfers data
        output_prefix: Output path prefix
        current_date: Current date string
        
    Returns:
        Processed DataFrame
    """
    try:
        transfers_dfs = []
        for player in data['data']:
            normalized = pd.json_normalize(
                player['players'],
                ['transfers'],
                ['id', 'updatedAt'],
                sep='_',
                record_prefix='player_',
                meta_prefix='players_',
                errors='raise'
            )
            transfers_dfs.append(normalized)
        
        if not transfers_dfs:
            raise ValueError("No player transfers data available")
        
        df = pd.concat(transfers_dfs, ignore_index=True)
        
        # Date transformations
        df['player_date'] = pd.to_datetime(df['player_date'], format='%Y-%m-%d', errors='raise')
        df['player_updatedAt'] = pd.to_datetime(df['player_updatedAt'], errors='raise')
        
        # Market value processing
        df['player_marketValue'] = (
            df['player_marketValue']
            .fillna('')
            .astype(str)
            .str.replace('€', '')
            .map(parse_market_value)
        )
        
        # Rename columns
        new_columns = []
        for col in df.columns:
            if col == 'player_id':
                new_columns.append('transaction_id')
            else:
                new_columns.append(col.replace('players', 'player'))
        df.columns = new_columns
        
        from .io_utils import S3Client
        s3_client = S3Client()
        s3_client.upload_dataframe(
            df,
            f'{output_prefix}/player_transfers_data/player_transfers_data_transformed_{current_date}.csv'
        )
        return df
    except Exception as e:
        logging.error(f"Error processing player transfers: {e}", exc_info=True)
        raise


@log_execution_time
def process_league_data(data: List[Dict[str, Any]], output_prefix: str, current_date: str) -> pd.DataFrame:
    """
    Process league table data.
    
    Args:
        data: Raw league data
        output_prefix: Output path prefix
        current_date: Current date string
        
    Returns:
        Processed DataFrame
    """
    try:
        df = pd.json_normalize(data, errors='raise')
        df = df.dropna(subset=['position'])
        df = df.dropna(axis=1, how='all')
        
        # Process goals column
        if 'goals' in df.columns:
            df[['goals_scored', 'goals_conceded']] = df['goals'].str.split(':', expand=True)
            df['goals_scored'] = pd.to_numeric(df['goals_scored'], errors='raise')
            df['goals_conceded'] = pd.to_numeric(df['goals_conceded'], errors='raise')
        
        df['league_updated_at'] = pd.to_datetime(datetime.now())
        
        from .io_utils import S3Client
        s3_client = S3Client()
        s3_client.upload_dataframe(
            df, 
            f'{output_prefix}/league_data/league_data_transformed_{current_date}.csv'
        )
        return df
    except Exception as e:
        logging.error(f"Error processing league data: {e}", exc_info=True)
        raise