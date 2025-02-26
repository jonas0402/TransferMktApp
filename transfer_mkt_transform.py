import pandas as pd
import boto3
import json
from io import BytesIO
from typing import Optional, Any, Dict
import logging
from datetime import datetime
import time
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Access AWS credentials from environment variables
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = "transfermkt-data"

# Initialize boto3 clients
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name='us-east-1'
)
glue_client = boto3.client(
    'glue',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name='us-east-1'
)


def parse_market_value(value: str) -> float:
    """Parse a market value string with units (k/m) into a float."""
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


def get_latest_file_from_s3(bucket_name: str, folder_key: str) -> Optional[str]:
    """Retrieve the most recent file from an S3 bucket folder."""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_key)
        if 'Contents' not in response:
            logging.error(f"No files found in {bucket_name}/{folder_key}")
            return None
        files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
        return files[0]['Key']
    except Exception as e:
        logging.error(f"Error listing objects in {folder_key}: {e}", exc_info=True)
        return None


def read_json_from_s3(folder_key: str, bucket_name: str) -> Optional[Dict[str, Any]]:
    """
    Read the latest JSON file from a folder in an S3 bucket and return it as a Python dictionary.
    """
    last_file_key = get_latest_file_from_s3(bucket_name, folder_key)
    if last_file_key:
        response = s3_client.get_object(Bucket=bucket_name, Key=last_file_key)
        status = response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        if status == 200:
            logging.info(f"Successful S3 get_object response for key: {last_file_key}")
            json_content = response['Body'].read().decode('utf-8')
            return json.loads(json_content)
        else:
            logging.error(f"Unsuccessful S3 get_object response. Status - {status}")
            return None
    else:
        logging.error("No files found in the specified folder.")
        return None


def write_dataframe_to_s3(df: pd.DataFrame, key: str, bucket_name: str) -> None:
    """Write a DataFrame to an S3 bucket in CSV format.
    
    This function creates a copy of the DataFrame for output, converts the column names to lowercase,
    and then writes the CSV to S3 without affecting the original DataFrame.
    """
    df_for_output = df.copy()
    df_for_output.columns = df_for_output.columns.str.lower()
    buffer = BytesIO()
    df_for_output.to_csv(buffer, index=False, sep='|')
    buffer.seek(0)
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=buffer.getvalue())
    logging.info(f"DataFrame written to S3 under key: {key}")


def delete_all_except_last_n(bucket_name: str, files_to_keep: int, folder_name: str) -> None:
    """
    Delete all files in the specified folder of the S3 bucket except for the most recent 'files_to_keep' files.
    """
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
        if 'Contents' not in response:
            logging.info(f"No objects found in {folder_name}.")
            return
        objects = response['Contents']
        sorted_objects = sorted(objects, key=lambda x: x['LastModified'], reverse=True)
        files_to_delete = sorted_objects[files_to_keep:]
        for obj in files_to_delete:
            s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
            logging.info(f"Deleted file: {obj['Key']}")
    except Exception as e:
        logging.error(f"Error deleting files in {folder_name}: {e}", exc_info=True)


def start_crawler(crawler_name: str) -> Dict[str, Any]:
    """Start an AWS Glue crawler."""
    logging.info(f"Starting crawler: {crawler_name}")
    response = glue_client.start_crawler(Name=crawler_name)
    return response


# --- Data Processing Functions ---

def process_club_profiles(data: Dict[str, Any], output_prefix: str, current_date: str) -> pd.DataFrame:
    try:
        df = pd.json_normalize(
            data['data'],
            'clubs',
            ['seasonId', 'updatedAt'],
            meta_prefix='club_',
            record_prefix='club_'
        )
        df['club_updatedAt'] = pd.to_datetime(df['club_updatedAt'], errors='raise')
        write_dataframe_to_s3(df, f'{output_prefix}/club_profiles_data/club_profile_data_transformed_{current_date}.csv', S3_BUCKET_NAME)
        return df
    except Exception as e:
        logging.error(f"Error processing club profiles: {e}", exc_info=True)
        raise


def process_players_profile(data: Dict[str, Any], output_prefix: str, current_date: str) -> pd.DataFrame:
    try:
        df = pd.json_normalize(data['data'], sep='_')
        df.columns = df.columns.str.replace('players', 'player')
        df['player_dateOfBirth'] = pd.to_datetime(df['player_dateOfBirth'], format='%Y-%m-%d', errors='raise')
        df['player_age'] = pd.to_numeric(df['player_age'], downcast='integer', errors='raise')
        df['player_height'] = (
            df['player_height']
            .fillna('')
            .astype(str)
            .str.replace('m', '')
            .str.replace(',', '.')
        )
        df['player_height'] = pd.to_numeric(df['player_height'], errors='raise')
        df['player_shirtNumber'] = df['player_shirtNumber'].str.replace('#', '')
        df['player_club_joined'] = pd.to_datetime(df['player_club_joined'], format='%Y-%m-%d', errors='raise')
        df['player_club_contractExpires'] = pd.to_datetime(df['player_club_contractExpires'], format='%Y-%m-%d', errors='raise')
        df['player_marketValue'] = (
            df['player_marketValue']
            .fillna('')
            .astype(str)
            .str.replace('€', '')
            .map(parse_market_value)
        )
        df['player_updatedAt'] = pd.to_datetime(df['player_updatedAt'], errors='raise')
        citizenship_df = df['player_citizenship'].apply(pd.Series)
        citizenship_df.columns = [f'player_citizenship_{i+1}' for i in range(citizenship_df.shape[1])]
        df = pd.concat([df, citizenship_df], axis=1)
        position_df = df['player_position_other'].apply(pd.Series)
        position_df.columns = [f'player_position_other_{i+1}' for i in range(position_df.shape[1])]
        df = pd.concat([df, position_df], axis=1)
        df = df.loc[:, ~df.columns.duplicated()]
        write_dataframe_to_s3(df, f'{output_prefix}/player_profile_data/player_profile_data_transformed_{current_date}.csv', S3_BUCKET_NAME)
        return df
    except Exception as e:
        logging.error(f"Error processing players profile: {e}", exc_info=True)
        raise


def process_player_stats(data: Dict[str, Any], output_prefix: str, current_date: str) -> pd.DataFrame:
    try:
        stats_dfs = []
        for row in data['data']:
            if 'stats' in row['players']:
                normalized = pd.json_normalize(
                    row['players'],
                    'stats',
                    ['id', 'player_id', 'updatedAt'],
                    sep='_',
                    meta_prefix='player_',
                    record_prefix='player_',
                    errors='ignore'
                )
                stats_dfs.append(normalized)
        if not stats_dfs:
            raise ValueError("No player stats data available")
        df = pd.concat(stats_dfs, ignore_index=True)
        df['player_minutesPlayed'] = df['player_minutesPlayed'].astype(str).str.replace("'", "", regex=False)
        df['player_minutesPlayed'] = pd.to_numeric(df['player_minutesPlayed'], errors='coerce')
        for col in ['player_appearances', 'player_goalsConceded', 'player_cleanSheets',
                    'player_yellowCards', 'player_redCards', 'player_goals',
                    'player_secondYellowCards', 'player_assists']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='raise')
        df['player_updatedAt'] = pd.to_datetime(df['player_updatedAt'], errors='raise')
        write_dataframe_to_s3(df, f'{output_prefix}/player_stats_data/player_stats_data_transformed_{current_date}.csv', S3_BUCKET_NAME)
        return df
    except Exception as e:
        logging.error(f"Error processing player stats: {e}", exc_info=True)
        raise


def process_players_achievements(data: Dict[str, Any], output_prefix: str, current_date: str) -> pd.DataFrame:
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
        write_dataframe_to_s3(df, f'{output_prefix}/player_achievements_data/player_achievements_data_transformed_{current_date}.csv', S3_BUCKET_NAME)
        return df
    except Exception as e:
        logging.error(f"Error processing player achievements: {e}", exc_info=True)
        raise


def process_players_data(data: Dict[str, Any], output_prefix: str, current_date: str) -> pd.DataFrame:
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
        df['player_dateOfBirth'] = pd.to_datetime(df['player_dateOfBirth'], format='%Y-%m-%d', errors='raise')
        df['player_age'] = pd.to_numeric(df['player_age'], downcast='integer', errors='raise')
        df['player_height'] = (
            df['player_height']
            .fillna('')
            .astype(str)
            .str.replace('m', '')
            .str.replace(',', '.')
        )
        df['player_height'] = pd.to_numeric(df['player_height'], errors='raise')
        # Use ISO format for joinedOn and contract since data like "2019-12-23" is provided.
        df['player_joinedOn'] = pd.to_datetime(df['player_joinedOn'], format='%Y-%m-%d', errors='raise')
        df['player_contract'] = pd.to_datetime(df['player_contract'], format='%Y-%m-%d', errors='raise')
        df['player_marketValue'] = (
            df['player_marketValue']
            .fillna('')
            .astype(str)
            .str.replace('€', '')
            .map(parse_market_value)
        )
        df['player_updatedAt'] = pd.to_datetime(df['player_updatedAt'], errors='raise')
        citizenship_df = df['player_nationality'].apply(pd.Series)
        citizenship_df.columns = [f'player_nationality_{i+1}' for i in range(citizenship_df.shape[1])]
        df = pd.concat([df, citizenship_df], axis=1)
        write_dataframe_to_s3(df, f'{output_prefix}/players_data/club_players_data_transformed_{current_date}.csv', S3_BUCKET_NAME)
        return df
    except Exception as e:
        logging.error(f"Error processing players data: {e}", exc_info=True)
        raise


def process_players_injuries(data: Dict[str, Any], output_prefix: str, current_date: str) -> pd.DataFrame:
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
        # Rename JSON keys (with record prefix) to our expected column names.
        df = df.rename(columns={
            'player_fromDate': 'player_from',
            'player_untilDate': 'player_until',
            'player_days': 'player_days',
            'player_gamesMissed': 'player_gamesMissed',
            'player_gamesMissedClubs': 'player_gamesMissedClubs'
        })
        # Check for required columns and abort if any are missing.
        required_columns = ['player_from', 'player_until', 'player_days', 'player_gamesMissed', 'player_gamesMissedClubs']
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns in injuries data: {missing}")
        
        df['player_from'] = pd.to_datetime(df['player_from'], format='%Y-%m-%d', errors='raise')
        df['player_until'] = pd.to_datetime(df['player_until'], format='%Y-%m-%d', errors='raise')
        df['player_days'] = df['player_days'].apply(lambda x: str(x).replace(' days', ''))
        df['player_days'] = pd.to_numeric(df['player_days'], errors='raise', downcast='integer')
        df['player_gamesMissed'] = pd.to_numeric(df['player_gamesMissed'], errors='raise', downcast='integer')
        df['player_updatedAt'] = pd.to_datetime(df['player_updatedAt'], errors='raise')
        games_missed_df = df['player_gamesMissedClubs'].apply(pd.Series)
        games_missed_df.columns = [f'player_gamesMissedClubs_{i+1}' for i in range(games_missed_df.shape[1])]
        df = pd.concat([df, games_missed_df], axis=1)
        write_dataframe_to_s3(df, f'{output_prefix}/player_injuries_data/player_injuries_data_transformed_{current_date}.csv', S3_BUCKET_NAME)
        return df
    except Exception as e:
        logging.error(f"Error processing player injuries: {e}", exc_info=True)
        raise


def process_players_market_value(data: Dict[str, Any], output_prefix: str, current_date: str) -> pd.DataFrame:
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
        merged_df.columns = [col.replace('players', 'player') for col in merged_df.columns]
        merged_df.columns = [col.replace(' ', '_').replace('-', '_').replace(',', '').replace('.', '').lower()
                             for col in merged_df.columns]
        merged_df['player_age'] = pd.to_numeric(merged_df['player_age'], downcast='integer', errors='raise')
        for col in merged_df.columns:
            if 'ranking' in col and 'worldwide' not in col:
                merged_df[col] = pd.to_numeric(merged_df[col], downcast='integer', errors='raise')
            elif 'ranking_worldwide' in col:
                merged_df[col] = pd.to_numeric(merged_df[col], downcast='float', errors='raise')
        if 'player_date' in merged_df.columns:
            merged_df['player_date'] = pd.to_datetime(merged_df['player_date'], format='%Y-%m-%d', errors='raise')
        if 'player_marketvalue' in merged_df.columns:
            pmv = merged_df.get('player_marketvalue')
            if isinstance(pmv, pd.DataFrame):
                pmv = pmv.iloc[:, 0]
            merged_df['player_marketvalue'] = (
                pmv.fillna('')
                .astype(str)
                .str.replace('€', '')
                .map(parse_market_value)
            )
        if 'player_value' in merged_df.columns:
            pv = merged_df.get('player_value')
            if isinstance(pv, pd.DataFrame):
                pv = pv.iloc[:, 0]
            merged_df['player_value'] = (
                pv.fillna('')
                .astype(str)
                .str.replace('€', '')
                .map(parse_market_value)
            )
        if 'player_updatedat' in merged_df.columns:
            merged_df['player_updatedat'] = pd.to_datetime(merged_df['player_updatedat'], errors='raise')
        write_dataframe_to_s3(merged_df, f'{output_prefix}/player_market_value_data/player_market_value_data_transformed_{current_date}.csv', S3_BUCKET_NAME)
        return merged_df
    except Exception as e:
        logging.error(f"Error processing player market value data: {e}", exc_info=True)
        raise


def process_players_transfers(data: Dict[str, Any], output_prefix: str, current_date: str) -> pd.DataFrame:
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
        df['player_date'] = pd.to_datetime(df['player_date'], format='%Y-%m-%d', errors='raise')
        df['player_marketValue'] = (
            df['player_marketValue']
            .fillna('')
            .astype(str)
            .str.replace('€', '')
            .map(parse_market_value)
        )
        df.columns = [col.replace('players', 'player') for col in df.columns]
        df['player_updatedAt'] = pd.to_datetime(df['player_updatedAt'], errors='raise')
        write_dataframe_to_s3(df, f'{output_prefix}/player_transfers_data/player_transfers_data_transformed_{current_date}.csv', S3_BUCKET_NAME)
        return df
    except Exception as e:
        logging.error(f"Error processing player transfers: {e}", exc_info=True)
        raise


def process_league_data(data: Dict[str, Any], output_prefix: str, current_date: str) -> pd.DataFrame:
    try:
        df = pd.json_normalize(data, errors='raise')
        df = df.dropna(subset=['position'])
        df = df.dropna(axis=1, how='all')
        if 'goals' in df.columns:
            df[['goals_scored', 'goals_conceded']] = df['goals'].str.split(':', expand=True)
            df['goals_scored'] = pd.to_numeric(df['goals_scored'], errors='raise')
            df['goals_conceded'] = pd.to_numeric(df['goals_conceded'], errors='raise')
        df['league_updated_at'] = pd.to_datetime(datetime.now())
        write_dataframe_to_s3(df, f'{output_prefix}/league_data/league_data_transformed_{current_date}.csv', S3_BUCKET_NAME)
        return df
    except Exception as e:
        logging.error(f"Error processing league data: {e}", exc_info=True)
        raise


# --- Main ETL Execution ---

def main() -> None:
    start_time = time.time()
    current_date = str(datetime.now().date())
    output_prefix = "transformed_data"

    try:
        club_profiles_data = read_json_from_s3('raw_data/club_profiles_data/', S3_BUCKET_NAME)
        players_profile_data = read_json_from_s3('raw_data/players_profile_data/', S3_BUCKET_NAME)
        player_stats_data = read_json_from_s3('raw_data/player_stats_data/', S3_BUCKET_NAME)
        players_achievements_data = read_json_from_s3('raw_data/players_achievements_data/', S3_BUCKET_NAME)
        players_data = read_json_from_s3('raw_data/players_data/', S3_BUCKET_NAME)
        players_injuries_data = read_json_from_s3('raw_data/players_injuries_data/', S3_BUCKET_NAME)
        players_market_value_data = read_json_from_s3('raw_data/players_market_value_data/', S3_BUCKET_NAME)
        players_transfers_data = read_json_from_s3('raw_data/players_transfers_data/', S3_BUCKET_NAME)
        leagues_table_data = read_json_from_s3('raw_data/league_data/', S3_BUCKET_NAME)

        if club_profiles_data is None:
            raise ValueError("Club profiles data is missing.")
        if players_profile_data is None:
            raise ValueError("Players profile data is missing.")
        if player_stats_data is None:
            raise ValueError("Player stats data is missing.")
        if players_achievements_data is None:
            raise ValueError("Players achievements data is missing.")
        if players_data is None:
            raise ValueError("Players data is missing.")
        if players_injuries_data is None:
            raise ValueError("Players injuries data is missing.")
        if players_market_value_data is None:
            raise ValueError("Players market value data is missing.")
        if players_transfers_data is None:
            raise ValueError("Players transfers data is missing.")
        if leagues_table_data is None:
            raise ValueError("League data is missing.")

        process_club_profiles(club_profiles_data, output_prefix, current_date)
        process_players_profile(players_profile_data, output_prefix, current_date)
        process_player_stats(player_stats_data, output_prefix, current_date)
        process_players_achievements(players_achievements_data, output_prefix, current_date)
        process_players_injuries(players_injuries_data, output_prefix, current_date)
        process_players_market_value(players_market_value_data, output_prefix, current_date)
        process_players_transfers(players_transfers_data, output_prefix, current_date)
        process_players_data(players_data, output_prefix, current_date)
        process_league_data(leagues_table_data, output_prefix, current_date)

    except Exception as e:
        logging.error("One or more transformations failed. Aborting crawler execution.", exc_info=True)
        return

    delete_all_except_last_n(S3_BUCKET_NAME, 1, 'transformed_data/club_profiles_data')
    delete_all_except_last_n(S3_BUCKET_NAME, 1, 'transformed_data/player_profile_data')
    delete_all_except_last_n(S3_BUCKET_NAME, 1, 'transformed_data/player_stats_data')
    delete_all_except_last_n(S3_BUCKET_NAME, 1, 'transformed_data/player_achievements_data')
    delete_all_except_last_n(S3_BUCKET_NAME, 1, 'transformed_data/player_injuries_data')
    delete_all_except_last_n(S3_BUCKET_NAME, 1, 'transformed_data/player_transfers_data')
    delete_all_except_last_n(S3_BUCKET_NAME, 1, 'transformed_data/players_data')
    delete_all_except_last_n(S3_BUCKET_NAME, 1, 'transformed_data/player_market_value_data')
    delete_all_except_last_n(S3_BUCKET_NAME, 1, 'transformed_data/league_data')

    crawler_names = [
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
    for crawler in crawler_names:
        start_crawler(crawler)

    end_time = time.time()
    elapsed_time = end_time - start_time
    logging.info(f"Script executed in {elapsed_time:.2f} seconds")


if __name__ == '__main__':
    main()
