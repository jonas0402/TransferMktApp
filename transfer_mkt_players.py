import pandas as pd
from io import StringIO
import requests
import json
import logging
from datetime import datetime
import numpy as np
import boto3
from concurrent.futures import ThreadPoolExecutor
import time
import os

# Access the credentials
access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# API and AWS S3 configuration
base_url = "https://transfermarkt-api.fly.dev/"
s3_bucket_name = "transfermkt-data"
s3_client = boto3.client('s3',aws_access_key_id=access_key,aws_secret_access_key=secret_key,region_name='us-east-1')

# Data paths on S3
club_profiles_data = 'raw_data/club_profiles_data'
players_data = 'raw_data/players_data'
player_profile_data = 'raw_data/players_profile_data'
player_jersey_numbers_data = 'raw_data/players_jersey_numbers_data'
player_market_value_data = 'raw_data/players_market_value_data'
player_stats_data = 'raw_data/player_stats_data'
player_injuries_data = 'raw_data/players_injuries_data'
player_achievements_data = 'raw_data/players_achievements_data'
player_transfers_data = 'raw_data/players_transfers_data'
league_table_data = 'raw_data/league_data'


# Placeholder for player IDs
player_ids = []
club_ids = []
competition_code = 'MLS1'
    
def log_execution_time(func):
    """Decorator to log the execution time of a function."""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logging.info(f"Function '{func.__name__}' took {end_time - start_time:.2f} seconds")
        return result
    return wrapper

@log_execution_time
def get_club_ids(competition_id):
    """Retrieve club IDs for a given competition."""
    endpoint = f"competitions/{competition_id}/clubs"
    data_dict = {}
    try:
        response = requests.get(f"{base_url}{endpoint}")
        if response.status_code != 200:
            logging.error(f"API call failed with status code {response.status_code}: {response.text}")
            raise Exception("API call failed")
        data_dict["data"] = response.json()
        club_ids.extend([club['id'] for club in response.json()['clubs']])
        logging.info(f"Fetched {len(club_ids)} club IDs for competition ID: {competition_id}")
        return data_dict
    except Exception as e:
        logging.error(f"Error fetching club IDs: {e}")
        return None

@log_execution_time
def get_club_players(club_ids):
    """Retrieve players for each club."""
    data_dict = {"data": []}  # Initialize "data" as a dictionary
    for id in club_ids:
        endpoint = f"clubs/{id}/players"
        try:
            response = requests.get(f"{base_url}{endpoint}")
            if response.status_code != 200:
                logging.error(f"API call failed with status code {response.status_code}: {response.text}")
                raise Exception("API call failed")
            club_player_data ={
                "club_id":id,
                "players": response.json()
            }
            data_dict["data"].append(club_player_data)
            player_ids.extend([player['id'] for player in response.json()['players']])
            logging.info(f"Fetched players for club ID: {id}")
        except Exception as e:
            logging.error(f"Error fetching players for club ID {id}: {e}")
    return data_dict

@log_execution_time
def get_player_data(endpoint, player_ids):
    """Generic function to fetch player data."""
    data_dict = {"data": []}  # Initialize the "data" key as a dictionary
    for id in player_ids:
        try:
            response = requests.get(f"{base_url}{endpoint.format(id)}")
            if response.status_code != 200:
                logging.error(f"API call failed with status code {response.status_code}: {response.text}")
                raise Exception("API call failed")
            players_data ={
                "player_id":id,
                "players": response.json()
            }
            data_dict["data"].append(players_data)
            logging.info(f"Fetched data for player ID: {id}")
        except Exception as e:
            logging.error(f"Error fetching player data for ID {id}: {e}")
    return data_dict

@log_execution_time    
def get_table_league(comp_name):
    logging.info(f"Fetching league table data for competition: {comp_name}")

    current_year = datetime.now().year
    comp_name = comp_name.replace(" ", "-").lower()

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/58.0.3029.110 Safari/537.3'
    }

    url = f'https://www.transfermarkt.us/{comp_name}/tabelle/wettbewerb/MLS1/saison_id/{current_year}'
    logging.info(f"Fetching data from initial URL: {url}")
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
                logging.error(f"API call failed with status code {response.status_code}: {response.text}")
                raise Exception("API call failed")
    
    html_content = StringIO(response.text)
    tables = pd.read_html(html_content)

    seasons = tables[0][1][0].split('  ')
    result = []

    for year in seasons:
        url = f'https://www.transfermarkt.us/{comp_name}/tabelle/wettbewerb/MLS1/saison_id/{int(year)-1}'
        logging.info(f"Fetching season data for year {year} from URL: {url}")

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
                logging.error(f"API call failed with status code {response.status_code}: {response.text}")
                raise Exception("API call failed")
        tables = pd.read_html(StringIO(response.text))

        logging.info(f"Assigning conference and year to tables for year {year}")
        tables[1]['conference'] = 'eastern'
        tables[2]['conference'] = 'western'
        tables[1]['year'] = year
        tables[2]['year'] = year

        # Replace NaN with None before converting to dictionary
        tables[1].replace({np.nan: None}, inplace=True)
        tables[2].replace({np.nan: None}, inplace=True)

        result.extend(tables[1].to_dict(orient='records'))
        result.extend(tables[2].to_dict(orient='records'))

    logging.info("Renaming keys for meaningful representation")
    key_mapping = {
        '#': 'position',
        'Club.1': 'club_name',
        'Unnamed: 3': 'matches_played',
        'W': 'wins',
        'D': 'draws',
        'L': 'losses',
        'Goals': 'goals',
        '+/-': 'goal_difference',
        'Pts': 'points',
        'conference': 'conference',
        'year': 'year'
    }

    renamed_result = [
        {key_mapping.get(key, key): value for key, value in item.items() if key != 'Club'}
        for item in result
    ]

    #result_json = json.dumps(renamed_result, indent=4)
    logging.info("Completed fetching and processing league table data")

    return renamed_result
    
@log_execution_time
def upload_to_s3(data, file_name, folder_name):
    """Upload data to S3 bucket."""
    date_file = str(datetime.now().date())
    s3_key = f"{folder_name}/{file_name}_{date_file}.json"
    s3_client.put_object(Body=json.dumps(data), Bucket=s3_bucket_name, Key=s3_key)
    logging.info(f"Uploaded file to S3: {s3_key}")

@log_execution_time
def delete_all_except_last_n(bucket_name, files_to_keep, folder_name):
    """Keep only the last N files in a specified S3 folder."""
    try:
        objects = s3_client.list_objects(Bucket=bucket_name, Prefix=folder_name)['Contents']
        sorted_objects = sorted(objects, key=lambda x: x['LastModified'], reverse=True)
        files_to_delete = sorted_objects[files_to_keep:]
        for obj in files_to_delete:
            s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
            logging.info(f"Deleted file: {obj['Key']}")
    except Exception as e:
        logging.error(f"Error deleting files in folder {folder_name}: {e}")

@log_execution_time
def main():
    """Main function to orchestrate data fetching and processing."""
    start_time = time.time()
    clup_profile_data = get_club_ids(competition_code)
    club_players_data = get_club_players(club_ids)
    leagues_table_data = get_table_league('major league soccer')

    # Concurrent fetching of player data
    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = {
            'players_profile': executor.submit(get_player_data, 'players/{}/profile', player_ids),
            'player_stats': executor.submit(get_player_data, 'players/{}/stats', player_ids),
            'players_market_value': executor.submit(get_player_data, 'players/{}/market_value', player_ids),
            'players_achievements': executor.submit(get_player_data, 'players/{}/achievements', player_ids),
            'players_injuries': executor.submit(get_player_data, 'players/{}/injuries', player_ids),
            'players_transfers': executor.submit(get_player_data, 'players/{}/transfers', player_ids),
        }
        results = {key: future.result() for key, future in futures.items()}

    # Upload results to S3
    for key, data in results.items():
        upload_to_s3(data, f"{key}_data", f"raw_data/{key}_data")
    upload_to_s3(clup_profile_data,'club_profile_data',club_profiles_data)
    upload_to_s3(club_players_data,'club_players_data',players_data)
    upload_to_s3(leagues_table_data,'league_table_data',league_table_data)

    # Example of deleting old files
    delete_all_except_last_n(s3_bucket_name, 1, club_profiles_data)
    delete_all_except_last_n(s3_bucket_name, 1, players_data)
    delete_all_except_last_n(s3_bucket_name, 1, player_profile_data)
    delete_all_except_last_n(s3_bucket_name, 1, player_market_value_data)
    delete_all_except_last_n(s3_bucket_name, 1, player_stats_data)
    delete_all_except_last_n(s3_bucket_name, 1, player_injuries_data)
    delete_all_except_last_n(s3_bucket_name, 1, player_achievements_data)
    delete_all_except_last_n(s3_bucket_name, 1, player_transfers_data)
    delete_all_except_last_n(s3_bucket_name, 1, league_table_data)

    end_time = time.time()
    logging.info(f"Total script execution time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
