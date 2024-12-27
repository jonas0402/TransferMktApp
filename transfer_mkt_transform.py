import pandas as pd
import numpy as np
import boto3
import json
from io import BytesIO
import logging
from datetime import datetime
import time
import os

# Access the credentials
access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# AWS S3 Configuration
aws_access_key_id = access_key
aws_secret_access_key = secret_key
s3_bucket_name = "transfermkt-data"

# Initialize S3 client with credentials
s3_client = boto3.client('s3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name='us-east-1'  # Specify the appropriate region
)
glue_client = boto3.client('glue',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name='us-east-1'  # Specify the appropriate region
)

def get_latest_file_from_s3(bucket_name, folder_key):
    """ Retrieve the most recent file from an S3 bucket folder. """
    # List all files in the specified folder
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_key)
    
    if 'Contents' not in response:
        logging.error(f"No files found in {bucket_name}/{folder_key}")
        return None
    
    # Sort the files by the last modified date in descending order
    files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
    
    # Return the key of the most recently modified file
    return files[0]['Key']

    
def read_json_from_s3(folder_key, bucket_name):
    """ Read the latest JSON file from a folder in an S3 bucket and return as a normalized dataframe. """    
    
    # Get the key of the latest file
    last_file_key = get_latest_file_from_s3(bucket_name, folder_key)
    
    if last_file_key:
        # Get the object using the key of the last file
        response = s3_client.get_object(Bucket=bucket_name, Key=last_file_key)
        status = response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        
        if status == 200:
            logging.info(f"Successful S3 get_object response. Status - {status}")
            json_content = response['Body'].read().decode('utf-8')
            data = json.loads(json_content)  # Load JSON data into a Python dictionary
            return data
        else:
            logging.error(f"Unsuccessful S3 get_object response. Status - {status}")
            return None
    else:
        logging.error("No files found in the specified folder.")
        return None

def write_dataframe_to_s3(df, key, bucket_name):
    """ Write a dataframe to S3 bucket in CSV format. """
    # Create a buffer
    buffer = BytesIO()
    # Write the dataframe to buffer in CSV format
    df.to_csv(buffer, index=False, sep='|')
    # Important: move the buffer's cursor to the start of the stream
    buffer.seek(0)
    # Put the CSV data from the buffer to the S3 bucket
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=buffer.getvalue())
    # Log the action
    logging.info(f"Dataframe written to S3 under key: {key} in CSV format")

def delete_all_except_last_n(bucket_name, files_to_keep, folder_name):
    try:
        # List objects in the specified folder of the S3 bucket
        objects = s3_client.list_objects(Bucket=bucket_name, Prefix=folder_name)['Contents']

        # Sort objects by last modified date in descending order
        sorted_objects = sorted(objects, key=lambda x: x['LastModified'], reverse=True)

        # Keep the last n objects (files) and delete the rest
        files_to_delete = sorted_objects[files_to_keep:]

        # Iterate through objects to be deleted
        for obj in files_to_delete:
            # Delete the object
            s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
            print(f"Deleted file: {obj['Key']}")

    except Exception as e:
        print(f"Error: {e}")

def delete_all_except_last_n(bucket_name, files_to_keep, folder_name):
    try:
        # List objects in the specified folder of the S3 bucket
        objects = s3_client.list_objects(Bucket=bucket_name, Prefix=folder_name)['Contents']

        # Sort objects by last modified date in descending order
        sorted_objects = sorted(objects, key=lambda x: x['LastModified'], reverse=True)

        # Keep the last n objects (files) and delete the rest
        files_to_delete = sorted_objects[files_to_keep:]

        # Iterate through objects to be deleted
        for obj in files_to_delete:
            # Delete the object
            s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
            print(f"Deleted file: {obj['Key']}")

    except Exception as e:
        print(f"Error: {e}")
        
def start_crawler(crawler_name):
    logging.info(f"Starting crawler: {crawler_name}")
    response = glue_client.start_crawler(Name=crawler_name)
    
    return response



def main():
    
    # Read data from S3
    club_profiles_data = read_json_from_s3('raw_data/club_profiles_data/', s3_bucket_name)  # Folder key only
    players_profile_data = read_json_from_s3('raw_data/players_profile_data/', s3_bucket_name)  # Folder key only
    player_stats_data = read_json_from_s3('raw_data/player_stats_data/', s3_bucket_name)  # Folder key only
    players_achievements_data = read_json_from_s3('raw_data/players_achievements_data/', s3_bucket_name)  # Folder key only
    players_data = read_json_from_s3('raw_data/players_data/', s3_bucket_name)  # Folder key only
    players_injuries_data = read_json_from_s3('raw_data/players_injuries_data/', s3_bucket_name)  # Folder key only
    players_market_value_data = read_json_from_s3('raw_data/players_market_value_data/', s3_bucket_name)  # Folder key only
    players_transfers_data = read_json_from_s3('raw_data/players_transfers_data/', s3_bucket_name)  # Folder key only
    leagues_table_data = read_json_from_s3('raw_data/league_data/', s3_bucket_name)  # Folder key only

    
    ######              Club Profile Clean Up               #####
    club_profiles_df = pd.json_normalize(club_profiles_data['data'], 'clubs', ['seasonID','updatedAt'], meta_prefix='club_', record_prefix='club_')
    club_profiles_df['club_updatedAt'] = pd.to_datetime(club_profiles_df['club_updatedAt'], errors='coerce')

    #####               Player Profile Clean Up                #####
    players_profile_df = pd.json_normalize(players_profile_data['data'], sep='_')
    players_profile_df.columns = players_profile_df.columns.str.replace('players','player')
    #player_dateOfBirth
    players_profile_df['player_dateOfBirth'] = pd.to_datetime(players_profile_df['player_dateOfBirth'], format='%b %d, %Y')

    #player_age
    players_profile_df['player_age'] = pd.to_numeric(players_profile_df['player_age'],downcast='integer')


    # Step 1: Remove the "m" and replace the comma with a period
    players_profile_df['player_height'] = players_profile_df['player_height'].fillna('').astype(str)
    players_profile_df['player_height'] = players_profile_df['player_height'].str.replace('m', '').str.replace(',', '.')
    players_profile_df['player_height'] = pd.to_numeric(players_profile_df['player_height'], errors='coerce')

    #player_shirtNumber
    players_profile_df['player_shirtNumber'] = players_profile_df['player_shirtNumber'].str.replace('#', '')

    #player_club_joined
    players_profile_df['player_club_joined'] = pd.to_datetime(players_profile_df['player_club_joined'], format='%b %d, %Y')

    #player_club_contractExpires
    players_profile_df['player_club_contractExpires'] = pd.to_datetime(players_profile_df['player_club_contractExpires'], format='%b %d, %Y')

    #player_marketValue
    players_profile_df['player_marketValue'] = players_profile_df['player_marketValue'].str.replace('€', '')
    players_profile_df['player_marketValue'] = players_profile_df['player_marketValue'].fillna('').astype(str)
    players_profile_df['player_marketValue'] = players_profile_df['player_marketValue'].map(lambda value: 
        float(value.replace('k', '')) * 1000 if 'k' in value else
        float(value.replace('m', '')) * 1000000 if 'm' in value else
        float(value) if value != '' else float('nan')  # Handle empty string as NaN
    )

    #player_updatedAt
    players_profile_df['player_updatedAt'] = pd.to_datetime(players_profile_df['player_updatedAt'])

    #exploding nationality
    citizenship_df = players_profile_df['player_citizenship'].apply(pd.Series)
    citizenship_df.columns = [f'player_citizenship_{i+1}' for i in range(citizenship_df.shape[1])]
    players_profile_df = pd.concat([players_profile_df, citizenship_df], axis=1)

    #exploding player position
    position_df = players_profile_df['player_position_other'].apply(pd.Series)
    position_df.columns = [f'player_position_other_{i+1}' for i in range(position_df.shape[1])]
    players_profile_df = pd.concat([players_profile_df, position_df], axis=1)

    #drop dupe columns
    players_profile_df = players_profile_df.loc[:, ~players_profile_df.columns.duplicated()]

    ######              Players Stats Clean Up              ######
    #Parse data
    player_stats_dfs = [pd.json_normalize(row['players'],
                        'stats',
                        ['id', 'player_id','updatedAt'],
                        sep='_', 
                        meta_prefix='player_',
                        record_prefix='player_',
                        errors='ignore') 
                        for row in player_stats_data['data'] if 'stats' in row['players']]
    player_stats_df = pd.concat(player_stats_dfs, ignore_index=True)
    
    #Clean minutes played
    player_stats_df['player_minutesPlayed'] = player_stats_df['player_minutesPlayed'].str.replace("'", "", regex=False)
    player_stats_df['player_minutesPlayed'] = pd.to_numeric(player_stats_df['player_minutesPlayed'], errors='coerce')

    #stats to numeric
    player_stats_df['player_appearances'] = pd.to_numeric(player_stats_df['player_appearances'], errors='coerce')
    player_stats_df['player_goalsConceded'] = pd.to_numeric(player_stats_df['player_goalsConceded'], errors='coerce')
    player_stats_df['player_cleanSheets'] = pd.to_numeric(player_stats_df['player_cleanSheets'], errors='coerce')
    player_stats_df['player_yellowCards'] = pd.to_numeric(player_stats_df['player_yellowCards'], errors='coerce')
    player_stats_df['player_redCards'] = pd.to_numeric(player_stats_df['player_redCards'], errors='coerce')
    player_stats_df['player_goals'] = pd.to_numeric(player_stats_df['player_goals'], errors='coerce')
    player_stats_df['player_secondYellowCards'] = pd.to_numeric(player_stats_df['player_secondYellowCards'], errors='coerce')
    player_stats_df['player_assists'] = pd.to_numeric(player_stats_df['player_assists'], errors='coerce')


    #Clean updatedAt
    player_stats_df['player_updatedAt'] = pd.to_datetime(player_stats_df['player_updatedAt'], errors='coerce')

    ######              Players Achievements Clean Up              ######
    players_achievements_dfs = [pd.json_normalize(player['players'],['achievements',['details']], ['id',['achievements','title'],['achievements','count'],'updatedAt'], sep='_', meta_prefix='player_',record_prefix='player_', errors='ignore') for player in players_achievements_data['data'] if 'achievements' in player['players']]
    players_achievements_df = pd.concat(players_achievements_dfs, ignore_index=True)

    #Clean updatedAt
    players_achievements_df['player_updatedAt'] = pd.to_datetime(players_achievements_df['player_updatedAt'], errors='coerce')


    ######              Players Data Clean Up              ######
    players_dfs = [pd.json_normalize(player['players'],['players'],['updatedAt'], sep='_', meta_prefix='player_', record_prefix='player_') for player in players_data['data']]
    players_df = pd.concat(players_dfs, ignore_index=True)

    #player_dateOfBirth
    players_df['player_dateOfBirth'] = pd.to_datetime(players_df['player_dateOfBirth'], format='%b %d, %Y')

    #player_age
    players_df['player_age'] = pd.to_numeric(players_df['player_age'],downcast='integer')

    # Step 1: Remove the "m" and replace the comma with a period
    players_df['player_height'] = players_df['player_height'].fillna('').astype(str)
    players_df['player_height'] = players_df['player_height'].str.replace('m', '').str.replace(',', '.')
    players_df['player_height'] = pd.to_numeric(players_df['player_height'], errors='coerce')


    #player_club_joined
    players_df['player_joinedOn'] = pd.to_datetime(players_df['player_joinedOn'], format='%b %d, %Y')

    #player_club_contractExpires
    players_df['player_contract'] = pd.to_datetime(players_df['player_contract'], format='%b %d, %Y')

    #player_marketValue
    players_df['player_marketValue'] = players_df['player_marketValue'].str.replace('€', '')
    players_df['player_marketValue'] = players_df['player_marketValue'].fillna('').astype(str)
    players_df['player_marketValue'] = players_df['player_marketValue'].map(lambda value: 
        float(value.replace('k', '')) * 1000 if 'k' in value else
        float(value.replace('m', '')) * 1000000 if 'm' in value else
        float(value) if value != '' else float('nan')  # Handle empty string as NaN
    )

    #player_updatedAt
    players_df['player_updatedAt'] = pd.to_datetime(players_df['player_updatedAt'])

    #exploding nationality
    citizenship_df = players_df['player_nationality'].apply(pd.Series)
    citizenship_df.columns = [f'player_nationality_{i+1}' for i in range(citizenship_df.shape[1])]
    players_df = pd.concat([players_df, citizenship_df], axis=1) 

    ######              Players Injuries Clean Up              ######
    players_injuries_dfs = [pd.json_normalize(player['players'],['injuries'],['updatedAt','id'], sep='_', meta_prefix='player_', record_prefix='player_') for player in players_injuries_data['data'] if 'injuries' in player['players']]
    players_injuries_df = pd.concat(players_injuries_dfs, ignore_index=True)

    #player_from
    players_injuries_df['player_from'] = pd.to_datetime(players_injuries_df['player_from'], format='%b %d, %Y')

    #player_until
    players_injuries_df['player_until'] = pd.to_datetime(players_injuries_df['player_until'], format='%b %d, %Y')

    #player_days
    players_injuries_df['player_days'] = players_injuries_df['player_days'].str.replace(' days', '', regex=False)
    players_injuries_df['player_days'] = pd.to_numeric(players_injuries_df['player_days'], errors='coerce', downcast='integer')

    #player_games_missed
    players_injuries_df['player_gamesMissed'] = pd.to_numeric(players_injuries_df['player_gamesMissed'], errors='coerce', downcast='integer')

    #player_updatedAt
    players_injuries_df['player_updatedAt'] = pd.to_datetime(players_injuries_df['player_updatedAt'])

    #exploding nationality
    gamesMissedClubs_df = players_injuries_df['player_gamesMissedClubs'].apply(pd.Series)
    gamesMissedClubs_df.columns = [f'player_gamesMissedClubs_{i+1}' for i in range(gamesMissedClubs_df.shape[1])]
    players_injuries_df = pd.concat([players_injuries_df, gamesMissedClubs_df], axis=1)

    ######              Players Market value Clean Up              ######
    players_market_value_df = pd.json_normalize(players_market_value_data['data'], sep='_', errors='ignore')

    market_value_history = []

    # Iterate over 'players_marketValueHistory' and 'player_id' columns together using zip
    for history, player_id in zip(players_market_value_df['players_marketValueHistory'], players_market_value_df['player_id']):
        if isinstance(history, list):  # Ensure history is a list before normalizing
            # Normalize the history list and add player_id as a new column in each normalized DataFrame
            market_value_history_data = pd.json_normalize(history, errors='ignore')
            market_value_history_data['id'] = player_id
            
            # Rename columns with the desired prefix
            market_value_history_data.columns = [f'player_{col}' for col in market_value_history_data.columns]
            
            # Append the normalized data to the list
            market_value_history.append(market_value_history_data)

    # Concatenate all normalized DataFrames
    players_market_value_history_df = pd.concat(market_value_history, ignore_index=True)

    #Build final df
    players_market_value_df = players_market_value_df.merge(players_market_value_history_df, on='player_id', how='left').drop(columns=['players_id','players_marketValueHistory'])

    #Clean up columns
    players_market_value_df.columns = [col.replace('players', 'player') for col in players_market_value_df.columns]
    players_market_value_df.columns = [
        col.replace(' ', '_')
        .replace('-', '_')
        .replace(',', '')
        .replace('.', '')
        .lower()
        for col in players_market_value_df.columns
    ]

    #player_age
    players_market_value_df['player_age'] = pd.to_numeric(players_market_value_df['player_age'], downcast='integer')

    #player_rankings
    for col in players_market_value_df.columns:
        if 'ranking' in col and 'Worldwide' not in col:
            players_market_value_df[col] = pd.to_numeric(players_market_value_df[col], downcast='integer')
        elif 'ranking_Worldwide' in col:
            players_market_value_df[col] = pd.to_numeric(players_market_value_df[col], downcast='float')

    #player_club_joined
    players_market_value_df['player_date'] = pd.to_datetime(players_market_value_df['player_date'], format='%b %d, %Y')

    #player_marketValue
    players_market_value_df['player_marketvalue'] = players_market_value_df['player_marketvalue'].str.replace('€', '')
    players_market_value_df['player_marketvalue'] = players_market_value_df['player_marketvalue'].fillna('').astype(str)
    players_market_value_df['player_marketvalue'] = players_market_value_df['player_marketvalue'].map(lambda value: 
        float(value.replace('k', '')) * 1000 if 'k' in value else
        float(value.replace('m', '')) * 1000000 if 'm' in value else
        float(value) if value != '' else float('nan')  # Handle empty string as NaN
    )

    #player_value
    players_market_value_df['player_value'] = players_market_value_df['player_value'].str.replace('€', '')
    players_market_value_df['player_value'] = players_market_value_df['player_value'].fillna('').astype(str)
    players_market_value_df['player_value'] = players_market_value_df['player_value'].map(lambda value: 
        float(value.replace('k', '')) * 1000 if 'k' in value else
        float(value.replace('m', '')) * 1000000 if 'm' in value else
        float(value) if value != '' else float('nan')  # Handle empty string as NaN
    )

    #player_updatedAt
    players_market_value_df['player_updatedat'] = pd.to_datetime(players_market_value_df['player_updatedat'])

    ######              Players Transfer Data Clean Up              ######
    players_transfers_dfs = [pd.json_normalize(player['players'],['transfers'],['id','updatedAt'], sep='_', record_prefix='player_', meta_prefix='players_') for player in players_transfers_data['data']]
    players_transfers_df = pd.concat(players_transfers_dfs, ignore_index=True)

    #player_dateOfBirth
    players_transfers_df['player_date'] = pd.to_datetime(players_transfers_df['player_date'], format='%b %d, %Y')

    #player_marketValue
    players_transfers_df['player_marketValue'] = players_transfers_df['player_marketValue'].str.replace('€', '')
    players_transfers_df['player_marketValue'] = players_transfers_df['player_marketValue'].fillna('').astype(str)
    players_transfers_df['player_marketValue'] = players_transfers_df['player_marketValue'].map(lambda value: 
        float(value.replace('k', '')) * 1000 if 'k' in value else
        float(value.replace('m', '')) * 1000000 if 'm' in value else
        float(value) if value != '' else float('nan')  # Handle empty string as NaN
    )

    #Columns rename
    players_transfers_df.columns = [col.replace('players', 'player') for col in players_transfers_df.columns]

    #player_updatedAt
    players_transfers_df['player_updatedAt'] = pd.to_datetime(players_transfers_df['player_updatedAt'])
    
    ######              League Data Clean Up              ######
    # Read the dictionary into a DataFrame
    league_df = pd.json_normalize(leagues_table_data)

    # Drop rows where 'position' is null
    league_df = league_df.dropna(subset=['position'])

    # Drop columns that contain only NaN values
    league_df = league_df.dropna(axis=1, how='all')

    # Split the 'goals' column into 'goals_scored' and 'goals_conceded'
    league_df[['goals_scored', 'goals_conceded']] = league_df['goals'].str.split(':', expand=True)

    # Convert the new columns to numeric values
    league_df['goals_scored'] = pd.to_numeric(league_df['goals_scored'], errors='coerce')
    league_df['goals_conceded'] = pd.to_numeric(league_df['goals_conceded'], errors='coerce')

    # Add the timestamp as a new column to the DataFrame
    league_df['league_updated_at'] = pd.to_datetime(datetime.now())

    #write to S3 club_profiles_data
    write_dataframe_to_s3(club_profiles_df,f'transformed_data/club_profiles_data/club_profile_data_transformed_{str(datetime.now().date())}.csv',s3_bucket_name)
    write_dataframe_to_s3(players_profile_df,f'transformed_data/player_profile_data/player_profile_data_transformed_{str(datetime.now().date())}.csv',s3_bucket_name)
    write_dataframe_to_s3(player_stats_df,f'transformed_data/player_stats_data/player_stats_data_transformed_{str(datetime.now().date())}.csv',s3_bucket_name)
    write_dataframe_to_s3(players_achievements_df,f'transformed_data/player_achievements_data/player_achievements_data_transformed_{str(datetime.now().date())}.csv',s3_bucket_name)
    write_dataframe_to_s3(players_injuries_df,f'transformed_data/player_injuries_data/player_injuries_data_transformed_{str(datetime.now().date())}.csv',s3_bucket_name)
    write_dataframe_to_s3(players_market_value_df,f'transformed_data/player_market_value_data/player_market_value_data_transformed_{str(datetime.now().date())}.csv',s3_bucket_name)
    write_dataframe_to_s3(players_transfers_df,f'transformed_data/player_transfers_data/player_transfers_data_transformed_{str(datetime.now().date())}.csv',s3_bucket_name)
    write_dataframe_to_s3(players_df,f'transformed_data/players_data/club_players_data_transformed_{str(datetime.now().date())}.csv',s3_bucket_name)
    write_dataframe_to_s3(league_df,f'transformed_data/league_data/league_data_transformed_{str(datetime.now().date())}.csv',s3_bucket_name)


    #Remove old
    delete_all_except_last_n(s3_bucket_name, 1, 'transformed_data/club_profiles_data')
    delete_all_except_last_n(s3_bucket_name, 1, 'transformed_data/player_profile_data')
    delete_all_except_last_n(s3_bucket_name, 1, 'transformed_data/player_stats_data')
    delete_all_except_last_n(s3_bucket_name, 1, 'transformed_data/player_achievements_data')
    delete_all_except_last_n(s3_bucket_name, 1, 'transformed_data/player_injuries_data')
    delete_all_except_last_n(s3_bucket_name, 1, 'transformed_data/player_stats_data')
    delete_all_except_last_n(s3_bucket_name, 1, 'transformed_data/player_transfers_data')
    delete_all_except_last_n(s3_bucket_name, 1, 'transformed_data/players_data')
    delete_all_except_last_n(s3_bucket_name, 1, 'transformed_data/player_market_value_data')
    delete_all_except_last_n(s3_bucket_name, 1, 'transformed_data/league_data')

    #Start Crawler
    start_crawler('club_profile_crawler')
    start_crawler('league_data_crawler')
    start_crawler('players_data_crawler')
    start_crawler('player_achievements_crawler')
    start_crawler('player_injuries_crawler')
    start_crawler('player_market_value_crawler')
    start_crawler('player_profile_crawler')
    start_crawler('player_stats_crawler')
    start_crawler('player_transfers_crawler')

# Start time
start_time = time.time()
logging.info(f'Script started {str(start_time)}')

main()

# End time and elapsed time
end_time = time.time()
elapsed_time = end_time - start_time
logging.info(f'Script ended {str(elapsed_time)}')

