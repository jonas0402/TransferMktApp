import boto3
import os
import logging
import pandas as pd
from io import StringIO
from typing import Dict, Any

# Set up logging
logging.basicConfig(level=logging.INFO)

# S3 bucket details (adjust if needed)
BUCKET_NAME = "transfermkt-data"
REGION = 'us-east-1'

# Crawler names to start after updating schemas
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

def read_pipe_delimited_from_s3(bucket_name, file_key):
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=REGION
    )
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    data = response['Body'].read().decode('utf-8')
    return data

def list_glue_tables(database_name, region=REGION):
    glue = boto3.client('glue', region_name=region)
    tables = []
    paginator = glue.get_paginator('get_tables')
    for page in paginator.paginate(DatabaseName=database_name):
        tables.extend(page['TableList'])
    return tables

def get_latest_s3_file(bucket_name, prefix):
    """
    List objects in the given S3 bucket with the specified prefix and return the key of the most recently modified object.
    """
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=REGION
    )
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if 'Contents' not in response:
        return None
    objects = response['Contents']
    # Sort objects by LastModified descending and return the first key
    objects.sort(key=lambda x: x['LastModified'], reverse=True)
    return objects[0]['Key']

def get_glue_table_columns(database_name, table_name, region=REGION):
    glue = boto3.client('glue', region_name=region)
    response = glue.get_table(DatabaseName=database_name, Name=table_name)
    columns = response['Table']['StorageDescriptor']['Columns']
    return [col['Name'] for col in columns]

def infer_glue_type(column, series):
    """
    Infer the Glue column data type based on both the column name and its actual data.
    
    Rules:
    - If the column name contains 'clubid' or '_id' (case insensitive), always return 'string'.
    - If the column name contains 'updatedat', try to parse the series as datetime and return 'timestamp' if successful.
    - If the column name contains 'date', try to parse the series as datetime and return 'date' if successful.
    - Otherwise, use Pandas type inference:
         * Integers → 'int'
         * Floats → 'double'
         * Booleans → 'boolean'
         * Datetime types → 'timestamp'
         * Else default to 'string'
    """
    col_lower = column.lower()
    if 'clubid' in col_lower or '_id' in col_lower:
        return 'string'
    
    if 'updatedat' in col_lower:
        try:
            parsed = pd.to_datetime(series, errors='coerce')
            if parsed.notna().sum() > 0:
                return 'timestamp'
        except Exception:
            pass

    if 'date' in col_lower:
        try:
            parsed = pd.to_datetime(series, errors='coerce')
            if parsed.notna().sum() > 0:
                return 'date'
        except Exception:
            pass

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

def update_glue_table_columns(database_name, table_name, df, new_columns, region=REGION):
    glue = boto3.client('glue', region_name=region)
    
    # Retrieve current table definition
    current_table_response = glue.get_table(DatabaseName=database_name, Name=table_name)
    current_table = current_table_response['Table']
    
    # Build a new list of columns based on the file's columns and the inferred types
    new_columns_list = []
    for col in new_columns:
        dtype = infer_glue_type(col, df[col])
        new_columns_list.append({"Name": col, "Type": dtype, "Comment": ""})
    
    # Construct TableInput from allowed keys of the current table definition
    allowed_keys = ['Name', 'Description', 'Owner', 'Retention', 'StorageDescriptor', 'PartitionKeys', 'TableType', 'Parameters']
    table_input = { key: current_table[key] for key in allowed_keys if key in current_table }
    
    # Replace the columns with the new columns list
    table_input['StorageDescriptor']['Columns'] = new_columns_list
    
    logging.info(f"\nUpdating Glue table '{table_name}' schema:")
    for col in new_columns_list:
        logging.info(f"  Column: {col['Name']} -> Type: {col['Type']}")
    
    response = glue.update_table(
        DatabaseName=database_name,
        TableInput=table_input
    )
    return response

def process_glue_tables(database_name, s3_bucket, s3_prefix_root="transformed_data/"):
    tables = list_glue_tables(database_name)
    for table in tables:
        table_name = table['Name']
        logging.info(f"\nProcessing table: {table_name}")
        # Expect the corresponding S3 files to be under "transformed_data/<table_name>/"
        prefix = f"{s3_prefix_root}{table_name}/"
        s3_file_key = get_latest_s3_file(s3_bucket, prefix)
        if not s3_file_key:
            logging.info(f"No S3 file found for table '{table_name}' with prefix '{prefix}'. Skipping.")
            continue
        
        logging.info(f"Found S3 file for table '{table_name}': {s3_file_key}")
        try:
            pipe_delimited_data = read_pipe_delimited_from_s3(s3_bucket, s3_file_key)
            df = pd.read_csv(StringIO(pipe_delimited_data), sep='|')
        except Exception as e:
            logging.error(f"Error processing S3 file '{s3_file_key}' for table '{table_name}': {e}")
            continue
        
        transformed_columns = df.columns.tolist()
        glue_columns = get_glue_table_columns(database_name, table_name)
        
        logging.info("Columns in the S3 file:")
        logging.info(transformed_columns)
        logging.info("Columns in the Glue table schema:")
        logging.info(glue_columns)
        
        if transformed_columns == glue_columns:
            logging.info("The column names and order match exactly. No update needed.")
        else:
            logging.info("Mismatch detected. Updating Glue table schema...")
            update_response = update_glue_table_columns(database_name, table_name, df, transformed_columns)
            logging.info("Glue table update response:")
            logging.info(update_response)

def start_crawler(crawler_name: str) -> Dict[str, Any]:
    """Start an AWS Glue crawler."""
    glue_client = boto3.client('glue', region_name=REGION)
    logging.info(f"Starting crawler: {crawler_name}")
    response = glue_client.start_crawler(Name=crawler_name)
    return response

def start_all_crawlers(crawler_names):
    for crawler in crawler_names:
        try:
            response = start_crawler(crawler)
            logging.info(f"Crawler '{crawler}' started successfully. Response: {response}")
        except Exception as e:
            logging.error(f"Error starting crawler '{crawler}': {e}")

def main():
    # Specify your Glue database name here
    glue_database = 'transfermarket_analytics'
    
    # First update the Glue table schemas based on the latest S3 files
    process_glue_tables(glue_database, BUCKET_NAME)
    
    # Then, start the crawlers to refresh the Glue Data Catalog
    start_all_crawlers(crawler_names)

if __name__ == "__main__":
    main()
