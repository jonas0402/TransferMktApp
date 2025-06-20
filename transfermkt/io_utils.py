"""
Input/Output utilities for TransferMkt data pipeline.

This module handles all I/O operations including S3 interactions, 
API calls, and file operations.
"""

import json
import pandas as pd
import boto3
import requests
from io import StringIO, BytesIO
from typing import Optional, Dict, Any, List
import logging
from datetime import datetime

from .config import Config
from .logger import log_execution_time


class S3Client:
    """S3 client wrapper for TransferMkt data operations."""
    
    def __init__(self):
        """Initialize S3 client with configuration."""
        self.client = boto3.client(
            's3',
            aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY,
            region_name=Config.AWS_REGION
        )
    
    @log_execution_time
    def upload_json(self, data: Any, file_name: str, folder_name: str) -> None:
        """
        Upload data to S3 bucket as JSON.
        
        Args:
            data: Data to upload
            file_name: Name of the file
            folder_name: S3 folder path
        """
        date_file = str(datetime.now().date())
        s3_key = f"{folder_name}/{file_name}_{date_file}.json"
        self.client.put_object(
            Body=json.dumps(data), 
            Bucket=Config.S3_BUCKET_NAME, 
            Key=s3_key
        )
        logging.info(f"Uploaded JSON file to S3: {s3_key}")
    
    @log_execution_time
    def upload_dataframe(self, df: pd.DataFrame, key: str) -> None:
        """
        Upload a DataFrame to S3 as pipe-delimited CSV.
        
        Args:
            df: DataFrame to upload
            key: S3 key path
        """
        df_for_output = df.copy()
        df_for_output.columns = df_for_output.columns.str.lower()
        buffer = BytesIO()
        df_for_output.to_csv(buffer, index=False, sep='|')
        buffer.seek(0)
        self.client.put_object(
            Bucket=Config.S3_BUCKET_NAME, 
            Key=key, 
            Body=buffer.getvalue()
        )
        logging.info(f"DataFrame written to S3 under key: {key}")
    
    def get_latest_file_key(self, folder_prefix: str) -> Optional[str]:
        """
        Get the most recent file key from an S3 folder.
        
        Args:
            folder_prefix: S3 folder prefix
            
        Returns:
            Most recent file key or None if no files found
        """
        try:
            response = self.client.list_objects_v2(
                Bucket=Config.S3_BUCKET_NAME, 
                Prefix=folder_prefix
            )
            if 'Contents' not in response:
                logging.error(f"No files found in {Config.S3_BUCKET_NAME}/{folder_prefix}")
                return None
            
            files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
            return files[0]['Key']
        except Exception as e:
            logging.error(f"Error listing objects in {folder_prefix}: {e}", exc_info=True)
            return None
    
    def read_json_from_s3(self, folder_key: str) -> Optional[Dict[str, Any]]:
        """
        Read the latest JSON file from an S3 folder.
        
        Args:
            folder_key: S3 folder path
            
        Returns:
            JSON data as dictionary or None if error
        """
        latest_file_key = self.get_latest_file_key(folder_key)
        if not latest_file_key:
            logging.error("No files found in the specified folder.")
            return None
        
        try:
            response = self.client.get_object(
                Bucket=Config.S3_BUCKET_NAME, 
                Key=latest_file_key
            )
            if response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200:
                logging.info(f"Successful S3 get_object response for key: {latest_file_key}")
                json_content = response['Body'].read().decode('utf-8')
                return json.loads(json_content)
            else:
                logging.error(f"Unsuccessful S3 get_object response")
                return None
        except Exception as e:
            logging.error(f"Error reading JSON from S3: {e}", exc_info=True)
            return None
    
    def read_pipe_delimited_from_s3(self, file_key: str) -> str:
        """
        Read pipe-delimited data from S3.
        
        Args:
            file_key: S3 file key
            
        Returns:
            File content as string
        """
        response = self.client.get_object(Bucket=Config.S3_BUCKET_NAME, Key=file_key)
        return response['Body'].read().decode('utf-8')
    
    @log_execution_time
    def delete_old_files(self, folder_name: str, files_to_keep: int = 1) -> None:
        """
        Delete old files in S3 folder, keeping only the most recent N files.
        
        Args:
            folder_name: S3 folder path
            files_to_keep: Number of recent files to keep
        """
        try:
            response = self.client.list_objects_v2(
                Bucket=Config.S3_BUCKET_NAME, 
                Prefix=folder_name
            )
            if 'Contents' not in response:
                logging.info(f"No objects found in {folder_name}.")
                return
            
            objects = response['Contents']
            sorted_objects = sorted(objects, key=lambda x: x['LastModified'], reverse=True)
            files_to_delete = sorted_objects[files_to_keep:]
            
            for obj in files_to_delete:
                self.client.delete_object(Bucket=Config.S3_BUCKET_NAME, Key=obj['Key'])
                logging.info(f"Deleted file: {obj['Key']}")
        except Exception as e:
            logging.error(f"Error deleting files in folder {folder_name}: {e}")


class GlueClient:
    """AWS Glue client wrapper for schema management."""
    
    def __init__(self):
        """Initialize Glue client with configuration."""
        self.client = boto3.client(
            'glue',
            aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY,
            region_name=Config.AWS_REGION
        )
    
    def list_tables(self, database_name: str) -> List[Dict[str, Any]]:
        """
        List all tables in a Glue database.
        
        Args:
            database_name: Name of the Glue database
            
        Returns:
            List of table metadata
        """
        tables = []
        paginator = self.client.get_paginator('get_tables')
        for page in paginator.paginate(DatabaseName=database_name):
            tables.extend(page['TableList'])
        return tables
    
    def get_table_columns(self, database_name: str, table_name: str) -> List[str]:
        """
        Get column names for a Glue table.
        
        Args:
            database_name: Name of the Glue database
            table_name: Name of the table
            
        Returns:
            List of column names
        """
        response = self.client.get_table(DatabaseName=database_name, Name=table_name)
        columns = response['Table']['StorageDescriptor']['Columns']
        return [col['Name'] for col in columns]
    
    def update_table_schema(self, database_name: str, table_name: str, 
                           df: pd.DataFrame, new_columns: List[str]) -> Dict[str, Any]:
        """
        Update Glue table schema based on DataFrame structure.
        
        Args:
            database_name: Name of the Glue database
            table_name: Name of the table
            df: DataFrame with new schema
            new_columns: List of new column names
            
        Returns:
            Update response from Glue
        """
        from .transform_utils import infer_glue_type
        
        # Retrieve current table definition
        current_table_response = self.client.get_table(
            DatabaseName=database_name, 
            Name=table_name
        )
        current_table = current_table_response['Table']
        
        # Build new columns list with inferred types
        new_columns_list = []
        for col in new_columns:
            dtype = infer_glue_type(col, df[col])
            new_columns_list.append({"Name": col, "Type": dtype, "Comment": ""})
        
        # Construct TableInput from allowed keys
        allowed_keys = [
            'Name', 'Description', 'Owner', 'Retention', 
            'StorageDescriptor', 'PartitionKeys', 'TableType', 'Parameters'
        ]
        table_input = {
            key: current_table[key] 
            for key in allowed_keys 
            if key in current_table
        }
        
        # Replace columns with new schema
        table_input['StorageDescriptor']['Columns'] = new_columns_list
        
        logging.info(f"\nUpdating Glue table '{table_name}' schema:")
        for col in new_columns_list:
            logging.info(f"  Column: {col['Name']} -> Type: {col['Type']}")
        
        response = self.client.update_table(
            DatabaseName=database_name,
            TableInput=table_input
        )
        return response
    
    def start_crawler(self, crawler_name: str) -> Dict[str, Any]:
        """
        Start an AWS Glue crawler.
        
        Args:
            crawler_name: Name of the crawler
            
        Returns:
            Start crawler response
        """
        logging.info(f"Starting crawler: {crawler_name}")
        return self.client.start_crawler(Name=crawler_name)


class APIClient:
    """API client for TransferMarkt data extraction."""
    
    def __init__(self):
        """Initialize API client with base URL."""
        self.base_url = Config.BASE_URL
    
    @log_execution_time
    def make_request(self, endpoint: str) -> Optional[Dict[str, Any]]:
        """
        Make a request to the TransferMarkt API.
        
        Args:
            endpoint: API endpoint
            
        Returns:
            JSON response or None if error
        """
        try:
            response = requests.get(f"{self.base_url}{endpoint}")
            if response.status_code != 200:
                logging.error(f"API call failed with status code {response.status_code}: {response.text}")
                raise Exception("API call failed")
            return response.json()
        except Exception as e:
            logging.error(f"Error making API request to {endpoint}: {e}")
            return None
    
    def scrape_transfermarkt_table(self, comp_name: str) -> List[Dict[str, Any]]:
        """
        Scrape league table data from transfermarkt.us website.
        
        Args:
            comp_name: Competition name
            
        Returns:
            List of league table records
        """
        import numpy as np
        
        logging.info(f"Fetching league table data for competition: {comp_name}")
        
        current_year = datetime.now().year
        comp_name = comp_name.replace(" ", "-").lower()
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                         '(KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
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
        
        logging.info("Completed fetching and processing league table data")
        return renamed_result