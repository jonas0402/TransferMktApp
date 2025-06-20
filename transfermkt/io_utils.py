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
    """API client for TransferMarkt data extraction with retry logic and rate limiting."""
    
    def __init__(self):
        """Initialize API client with base URL and session."""
        self.base_url = Config.BASE_URL
        self.session = requests.Session()
        
        # Set up headers to mimic a real browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                         '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })
    
    def _wait_for_rate_limit(self):
        """Apply rate limiting delay between requests."""
        import time
        time.sleep(Config.RATE_LIMIT_DELAY)
    
    def _should_retry(self, status_code: int, attempt: int) -> bool:
        """
        Determine if a request should be retried based on status code and attempt number.
        
        Args:
            status_code: HTTP status code
            attempt: Current attempt number (0-based)
            
        Returns:
            True if should retry, False otherwise
        """
        # Retry on server errors (5xx) and rate limiting (429)
        retryable_codes = [429, 500, 502, 503, 504]
        return status_code in retryable_codes and attempt < Config.MAX_RETRIES
    
    def _calculate_delay(self, attempt: int, base_delay: float = None) -> float:
        """
        Calculate exponential backoff delay.
        
        Args:
            attempt: Current attempt number (0-based)
            base_delay: Base delay in seconds
            
        Returns:
            Delay in seconds
        """
        if base_delay is None:
            base_delay = Config.RETRY_DELAY
        return base_delay * (Config.RETRY_BACKOFF ** attempt)
    
    @log_execution_time
    def make_request(self, endpoint: str) -> Optional[Dict[str, Any]]:
        """
        Make a request to the TransferMarkt API with retry logic and rate limiting.
        
        Args:
            endpoint: API endpoint
            
        Returns:
            JSON response or None if all retries failed
        """
        import time
        
        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(Config.MAX_RETRIES + 1):
            try:
                # Apply rate limiting (except on first attempt)
                if attempt > 0:
                    delay = self._calculate_delay(attempt - 1)
                    logging.info(f"Retry attempt {attempt} for {endpoint} after {delay:.1f}s delay")
                    time.sleep(delay)
                else:
                    self._wait_for_rate_limit()
                
                # Make the request
                response = self.session.get(
                    url, 
                    timeout=Config.REQUEST_TIMEOUT
                )
                
                # Handle different status codes
                if response.status_code == 200:
                    try:
                        return response.json()
                    except ValueError as e:
                        logging.error(f"Invalid JSON response from {endpoint}: {e}")
                        if not self._should_retry(response.status_code, attempt):
                            return None
                        continue
                
                elif response.status_code == 404:
                    logging.warning(f"Resource not found (404) for endpoint: {endpoint}")
                    return None  # Don't retry 404s
                
                elif response.status_code == 429:
                    logging.warning(f"Rate limited (429) for endpoint: {endpoint}")
                    if not self._should_retry(response.status_code, attempt):
                        logging.error(f"Max retries exceeded for rate limiting on {endpoint}")
                        return None
                
                elif self._should_retry(response.status_code, attempt):
                    logging.warning(
                        f"API call failed with status {response.status_code} for {endpoint}. "
                        f"Attempt {attempt + 1}/{Config.MAX_RETRIES + 1}. Response: {response.text[:200]}"
                    )
                else:
                    logging.error(
                        f"API call failed with status {response.status_code} for {endpoint}. "
                        f"Max retries exceeded. Response: {response.text[:200]}"
                    )
                    return None
                    
            except requests.exceptions.Timeout:
                logging.warning(f"Request timeout for {endpoint}. Attempt {attempt + 1}/{Config.MAX_RETRIES + 1}")
                if not self._should_retry(500, attempt):  # Treat timeout as server error
                    logging.error(f"Max retries exceeded for timeout on {endpoint}")
                    return None
                    
            except requests.exceptions.ConnectionError as e:
                logging.warning(f"Connection error for {endpoint}: {e}. Attempt {attempt + 1}/{Config.MAX_RETRIES + 1}")
                if not self._should_retry(500, attempt):  # Treat connection error as server error
                    logging.error(f"Max retries exceeded for connection error on {endpoint}")
                    return None
                    
            except Exception as e:
                logging.error(f"Unexpected error making API request to {endpoint}: {e}")
                return None
        
        logging.error(f"All retry attempts failed for {endpoint}")
        return None
    
    def make_request_with_fallback(self, endpoint: str, fallback_value: Any = None) -> Any:
        """
        Make API request with a fallback value if all retries fail.
        
        Args:
            endpoint: API endpoint
            fallback_value: Value to return if request fails
            
        Returns:
            API response or fallback value
        """
        result = self.make_request(endpoint)
        if result is None:
            logging.warning(f"Using fallback value for failed endpoint: {endpoint}")
            return fallback_value
        return result
    
    def scrape_transfermarkt_table(self, comp_name: str) -> List[Dict[str, Any]]:
        """
        Scrape league table data from transfermarkt.us website with retry logic.
        
        Args:
            comp_name: Competition name
            
        Returns:
            List of league table records
        """
        import numpy as np
        import time
        
        logging.info(f"Fetching league table data for competition: {comp_name}")
        
        current_year = datetime.now().year
        comp_name = comp_name.replace(" ", "-").lower()
        
        # Enhanced headers for web scraping
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                         '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        def make_web_request(url: str) -> requests.Response:
            """Make web request with retry logic."""
            for attempt in range(Config.MAX_RETRIES + 1):
                try:
                    if attempt > 0:
                        delay = self._calculate_delay(attempt - 1)
                        logging.info(f"Retrying web request after {delay:.1f}s delay")
                        time.sleep(delay)
                    else:
                        time.sleep(Config.RATE_LIMIT_DELAY)
                    
                    response = requests.get(url, headers=headers, timeout=Config.REQUEST_TIMEOUT)
                    
                    if response.status_code == 200:
                        return response
                    elif self._should_retry(response.status_code, attempt):
                        logging.warning(f"Web request failed with status {response.status_code}. Retrying...")
                        continue
                    else:
                        raise Exception(f"Web request failed with status {response.status_code}")
                        
                except Exception as e:
                    if attempt < Config.MAX_RETRIES:
                        logging.warning(f"Web request attempt {attempt + 1} failed: {e}")
                        continue
                    else:
                        raise
            
            raise Exception("All web request retry attempts failed")
        
        url = f'https://www.transfermarkt.us/{comp_name}/tabelle/wettbewerb/MLS1/saison_id/{current_year}'
        logging.info(f"Fetching data from initial URL: {url}")
        
        response = make_web_request(url)
        html_content = StringIO(response.text)
        tables = pd.read_html(html_content)
        
        seasons = tables[0][1][0].split('  ')
        result = []
        
        for year in seasons:
            url = f'https://www.transfermarkt.us/{comp_name}/tabelle/wettbewerb/MLS1/saison_id/{int(year)-1}'
            logging.info(f"Fetching season data for year {year} from URL: {url}")
            
            response = make_web_request(url)
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