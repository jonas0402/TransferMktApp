"""
TransferMkt Data Loader and Schema Management Script

This script serves as a thin wrapper that orchestrates Glue schema updates
and crawler management using the modular transfermkt package. It maintains 
compatibility with existing Airflow DAG while leveraging the refactored 
modular architecture.
"""

import time
import logging
from io import StringIO

from transfermkt.logger import setup_logging, log_execution_time
from transfermkt.config import Config
from transfermkt.io_utils import S3Client, GlueClient
from transfermkt.transform_utils import infer_glue_type


class SchemaManager:
    """Manages Glue table schema updates and crawler operations."""
    
    def __init__(self):
        """Initialize the SchemaManager with required clients."""
        self.s3_client = S3Client()
        self.glue_client = GlueClient()
    
    @log_execution_time
    def process_glue_tables(self, database_name: str, s3_prefix_root: str = "transformed_data/") -> None:
        """
        Process all Glue tables and update schemas based on latest S3 files.
        
        Args:
            database_name: Name of the Glue database
            s3_prefix_root: Root prefix for transformed data in S3
        """
        tables = self.glue_client.list_tables(database_name)
        
        for table in tables:
            table_name = table['Name']
            logging.info(f"\nProcessing table: {table_name}")
            
            # Expect corresponding S3 files under "transformed_data/<table_name>/"
            prefix = f"{s3_prefix_root}{table_name}/"
            s3_file_key = self.s3_client.get_latest_file_key(prefix)
            
            if not s3_file_key:
                logging.info(f"No S3 file found for table '{table_name}' with prefix '{prefix}'. Skipping.")
                continue
            
            logging.info(f"Found S3 file for table '{table_name}': {s3_file_key}")
            
            try:
                # Read and parse the pipe-delimited data
                pipe_delimited_data = self.s3_client.read_pipe_delimited_from_s3(s3_file_key)
                import pandas as pd
                df = pd.read_csv(StringIO(pipe_delimited_data), sep='|')
                
                # Get current and new column schemas
                transformed_columns = df.columns.tolist()
                glue_columns = self.glue_client.get_table_columns(database_name, table_name)
                
                logging.info("Columns in the S3 file:")
                logging.info(transformed_columns)
                logging.info("Columns in the Glue table schema:")
                logging.info(glue_columns)
                
                # Check if schema update is needed
                if transformed_columns == glue_columns:
                    logging.info("The column names and order match exactly. No update needed.")
                else:
                    logging.info("Mismatch detected. Updating Glue table schema...")
                    update_response = self.glue_client.update_table_schema(
                        database_name, table_name, df, transformed_columns
                    )
                    logging.info("Glue table update response:")
                    logging.info(update_response)
                    
            except Exception as e:
                logging.error(f"Error processing S3 file '{s3_file_key}' for table '{table_name}': {e}")
                continue
    
    @log_execution_time
    def start_all_crawlers(self, crawler_names: list) -> None:
        """
        Start all specified Glue crawlers.
        
        Args:
            crawler_names: List of crawler names to start
        """
        for crawler in crawler_names:
            try:
                response = self.glue_client.start_crawler(crawler)
                logging.info(f"Crawler '{crawler}' started successfully. Response: {response}")
            except Exception as e:
                logging.error(f"Error starting crawler '{crawler}': {e}")


@log_execution_time
def main():
    """
    Main function to orchestrate schema management and crawler operations.
    
    This function serves as the entry point for the Airflow DAG task,
    maintaining the same interface while using the modular architecture.
    """
    # Set up logging
    setup_logging()
    
    # Validate AWS credentials
    if not Config.validate_aws_credentials():
        raise ValueError("AWS credentials not found in environment variables")
    
    # Initialize the schema manager
    schema_manager = SchemaManager()
    
    try:
        logging.info("Starting schema management and crawler process...")
        
        # Step 1: Update Glue table schemas based on latest S3 files
        schema_manager.process_glue_tables(Config.GLUE_DATABASE)
        
        # Step 2: Start all crawlers to refresh the Glue Data Catalog
        schema_manager.start_all_crawlers(Config.CRAWLER_NAMES)
        
        logging.info("Schema management and crawler process completed successfully")
        
    except Exception as e:
        logging.error(f"Schema management process failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    logging.info(f"Total script execution time: {end_time - start_time:.2f} seconds")
