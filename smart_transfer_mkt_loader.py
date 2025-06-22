#!/usr/bin/env python3
"""
Smart TransferMkt data loader with watermark-based incremental loading.

This script uses a watermark/control table to track data completeness and only
fetches missing data, making the pipeline more robust against API failures.
"""

import sys
import os
from datetime import datetime
from typing import Dict, List
import logging

# Add the transfermkt package to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from transfermkt.config import Config
from transfermkt.logger import setup_logging, log_execution_time
from transfermkt.watermark_utils import WatermarkManager
from transfermkt.io_utils import APIClient, S3Client
from transfermkt.player_logic import PlayerDataManager


@log_execution_time
def check_data_completeness(date: str) -> Dict:
    """Check current data completeness and return missing data sources"""
    watermark_manager = WatermarkManager()
    
    # Generate completeness report
    report = watermark_manager.get_data_completeness_report(date)
    logging.info(f"Data completeness for {date}: {report.get('overall_completeness', 0):.1f}%")
    
    # Get missing data sources
    missing_data = watermark_manager.get_missing_data_sources(date)
    
    if not missing_data:
        logging.info("✅ All data sources are complete for this date!")
        return {"complete": True, "missing": {}}
    
    logging.info(f"❌ Found missing data for {len(missing_data)} teams:")
    for team_id, sources in missing_data.items():
        logging.info(f"  Team {team_id}: {', '.join(sources)}")
    
    return {"complete": False, "missing": missing_data, "report": report}


@log_execution_time
def fetch_missing_data_sources(missing_data: Dict[str, List[str]], date: str):
    """Fetch only the missing data sources"""
    api_client = APIClient()
    s3_client = S3Client()
    player_manager = PlayerDataManager()
    watermark_manager = WatermarkManager()
    
    # Test API connectivity first
    if not api_client.test_api_connectivity():
        logging.error("API connectivity test failed. Aborting.")
        return False
    
    success_count = 0
    total_attempts = 0
    
    for team_id, missing_sources in missing_data.items():
        logging.info(f"\n🔄 Processing missing data for team {team_id}")
        
        for source in missing_sources:
            total_attempts += 1
            logging.info(f"  📥 Fetching {source} for team {team_id}")
            
            try:
                success = False
                record_count = 0
                
                if source == 'club_profiles':
                    data = player_manager.get_club_profiles_data()
                    if data:
                        s3_client.upload_json(data, 'club_profile_data', 'raw_data/club_profiles_data')
                        record_count = len(data.get('data', []))
                        success = True
                
                elif source == 'players_profile':
                    data = player_manager.get_players_profile_data()
                    if data:
                        s3_client.upload_json(data, 'players_profile_data', 'raw_data/players_profile_data')
                        record_count = len(data.get('data', []))
                        success = True
                
                elif source == 'player_stats':
                    data = player_manager.get_player_stats_data()
                    if data:
                        s3_client.upload_json(data, 'player_stats_data', 'raw_data/player_stats_data')
                        record_count = len(data.get('data', []))
                        success = True
                
                elif source == 'players_achievements':
                    data = player_manager.get_players_achievements_data()
                    if data:
                        s3_client.upload_json(data, 'players_achievements_data', 'raw_data/players_achievements_data')
                        record_count = len(data.get('data', []))
                        success = True
                
                elif source == 'players_data':
                    data = player_manager.get_players_data()
                    if data:
                        s3_client.upload_json(data, 'club_players_data', 'raw_data/players_data')
                        record_count = len(data.get('data', []))
                        success = True
                
                elif source == 'players_injuries':
                    data = player_manager.get_players_injuries_data()
                    if data:
                        s3_client.upload_json(data, 'players_injuries_data', 'raw_data/players_injuries_data')
                        record_count = len(data.get('data', []))
                        success = True
                
                elif source == 'players_market_value':
                    data = player_manager.get_players_market_value_data()
                    if data:
                        s3_client.upload_json(data, 'players_market_value_data', 'raw_data/players_market_value_data')
                        record_count = len(data.get('data', []))
                        success = True
                
                elif source == 'players_transfers':
                    data = player_manager.get_players_transfers_data()
                    if data:
                        s3_client.upload_json(data, 'players_transfers_data', 'raw_data/players_transfers_data')
                        record_count = len(data.get('data', []))
                        success = True
                
                elif source == 'leagues_table':
                    data = api_client.scrape_transfermarkt_table("major-league-soccer")
                    if data:
                        s3_client.upload_json(data, 'league_table_data', 'raw_data/league_data')
                        record_count = len(data)
                        success = True
                
                # Update watermark table
                watermark_manager.update_data_status(
                    date=date,
                    team_id=team_id,
                    data_source=source,
                    success=success,
                    record_count=record_count
                )
                
                if success:
                    success_count += 1
                    logging.info(f"    ✅ Successfully fetched {source} ({record_count} records)")
                else:
                    logging.warning(f"    ❌ Failed to fetch {source}")
                    
            except Exception as e:
                logging.error(f"    💥 Error fetching {source} for team {team_id}: {e}")
                # Update watermark as failed
                watermark_manager.update_data_status(
                    date=date,
                    team_id=team_id,
                    data_source=source,
                    success=False
                )
    
    logging.info(f"\n📊 Fetch Summary:")
    logging.info(f"  Total attempts: {total_attempts}")
    logging.info(f"  Successful: {success_count}")
    logging.info(f"  Failed: {total_attempts - success_count}")
    logging.info(f"  Success rate: {(success_count / total_attempts * 100):.1f}%")
    
    return success_count > 0


@log_execution_time
def smart_data_loading_workflow(date: str = None):
    """
    Main workflow for smart data loading with watermark-based incremental loading
    """
    if not date:
        date = str(datetime.now().date())
    
    logging.info(f"🚀 Starting smart data loading workflow for {date}")
    
    try:
        # Step 1: Check current data completeness
        logging.info("📋 Step 1: Checking data completeness...")
        completeness_status = check_data_completeness(date)
        
        if completeness_status["complete"]:
            logging.info("🎉 All data is already complete! No action needed.")
            return True
        
        # Step 2: Fetch only missing data
        logging.info("📥 Step 2: Fetching missing data sources...")
        fetch_success = fetch_missing_data_sources(completeness_status["missing"], date)
        
        if not fetch_success:
            logging.error("❌ Failed to fetch any missing data sources")
            return False
        
        # Step 3: Final completeness check
        logging.info("🔍 Step 3: Final completeness check...")
        final_status = check_data_completeness(date)
        
        if final_status["complete"]:
            logging.info("🎉 Data loading workflow completed successfully! All data is now complete.")
        else:
            remaining_missing = len([item for sublist in final_status["missing"].values() for item in sublist])
            logging.warning(f"⚠️  Workflow completed with {remaining_missing} data sources still missing")
            logging.info("💡 You can run this script again to retry failed data sources")
        
        return True
        
    except Exception as e:
        logging.error(f"💥 Smart data loading workflow failed: {e}", exc_info=True)
        return False


def main():
    """Main entry point"""
    setup_logging()
    
    # Get date from command line argument or use today
    date = sys.argv[1] if len(sys.argv) > 1 else str(datetime.now().date())
    
    logging.info("=" * 60)
    logging.info("🎯 SMART TRANSFERMKT DATA LOADER")
    logging.info("=" * 60)
    logging.info(f"Target date: {date}")
    logging.info(f"Using watermark-based incremental loading")
    
    try:
        success = smart_data_loading_workflow(date)
        
        if success:
            logging.info("\n✅ Smart data loading completed successfully!")
            sys.exit(0)
        else:
            logging.error("\n❌ Smart data loading failed!")
            sys.exit(1)
            
    except Exception as e:
        logging.error(f"\n💥 Unexpected error in main: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()