#!/usr/bin/env python3
"""
Force refresh watermark table to check actual team data completeness.

This script forces a complete rebuild of the watermark table by checking
if each team actually has data in each data source file.
"""

import sys
import os
from datetime import datetime
import logging

# Add the transfermkt package to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from transfermkt.logger import setup_logging
from transfermkt.watermark_utils import WatermarkManager


def main():
    """Force refresh watermark table"""
    setup_logging()
    
    # Get date from command line argument or use today
    date = sys.argv[1] if len(sys.argv) > 1 else str(datetime.now().date())
    
    print("=" * 60)
    print("🔍 FORCE REFRESH WATERMARK TABLE")
    print("=" * 60)
    print(f"Target date: {date}")
    print("Checking actual team data completeness...")
    print()
    
    try:
        watermark_manager = WatermarkManager()
        
        # Force refresh the watermark table
        logging.info("🔄 Force refreshing watermark table...")
        df = watermark_manager.create_watermark_table(date, force_refresh=True)
        
        # Generate detailed report
        report = watermark_manager.get_data_completeness_report(date)
        
        print("📊 COMPLETENESS REPORT:")
        print(f"Overall completeness: {report.get('overall_completeness', 0):.1f}%")
        print(f"Total expected files: {report.get('total_expected_files', 0)}")
        print(f"Complete files: {report.get('total_complete_files', 0)}")
        print(f"Missing files: {report.get('missing_files', 0)}")
        print()
        
        # Show missing data by team
        missing_data = watermark_manager.get_missing_data_sources(date)
        if missing_data:
            print("❌ TEAMS WITH MISSING DATA:")
            for team_id, sources in missing_data.items():
                print(f"  Team {team_id}: {', '.join(sources)}")
        else:
            print("✅ All teams have complete data!")
        
        print()
        print("✅ Watermark refresh completed!")
        
    except Exception as e:
        print(f"❌ Error refreshing watermark: {e}")
        logging.error(f"Error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()