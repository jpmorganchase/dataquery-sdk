#!/usr/bin/env python3
"""
Lean auto-download example that takes inputs from the console.

Prompts for:
- Group ID to watch
- Destination directory
- Interval (minutes)
- Check only today (Y/n)
Press Ctrl+C to stop.
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime
import signal

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dataquery import DataQuery
from dataquery.models import DownloadProgress
from dataquery.exceptions import AuthenticationError


async def main():
    print("🚀 dataquery-sdk - Lean Auto-Download Example")
    print("=" * 60)
    print("This script will watch a group and download available files.")
    print("Press Ctrl+C to stop.\n")

    try:
        # Prompt inputs
        group_id = input("Enter group ID to watch: ").strip()
        if not group_id:
            print("❌ Group ID is required")
            return
        destination = input("Destination directory [./downloads]: ").strip() or "./downloads"
        try:
            interval = int(input("Check interval in minutes [30]: ").strip() or "30")
        except ValueError:
            print("❌ Invalid interval; must be an integer")
            return
        only_today_raw = input("Check only today? (Y/n) [Y]: ").strip().lower()
        check_current_date_only = (only_today_raw in ("", "y", "yes"))

        # Run watcher
        async with DataQuery() as dq:
            print(f"👀 Watching group '{group_id}' every {interval} min; saving to: {destination}")
            manager = await dq.start_auto_download_async(
                group_id=group_id,
                destination_dir=destination,
                interval_minutes=interval,
                check_current_date_only=check_current_date_only,
            )
            try:
                while True:
                    await asyncio.sleep(60)
            except KeyboardInterrupt:
                print("\n⚠️  Stopping watcher...")
                await manager.stop()
                stats = manager.get_stats()
                print(f"✅ Stopped. Files downloaded: {stats.get('files_downloaded')} | Failures: {stats.get('download_failures')}")
    except AuthenticationError as e:
        print(f"❌ Authentication failed: {e}")
        print("💡 Check your credentials in .env file")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
