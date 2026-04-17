#!/usr/bin/env python3
"""
Notification-driven download example.

Subscribes to the DataQuery /notification SSE endpoint for a given group.
Whenever a notification arrives the SDK fetches the available-files list and
downloads any files not already present in the destination directory.

An initial availability check is performed on start so files that became
available before the SSE connection was established are not missed.

Press Ctrl+C to stop.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # noqa: E402

from dataquery import DataQuery  # noqa: E402
from dataquery.exceptions import AuthenticationError  # noqa: E402


async def main():
    print("[Start] dataquery-sdk - Notification Download Example")
    print("=" * 60)
    print("Subscribes to /notification SSE and downloads new files.")
    print("Press Ctrl+C to stop.\n")

    group_id = input("Enter group ID to watch: ").strip()
    if not group_id:
        print("[Error] Group ID is required")
        return

    destination = input("Destination directory [./downloads]: ").strip() or "./downloads"

    try:
        async with DataQuery() as dq:
            print(f"[Info] Subscribing to notifications for group '{group_id}'")
            print(f"[Info] Files will be saved to: {destination}\n")

            manager = await dq.auto_download_async(
                group_id=group_id,
                destination_dir=destination,
                initial_check=True,
            )

            try:
                while True:
                    await asyncio.sleep(60)
            except KeyboardInterrupt:
                print("\n[Warning] Stopping...")
                await manager.stop()
                stats = manager.get_stats()
                print(
                    f"[Success] Stopped.\n"
                    f"  Notifications received : {stats['notifications_received']}\n"
                    f"  Checks triggered       : {stats['checks_triggered']}\n"
                    f"  Files downloaded       : {stats['files_downloaded']}\n"
                    f"  Files skipped          : {stats['files_skipped']}\n"
                    f"  Download failures      : {stats['download_failures']}"
                )
    except AuthenticationError as exc:
        print(f"[Error] Authentication failed: {exc}")
        print("[Tip] Check your credentials in .env file")
    except Exception as exc:
        print(f"[Error] Unexpected error: {exc}")


if __name__ == "__main__":
    asyncio.run(main())
