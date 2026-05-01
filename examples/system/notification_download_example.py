#!/usr/bin/env python3
"""
Notification-driven download example.

Subscribes to the DataQuery /notification SSE endpoint for a given group and,
optionally, one or more file-group ids (sent to the server so events are
filtered at the source). New files are downloaded as notifications arrive.

Cross-process event replay is enabled by default: the most recent event id is
persisted under ``<destination>/.sse_state/`` and replayed via the
``last-event-id`` URL parameter on the next run, so events published while the
process was down are not lost.

Press Ctrl+C to stop.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # noqa: E402

from dataquery import DataQuery  # noqa: E402
from dataquery.types.exceptions import AuthenticationError  # noqa: E402


def _parse_file_group_ids(raw: str):
    """Turn ``"FG1, FG2  FG3"`` into ``["FG1", "FG2", "FG3"]`` (or ``None``)."""
    if not raw:
        return None
    parts = [p.strip() for p in raw.replace(",", " ").split() if p.strip()]
    if not parts:
        return None
    return parts[0] if len(parts) == 1 else parts


async def main():
    print("[Start] dataquery-sdk - Notification Download Example")
    print("=" * 60)

    group_id = input("Group ID to watch: ").strip()
    if not group_id:
        print("[Error] Group ID is required")
        return

    raw_fgs = input("file-group-id filter (optional, space/comma separated): ").strip()
    file_group_id = _parse_file_group_ids(raw_fgs)

    destination = input("Destination directory [./downloads]: ").strip() or "./downloads"

    try:
        async with DataQuery() as dq:
            print(f"\n[Info] Watching group '{group_id}' -> {destination}")
            if file_group_id is not None:
                print(f"[Info] file-group-id filter: {file_group_id}")
            print("[Info] Event replay ENABLED (resumes via last-event-id).")
            print("[Info] Press Ctrl+C to stop.\n")

            manager = await dq.auto_download_async(
                group_id=group_id,
                destination_dir=destination,
                file_group_id=file_group_id,
                initial_check=False,  # Rely on SSE notifications only
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
                    f"  Files downloaded       : {stats['files_downloaded']}\n"
                    f"  Files skipped          : {stats['files_skipped']}\n"
                    f"  Download failures      : {stats['download_failures']}\n"
                    f"  Last event id          : {stats.get('last_event_id') or '(none)'}"
                )
    except AuthenticationError as exc:
        print(f"[Error] Authentication failed: {exc}")
        print("[Tip] Check your credentials in .env file")
    except Exception as exc:
        print(f"[Error] Unexpected error: {exc}")


if __name__ == "__main__":
    asyncio.run(main())
