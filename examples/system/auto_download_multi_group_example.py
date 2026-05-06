#!/usr/bin/env python3
"""
Multi-group notification-driven download example.

Subscribes to several DataQuery groups concurrently. Each group opens its own
SSE connection to ``/events/notification`` and gets its own
``NotificationDownloadManager`` and replay-state file (the on-disk fingerprint
in ``<destination>/.sse_state/`` is derived from the group + file-group tuple,
so the managers do not interfere with each other).

Authentication and rate limiting are shared through the single ``DataQuery``
instance.

Configure the subscriptions below in ``SUBSCRIPTIONS``. Each entry is a dict
with:
    - ``group_id``       (required)
    - ``destination``    (required) — a per-group directory keeps state clean
    - ``file_group_id``  (optional) — str, list[str], or omitted

Press Ctrl+C to stop all subscriptions cleanly.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # noqa: E402

from dataquery import DataQuery  # noqa: E402
from dataquery.types.exceptions import AuthenticationError  # noqa: E402


SUBSCRIPTIONS = [
    {
        "group_id": "JPMAQS",
        "destination": "./downloads/jpmaqs",
        "file_group_id": ["JPMAQS_ECONOMIC_SURPRISES_EXPLORER", "JPMAQS_FX_VOL"],
    },
    {
        "group_id": "MARKETS",
        "destination": "./downloads/markets",
    },
    {
        "group_id": "ECON",
        "destination": "./downloads/econ",
    },
]


async def _start_manager(dq: DataQuery, sub: dict):
    """Start one manager for a single subscription and return it."""
    return await dq.auto_download_async(
        group_id=sub["group_id"],
        destination_dir=sub["destination"],
        file_group_id=sub.get("file_group_id"),
        initial_check=False,
        enable_event_replay=True,
        heartbeat_timeout=90.0,
        max_tracked_files=10_000,
        max_tracked_errors=1_000,
    )


def _print_stats(label: str, stats: dict) -> None:
    print(
        f"  [{label}]\n"
        f"    Notifications received : {stats['notifications_received']}\n"
        f"    Files downloaded       : {stats['files_downloaded']}\n"
        f"    Files skipped          : {stats['files_skipped']}\n"
        f"    Download failures      : {stats['download_failures']}\n"
        f"    Last event id          : {stats.get('last_event_id') or '(none)'}"
    )


async def main():
    print("[Start] dataquery-sdk - Multi-Group Notification Download")
    print("=" * 60)
    for sub in SUBSCRIPTIONS:
        fg = sub.get("file_group_id")
        fg_label = f" file-group-id={fg}" if fg else ""
        print(f"  - group={sub['group_id']!r}{fg_label} -> {sub['destination']}")
    print()

    try:
        async with DataQuery() as dq:
            print("[Info] Event replay ENABLED (each group resumes via last-event-id).")
            print("[Info] Press Ctrl+C to stop.\n")

            # Start all managers concurrently. asyncio.gather lets the SSE
            # connections open in parallel rather than serially.
            managers = await asyncio.gather(
                *(_start_manager(dq, sub) for sub in SUBSCRIPTIONS)
            )

            try:
                while True:
                    await asyncio.sleep(60)
            except KeyboardInterrupt:
                print("\n[Warning] Stopping all subscriptions...")
                # Stop in parallel; return_exceptions so one slow stop doesn't
                # block the others.
                await asyncio.gather(
                    *(m.stop() for m in managers),
                    return_exceptions=True,
                )
                print("[Success] Stopped. Final stats:")
                for sub, m in zip(SUBSCRIPTIONS, managers):
                    _print_stats(sub["group_id"], m.get_stats())
    except AuthenticationError as exc:
        print(f"[Error] Authentication failed: {exc}")
        print("[Tip] Check your credentials in .env file")
    except Exception as exc:
        print(f"[Error] Unexpected error: {exc}")


if __name__ == "__main__":
    asyncio.run(main())
