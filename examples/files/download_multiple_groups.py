#!/usr/bin/env python3
"""
Example: Download Multiple Groups by Date Range

Downloads files for a predefined list of groups between a from-date and to-date.
Each group is downloaded sequentially; files within a group are downloaded
concurrently per `--max-concurrent-files`.

Usage:
    python download_multiple_groups.py FROM_DATE TO_DATE DESTINATION
    python download_multiple_groups.py 20250101 20250131 ./data

Options:
    --groups: Optional comma-separated subset of group IDs to download
    --max-concurrent-files: Max concurrent file downloads per group (default: 5)
    --num-parts: Number of parallel parts per file download (default: 1)
    --delay: Delay between file downloads in seconds (default: 0.04 for 25 TPS)
    --continue-on-error: Continue with next group if one fails (default: True)
"""

import argparse
import asyncio
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from dataquery import DataQuery  # noqa: E402


# All groups to download
GROUPS = [
    "SPECIALIST_SALES",
    "DATA_ASSETS_ALPHA_GROUP",
    "GENERALIST_SALES",
    "SALES_TRADERS",
]


async def download_one_group(dq, group_id, args, dest_root):
    """Download a single group into its own subdirectory."""
    group_dir = dest_root / group_id
    group_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'-' * 60}")
    print(f"[Group] {group_id}")
    print(f"  Destination: {group_dir}")
    print(f"{'-' * 60}")

    g_start = time.time()
    try:
        report = await dq.run_group_download_async(
            group_id=group_id,
            start_date=args.from_date,
            end_date=args.to_date,
            destination_dir=group_dir,
            max_concurrent=args.max_concurrent_files,
            num_parts=args.num_parts,
            delay_between_downloads=args.delay,
            max_retries=3,
        )
        elapsed = time.time() - g_start
        counts = report.counts or {}
        total = counts.get("total_files", 0)
        success = counts.get("successful_downloads", 0)
        failed = counts.get("failed_downloads", 0)
        print(
            f"  [Done] total={total} success={success} failed={failed} "
            f"time={elapsed:.1f}s"
        )
        return {
            "group_id": group_id,
            "status": "ok",
            "total": total,
            "success": success,
            "failed": failed,
            "elapsed": elapsed,
        }
    except Exception as e:
        elapsed = time.time() - g_start
        print(f"  [Error] {group_id}: {e}")
        return {
            "group_id": group_id,
            "status": "error",
            "error": str(e),
            "elapsed": elapsed,
        }


async def main():
    parser = argparse.ArgumentParser(
        description="Download multiple groups by date range",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("from_date", help="Start date in YYYYMMDD format")
    parser.add_argument("to_date", help="End date in YYYYMMDD format")
    parser.add_argument("destination", help="Root destination directory")
    parser.add_argument(
        "--groups",
        default=None,
        help="Optional comma-separated subset of group IDs (default: all)",
    )
    parser.add_argument("--max-concurrent-files", type=int, default=5)
    parser.add_argument("--num-parts", type=int, default=1)
    parser.add_argument("--delay", type=float, default=0.04)
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop processing groups when one fails (default: continue)",
    )

    args = parser.parse_args()

    dest_root = Path(args.destination)
    dest_root.mkdir(parents=True, exist_ok=True)

    groups = (
        [g.strip() for g in args.groups.split(",") if g.strip()]
        if args.groups
        else GROUPS
    )

    print("=" * 60)
    print("Multi-Group Download")
    print("=" * 60)
    print(f"Groups:         {len(groups)}")
    print(f"From Date:      {args.from_date}")
    print(f"To Date:        {args.to_date}")
    print(f"Destination:    {dest_root}")
    print(f"Concurrent:     {args.max_concurrent_files}")
    print(f"Parts per File: {args.num_parts}")
    print("=" * 60)

    start_time = time.time()
    results = []

    async with DataQuery() as dq:
        for group_id in groups:
            res = await download_one_group(dq, group_id, args, dest_root)
            results.append(res)
            if args.stop_on_error and res["status"] == "error":
                print(f"\n[Abort] Stopping due to error in {group_id}")
                break

    elapsed = time.time() - start_time

    total_files = sum(r.get("total", 0) for r in results)
    total_success = sum(r.get("success", 0) for r in results)
    total_failed = sum(r.get("failed", 0) for r in results)
    ok_groups = sum(1 for r in results if r["status"] == "ok")
    err_groups = sum(1 for r in results if r["status"] == "error")

    print(f"\n{'=' * 60}")
    print("OVERALL SUMMARY")
    print("=" * 60)
    print(f"Groups Processed: {len(results)} (ok={ok_groups}, error={err_groups})")
    print(f"Total Files:      {total_files}")
    print(f"Successful:       {total_success}")
    print(f"Failed:           {total_failed}")
    if total_files > 0:
        print(f"Success Rate:     {(total_success / total_files) * 100:.1f}%")
    print(f"Total Time:       {elapsed:.1f}s ({elapsed / 60:.1f} minutes)")
    print("=" * 60)

    print("\nPer-group results:")
    for r in results:
        if r["status"] == "ok":
            print(
                f"  [OK]    {r['group_id']:<40} "
                f"files={r['total']:>5} success={r['success']:>5} "
                f"failed={r['failed']:>3} time={r['elapsed']:.1f}s"
            )
        else:
            print(f"  [ERROR] {r['group_id']:<40} {r.get('error', '')}")

    if err_groups > 0:
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n[Info] Operation cancelled by user.")
