#!/usr/bin/env python3
"""
Lean example: interactively list files in a group.
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))  # noqa: E402

from dataquery import DataQuery  # noqa: E402
from dataquery.exceptions import AuthenticationError, NotFoundError  # noqa: E402


async def main():
    print("[Start] List Files (lean)")
    group_id = input("Enter group ID: ").strip()
    if not group_id:
        print("[Error] Group ID is required")
        return
    specific = input("Filter by file_group_id (optional): ").strip() or None
    try:
        async with DataQuery() as dq:
            files = await dq.list_files_async(group_id, specific)
            count = (
                len(files)
                if hasattr(files, "__len__")
                else getattr(files, "file_count", 0)
            )

            print(f"[Success] Files: {count}")
            for i, fi in enumerate(
                (
                    files[:20]
                    if hasattr(files, "__getitem__")
                    else files.file_group_ids[:20]
                ),
                1,
            ):
                fid = getattr(fi, "file_group_id", "")
                ftypes = getattr(fi, "file_type", None)
                ftype = (
                    ", ".join(ftypes) if isinstance(ftypes, list) else (ftypes or "")
                )
                print(f"{i}. {fid} {('(' + ftype + ')') if ftype else ''}")
    except NotFoundError:
        print("[Info] Group or file not found")
    except AuthenticationError as e:
        print(f"[Error] Authentication failed: {e}")
    except Exception as e:
        print(f"[Error] Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
