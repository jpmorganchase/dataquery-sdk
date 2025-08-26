#!/usr/bin/env python3
"""
Lean example: interactively list files in a group.
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dataquery import DataQuery
from dataquery.exceptions import AuthenticationError, NotFoundError


async def main():
    print("🚀 List Files (lean)")
    group_id = input("Enter group ID: ").strip()
    if not group_id:
        print("❌ Group ID is required")
        return
    specific = input("Filter by file_group_id (optional): ").strip() or None
    try:
        async with DataQuery() as dq:
            files = await dq.list_files_async(group_id, specific)
            count = len(files) if hasattr(files, '__len__') else getattr(files, 'file_count', 0)
            print(f"✅ Files: {count}")

    except NotFoundError:
        print("📭 Group or file not found")
    except AuthenticationError as e:
        print(f"❌ Authentication failed: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())


 
