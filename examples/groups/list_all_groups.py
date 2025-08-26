#!/usr/bin/env python3
"""
Example: List All Groups

This example demonstrates how to list ALL available data groups using automatic pagination.

Features demonstrated:
- Automatic pagination to get all groups
- Async and sync usage patterns
- Progress tracking for large datasets
- Error handling
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dataquery import DataQuery
from dataquery.exceptions import AuthenticationError


async def async_example():
    """Demonstrate async listing of all groups with pagination."""
    print("🔄 Async Example: Listing ALL Groups (with pagination)")
    print("=" * 60)
    
    try:
        async with DataQuery() as dq:
            print("📋 Fetching all groups (this may take a moment)...")
            
            # Fetch all groups (no limit -> SDK paginates internally)
            all_groups = await dq.list_groups_async()
            
            print(f"✅ Total groups found: {len(all_groups)}")
            print("\n📊 Groups breakdown:")
            
            # Show first 10 groups
            print("\n🔝 First 10 groups:")
            for i, group in enumerate(all_groups[:10], 1):
                print(f"  {i}. {group.group_id}")
                if hasattr(group, 'description') and group.description:
                    print(f"     Description: {group.description[:100]}...")
            
            # Show last 5 groups if we have more than 10
            if len(all_groups) > 10:
                print(f"\n🔚 Last 5 groups:")
                for i, group in enumerate(all_groups[-5:], len(all_groups)-4):
                    print(f"  {i}. {group.group_id}")
            
            # Group statistics
            print(f"\n📈 Statistics:")
            print(f"   • Total groups: {len(all_groups)}")
            
            # Count groups by type if available
            group_types = {}
            for group in all_groups:
                if hasattr(group, 'group_type') and group.group_type:
                    group_types[group.group_type] = group_types.get(group.group_type, 0) + 1
            
            if group_types:
                print("   • Groups by type:")
                for group_type, count in sorted(group_types.items()):
                    print(f"     - {group_type}: {count}")
            
    except AuthenticationError as e:
        print(f"❌ Authentication failed: {e}")
        print("💡 Check your credentials in .env file")
    except DataQueryError as e:
        print(f"❌ API error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


def sync_example():
    """Demonstrate sync listing of all groups."""
    print("\n🔄 Sync Example: Listing ALL Groups")
    print("=" * 50)
    
    dq = DataQuery()
    try:
        print("📋 Fetching all groups...")
        
        # Fetch all groups
        all_groups = dq.list_groups()
        
        print(f"✅ Total groups found: {len(all_groups)}")
        
        # Show sample of groups
        print("\n📋 Sample groups (first 5):")
        for i, group in enumerate(all_groups[:5], 1):
            print(f"  {i}. {group.group_id}")
            if hasattr(group, 'description') and group.description:
                print(f"     Description: {group.description[:80]}...")
            
    except AuthenticationError as e:
        print(f"❌ Authentication failed: {e}")
        print("💡 Check your credentials in .env file")
    except DataQueryError as e:
        print(f"❌ API error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        dq.cleanup()


async def interactive_main():
    print("🚀 List All Groups (lean)")
    print("Leave limit empty to fetch all (may take time).")
    raw = input("Limit [empty=all]: ").strip()
    try:
        limit = int(raw) if raw else None
    except ValueError:
        print("❌ Invalid limit")
        return
    try:
        async with DataQuery() as dq:
            groups = await dq.list_groups_async(limit=limit) if limit else await dq.list_groups_async()
            print(f"✅ Total groups: {len(groups)}")
            for i, g in enumerate(groups[:20], 1):
                print(f"{i}. {getattr(g, 'group_id', '')} | {getattr(g, 'group_name', '')}")
            if len(groups) > 20:
                print(f"... and {len(groups)-20} more")
    except AuthenticationError as e:
        print(f"❌ Authentication failed: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    asyncio.run(interactive_main())
