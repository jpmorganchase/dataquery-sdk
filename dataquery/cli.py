#!/usr/bin/env python3
"""
Command-line interface for the DATAQUERY SDK.
"""

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Optional

from .dataquery import DataQuery
from .logging_config import (
    create_logging_config,
    create_logging_manager,
    LogLevel,
    LogFormat,
)
from .models import DownloadOptions
from .exceptions import DataQueryError, AuthenticationError, ValidationError


def create_parser() -> argparse.ArgumentParser:
    """Create the command-line argument parser."""
    parser = argparse.ArgumentParser(
        description="DATAQUERY SDK Command Line Interface",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all available data groups
  dataquery groups

  # Search for specific groups
  dataquery groups --search "economic"

  # List files in a group
  dataquery files --group-id economic-data

  # Check file availability
  dataquery availability --file-group-id gdp-data --file-datetime 20240101

  # Download a file
  dataquery download --file-group-id gdp-data --file-datetime 20240101

  # Download with custom destination
  dataquery download --file-group-id gdp-data --file-datetime 20240101 --destination ./my_data/

  # Show configuration
  dataquery config show

  # Test authentication
  dataquery auth test
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Groups command
    groups_parser = subparsers.add_parser('groups', help='List available data groups')
    groups_parser.add_argument('--limit', type=int, help='Limit number of groups to return')
    groups_parser.add_argument('--search', help='Search keywords for groups')
    groups_parser.add_argument('--json', action='store_true', help='Output results as JSON')
    
    # Files command
    files_parser = subparsers.add_parser('files', help='List files in a group')
    files_parser.add_argument('--group-id', required=True, help='Group ID to list files for')
    files_parser.add_argument('--file-group-id', help='Specific file group ID to filter by')
    files_parser.add_argument('--limit', type=int, help='Limit number of files to display')
    files_parser.add_argument('--json', action='store_true', help='Output results as JSON')
    
    # Availability command
    availability_parser = subparsers.add_parser('availability', help='Check file availability')
    availability_parser.add_argument('--file-group-id', required=True, help='File group ID to check')
    availability_parser.add_argument('--file-datetime', required=True, help='File datetime (YYYYMMDD)')
    availability_parser.add_argument('--json', action='store_true', help='Output results as JSON')
    
    # Download command
    download_parser = subparsers.add_parser('download', help='Download a file or watch a group')
    # Single file download params
    download_parser.add_argument('--file-group-id', help='File group ID to download')
    download_parser.add_argument('--file-datetime', help='File datetime (YYYYMMDD)')
    download_parser.add_argument('--destination', type=Path, help='Download destination path')
    download_parser.add_argument('--overwrite', action='store_true', help='Overwrite existing files')
    download_parser.add_argument('--json', action='store_true', help='Output result as JSON')
    # Watch mode params
    download_parser.add_argument('--watch', action='store_true', help='Continuously watch a group and download new files')
    download_parser.add_argument('--group-id', help='Group ID to watch (required with --watch)')
    download_parser.add_argument('--interval-minutes', type=int, default=30, help='Check interval when watching (minutes)')
    download_parser.add_argument('--all-dates', action='store_true', help='When watching, check today and previous 2 days')
    
    # Config command
    config_parser = subparsers.add_parser('config', help='Configuration management')
    config_subparsers = config_parser.add_subparsers(dest='config_command', help='Config commands')
    
    config_show_parser = config_subparsers.add_parser('show', help='Show current configuration')
    config_validate_parser = config_subparsers.add_parser('validate', help='Validate configuration')
    config_template_parser = config_subparsers.add_parser('template', help='Create configuration template')
    config_template_parser.add_argument('--output', type=Path, help='Output file path')
    
    # Auth command
    auth_parser = subparsers.add_parser('auth', help='Authentication management')
    auth_subparsers = auth_parser.add_subparsers(dest='auth_command', help='Auth commands')
    
    auth_test_parser = auth_subparsers.add_parser('test', help='Test authentication')
    auth_clear_parser = auth_subparsers.add_parser('clear', help='Clear stored tokens')
    
    # Global options
    parser.add_argument('--env-file', type=Path, help='Path to .env file')
    parser.add_argument('--log-level', default='INFO', 
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level')
    parser.add_argument('--async-mode', dest='async_mode', action='store_true', help='Use async mode')
    
    return parser


async def cmd_groups(args: argparse.Namespace) -> int:
    """Handle groups command."""
    try:
        async with DataQuery(args.env_file) as dq:
            if args.search:
                print(f"Searching for groups with keyword: '{args.search}'")
                groups = await dq.search_groups_async(args.search, limit=args.limit)
            else:
                print("Listing available data groups...")
                groups = await dq.list_groups_async(limit=args.limit)
            
            if args.json:
                import json
                from pydantic.json import pydantic_encoder
                print(json.dumps([g.model_dump() if hasattr(g, 'model_dump') else g.__dict__ for g in groups]))
            else:
                print(f"\nFound {len(groups)} groups:")
                for group in groups[: args.limit or len(groups) ]:
                    print(f"\n📁 {group.group_name}")
                    print(f"   ID: {group.group_id}")
                    if group.description:
                        print(f"   Description: {group.description}")
                    if group.provider:
                        print(f"   Provider: {group.provider}")
                    print(f"   Files: {group.associated_file_count or 0}")
                print(f"\n✅ Successfully listed {len(groups)} groups!")
            return 0
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1


async def cmd_files(args: argparse.Namespace) -> int:
    """Handle files command."""
    try:
        async with DataQuery(args.env_file) as dq:
            print(f"Listing files in group: {args.group_id}")
            files = await dq.list_files_async(args.group_id, args.file_group_id)
            
            if args.json:
                import json
                print(json.dumps([f.model_dump() if hasattr(f, 'model_dump') else f.__dict__ for f in files]))
            else:
                print(f"\nFound {len(files)} files:")
                for file_info in files[: args.limit or len(files) ]:
                    print(f"\n📄 {getattr(file_info, 'file_group_id', None)}")
                    print(f"   Type: {file_info.file_type}")
                    if file_info.description:
                        print(f"   Description: {file_info.description}")
                print(f"\n✅ Successfully listed {len(files)} files!")
            return 0
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1


async def cmd_availability(args: argparse.Namespace) -> int:
    """Handle availability command."""
    try:
        async with DataQuery(args.env_file) as dq:
            print(f"Checking availability for file: {args.file_group_id}")
            print(f"Date: {args.file_datetime}")
            
            availability = await dq.check_availability_async(args.file_group_id, args.file_datetime)
            
            if args.json:
                import json
                print(json.dumps(availability.model_dump()))
            else:
                print(f"\n📊 Availability Results:")
                print(f"Group ID: {availability.group_id}")
                print(f"File Group ID: {availability.file_group_id}")
                print(f"Date Range: {availability.date_range.earliest} to {availability.date_range.latest}")
                print(f"Total Files: {len(availability.availability)}")
                print(f"Available Files: {len(availability.available_files)}")
                print(f"Availability Rate: {availability.availability_rate:.1f}%")
                print(f"\n📋 File Details:")
                for file_info in availability.availability:
                    status = "✅ Available" if file_info.is_available else "❌ Not Available"
                    print(f"  {file_info.file_date}: {status}")
                    if file_info.is_available:
                        print(f"    File: {file_info.file_name}")
                        print(f"    Created: {file_info.first_created_on}")
                        print(f"    Modified: {file_info.last_modified}")
                print(f"\n✅ Availability check completed!")
            return 0
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1


async def cmd_download(args: argparse.Namespace) -> int:
    """Handle download command.

    Supports two modes:
    - Single download (default): requires --file-group-id; optional --file-datetime
    - Watch mode (--watch): requires --group-id; runs until interrupted
    """
    try:
        async with DataQuery(args.env_file) as dq:
            # Watch mode
            if args.watch:
                if not args.group_id:
                    print("❌ --group-id is required when using --watch")
                    return 1
                destination = args.destination or Path('./downloads')
                check_current_date_only = not args.all_dates
                print(f"👀 Watching group '{args.group_id}' every {args.interval_minutes} min; saving to: {destination}")
                manager = await dq.start_auto_download_async(
                    group_id=args.group_id,
                    destination_dir=str(destination),
                    interval_minutes=int(args.interval_minutes),
                    check_current_date_only=check_current_date_only,
                )
                try:
                    # Run indefinitely until Ctrl+C
                    while True:
                        await asyncio.sleep(60)
                except KeyboardInterrupt:
                    print("\n⚠️  Stopping watcher...")
                    await manager.stop()
                    stats = manager.get_stats()
                    print(f"✅ Stopped. Files downloaded: {stats.get('files_downloaded')} | Failures: {stats.get('download_failures')}")
                    return 0
                except Exception as e:
                    print(f"❌ Watcher error: {e}")
                    try:
                        await manager.stop()
                    except Exception:
                        pass
                    return 1

            # Single file download
            print(f"Downloading file: {args.file_group_id}")
            if args.file_datetime:
                print(f"Date: {args.file_datetime}")
            if args.destination:
                print(f"Destination: {args.destination}")
            
            # Create download options
            options = None
            if args.destination or args.overwrite:
                options = DownloadOptions(
                    destination_path=args.destination,
                    overwrite_existing=args.overwrite,
                    show_progress=True
                )
            
            if not args.file_group_id:
                print("❌ --file-group-id is required for single download (omit --watch)")
                return 1
            result = await dq.download_file_async(
                args.file_group_id,
                args.file_datetime,
                args.destination,
                options,
            )
            
            if args.json:
                import json
                print(json.dumps(result.model_dump()))
                return 0 if result.status.value == "completed" else 1
            if result.status.value == "completed":
                print(f"\n✅ Download completed successfully!")
                print(f"📁 File saved to: {result.local_path}")
                print(f"📊 File size: {result.file_size:,} bytes")
                print(f"⏱️  Download time: {result.download_time:.2f} seconds")
                print(f"🚀 Speed: {result.speed_mbps:.2f} MB/s")
                return 0
            else:
                print(f"\n❌ Download failed: {result.error_message}")
                return 1
                
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1


def cmd_config_show(args: argparse.Namespace) -> int:
    """Handle config show command."""
    try:
        from .config import EnvConfig
        
        # Load configuration
        config = EnvConfig.create_client_config(args.env_file)
        
        print("📋 Current Configuration:")
        print(f"   Base URL: {config.base_url}")
        if getattr(config, 'files_base_url', None):
            print(f"   Files Base URL: {config.files_base_url}")
        print(f"   OAuth Enabled: {config.oauth_enabled}")
        print(f"   Has OAuth Credentials: {config.has_oauth_credentials}")
        print(f"   Has Bearer Token: {config.has_bearer_token}")
        print(f"   Download Directory: {config.download_dir}")
        print(f"   Timeout: {config.timeout}s")
        print(f"   Max Retries: {config.max_retries}")
        print(f"   Rate Limit: {config.requests_per_minute} requests/minute")
        print(f"   Log Level: {config.log_level}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1


def cmd_config_validate(args: argparse.Namespace) -> int:
    """Handle config validate command."""
    try:
        from .config import EnvConfig
        
        # Validate configuration
        config = EnvConfig.create_client_config(args.env_file)
        EnvConfig.validate_config(config)
        
        print("✅ Configuration is valid!")
        return 0
        
    except Exception as e:
        print(f"❌ Configuration validation failed: {e}")
        return 1


def cmd_config_template(args: argparse.Namespace) -> int:
    """Handle config template command."""
    try:
        from .utils import create_env_template
        
        output_path = args.output or Path(".env.template")
        template_path = create_env_template(output_path)
        
        print(f"✅ Configuration template created: {template_path}")
        print("📝 Edit the template file and rename it to .env to use it.")
        return 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1


async def cmd_auth_test(args: argparse.Namespace) -> int:
    """Handle auth test command."""
    try:
        async with DataQuery(args.env_file) as dq:
            print("🔐 Testing authentication...")
            
            # Try to list groups (this will test authentication)
            groups = await dq.list_groups_async(limit=1)
            
            print("✅ Authentication successful!")
            print(f"   Found {len(groups)} group(s)")
            return 0
            
    except AuthenticationError as e:
        print(f"❌ Authentication failed: {e}")
        print("💡 Check your credentials in the .env file")
        return 1
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1


def cmd_auth_clear(args: argparse.Namespace) -> int:
    """Handle auth clear command."""
    try:
        from .config import EnvConfig
        
        # Load configuration to get token storage path
        config = EnvConfig.create_client_config(args.env_file)
        
        # Clear tokens
        from .auth import TokenManager
        token_manager = TokenManager(config)
        token_manager.clear_token()
        
        print("✅ Stored tokens cleared successfully!")
        return 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1


async def main_async(args: argparse.Namespace) -> int:
    """Main async function."""
    # Setup logging via LoggingManager (preferred)
    try:
        level = LogLevel[args.log_level.upper()]
    except Exception:
        level = LogLevel.INFO
    logging_config = create_logging_config(
        level=level,
        format=LogFormat.CONSOLE,
        enable_console=True,
        enable_file=False,
        enable_request_logging=False,
        enable_performance_logging=False,
    )
    create_logging_manager(logging_config)
    
    if args.command == 'groups':
        return await cmd_groups(args)
    elif args.command == 'files':
        return await cmd_files(args)
    elif args.command == 'availability':
        return await cmd_availability(args)
    elif args.command == 'download':
        return await cmd_download(args)
    elif args.command == 'auth':
        if args.auth_command == 'test':
            return await cmd_auth_test(args)
        elif args.auth_command == 'clear':
            return cmd_auth_clear(args)
        else:
            print("❌ Unknown auth command")
            return 1
    else:
        print("❌ Unknown command")
        return 1


def main_sync(args: argparse.Namespace) -> int:
    """Main sync function."""
    if args.command == 'config':
        if args.config_command == 'show':
            return cmd_config_show(args)
        elif args.config_command == 'validate':
            return cmd_config_validate(args)
        elif args.config_command == 'template':
            return cmd_config_template(args)
        else:
            print("❌ Unknown config command")
            return 1
    elif args.command == 'auth' and args.auth_command == 'clear':
        return cmd_auth_clear(args)
    else:
        # For other commands, use async mode
        return asyncio.run(main_async(args))


def main() -> int:
    """Main entry point."""
    parser = create_parser()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    try:
        if args.async_mode or args.command in ['groups', 'files', 'availability', 'download', 'auth']:
            return asyncio.run(main_async(args))
        else:
            return main_sync(args)
    except KeyboardInterrupt:
        print("\n⚠️  Operation cancelled by user")
        return 1
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main()) 