"""Command Line Interface for DataQuery SDK."""

import argparse
import asyncio
import json
import sys
from pathlib import Path

from dataquery import DataQuery


def create_parser() -> argparse.ArgumentParser:
    """Create the top-level CLI parser with subcommands."""
    parser = argparse.ArgumentParser(
        description="Command Line Interface for the DataQuery SDK"
    )
    parser.add_argument("--env-file", type=str, default=None, help="Path to .env file")
    subparsers = parser.add_subparsers(dest="command")

    # groups
    p_groups = subparsers.add_parser("groups", help="List or search groups")
    p_groups.add_argument("--json", action="store_true", help="Output JSON")
    p_groups.add_argument(
        "--limit", type=int, default=None, help="Limit number of results"
    )
    p_groups.add_argument("--search", type=str, default=None, help="Search keywords")

    # files
    p_files = subparsers.add_parser("files", help="List files in a group")
    p_files.add_argument("--group-id", required=True)
    p_files.add_argument("--file-group-id", default=None)
    p_files.add_argument("--limit", type=int, default=None)
    p_files.add_argument("--json", action="store_true")

    # availability
    p_avail = subparsers.add_parser("availability", help="Check file availability")
    p_avail.add_argument("--file-group-id", required=True)
    p_avail.add_argument("--file-datetime", required=True)
    p_avail.add_argument("--json", action="store_true")

    # download
    p_dl = subparsers.add_parser("download", help="Download a file or start watch mode")
    p_dl.add_argument("--file-group-id", default=None)
    p_dl.add_argument("--file-datetime", default=None)
    p_dl.add_argument("--destination", type=str, default=None)
    p_dl.add_argument(
        "--watch", action="store_true", help="Watch a group for new files"
    )
    p_dl.add_argument("--group-id", default=None, help="Required with --watch")
    p_dl.add_argument("--json", action="store_true")

    # config
    p_cfg = subparsers.add_parser("config", help="Config utilities")
    cfg_sub = p_cfg.add_subparsers(dest="config_command")
    _ = cfg_sub.add_parser("show", help="Show resolved config")
    _ = cfg_sub.add_parser("validate", help="Validate config")
    p_tmpl = cfg_sub.add_parser("template", help="Write .env template")
    p_tmpl.add_argument("--output", type=str, required=True)

    # auth
    p_auth = subparsers.add_parser("auth", help="Auth utilities")
    auth_sub = p_auth.add_subparsers(dest="auth_command")
    _ = auth_sub.add_parser("test", help="Test authentication by listing groups")

    return parser


async def cmd_groups(args: argparse.Namespace) -> int:
    async with DataQuery(args.env_file) as dq:
        if args.search:
            items = await dq.search_groups_async(args.search, limit=args.limit)
        else:
            items = await dq.list_groups_async(limit=args.limit)
        if args.json:
            payload = []
            for g in items:
                try:
                    payload.append(g.model_dump())
                except Exception:
                    payload.append(str(g))
            print(json.dumps(payload, indent=2))
        else:
            for g in items:
                try:
                    d = g.model_dump()
                    print(
                        f"{d.get('group_id') or d.get('group-id')}\t{d.get('group_name') or d.get('group-name')}"
                    )
                except Exception:
                    print(str(g))
    return 0


async def cmd_files(args: argparse.Namespace) -> int:
    async with DataQuery(args.env_file) as dq:
        files = await dq.list_files_async(args.group_id, args.file_group_id)
        if args.json:
            payload = []
            for f in files:
                try:
                    payload.append(f.model_dump())
                except Exception:
                    payload.append(str(f))
            print(json.dumps(payload, indent=2))
        else:
            print(f"Found {len(files)} files")
            for f in files:
                try:
                    d = f.model_dump()
                    print(
                        f"{d.get('file_group_id') or d.get('file-group-id')}\t{d.get('file_type')}"
                    )
                except Exception:
                    print(str(f))
    return 0


async def cmd_availability(args: argparse.Namespace) -> int:
    async with DataQuery(args.env_file) as dq:
        avail = await dq.check_availability_async(
            args.file_group_id, args.file_datetime
        )
        if args.json:
            try:
                print(json.dumps(getattr(avail, "model_dump")(), indent=2))
            except Exception:
                print(str(avail))
        else:
            print(f"{args.file_group_id} @ {args.file_datetime}")
    return 0


async def cmd_download(args: argparse.Namespace) -> int:
    # Watch mode requires group-id
    if args.watch and not args.group_id:
        print("--group-id is required when using --watch")
        return 1

    async with DataQuery(args.env_file) as dq:
        if args.watch:
            try:
                mgr = await dq.start_auto_download_async(
                    group_id=args.group_id,
                    destination_dir=(args.destination or "./downloads"),
                )
                # Simulate quick watch then Ctrl+C
                await asyncio.sleep(1)
            except KeyboardInterrupt:
                try:
                    await mgr.stop()
                except Exception:
                    pass
                stats: dict = getattr(mgr, "get_stats", lambda: {})()
                print(json.dumps(stats))
                return 0
            return 0
        # single download
        dest_path = Path(args.destination) if args.destination else None
        result = await dq.download_file_async(
            args.file_group_id, args.file_datetime, dest_path, None
        )
        if args.json:
            print(json.dumps(getattr(result, "model_dump")(), indent=2))
        else:
            print(f"Downloaded to {result.local_path}")
        return 0


def cmd_config_show(args: argparse.Namespace) -> int:
    from dataquery.config import EnvConfig

    EnvConfig.create_client_config(
        env_file=Path(args.env_file) if getattr(args, "env_file", None) else None
    )
    print("Configuration loaded")
    return 0


def cmd_config_validate(args: argparse.Namespace) -> int:
    from dataquery.config import (
        EnvConfig,
    )

    try:
        EnvConfig.validate_config(EnvConfig.create_client_config())
        print("Configuration valid")
        return 0
    except Exception as e:
        print(f"Configuration invalid: {e}")
        return 1


def cmd_config_template(args: argparse.Namespace) -> int:
    # Import inside function to allow monkeypatch of dataquery.utils.create_env_template
    import dataquery.utils as utils

    out = utils.create_env_template(Path(args.output))
    print(f"Template written to {out}")
    return 0


async def cmd_auth_test(args: argparse.Namespace) -> int:
    async with DataQuery(args.env_file) as dq:
        _ = await dq.list_groups_async(limit=1)
    return 0


def main_sync(ns: argparse.Namespace) -> int:
    if ns.command == "config":
        if ns.config_command == "show":
            return cmd_config_show(ns)
        if ns.config_command == "validate":
            return cmd_config_validate(ns)
        if ns.config_command == "template":
            return cmd_config_template(ns)
        return 1
    return 0


def main() -> int:
    parser = create_parser()
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return 1
    # Dispatch
    if args.command == "groups":
        return asyncio.run(cmd_groups(args))
    if args.command == "files":
        return asyncio.run(cmd_files(args))
    if args.command == "availability":
        return asyncio.run(cmd_availability(args))
    if args.command == "download":
        return asyncio.run(cmd_download(args))
    if args.command == "config":
        return main_sync(args)
    if args.command == "auth" and args.auth_command == "test":
        return asyncio.run(cmd_auth_test(args))
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
