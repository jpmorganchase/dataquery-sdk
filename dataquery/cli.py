"""Command Line Interface for DataQuery SDK."""

import argparse
import asyncio
import json
import sys
from pathlib import Path

from dataquery import DataQuery


def create_parser() -> argparse.ArgumentParser:
    """Create the top-level CLI parser with subcommands."""
    parser = argparse.ArgumentParser(description="Command Line Interface for the DataQuery SDK")
    parser.add_argument("--env-file", type=str, default=None, help="Path to .env file")
    subparsers = parser.add_subparsers(dest="command")

    # groups
    p_groups = subparsers.add_parser("groups", help="List or search groups")
    p_groups.add_argument("--json", action="store_true", help="Output JSON")
    p_groups.add_argument("--limit", type=int, default=None, help="Limit number of results")
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
    p_dl = subparsers.add_parser(
        "download",
        help="Download a single file, or with --watch subscribe to the SSE notification stream",
        description=(
            "Two modes:\n"
            "  single-file  --file-group-id FG --file-datetime YYYYMMDD [--destination DIR]\n"
            "  watch        --watch --group-id GROUP [--file-group-id FG1 FG2 ...] [--destination DIR]\n"
            "               (server-side filtered; Ctrl+C to stop)"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p_dl.add_argument(
        "--file-group-id",
        nargs="+",
        default=None,
        metavar="FILE_GROUP_ID",
        help=(
            "Single-file mode: one id; watch mode: one or more ids sent as the "
            "'file-group-id' query parameter so the server filters events."
        ),
    )
    p_dl.add_argument("--file-datetime", default=None, help="YYYYMMDD (single-file mode)")
    p_dl.add_argument("--destination", type=str, default=None)
    p_dl.add_argument(
        "--watch",
        action="store_true",
        help="Subscribe to the SSE notification stream and download files as they're published",
    )
    p_dl.add_argument("--group-id", default=None, help="Group to watch (required with --watch)")
    p_dl.add_argument("--json", action="store_true")
    p_dl.add_argument("--num-parts", type=int, default=5, help="Number of parallel parts (single-file mode)")
    p_dl.add_argument("--chunk-size", type=int, default=None, help="Chunk size in bytes (single-file mode)")
    p_dl.add_argument(
        "--no-event-replay",
        action="store_true",
        help=(
            "Watch mode: disable cross-process SSE event replay. The legacy "
            "behaviour (bulk availability check on every startup) is restored."
        ),
    )
    p_dl.add_argument(
        "--reset-event-id",
        action="store_true",
        help=(
            "Watch mode: delete the persisted SSE last-event-id before subscribing, so the next session starts fresh."
        ),
    )

    # download-group
    p_dlg = subparsers.add_parser(
        "download-group",
        help="Download files in a group for a date range (optionally filtered to one file-group-id)",
    )
    p_dlg.add_argument("--group-id", required=True)
    p_dlg.add_argument("--start-date", required=True)
    p_dlg.add_argument("--end-date", required=True)
    p_dlg.add_argument(
        "--file-group-id",
        nargs="+",
        default=None,
        metavar="FILE_GROUP_ID",
        help="Optional: restrict the date-range download to one or more file-group-ids "
        "(space-separated, e.g. --file-group-id FG1 FG2 FG3)",
    )
    p_dlg.add_argument("--destination", type=str, default="./downloads")
    p_dlg.add_argument("--max-concurrent", type=int, default=3)
    p_dlg.add_argument("--num-parts", type=int, default=5)
    p_dlg.add_argument("--json", action="store_true")

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
                    print(f"{d.get('group_id') or d.get('group-id')}\t{d.get('group_name') or d.get('group-name')}")
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
                    print(f"{d.get('file_group_id') or d.get('file-group-id')}\t{d.get('file_type')}")
                except Exception:
                    print(str(f))
    return 0


async def cmd_availability(args: argparse.Namespace) -> int:
    async with DataQuery(args.env_file) as dq:
        avail = await dq.check_availability_async(args.file_group_id, args.file_datetime)
        if args.json:
            try:
                print(json.dumps(getattr(avail, "model_dump")(), indent=2))
            except Exception:
                print(str(avail))
        else:
            print(f"{args.file_group_id} @ {args.file_datetime}")
    return 0


async def cmd_download(args: argparse.Namespace) -> int:
    if args.watch:
        if not args.group_id:
            print("--group-id is required when using --watch")
            return 1
    else:
        if not args.file_group_id:
            print("--file-group-id is required for a single-file download")
            return 1

    # argparse gives a list (nargs="+"). In single-file mode we need exactly one.
    file_group_id = args.file_group_id
    if not args.watch and isinstance(file_group_id, list):
        if len(file_group_id) != 1:
            print("--file-group-id must be a single value for single-file download")
            return 1
        file_group_id = file_group_id[0]

    async with DataQuery(args.env_file) as dq:
        if args.watch:
            # SSE-driven: subscribe to /sse/event/notification with group-id
            # (and optionally file-group-id) as query parameters so the server
            # filters events. An initial availability check covers anything
            # published before the subscription started (only used on the very
            # first run; subsequent runs replay missed events via last-event-id).
            destination_dir = args.destination or "./downloads"
            if getattr(args, "reset_event_id", False):
                # Resolve the store directly so we can clear it before any
                # connection is made.
                from .sse.event_store import Subscription, build_event_id_store

                client = dq._ensure_client()
                store = build_event_id_store(
                    client.config,
                    Subscription.from_user(args.group_id, file_group_id),
                )
                if store is not None:
                    store.clear()
            mgr = await dq.auto_download_async(
                group_id=args.group_id,
                destination_dir=destination_dir,
                file_group_id=file_group_id,
                enable_event_replay=not getattr(args, "no_event_replay", False),
            )
            try:
                # Stay connected until interrupted. Sleeping in short slices
                # keeps the loop responsive to Ctrl+C on all platforms.
                while True:
                    await asyncio.sleep(60)
            except (KeyboardInterrupt, asyncio.CancelledError):
                pass
            finally:
                try:
                    await mgr.stop()
                except Exception as e:
                    print(f"Failed to stop notification manager: {e}", file=sys.stderr)
            stats: dict = getattr(mgr, "get_stats", lambda: {})()
            print(json.dumps(stats))
            return 0

        # Single-file download path.
        dest_path = Path(args.destination) if args.destination else None

        from dataquery.models import DownloadOptions

        options = DownloadOptions(
            destination_path=dest_path,
            chunk_size=args.chunk_size if args.chunk_size is not None else 1048576,
        )

        result = await dq.download_file_async(
            file_group_id,
            args.file_datetime,
            options=options,
            num_parts=args.num_parts,
        )
        if args.json:
            print(json.dumps(getattr(result, "model_dump")(), indent=2))
        else:
            print(f"Downloaded to {result.local_path}")
        return 0


async def cmd_download_group(args: argparse.Namespace) -> int:
    async with DataQuery(args.env_file) as dq:
        results = await dq.run_group_download_async(
            group_id=args.group_id,
            start_date=args.start_date,
            end_date=args.end_date,
            destination_dir=args.destination,
            max_concurrent=args.max_concurrent,
            num_parts=args.num_parts,
            file_group_id=args.file_group_id,
        )

        if args.json:
            print(json.dumps(results, indent=2))
        else:
            print(f"Downloaded {results.get('successful', 0)} files to {args.destination}")
            if results.get("failed", 0) > 0:
                print(f"Failed: {results.get('failed', 0)}")
    return 0


def cmd_config_show(args: argparse.Namespace) -> int:
    from dataquery.config import EnvConfig

    EnvConfig.create_client_config(env_file=Path(args.env_file) if getattr(args, "env_file", None) else None)
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
    if args.command == "download-group":
        return asyncio.run(cmd_download_group(args))
    if args.command == "config":
        return main_sync(args)
    if args.command == "auth" and args.auth_command == "test":
        return asyncio.run(cmd_auth_test(args))
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
