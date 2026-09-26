"""Command Line Interface for DataQuery SDK."""

import argparse
import asyncio
import getpass
import hashlib
import importlib.util
import json
import os
import sys
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any, Dict, List, Optional

from dataquery import DataQuery
from dataquery.types.exceptions import DataQueryError


def _to_dict(payload: Any) -> Dict[str, Any]:
    """Normalize a Pydantic model or dict into a plain dict for JSON dump."""
    if payload is None:
        return {}
    if hasattr(payload, "model_dump"):
        return payload.model_dump(by_alias=True)
    if isinstance(payload, dict):
        return payload
    return {"value": payload}


def _split_csv_list(value: Optional[str]) -> Optional[List[str]]:
    """Split a comma-separated string into a clean list, or return None."""
    if value is None:
        return None
    items = [v.strip() for v in value.split(",") if v.strip()]
    return items or None


def _count_timeseries(data: Dict[str, Any]) -> tuple[int, int, str, str]:
    """Return (instrument_count, data_point_count, first_date, last_date)."""
    instruments = data.get("instruments", []) or []
    total = 0
    first = ""
    last = ""
    for inst in instruments:
        for attr in inst.get("attributes", []) or []:
            ts = attr.get("time-series") or attr.get("time_series") or []
            total += len(ts)
            for point in ts:
                if isinstance(point, list) and point:
                    d = str(point[0])
                    if not first or d < first:
                        first = d
                    if not last or d > last:
                        last = d
    return len(instruments), total, first, last


def _print_endpoint_result(
    summary: str,
    payload: Any,
    *,
    csv_info: Optional[Dict[str, Any]] = None,
) -> None:
    """Print summary line(s) followed by ``--- JSON ---`` + raw JSON payload."""
    data = _to_dict(payload)
    envelope: Dict[str, Any] = {"status": "success", "data": data}
    if csv_info:
        envelope["csv_exported"] = csv_info.get("path", "stdout")
        envelope["csv_rows"] = csv_info.get("rows", 0)

    print(summary)
    if csv_info:
        print(f"CSV exported: {envelope['csv_exported']} ({envelope['csv_rows']} rows)")
    print("\n--- JSON ---")
    print(json.dumps(envelope, indent=2, default=str))


def _print_error(message: str, *, suggestion: Optional[str] = None) -> None:
    envelope: Dict[str, Any] = {"status": "error", "error_description": message}
    if suggestion:
        envelope["suggestion"] = suggestion
    print(message, file=sys.stderr)
    print("\n--- JSON ---")
    print(json.dumps(envelope, indent=2))


_MCP_CREDENTIAL_FLAGS = (
    ("client_id", "CLIENT_ID"),
    ("client_secret", "CLIENT_SECRET"),
    ("bearer_token", "BEARER_TOKEN"),
)

# Endpoint settings saved alongside the credentials, so a later DataQuery()
# mints tokens against the same environment instead of the PROD defaults.
_MCP_SAVED_SETTINGS = ("BASE_URL", "OAUTH_TOKEN_URL", "OAUTH_AUD")


def _export_mcp_credentials(args: argparse.Namespace) -> List[str]:
    """Export ``mcp-connect`` credential flags into the ``DATAQUERY_*`` process env.

    Flags win over anything already exported, so the rest of this process —
    the token manager here, and any SDK use later on — resolves them the usual
    way with no separate environment setup. Returns the unprefixed keys that
    are actually set afterwards, in save order.
    """
    from dataquery.config import EnvConfig

    for attr, env_key in _MCP_CREDENTIAL_FLAGS:
        value = getattr(args, attr, None)
        if value:
            os.environ[f"{EnvConfig.PREFIX}{env_key}"] = value

    keys = [env_key for _, env_key in _MCP_CREDENTIAL_FLAGS] + list(_MCP_SAVED_SETTINGS)
    # os.environ rather than get_env_var: model defaults must not be persisted
    # as if the user had chosen them.
    return [key for key in keys if os.environ.get(f"{EnvConfig.PREFIX}{key}")]


def _save_mcp_credentials(keys: List[str]) -> None:
    """Persist the resolved credentials to the user-level ``.env``.

    Best effort: a failure here is reported but never takes the MCP connection
    down with it. Messages go to stderr — stdout is the JSON-RPC channel.
    """
    from dataquery.config import EnvConfig

    if not any(key in keys for key in ("CLIENT_ID", "CLIENT_SECRET", "BEARER_TOKEN")):
        print(
            "--save-credentials: nothing to save; pass --client-id/--client-secret "
            "or set DATAQUERY_CLIENT_ID/DATAQUERY_CLIENT_SECRET.",
            file=sys.stderr,
        )
        return

    values = {key: os.environ.get(f"{EnvConfig.PREFIX}{key}") for key in keys}
    try:
        env_file = EnvConfig.save_user_env(values)
    except OSError as exc:
        print(f"--save-credentials: could not write credentials: {exc}", file=sys.stderr)
        return
    saved = ", ".join(f"{EnvConfig.PREFIX}{key}" for key in keys)
    print(f"Saved {saved} to {env_file} (owner-only)", file=sys.stderr)


def _has_mcp_credentials(keys: List[str]) -> bool:
    """A bearer token, or a complete OAuth client ID + secret pair."""
    return "BEARER_TOKEN" in keys or {"CLIENT_ID", "CLIENT_SECRET"} <= set(keys)


def _mcp_extra_installed() -> bool:
    return importlib.util.find_spec("fastmcp") is not None


# `mcp-install --app` targets: the JSON-config apps, plus Claude Code (its CLI) and ChatGPT (the Codex TOML).
MCP_INSTALL_APPS = ("claude-desktop", "claude-code", "chatgpt", "cursor", "vscode")


def _mcp_server_name(value: str) -> str:
    from dataquery.mcp_install import SERVER_NAME_RE

    if not SERVER_NAME_RE.fullmatch(value):
        raise argparse.ArgumentTypeError(f"invalid server name {value!r}: use letters, digits, '-' and '_' only")
    return value


def _prompt_for_mcp_credentials() -> None:
    """On a terminal, ask for whichever OAuth client credential is still missing (the secret unechoed)."""
    from dataquery.config import EnvConfig

    if not sys.stdin.isatty():
        return
    for key, label, ask in (
        ("CLIENT_ID", "DataQuery client ID: ", input),
        ("CLIENT_SECRET", "DataQuery client secret: ", getpass.getpass),
    ):
        env_key = f"{EnvConfig.PREFIX}{key}"
        if not os.environ.get(env_key):
            value = ask(label).strip()
            if value:
                os.environ[env_key] = value


def create_parser() -> argparse.ArgumentParser:
    """Create the top-level CLI parser with subcommands."""
    parser = argparse.ArgumentParser(description="Command Line Interface for the DataQuery SDK")
    parser.add_argument("--env-file", type=str, default=None, help="Path to .env file")
    subparsers = parser.add_subparsers(dest="command")

    p_groups = subparsers.add_parser("groups", help="List or search groups")
    p_groups.add_argument("--json", action="store_true", help="Output JSON")
    p_groups.add_argument("--limit", type=int, default=None, help="Limit number of results")
    p_groups.add_argument("--search", type=str, default=None, help="Search keywords")

    p_files = subparsers.add_parser("files", help="List files in a group")
    p_files.add_argument("--group-id", required=True)
    p_files.add_argument("--file-group-id", default=None)
    p_files.add_argument("--limit", type=int, default=None)
    p_files.add_argument("--json", action="store_true")

    p_avail = subparsers.add_parser("availability", help="Check file availability")
    p_avail.add_argument("--file-group-id", required=True)
    p_avail.add_argument("--file-datetime", required=True)
    p_avail.add_argument("--json", action="store_true")

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

    p_cfg = subparsers.add_parser("config", help="Config utilities")
    cfg_sub = p_cfg.add_subparsers(dest="config_command")
    _ = cfg_sub.add_parser("show", help="Show resolved config")
    _ = cfg_sub.add_parser("validate", help="Validate config")
    p_tmpl = cfg_sub.add_parser("template", help="Write .env template")
    p_tmpl.add_argument("--output", type=str, required=True)

    p_auth = subparsers.add_parser("auth", help="Auth utilities")
    auth_sub = p_auth.add_subparsers(dest="auth_command")
    _ = auth_sub.add_parser("test", help="Test authentication by listing groups")

    p_search = subparsers.add_parser(
        "search",
        help="Search the DataQuery catalog using a natural-language query (POST /search)",
    )
    p_search.add_argument("--query", required=True, help="Free-text search query")
    p_search.add_argument("--json", action="store_true", help="Output raw JSON")

    p_fn = subparsers.add_parser(
        "function-help",
        help="Look up DQ function syntax from the local reference (no API call)",
    )
    p_fn.add_argument("--name", help="Function name (e.g. VOL, MOVAVG, BETA)")
    p_fn.add_argument(
        "--category",
        help=(
            "Filter by category (e.g. STATISTICAL, MATHEMATICAL, AGGREGATE, CALENDAR, MISCELLANEOUS, 'F&O SPECIFIC')"
        ),
    )
    p_fn.add_argument("--list", action="store_true", help="List all available functions")
    p_fn.add_argument("--json", action="store_true", help="Output raw JSON")

    def _ts_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("--data", choices=["REFERENCE_DATA", "NO_REFERENCE_DATA", "ALL"], default=None)
        p.add_argument("--start-date", help="YYYYMMDD or TODAY-Nx (x=D/W/M/Y). Default: TODAY-1D")
        p.add_argument("--end-date", help="YYYYMMDD or TODAY-Nx. Default: TODAY")
        p.add_argument("--calendar", default=None, help="Default: CAL_USBANK")
        p.add_argument(
            "--frequency",
            choices=["FREQ_INTRA", "FREQ_DAY", "FREQ_WEEK", "FREQ_MONTH", "FREQ_QUARTER", "FREQ_ANN"],
            default=None,
        )
        p.add_argument("--conversion", default=None, help="Default: CONV_LASTBUS_ABS")
        p.add_argument(
            "--nan-treatment",
            choices=["NA_NOTHING", "NA_LAST", "NA_NEXT", "NA_INTERP"],
            default=None,
        )
        p.add_argument("--page", default=None)
        p.add_argument(
            "--output-csv",
            metavar="FILE",
            default=None,
            help="Export time-series results to CSV (use '-' for stdout)",
        )

    p_gsearch = subparsers.add_parser("groups-search", help="Search datasets by keyword")
    p_gsearch.add_argument("--keywords", required=True)
    p_gsearch.add_argument("--page", default=None)

    p_insts = subparsers.add_parser("instruments", help="List instruments for a dataset")
    p_insts.add_argument("--group-id", required=True)
    p_insts.add_argument("--instrument-id", default=None, help="Optional instrument ID filter")
    p_insts.add_argument("--page", default=None)

    p_isrch = subparsers.add_parser("instruments-search", help="Keyword-search instruments within a dataset")
    p_isrch.add_argument("--group-id", required=True)
    p_isrch.add_argument("--keywords", required=True)
    p_isrch.add_argument("--page", default=None)

    p_filt = subparsers.add_parser("filters", help="Get filter dimensions for a dataset")
    p_filt.add_argument("--group-id", required=True)
    p_filt.add_argument("--page", default=None)

    p_attr = subparsers.add_parser("attributes", help="Get analytic attributes for a dataset")
    p_attr.add_argument("--group-id", required=True)
    p_attr.add_argument("--instrument-id", default=None)
    p_attr.add_argument("--page", default=None)

    p_gts = subparsers.add_parser("group-timeseries", help="Bulk time-series for a group")
    p_gts.add_argument("--group-id", required=True)
    p_gts.add_argument("--attributes", required=True, help="Comma-separated attribute IDs (e.g. TR,YTDR)")
    p_gts.add_argument("--filter", default=None, help='Filter string (e.g. "currency(USD)")')
    _ts_args(p_gts)

    p_its = subparsers.add_parser("instrument-timeseries", help="Time-series by instrument IDs")
    p_its.add_argument(
        "--instruments",
        required=True,
        action="append",
        help="Instrument ID (repeat for multiple, max 20)",
    )
    p_its.add_argument("--attributes", required=True, help="Comma-separated attribute IDs")
    _ts_args(p_its)

    p_ets = subparsers.add_parser("expression-timeseries", help="Time-series by DQ expressions")
    p_ets.add_argument(
        "--expressions",
        required=True,
        action="append",
        help="DQ expression (repeat for multiple)",
    )
    _ts_args(p_ets)

    p_grid = subparsers.add_parser("grid-data", help="Grid data by expression or grid ID")
    p_grid.add_argument("--expr", default=None)
    p_grid.add_argument("--grid-id", default=None)
    p_grid.add_argument("--date", default=None)
    p_grid.add_argument(
        "--output-csv",
        metavar="FILE",
        default=None,
        help="Export grid results to CSV (use '-' for stdout)",
    )

    subparsers.add_parser("heartbeat", help="Check if DataQuery is running")

    p_connect = subparsers.add_parser(
        "mcp-connect",
        help="Connect a local stdio MCP client to a remote MCP server (AuthE OAuth)",
        description=(
            "Bridge a desktop MCP client (stdio) to a remote streamable-HTTP MCP\n"
            "server, authenticating with an OAuth client-credentials (AuthE) token\n"
            "minted from the DATAQUERY_* environment. Point your MCP client's\n"
            "`command` at:  dataquery mcp-connect\n"
            "\n"
            "The endpoint defaults to the PROD MCP server; override it with --url\n"
            "or DATAQUERY_MCP_URL to reach a different environment.\n"
            "\n"
            "Credentials passed as flags are exported into the DATAQUERY_*\n"
            "environment of this process; add --save-credentials to also write\n"
            "them to ~/.dataquery/.env, which every later SDK call picks up so\n"
            "no environment has to be set up again."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p_connect.add_argument(
        "--url",
        default=None,
        help="Remote MCP endpoint URL (default: DATAQUERY_MCP_URL, else the PROD MCP endpoint)",
    )
    p_connect.add_argument("--name", default="dataquery-mcp", help="Proxy server name (default: dataquery-mcp)")
    p_connect.add_argument("--client-id", default=None, help="OAuth client ID (exported as DATAQUERY_CLIENT_ID)")
    p_connect.add_argument(
        "--client-secret",
        default=None,
        help="OAuth client secret (exported as DATAQUERY_CLIENT_SECRET); visible in the process list, "
        "so prefer DATAQUERY_CLIENT_SECRET or --env-file on shared machines",
    )
    p_connect.add_argument(
        "--bearer-token",
        default=None,
        help="Bearer token to use instead of OAuth credentials (exported as DATAQUERY_BEARER_TOKEN)",
    )
    p_connect.add_argument(
        "--save-credentials",
        action="store_true",
        help="Also save the resolved credentials to ~/.dataquery/.env (owner-only) so later SDK use "
        "needs no environment variables",
    )

    p_install = subparsers.add_parser(
        "mcp-install",
        help="One-time MCP setup: save your credentials and add the DataQuery server to your MCP app",
        description=(
            "Save your DataQuery credentials to ~/.dataquery/.env (owner-only) and add a\n"
            "server that runs this environment's `dataquery mcp-connect` to your MCP\n"
            "app's user config. No secrets go into that config. Missing credentials are\n"
            "prompted for on a terminal, the secret without echo. Re-run any time to\n"
            "update the credentials or the entry."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    target = p_install.add_mutually_exclusive_group()
    target.add_argument(
        "--app",
        choices=MCP_INSTALL_APPS,
        default="claude-desktop",
        help="MCP app to configure (default: claude-desktop); chatgpt is the ChatGPT desktop app, "
        "whose config the Codex CLI and IDE extension share",
    )
    target.add_argument(
        "--config-file",
        type=Path,
        default=None,
        help="Add the server to this mcpServers JSON file instead, for any other app",
    )
    p_install.add_argument(
        "--name",
        type=_mcp_server_name,
        default="dataquery",
        help="Server name in the MCP config (default: dataquery)",
    )
    p_install.add_argument("--url", default=None, help="MCP endpoint for mcp-connect (default: the PROD endpoint)")
    p_install.add_argument("--client-id", default=None, help="OAuth client ID (prompted for when missing)")
    p_install.add_argument(
        "--client-secret",
        default=None,
        help="OAuth client secret; visible in the process list, so prefer the prompt",
    )
    p_install.add_argument("--bearer-token", default=None, help="Use a bearer token instead of OAuth")

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

    file_group_id = args.file_group_id
    if not args.watch and isinstance(file_group_id, list):
        if len(file_group_id) != 1:
            print("--file-group-id must be a single value for single-file download")
            return 1
        file_group_id = file_group_id[0]

    async with DataQuery(args.env_file) as dq:
        if args.watch:
            destination_dir = args.destination or "./downloads"
            if getattr(args, "reset_event_id", False):
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

        dest_path = Path(args.destination) if args.destination else None

        from dataquery.types.models import DownloadOptions

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
            print(results.model_dump_json(indent=2))
        else:
            successful = results.counts.get("successful_downloads", 0)
            failed = results.counts.get("failed_downloads", 0)
            print(f"Downloaded {successful} files to {args.destination}")
            if failed > 0:
                print(f"Failed: {failed}")
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
    import dataquery.utils as utils

    out = utils.create_env_template(Path(args.output))
    print(f"Template written to {out}")
    return 0


async def cmd_auth_test(args: argparse.Namespace) -> int:
    async with DataQuery(args.env_file) as dq:
        _ = await dq.list_groups_async(limit=1)
    return 0


async def cmd_search(args: argparse.Namespace) -> int:
    async with DataQuery(args.env_file) as dq:
        result = await dq.search_async(args.query)

    if args.json:
        print(json.dumps(result, indent=2))
        return 0

    results: list = []
    if isinstance(result, dict):
        for key in ("results", "groups", "items"):
            value = result.get(key)
            if isinstance(value, list):
                results = value
                break
    summary = f"Search returned {len(results)} result(s) for: {args.query}"
    _print_endpoint_result(summary, result)
    return 0


async def cmd_groups_search(args: argparse.Namespace) -> int:
    async with DataQuery(args.env_file) as dq:
        items = await dq.search_groups_async(args.keywords, page=args.page)
    summary = f"Found {len(items)} dataset(s) matching '{args.keywords}'"
    _print_endpoint_result(summary, {"groups": [_to_dict(g) for g in items]})
    return 0


async def cmd_instruments(args: argparse.Namespace) -> int:
    async with DataQuery(args.env_file) as dq:
        resp = await dq.list_instruments_async(args.group_id, args.instrument_id, args.page)
    data = _to_dict(resp)
    n = len(data.get("instruments", []) or [])
    summary = f"Found {n} instrument(s) in {args.group_id}"
    _print_endpoint_result(summary, resp)
    return 0


async def cmd_instruments_search(args: argparse.Namespace) -> int:
    async with DataQuery(args.env_file) as dq:
        resp = await dq.search_instruments_async(args.group_id, args.keywords, args.page)
    data = _to_dict(resp)
    n = len(data.get("instruments", []) or [])
    summary = f"Found {n} instrument(s) in {args.group_id} matching '{args.keywords}'"
    _print_endpoint_result(summary, resp)
    return 0


async def cmd_filters(args: argparse.Namespace) -> int:
    async with DataQuery(args.env_file) as dq:
        resp = await dq.get_group_filters_async(args.group_id, args.page)
    data = _to_dict(resp)
    n = len(data.get("filters", []) or [])
    summary = f"Found {n} filter dimension(s) for {args.group_id}"
    _print_endpoint_result(summary, resp)
    return 0


async def cmd_attributes(args: argparse.Namespace) -> int:
    async with DataQuery(args.env_file) as dq:
        resp = await dq.get_group_attributes_async(args.group_id, args.instrument_id, args.page)
    data = _to_dict(resp)
    n = len(data.get("instruments", []) or [])
    summary = f"Attributes for {n} instrument(s) in {args.group_id}"
    _print_endpoint_result(summary, resp)
    return 0


def _ts_kwargs(args: argparse.Namespace) -> Dict[str, Any]:
    """Map shared time-series CLI args to SDK keyword arguments, skipping None."""
    kwargs: Dict[str, Any] = {}
    if args.data is not None:
        kwargs["data"] = args.data
    if args.start_date is not None:
        kwargs["start_date"] = args.start_date
    if args.end_date is not None:
        kwargs["end_date"] = args.end_date
    if args.calendar is not None:
        kwargs["calendar"] = args.calendar
    if args.frequency is not None:
        kwargs["frequency"] = args.frequency
    if args.conversion is not None:
        kwargs["conversion"] = args.conversion
    if args.nan_treatment is not None:
        kwargs["nan_treatment"] = args.nan_treatment
    if args.page is not None:
        kwargs["page"] = args.page
    return kwargs


def _timeseries_summary(label: str, resp: Any) -> str:
    data = _to_dict(resp)
    n_inst, n_pts, first, last = _count_timeseries(data)
    date_range = f"{first} to {last}" if first and last else "no dates"
    return f"{label}: {n_inst} instrument(s), {n_pts} data point(s) ({date_range})"


def _maybe_export_csv(resp: Any, output_csv: Optional[str], is_grid: bool = False) -> Optional[Dict[str, Any]]:
    if not output_csv:
        return None
    from .export import export_grid_csv, export_timeseries_csv

    exporter = export_grid_csv if is_grid else export_timeseries_csv
    info = exporter(resp, output_csv)
    if info.get("content"):
        print(info["content"])
    return info


async def cmd_group_timeseries(args: argparse.Namespace) -> int:
    attributes = _split_csv_list(args.attributes) or []
    async with DataQuery(args.env_file) as dq:
        resp = await dq.get_group_time_series_async(
            args.group_id,
            attributes,
            filter=args.filter,
            **_ts_kwargs(args),
        )
    csv_info = _maybe_export_csv(resp, args.output_csv)
    _print_endpoint_result(_timeseries_summary("Group time-series", resp), resp, csv_info=csv_info)
    return 0


async def cmd_instrument_timeseries(args: argparse.Namespace) -> int:
    attributes = _split_csv_list(args.attributes) or []
    async with DataQuery(args.env_file) as dq:
        resp = await dq.get_instrument_time_series_async(
            args.instruments,
            attributes,
            **_ts_kwargs(args),
        )
    csv_info = _maybe_export_csv(resp, args.output_csv)
    _print_endpoint_result(_timeseries_summary("Instrument time-series", resp), resp, csv_info=csv_info)
    return 0


async def cmd_expression_timeseries(args: argparse.Namespace) -> int:
    async with DataQuery(args.env_file) as dq:
        resp = await dq.get_expressions_time_series_async(
            args.expressions,
            **_ts_kwargs(args),
        )
    csv_info = _maybe_export_csv(resp, args.output_csv)
    _print_endpoint_result(_timeseries_summary("Expression time-series", resp), resp, csv_info=csv_info)
    return 0


async def cmd_grid_data(args: argparse.Namespace) -> int:
    if not args.expr and not args.grid_id:
        _print_error(
            "Provide --expr or --grid-id for grid data.",
            suggestion="Example: --expr 'DBGRID(EQTY,2823 HK,ABS_REL,ATMF,CLOSE,VOL)'",
        )
        return 1
    async with DataQuery(args.env_file) as dq:
        resp = await dq.get_grid_data_async(expr=args.expr, grid_id=args.grid_id, date=args.date)
    data = _to_dict(resp)
    series = data.get("series", []) or []
    total_records = sum(len(s.get("records", []) or []) for s in series)
    summary = f"Grid: {len(series)} series, {total_records} record(s)"
    csv_info = _maybe_export_csv(resp, args.output_csv, is_grid=True)
    _print_endpoint_result(summary, resp, csv_info=csv_info)
    return 0


async def cmd_heartbeat(args: argparse.Namespace) -> int:
    async with DataQuery(args.env_file) as dq:
        ok = await dq.health_check_async()
    summary = "DataQuery is UP" if ok else "DataQuery is DOWN"
    _print_endpoint_result(summary, {"status": "ok" if ok else "down"})
    return 0 if ok else 1


def cmd_function_help(args: argparse.Namespace) -> int:
    from .function_registry import (
        format_function_syntax,
        list_functions_by_category,
        lookup_function,
    )

    if not args.name and not args.list and not args.category:
        print("Provide --name NAME, --list, or --category CATEGORY")
        return 1

    if args.name:
        spec = lookup_function(args.name)
        if not spec:
            error_payload: dict = {
                "status": "error",
                "error_description": f"Unknown function: {args.name.upper()}",
                "suggestion": "Use --list to see all available functions.",
            }
            if args.json:
                print(json.dumps(error_payload, indent=2))
            else:
                print(error_payload["error_description"])
                print(error_payload["suggestion"])
            return 1
        payload: dict = {
            "function": spec["name"],
            "syntax": format_function_syntax(args.name),
            "category": spec["category"],
            "description": spec["description"],
            "parameters": spec["params"],
        }
        if args.json:
            print(json.dumps(payload, indent=2))
        else:
            print(f"{payload['syntax']} [{payload['category']}]")
            if payload["description"]:
                print(f"  {payload['description']}")
            if payload["parameters"]:
                print("  Parameters:")
                for p in payload["parameters"]:
                    optional = " (optional)" if p["type"] == "OPTIONAL" else ""
                    varargs = ", ..." if p["kind"] == "PARAMETERLIST" else ""
                    print(f"    - {p['name']}{varargs}{optional}")
        return 0

    funcs = list_functions_by_category(args.category)
    items = [{"name": f["name"], "syntax": format_function_syntax(f["name"]), "category": f["category"]} for f in funcs]
    if args.json:
        print(json.dumps({"functions": items, "count": len(items)}, indent=2))
    else:
        print(f"Available functions: {len(items)}")
        for it in items:
            print(f"{it['syntax']}\t[{it['category']}]")
    return 0


def _mcp_token_storage_dir(config: Any) -> Path:
    """Token cache for ``mcp-connect``, fixed per credential set.

    MCP apps launch the bridge from arbitrary working directories, so the
    SDK default (``<download_dir>/.tokens``, relative) would pick up whatever
    token happens to sit there, possibly one issued for other credentials.
    """
    from dataquery.config import EnvConfig

    fingerprint = "\n".join([config.client_id or "", config.oauth_token_url or "", config.aud or ""])
    return EnvConfig.user_config_dir() / "tokens" / hashlib.sha256(fingerprint.encode()).hexdigest()[:16]


def _authe_auth(token_manager: Any) -> Any:
    """httpx auth stamping an AuthE bearer token per request, retrying once on 401."""
    import httpx

    async def valid_header() -> str:
        header = await token_manager.get_valid_token()
        if not header:
            raise DataQueryError(
                "Could not obtain an OAuth token \u2014 check DATAQUERY_CLIENT_ID, "
                "DATAQUERY_CLIENT_SECRET, DATAQUERY_OAUTH_TOKEN_URL and "
                "DATAQUERY_OAUTH_AUD."
            )
        return header

    class _AutheAuth(httpx.Auth):
        async def async_auth_flow(self, request: httpx.Request) -> AsyncGenerator[httpx.Request, httpx.Response]:
            header = await valid_header()
            request.headers["Authorization"] = header
            response = yield request
            if response.status_code != 401 or token_manager.config.has_bearer_token:
                return
            # The server rejected a token that still looks valid locally
            # (revoked, or cached for other credentials): drop it unless a
            # concurrent request already replaced it, then retry once.
            current = token_manager.current_token
            if current is not None and current.to_authorization_header() == header:
                token_manager.clear_token()
            request.headers["Authorization"] = await valid_header()
            yield request

    return _AutheAuth()


async def cmd_mcp_connect(args: argparse.Namespace) -> int:
    """Bridge a desktop MCP client (stdio) to a remote streamable-HTTP MCP server."""
    try:
        from fastmcp import FastMCP
        from fastmcp.client.transports import StreamableHttpTransport
    except ImportError:
        print(
            "The MCP bridge requires the 'mcp' extra. Install it with:\n"
            "    pip install 'dataquery-sdk[mcp]'\n"
            "or run it directly with:\n"
            "    uvx --from 'dataquery-sdk[mcp]' dataquery mcp-connect",
            file=sys.stderr,
        )
        return 1

    from dataquery.config import EnvConfig
    from dataquery.transport.auth import TokenManager

    if getattr(args, "env_file", None):
        EnvConfig.load_env_file(Path(args.env_file))
    # Load saved credentials before exporting, so they too land in the process
    # env and a re-launch with --save-credentials rewrites them instead of
    # reporting nothing to save. Loaded last, so it never wins over the above.
    EnvConfig.load_user_env_file()
    savable = _export_mcp_credentials(args)
    if getattr(args, "save_credentials", False):
        _save_mcp_credentials(savable)

    config = EnvConfig.create_client_config()
    # --url wins; otherwise DATAQUERY_MCP_URL, and failing that the model
    # default (PROD). Only an explicitly emptied env var leaves it unset.
    url = getattr(args, "url", None) or config.mcp_url
    if not url:
        print(
            "No MCP endpoint configured: pass --url or set DATAQUERY_MCP_URL.",
            file=sys.stderr,
        )
        return 1
    # An explicitly configured, enabled token store wins; the env default
    # (".tokens") is relative to the launch directory, so it does not count.
    if not (config.token_storage_enabled and os.environ.get(f"{EnvConfig.PREFIX}TOKEN_STORAGE_DIR")):
        config = config.model_copy(
            update={"token_storage_dir": str(_mcp_token_storage_dir(config)), "token_storage_enabled": True}
        )
    token_manager = TokenManager(config)

    transport = StreamableHttpTransport(url, auth=_authe_auth(token_manager))
    proxy = FastMCP.as_proxy(transport, name=args.name)
    await proxy.run_async(transport="stdio", show_banner=False)
    return 0


def cmd_mcp_install(args: argparse.Namespace) -> int:
    """Save the credentials once and add the DataQuery MCP server to an MCP app's user config."""
    from dataquery import mcp_install
    from dataquery.config import EnvConfig
    from dataquery.types.exceptions import ConfigurationError

    if not _mcp_extra_installed():
        print(
            "The MCP bridge needs the 'mcp' extra. Install it, then re-run:\n    pip install 'dataquery-sdk[mcp]'",
            file=sys.stderr,
        )
        return 1

    if getattr(args, "env_file", None):
        EnvConfig.load_env_file(Path(args.env_file))
    # Already-saved credentials count, so re-running only refreshes the entry.
    EnvConfig.load_user_env_file()
    keys = _export_mcp_credentials(args)
    if not _has_mcp_credentials(keys):
        _prompt_for_mcp_credentials()
        keys = _export_mcp_credentials(args)
    if not _has_mcp_credentials(keys):
        print(
            "No credentials: pass --client-id and --client-secret (or --bearer-token), set "
            "DATAQUERY_CLIENT_ID and DATAQUERY_CLIENT_SECRET, or run in a terminal to be prompted.",
            file=sys.stderr,
        )
        return 1

    entry = mcp_install.server_entry(args.url)
    try:
        # Saved first: an entry without credentials behind it would fail on every launch.
        env_file = EnvConfig.save_user_env({key: os.environ.get(f"{EnvConfig.PREFIX}{key}") for key in keys})
        print(f"Saved {', '.join(f'{EnvConfig.PREFIX}{key}' for key in keys)} to {env_file} (owner-only)")
        if args.config_file is not None:
            replaced = mcp_install.add_to_config_file(args.config_file, args.name, entry)
            print(f"{'Updated' if replaced else 'Added'} '{args.name}' in {args.config_file}")
            print("Restart your MCP app to load it.")
        elif args.app == "claude-code":
            mcp_install.add_to_claude_code(args.name, entry)
            print(f"Added '{args.name}' to Claude Code (user scope); check it with: claude mcp list")
        elif args.app == "chatgpt":
            path, replaced = mcp_install.add_to_codex(args.name, entry)
            print(f"{'Updated' if replaced else 'Added'} '{args.name}' in {path}")
            print("Restart the ChatGPT desktop app (or Codex) to load it; ChatGPT on the web can't run local servers.")
        else:
            app = mcp_install.JSON_APPS[args.app]
            path, replaced = app.add(args.name, entry)
            print(f"{'Updated' if replaced else 'Added'} '{args.name}' in {path}")
            print(f"Restart {app.label} to load it.")
    except (ConfigurationError, OSError) as exc:
        print(f"mcp-install: {exc}", file=sys.stderr)
        return 1
    print("The server runs:", " ".join([entry["command"], *entry["args"]]))
    print("No secrets were written to the MCP config; mcp-connect reads them from the saved file.")
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


_ASYNC_COMMANDS = {
    "groups": cmd_groups,
    "files": cmd_files,
    "availability": cmd_availability,
    "download": cmd_download,
    "download-group": cmd_download_group,
    "search": cmd_search,
    "groups-search": cmd_groups_search,
    "instruments": cmd_instruments,
    "instruments-search": cmd_instruments_search,
    "filters": cmd_filters,
    "attributes": cmd_attributes,
    "group-timeseries": cmd_group_timeseries,
    "instrument-timeseries": cmd_instrument_timeseries,
    "expression-timeseries": cmd_expression_timeseries,
    "grid-data": cmd_grid_data,
    "heartbeat": cmd_heartbeat,
    "mcp-connect": cmd_mcp_connect,
}


def main() -> int:
    parser = create_parser()
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return 1
    if args.command == "config":
        return main_sync(args)
    if args.command == "auth" and args.auth_command == "test":
        return asyncio.run(cmd_auth_test(args))
    if args.command == "function-help":
        return cmd_function_help(args)
    if args.command == "mcp-install":
        return cmd_mcp_install(args)

    handler = _ASYNC_COMMANDS.get(args.command)
    if handler is None:
        parser.print_help()
        return 1
    try:
        return asyncio.run(handler(args))
    except DataQueryError as exc:
        _print_error(str(exc), suggestion=getattr(exc, "suggestion", None))
        return 1


if __name__ == "__main__":
    sys.exit(main())
