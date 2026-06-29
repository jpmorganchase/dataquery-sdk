"""Command Line Interface for DataQuery SDK."""

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from dataquery import DataQuery
from dataquery.types.exceptions import DataQueryError

# ── Output helpers (legacy-CLI "summary + --- JSON ---" format) ────────────


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
        "text-search",
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

    # ── DataQuery API v2 endpoints (skill-facing surface) ────────────────

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


async def cmd_text_search(args: argparse.Namespace) -> int:
    async with DataQuery(args.env_file) as dq:
        result = await dq.text_search_async(args.query)

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


# ── DataQuery API v2 command handlers ────────────────────────────────────


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
    "text-search": cmd_text_search,
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
