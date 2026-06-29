"""CSV export for DataQuery time-series and grid-data responses.

Flattens API response payloads (Pydantic model dumps or raw dicts) into
tabular CSV rows. Used by the CLI's ``--output-csv`` flag.
"""

from __future__ import annotations

import csv
import io
from typing import Any, Dict, List

from .types.exceptions import DataQueryError


def _to_dict(payload: Any) -> Dict[str, Any]:
    """Normalize a Pydantic response or raw dict into a plain dict."""
    if hasattr(payload, "model_dump"):
        return payload.model_dump(by_alias=True)
    if isinstance(payload, dict):
        return payload
    raise DataQueryError(
        "Cannot export: response payload is not a dict or Pydantic model.",
    )


def export_timeseries_csv(response: Any, output_path: str) -> Dict[str, Any]:
    """Flatten a time-series response into a CSV file (or stdout when path is ``-``).

    Returns ``{"path": <file>, "rows": <int>}``; when ``output_path == "-"``,
    also includes ``"content"`` with the CSV text.
    """
    data = _to_dict(response)
    instruments = data.get("instruments") or []
    if not instruments:
        raise DataQueryError(
            "No instruments found in the response to export.",
        )

    rows: List[Dict[str, Any]] = []
    for inst in instruments:
        inst_id = inst.get("instrument-id") or inst.get("instrument_id", "")
        inst_name = inst.get("instrument-name") or inst.get("instrument_name", "")
        group = inst.get("group") or {}
        group_id = group.get("group-id") or group.get("group_id", "")
        group_name = group.get("group-name") or group.get("group_name", "")
        for attr in inst.get("attributes", []) or []:
            attr_id = attr.get("attribute-id") or attr.get("attribute_id", "")
            attr_name = attr.get("attribute-name") or attr.get("attribute_name", "")
            expression = attr.get("expression", "")
            label = attr.get("label", "")
            last_pub = attr.get("last-published") or attr.get("last_published", "")
            ts = attr.get("time-series") or attr.get("time_series") or []
            for point in ts:
                if isinstance(point, list) and len(point) >= 2:
                    rows.append(
                        {
                            "date": point[0],
                            "value": point[1],
                            "instrument_id": inst_id,
                            "instrument_name": inst_name,
                            "attribute_id": attr_id,
                            "attribute_name": attr_name,
                            "expression": expression,
                            "label": label,
                            "last_published": last_pub,
                            "group_id": group_id,
                            "group_name": group_name,
                        }
                    )

    if not rows:
        raise DataQueryError(
            "Response contains instruments but no time-series data points.",
        )

    fieldnames = [
        "date",
        "value",
        "instrument_id",
        "instrument_name",
        "attribute_id",
        "attribute_name",
        "expression",
        "label",
        "last_published",
        "group_id",
        "group_name",
    ]

    if output_path == "-":
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        return {"path": "stdout", "rows": len(rows), "content": buf.getvalue()}

    try:
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    except OSError as exc:
        raise DataQueryError(f"Failed to write CSV to {output_path}: {exc}") from exc

    return {"path": output_path, "rows": len(rows)}


def export_grid_csv(response: Any, output_path: str) -> Dict[str, Any]:
    """Flatten a grid-data response into a CSV file (or stdout when path is ``-``)."""
    data = _to_dict(response)
    series = data.get("series") or []
    if not series:
        raise DataQueryError(
            "No grid series found in the response to export.",
        )

    all_rows: List[Dict[str, Any]] = []
    for s in series:
        expr = s.get("expr") or s.get("expression", "")
        for record in s.get("records", []) or []:
            if isinstance(record, dict):
                row: Dict[str, Any] = {"expression": expr}
                row.update(record)
                all_rows.append(row)

    if not all_rows:
        raise DataQueryError(
            "Grid series found but no records to export.",
        )

    seen: Dict[str, None] = {}
    for row in all_rows:
        for key in row:
            seen.setdefault(key, None)
    fieldnames = list(seen)

    if output_path == "-":
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)
        return {"path": "stdout", "rows": len(all_rows), "content": buf.getvalue()}

    try:
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_rows)
    except OSError as exc:
        raise DataQueryError(f"Failed to write CSV to {output_path}: {exc}") from exc

    return {"path": output_path, "rows": len(all_rows)}
