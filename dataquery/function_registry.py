"""
Local DQ function registry (reference lookup, no API call).

Loads ``data/function.json`` and exposes helpers to look up DataQuery
function syntax, parameters, and categories without contacting the API.
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Static, frozen dataset of the 158 DataQuery functions. Derived from the
# glossary in skills/dataquery/references/functions.md; edit that file and
# regenerate by hand if the function list ever changes.
_FUNCTION_JSON = Path(__file__).resolve().parent / "data" / "function.json"
_function_registry: Optional[Dict[str, Dict[str, Any]]] = None


def _parse_function_params(func: Dict[str, Any]) -> List[Dict[str, str]]:
    """Extract parameter list from a function spec's functionExpressionPairs."""
    pairs = func.get("functionExpression", {}).get("functionExpressionPairs", [])

    real_params: List[Dict[str, str]] = []
    string_display: Optional[str] = None

    for p in pairs:
        key = p.get("key", "")
        value = p.get("value", {})
        if key == "STRING":
            string_display = value.get("name", "")
            continue
        if key in ("PARAMETER", "PARAMETERLIST"):
            real_params.append({
                "name": value.get("name", "?"),
                "type": value.get("type", "REQUIRED"),
                "kind": key,
            })

    if real_params:
        return real_params

    if string_display:
        match = re.match(r"\w+\(([^)]*)\)", string_display)
        if match and match.group(1).strip():
            for name in match.group(1).split(","):
                name = name.strip()
                if name:
                    is_opt = name.startswith("[") and name.endswith("]")
                    clean_name = name.strip("[] ")
                    real_params.append({
                        "name": clean_name,
                        "type": "OPTIONAL" if is_opt else "REQUIRED",
                        "kind": "PARAMETER",
                    })

    return real_params


def _load_function_registry() -> Dict[str, Dict[str, Any]]:
    """Load function.json and build a name -> spec lookup table."""
    with open(_FUNCTION_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    registry: Dict[str, Dict[str, Any]] = {}
    for func in data.get("functions", []):
        name = func.get("name", "").upper()
        if not name:
            continue

        desc = func.get("description", "")
        desc = re.sub(r"<[^>]+>", " ", desc).strip()
        desc = re.sub(r"\s+", " ", desc)

        registry[name] = {
            "name": name,
            "category": func.get("category", ""),
            "description": desc,
            "params": _parse_function_params(func),
        }

    return registry


def get_function_registry() -> Dict[str, Dict[str, Any]]:
    """Return the singleton DQ function registry."""
    global _function_registry
    if _function_registry is None:
        _function_registry = _load_function_registry()
    return _function_registry


def lookup_function(name: str) -> Optional[Dict[str, Any]]:
    """Look up a DQ function spec by name (case-insensitive)."""
    return get_function_registry().get(name.upper())


def format_function_syntax(name: str) -> Optional[str]:
    """Return a formatted syntax string like 'VOL(NDays, Expr)'."""
    spec = lookup_function(name)
    if not spec:
        return None

    parts: List[str] = []
    for p in spec["params"]:
        label = p["name"]
        if p["type"] == "OPTIONAL":
            label = f"[{label}]"
        if p["kind"] == "PARAMETERLIST":
            label += ",..."
        parts.append(label)

    return f"{spec['name']}({', '.join(parts)})"


def list_functions_by_category(category: Optional[str] = None) -> List[Dict[str, Any]]:
    """List all DQ functions, optionally filtered by category."""
    reg = get_function_registry()
    funcs = sorted(reg.values(), key=lambda f: f["name"])
    if category:
        cat_upper = category.upper()
        funcs = [f for f in funcs if f["category"].upper() == cat_upper]
    return funcs


def get_function_categories() -> List[str]:
    """Return a sorted list of distinct DQ function categories."""
    reg = get_function_registry()
    return sorted({f["category"] for f in reg.values() if f["category"]})


def get_function_param_counts(name: str) -> Optional[Tuple[int, int]]:
    """Return (min_params, max_params) for a function. ``-1`` max means variadic."""
    spec = lookup_function(name)
    if not spec:
        return None

    required = sum(1 for p in spec["params"] if p["type"] == "REQUIRED")
    has_varargs = any(p["kind"] == "PARAMETERLIST" for p in spec["params"])
    total = len(spec["params"])

    if has_varargs:
        return (required, -1)
    return (required, total)
