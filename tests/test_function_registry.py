"""Tests for the local DQ function registry and the function-help CLI command."""

import argparse
import json

import pytest

from dataquery import cli
from dataquery import function_registry as fr


def test_registry_loads_and_is_cached():
    reg1 = fr.get_function_registry()
    reg2 = fr.get_function_registry()
    assert reg1 is reg2  # singleton
    assert len(reg1) > 0
    # Names are upper-cased keys
    assert all(k == k.upper() for k in reg1)
    assert "VOL" in reg1


def test_lookup_function_case_insensitive():
    assert fr.lookup_function("vol") is fr.lookup_function("VOL")
    spec = fr.lookup_function("BETA")
    assert spec is not None
    assert spec["name"] == "BETA"
    assert spec["category"]
    assert isinstance(spec["params"], list)


def test_lookup_function_unknown_returns_none():
    assert fr.lookup_function("NOT_A_REAL_FUNCTION") is None


def test_format_function_syntax():
    syntax = fr.format_function_syntax("VOL")
    assert syntax is not None
    assert syntax.startswith("VOL(")
    assert syntax.endswith(")")
    assert fr.format_function_syntax("NOPE") is None


def test_format_function_syntax_marks_optional_and_varargs():
    spec = fr.lookup_function("ADJRSQR")
    assert spec is not None
    syntax = fr.format_function_syntax("ADJRSQR")
    assert syntax is not None
    if any(p["kind"] == "PARAMETERLIST" for p in spec["params"]):
        assert ",..." in syntax
    if any(p["type"] == "OPTIONAL" for p in spec["params"]):
        assert "[" in syntax and "]" in syntax


def test_list_functions_by_category():
    everything = fr.list_functions_by_category()
    assert len(everything) == len(fr.get_function_registry())
    # Sorted by name
    names = [f["name"] for f in everything]
    assert names == sorted(names)

    categories = fr.get_function_categories()
    assert categories
    first_cat = categories[0]
    filtered = fr.list_functions_by_category(first_cat)
    assert filtered
    assert all(f["category"].upper() == first_cat.upper() for f in filtered)


def test_list_functions_by_category_is_case_insensitive():
    cats = fr.get_function_categories()
    cat = cats[0]
    assert fr.list_functions_by_category(cat.lower()) == fr.list_functions_by_category(cat.upper())


def test_get_function_param_counts():
    assert fr.get_function_param_counts("UNKNOWN_FN") is None
    counts = fr.get_function_param_counts("VOL")
    assert counts is not None
    min_p, max_p = counts
    assert min_p >= 0
    # max is -1 (variadic) or >= min
    assert max_p == -1 or max_p >= min_p


# ── CLI: function-help ────────────────────────────────────────────────────


def _help_args(**kw) -> argparse.Namespace:
    base = {
        "command": "function-help",
        "name": None,
        "category": None,
        "list": False,
        "json": False,
    }
    base.update(kw)
    return argparse.Namespace(**base)


def test_cli_function_help_requires_an_argument(capsys):
    rc = cli.cmd_function_help(_help_args())
    out = capsys.readouterr().out
    assert rc == 1
    assert "--name" in out


def test_cli_function_help_by_name_json(capsys):
    rc = cli.cmd_function_help(_help_args(name="VOL", json=True))
    out = capsys.readouterr().out
    assert rc == 0
    payload = json.loads(out)
    assert payload["function"] == "VOL"
    assert payload["syntax"].startswith("VOL(")
    assert "parameters" in payload


def test_cli_function_help_unknown_name(capsys):
    rc = cli.cmd_function_help(_help_args(name="NOPE", json=True))
    out = capsys.readouterr().out
    assert rc == 1
    payload = json.loads(out)
    assert payload["status"] == "error"


def test_cli_function_help_list_json(capsys):
    rc = cli.cmd_function_help(_help_args(list=True, json=True))
    out = capsys.readouterr().out
    assert rc == 0
    payload = json.loads(out)
    assert payload["count"] == len(payload["functions"]) > 0


def test_cli_function_help_by_category_text(capsys):
    category = fr.get_function_categories()[0]
    rc = cli.cmd_function_help(_help_args(category=category))
    out = capsys.readouterr().out
    assert rc == 0
    assert "Available functions:" in out
