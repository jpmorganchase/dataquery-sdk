import argparse
import json
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dataquery import cli


def _parser():
    return cli.create_parser()


def test_cli_no_command_prints_help(capsys):
    parser = _parser()
    # Directly call main to hit the no-command branch
    with patch.object(cli, "create_parser", return_value=parser):
        with patch("sys.argv", ["dataquery"]):
            rc = cli.main()
    captured = capsys.readouterr()
    assert rc == 1
    assert "Command Line Interface" in captured.out or "Available commands" in captured.out


@pytest.mark.asyncio
async def test_cli_groups_json(monkeypatch, capsys):
    parser = _parser()
    args = parser.parse_args(["groups", "--json", "--limit", "1"])  # type: ignore[arg-type]

    fake_group = MagicMock()
    fake_group.model_dump = lambda: {"group_id": "G1", "group_name": "g"}

    fake_dq_ctx = MagicMock()
    fake_dq = MagicMock()
    fake_dq.__aenter__ = AsyncMock(return_value=fake_dq)
    fake_dq.__aexit__ = AsyncMock(return_value=None)
    fake_dq.list_groups_async = AsyncMock(return_value=[fake_group])
    fake_dq.search_groups_async = AsyncMock(return_value=[fake_group])

    monkeypatch.setattr(cli, "DataQuery", MagicMock(return_value=fake_dq))

    rc = await cli.cmd_groups(args)
    out = capsys.readouterr().out
    assert rc == 0
    assert "G1" in out


@pytest.mark.asyncio
async def test_cli_files_text(monkeypatch, capsys):
    parser = _parser()
    args = parser.parse_args(["files", "--group-id", "G", "--limit", "1"])  # type: ignore[arg-type]

    fake_file = MagicMock()
    fake_file.file_type = "csv"
    fake_file.description = "d"
    fake_file.model_dump = lambda: {"file_type": "csv"}

    fake_dq = MagicMock()
    fake_dq.__aenter__ = AsyncMock(return_value=fake_dq)
    fake_dq.__aexit__ = AsyncMock(return_value=None)
    fake_dq.list_files_async = AsyncMock(return_value=[fake_file])
    monkeypatch.setattr(cli, "DataQuery", MagicMock(return_value=fake_dq))

    rc = await cli.cmd_files(args)
    out = capsys.readouterr().out
    assert rc == 0
    assert "Found 1 files" in out


@pytest.mark.asyncio
async def test_cli_availability_json(monkeypatch, capsys):
    parser = _parser()
    args = parser.parse_args(["availability", "--file-group-id", "FG", "--file-datetime", "20240101", "--json"])  # type: ignore[arg-type]

    fake_avail = MagicMock()
    fake_avail.model_dump = lambda: {"file_group_id": "FG", "availability_rate": 100.0}

    fake_dq = MagicMock()
    fake_dq.__aenter__ = AsyncMock(return_value=fake_dq)
    fake_dq.__aexit__ = AsyncMock(return_value=None)
    fake_dq.check_availability_async = AsyncMock(return_value=fake_avail)
    monkeypatch.setattr(cli, "DataQuery", MagicMock(return_value=fake_dq))

    rc = await cli.cmd_availability(args)
    out = capsys.readouterr().out
    assert rc == 0
    assert "FG" in out


@pytest.mark.asyncio
async def test_cli_download_missing_group_id_in_watch(monkeypatch, capsys):
    parser = _parser()
    args = parser.parse_args(["download", "--watch"])  # type: ignore[arg-type]

    fake_dq = MagicMock()
    fake_dq.__aenter__ = AsyncMock(return_value=fake_dq)
    fake_dq.__aexit__ = AsyncMock(return_value=None)
    monkeypatch.setattr(cli, "DataQuery", MagicMock(return_value=fake_dq))

    rc = await cli.cmd_download(args)
    assert rc == 1
    assert "required when using --watch" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_cli_download_single_json(monkeypatch, tmp_path, capsys):
    parser = _parser()
    dest = tmp_path / "out"
    args = parser.parse_args(
        ["download", "--file-group-id", "FG", "--file-datetime", "20240101", "--destination", str(dest), "--json"]
    )  # type: ignore[arg-type]

    fake_result = MagicMock()
    fake_result.model_dump = lambda: {"status": "completed", "local_path": str(dest)}
    fake_result.status.value = "completed"

    fake_dq = MagicMock()
    fake_dq.__aenter__ = AsyncMock(return_value=fake_dq)
    fake_dq.__aexit__ = AsyncMock(return_value=None)
    fake_dq.download_file_async = AsyncMock(return_value=fake_result)
    monkeypatch.setattr(cli, "DataQuery", MagicMock(return_value=fake_dq))

    rc = await cli.cmd_download(args)
    out = capsys.readouterr().out
    assert rc == 0
    # Parse JSON output and compare paths properly
    json_output = json.loads(out)
    assert json_output["local_path"] == str(dest)


def test_cli_config_show_and_validate(monkeypatch, capsys, tmp_path):
    parser = _parser()
    # Global options must precede subcommands in argparse
    args_show = parser.parse_args(["--env-file", str(tmp_path / ".env"), "config", "show"])  # type: ignore[arg-type]
    args_validate = parser.parse_args(["config", "validate"])  # type: ignore[arg-type]
    args_template = parser.parse_args(["config", "template", "--output", str(tmp_path / "tmpl.env")])  # type: ignore[arg-type]

    monkeypatch.setattr("dataquery.config.EnvConfig.create_client_config", MagicMock())
    monkeypatch.setattr("dataquery.config.EnvConfig.validate_config", MagicMock())
    # create_env_template is imported inside the function, so patch the real provider
    monkeypatch.setattr(
        "dataquery.utils.create_env_template",
        MagicMock(return_value=tmp_path / "tmpl.env"),
    )

    assert cli.cmd_config_show(args_show) == 0
    assert cli.cmd_config_validate(args_validate) == 0
    assert cli.cmd_config_template(args_template) == 0


@pytest.mark.asyncio
async def test_cli_auth_test_success(monkeypatch, capsys):
    parser = _parser()
    args = parser.parse_args(["auth", "test"])  # type: ignore[arg-type]

    fake_dq = MagicMock()
    fake_dq.__aenter__ = AsyncMock(return_value=fake_dq)
    fake_dq.__aexit__ = AsyncMock(return_value=None)
    fake_dq.list_groups_async = AsyncMock(return_value=[object()])
    monkeypatch.setattr(cli, "DataQuery", MagicMock(return_value=fake_dq))

    rc = await cli.cmd_auth_test(args)
    assert rc == 0


def test_cli_main_sync_config_unknown_command(capsys):
    # Build a fake args namespace for main_sync
    ns = argparse.Namespace(command="config", config_command="unknown")
    rc = cli.main_sync(ns)
    assert rc == 1


@pytest.mark.asyncio
async def test_cli_download_watch_quick_exit(monkeypatch, capsys):
    parser = _parser()
    args = parser.parse_args(["download", "--watch", "--group-id", "G"])  # type: ignore[arg-type]

    class _Mgr:
        async def stop(self):
            return None

        def get_stats(self):
            return {"files_downloaded": 0, "download_failures": 0}

    fake_dq = MagicMock()
    fake_dq.__aenter__ = AsyncMock(return_value=fake_dq)
    fake_dq.__aexit__ = AsyncMock(return_value=None)
    fake_dq.auto_download_async = AsyncMock(return_value=_Mgr())
    monkeypatch.setattr(cli, "DataQuery", MagicMock(return_value=fake_dq))

    async def boom(_):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli.asyncio, "sleep", boom)

    rc = await cli.cmd_download(args)
    assert rc == 0


def _clear_dataquery_env():
    for key in [k for k in os.environ if k.startswith("DATAQUERY_")]:
        del os.environ[key]


@pytest.fixture
def mcp_env(tmp_path):
    """Clean DATAQUERY_* environment with the saved-credential file redirected.

    Plain ``os.environ`` rather than monkeypatch: the code under test exports
    credentials directly, and ``monkeypatch.delenv`` would record and then
    restore those on undo, leaking them into later tests.
    """
    saved = {k: v for k, v in os.environ.items() if k.startswith("DATAQUERY_")}
    _clear_dataquery_env()
    config_dir = tmp_path / "dataquery-home"
    os.environ["DATAQUERY_CONFIG_DIR"] = str(config_dir)
    yield config_dir / ".env"
    _clear_dataquery_env()
    os.environ.update(saved)


def _mcp_args(*extra):
    return _parser().parse_args(["mcp-connect", "--url", "https://mcp.example.com/mcp", *extra])


def test_mcp_connect_flags_export_to_environment(mcp_env):
    """Credential flags land in the process env, so later SDK use just works."""
    savable = cli._export_mcp_credentials(_mcp_args("--client-id", "cid", "--client-secret", "sec"))

    assert os.environ["DATAQUERY_CLIENT_ID"] == "cid"
    assert os.environ["DATAQUERY_CLIENT_SECRET"] == "sec"
    assert savable == ["CLIENT_ID", "CLIENT_SECRET"]


def test_mcp_connect_flags_override_environment(mcp_env):
    """An explicit flag beats whatever the MCP client exported."""
    os.environ["DATAQUERY_CLIENT_ID"] = "from-env"

    cli._export_mcp_credentials(_mcp_args("--client-id", "from-flag"))

    assert os.environ["DATAQUERY_CLIENT_ID"] == "from-flag"


def test_mcp_connect_savable_keys_exclude_model_defaults(mcp_env):
    """Only values actually set are savable — not ClientConfig's own defaults."""
    os.environ["DATAQUERY_OAUTH_AUD"] = "JPMC:URI:UAT"

    savable = cli._export_mcp_credentials(_mcp_args("--bearer-token", "tok"))

    assert savable == ["BEARER_TOKEN", "OAUTH_AUD"]
    assert "BASE_URL" not in savable
    assert "OAUTH_TOKEN_URL" not in savable


@pytest.mark.asyncio
async def test_mcp_connect_saves_credentials(mcp_env, capsys):
    """--save-credentials persists the credentials and still starts the proxy."""
    pytest.importorskip("fastmcp")
    args = _mcp_args("--client-id", "cid", "--client-secret", "sec/ret==", "--save-credentials")

    fake_proxy = MagicMock()
    fake_proxy.run_async = AsyncMock(return_value=None)
    with patch("fastmcp.FastMCP.as_proxy", return_value=fake_proxy) as as_proxy:
        rc = await cli.cmd_mcp_connect(args)

    assert rc == 0
    assert as_proxy.called
    fake_proxy.run_async.assert_awaited_once()

    content = mcp_env.read_text()
    assert "DATAQUERY_CLIENT_ID=cid" in content
    assert "sec/ret==" in content
    assert mcp_env.stat().st_mode & 0o077 == 0
    # stdout is the JSON-RPC channel: the notice must go to stderr only.
    captured = capsys.readouterr()
    assert captured.out == ""
    assert str(mcp_env) in captured.err
    assert "sec/ret==" not in captured.err


@pytest.mark.asyncio
async def test_mcp_connect_saves_credentials_from_environment(mcp_env):
    """MCP and the SDK share one credential set: env-supplied vars save too."""
    pytest.importorskip("fastmcp")
    os.environ["DATAQUERY_CLIENT_ID"] = "env-cid"
    os.environ["DATAQUERY_CLIENT_SECRET"] = "env-sec"
    args = _mcp_args("--save-credentials")

    fake_proxy = MagicMock()
    fake_proxy.run_async = AsyncMock(return_value=None)
    with patch("fastmcp.FastMCP.as_proxy", return_value=fake_proxy):
        rc = await cli.cmd_mcp_connect(args)

    assert rc == 0
    content = mcp_env.read_text()
    assert "DATAQUERY_CLIENT_ID=env-cid" in content
    assert "DATAQUERY_CLIENT_SECRET=env-sec" in content


@pytest.mark.asyncio
async def test_mcp_connect_relaunch_reuses_saved_credentials(mcp_env, capsys):
    """Re-launching with the flag but no env rewrites the file, without complaint."""
    pytest.importorskip("fastmcp")
    from dataquery.config import EnvConfig

    EnvConfig.save_user_env({"CLIENT_ID": "saved-cid", "CLIENT_SECRET": "saved-sec"})
    args = _mcp_args("--save-credentials")

    fake_proxy = MagicMock()
    fake_proxy.run_async = AsyncMock(return_value=None)
    with patch("fastmcp.FastMCP.as_proxy", return_value=fake_proxy):
        rc = await cli.cmd_mcp_connect(args)

    assert rc == 0
    assert "nothing to save" not in capsys.readouterr().err
    # Saved credentials are exported, so in-process SDK use needs no env either.
    assert os.environ["DATAQUERY_CLIENT_ID"] == "saved-cid"
    assert "DATAQUERY_CLIENT_SECRET=saved-sec" in mcp_env.read_text()


@pytest.mark.asyncio
async def test_mcp_connect_without_save_flag_writes_nothing(mcp_env):
    """Credentials only reach disk when explicitly asked for."""
    pytest.importorskip("fastmcp")
    args = _mcp_args("--client-id", "cid", "--client-secret", "sec")

    fake_proxy = MagicMock()
    fake_proxy.run_async = AsyncMock(return_value=None)
    with patch("fastmcp.FastMCP.as_proxy", return_value=fake_proxy):
        rc = await cli.cmd_mcp_connect(args)

    assert rc == 0
    assert not mcp_env.exists()


def test_save_mcp_credentials_without_credentials_warns(mcp_env, capsys):
    """Nothing to save is reported, not written as an empty file."""
    cli._save_mcp_credentials([])

    assert not mcp_env.exists()
    assert "nothing to save" in capsys.readouterr().err


def test_save_mcp_credentials_survives_write_failure(mcp_env, capsys):
    """A save failure is reported but never breaks the MCP connection."""
    with patch("dataquery.config.env.EnvConfig.save_user_env", side_effect=OSError("read-only fs")):
        cli._save_mcp_credentials(["CLIENT_ID"])

    err = capsys.readouterr().err
    assert "could not write credentials" in err
    assert "read-only fs" in err
