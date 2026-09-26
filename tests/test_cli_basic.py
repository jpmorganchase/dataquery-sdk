import argparse
import json
import os
import tomllib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dataquery import cli, mcp_install


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


async def _connect_and_capture_url(args):
    """Run cmd_mcp_connect against a stub proxy; return (rc, endpoint URL)."""
    fake_proxy = MagicMock()
    fake_proxy.run_async = AsyncMock(return_value=None)
    with patch("fastmcp.FastMCP.as_proxy", return_value=fake_proxy):
        with patch("fastmcp.client.transports.StreamableHttpTransport") as transport:
            rc = await cli.cmd_mcp_connect(args)
    return rc, transport.call_args.args[0] if transport.call_args else None


def test_mcp_connect_url_is_optional():
    """--url may be omitted; the endpoint then comes from config."""
    args = _parser().parse_args(["mcp-connect"])

    assert args.url is None


@pytest.mark.asyncio
async def test_mcp_connect_defaults_to_prod_endpoint(mcp_env):
    """No --url and no env var: the PROD MCP endpoint from ClientConfig is used."""
    pytest.importorskip("fastmcp")

    rc, url = await _connect_and_capture_url(_parser().parse_args(["mcp-connect"]))

    assert rc == 0
    assert url == "https://api-dataquery.jpmchase.com/research/dataquery-authe/v2/mcp"


@pytest.mark.asyncio
async def test_mcp_connect_url_env_var_overrides_default(mcp_env):
    """DATAQUERY_MCP_URL points the bridge at another environment."""
    pytest.importorskip("fastmcp")
    os.environ["DATAQUERY_MCP_URL"] = "https://mcp-uat.example.com/mcp"

    rc, url = await _connect_and_capture_url(_parser().parse_args(["mcp-connect"]))

    assert rc == 0
    assert url == "https://mcp-uat.example.com/mcp"


@pytest.mark.asyncio
async def test_mcp_connect_url_flag_wins_over_env_var(mcp_env):
    """An explicit --url beats DATAQUERY_MCP_URL."""
    pytest.importorskip("fastmcp")
    os.environ["DATAQUERY_MCP_URL"] = "https://mcp-uat.example.com/mcp"

    rc, url = await _connect_and_capture_url(_mcp_args())

    assert rc == 0
    assert url == "https://mcp.example.com/mcp"


@pytest.mark.asyncio
async def test_mcp_connect_without_any_endpoint_fails(mcp_env, capsys):
    """An explicitly emptied DATAQUERY_MCP_URL is an error, not a silent PROD connect."""
    pytest.importorskip("fastmcp")
    os.environ["DATAQUERY_MCP_URL"] = ""

    rc, url = await _connect_and_capture_url(_parser().parse_args(["mcp-connect"]))

    assert rc == 1
    assert url is None
    assert "No MCP endpoint configured" in capsys.readouterr().err


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


# ---------------------------------------------------------------------------
# mcp-install
# ---------------------------------------------------------------------------


@pytest.fixture
def install_env(mcp_env, monkeypatch):
    """mcp_env, with the mcp extra reported present and stdin not a terminal."""
    monkeypatch.setattr(cli, "_mcp_extra_installed", lambda: True)
    monkeypatch.setattr(cli.sys, "stdin", SimpleNamespace(isatty=lambda: False))
    return mcp_env


def _install_args(*extra):
    return _parser().parse_args(["mcp-install", *extra])


def test_mcp_install_saves_credentials_and_writes_a_secret_free_entry(install_env, tmp_path, capsys):
    config_file = tmp_path / "mcp.json"
    rc = cli.cmd_mcp_install(
        _install_args("--config-file", str(config_file), "--client-id", "cid", "--client-secret", "sec/ret==")
    )

    assert rc == 0
    saved = install_env.read_text()
    assert "DATAQUERY_CLIENT_ID=cid" in saved
    assert "sec/ret==" in saved
    assert install_env.stat().st_mode & 0o077 == 0
    entry = json.loads(config_file.read_text())["mcpServers"]["dataquery"]
    assert entry["args"][-1] == "mcp-connect"
    assert "env" not in entry
    assert "sec/ret==" not in config_file.read_text()
    out = capsys.readouterr().out
    assert f"Added 'dataquery' in {config_file}" in out
    assert "sec/ret==" not in out


def test_mcp_install_defaults_to_claude_desktop(install_env, mcp_app_dirs, capsys):
    _, app_data = mcp_app_dirs

    rc = cli.cmd_mcp_install(_install_args("--client-id", "cid", "--client-secret", "sec"))

    assert rc == 0
    desktop = app_data / "Claude" / "claude_desktop_config.json"
    assert "dataquery" in json.loads(desktop.read_text())["mcpServers"]
    assert "Restart Claude Desktop" in capsys.readouterr().out


@pytest.mark.parametrize(
    "app, servers_key, label",
    [("cursor", "mcpServers", "Cursor"), ("vscode", "servers", "VS Code")],
)
def test_mcp_install_json_apps(install_env, mcp_app_dirs, capsys, app, servers_key, label):
    rc = cli.cmd_mcp_install(_install_args("--app", app, "--client-id", "cid", "--client-secret", "sec"))

    assert rc == 0
    path = mcp_install.JSON_APPS[app].config_path()
    entry = json.loads(path.read_text())[servers_key]["dataquery"]
    assert entry["type"] == "stdio"
    assert entry["args"][-1] == "mcp-connect"
    assert f"Restart {label}" in capsys.readouterr().out


def test_mcp_install_chatgpt_writes_the_shared_codex_config(install_env, mcp_app_dirs, capsys):
    home, _ = mcp_app_dirs

    rc = cli.cmd_mcp_install(_install_args("--app", "chatgpt", "--client-id", "cid", "--client-secret", "s3cr3t-value"))

    assert rc == 0
    text = (home / ".codex" / "config.toml").read_text()
    assert tomllib.loads(text)["mcp_servers"]["dataquery"]["args"][-1] == "mcp-connect"
    assert "s3cr3t-value" not in text
    assert "s3cr3t-value" in install_env.read_text()
    assert "ChatGPT desktop app" in capsys.readouterr().out


def test_mcp_install_rejects_an_unsafe_server_name(capsys):
    with pytest.raises(SystemExit):
        _install_args("--name", "data query")
    assert "invalid server name" in capsys.readouterr().err


def test_mcp_install_every_app_is_handled():
    assert set(cli.MCP_INSTALL_APPS) == set(mcp_install.JSON_APPS) | {"claude-code", "chatgpt"}


def test_mcp_install_claude_code(install_env, monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(mcp_install, "add_to_claude_code", lambda name, entry: calls.append((name, entry)))

    rc = cli.cmd_mcp_install(
        _install_args("--app", "claude-code", "--name", "dq-uat", "--url", "https://uat/mcp", "--bearer-token", "tok")
    )

    assert rc == 0
    [(name, entry)] = calls
    assert name == "dq-uat"
    assert entry["args"][-3:] == ["mcp-connect", "--url", "https://uat/mcp"]
    assert "DATAQUERY_BEARER_TOKEN=tok" in install_env.read_text()
    assert "claude mcp list" in capsys.readouterr().out


def test_mcp_install_prompts_for_missing_credentials(install_env, tmp_path, monkeypatch):
    monkeypatch.setattr(cli.sys, "stdin", SimpleNamespace(isatty=lambda: True))
    monkeypatch.setattr("builtins.input", lambda prompt: "cid")
    secret_prompts = []
    monkeypatch.setattr(cli.getpass, "getpass", lambda prompt: secret_prompts.append(prompt) or "sec")

    rc = cli.cmd_mcp_install(_install_args("--config-file", str(tmp_path / "mcp.json")))

    assert rc == 0
    assert secret_prompts == ["DataQuery client secret: "]  # read without echo
    assert "DATAQUERY_CLIENT_SECRET=sec" in install_env.read_text()


def test_mcp_install_prompts_only_for_what_is_missing(install_env, tmp_path, monkeypatch):
    monkeypatch.setattr(cli.sys, "stdin", SimpleNamespace(isatty=lambda: True))
    monkeypatch.setattr("builtins.input", lambda prompt: pytest.fail("client ID was already given"))
    monkeypatch.setattr(cli.getpass, "getpass", lambda prompt: "sec")

    rc = cli.cmd_mcp_install(_install_args("--config-file", str(tmp_path / "mcp.json"), "--client-id", "cid"))

    assert rc == 0


def test_mcp_install_rerun_reuses_saved_credentials(install_env, tmp_path, monkeypatch):
    """Re-running refreshes the entry without asking again."""
    from dataquery.config import EnvConfig

    EnvConfig.save_user_env({"CLIENT_ID": "cid", "CLIENT_SECRET": "sec"})
    monkeypatch.setattr(cli.sys, "stdin", SimpleNamespace(isatty=lambda: True))
    monkeypatch.setattr("builtins.input", lambda prompt: pytest.fail("prompted"))
    monkeypatch.setattr(cli.getpass, "getpass", lambda prompt: pytest.fail("prompted"))

    rc = cli.cmd_mcp_install(_install_args("--config-file", str(tmp_path / "mcp.json")))

    assert rc == 0


def test_mcp_install_without_credentials_off_a_terminal_fails(install_env, tmp_path, capsys):
    config_file = tmp_path / "mcp.json"
    rc = cli.cmd_mcp_install(_install_args("--config-file", str(config_file), "--client-id", "cid"))

    assert rc == 1
    assert "No credentials" in capsys.readouterr().err
    assert not config_file.exists()
    assert not install_env.exists()


def test_mcp_install_requires_the_mcp_extra(install_env, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(cli, "_mcp_extra_installed", lambda: False)
    config_file = tmp_path / "mcp.json"
    rc = cli.cmd_mcp_install(
        _install_args("--config-file", str(config_file), "--client-id", "cid", "--client-secret", "sec")
    )

    assert rc == 1
    assert "pip install 'dataquery-sdk[mcp]'" in capsys.readouterr().err
    assert not config_file.exists()
    assert not install_env.exists()


def test_mcp_install_reports_an_unusable_config_file(install_env, tmp_path, capsys):
    config_file = tmp_path / "mcp.json"
    config_file.write_text("{not json")
    rc = cli.cmd_mcp_install(
        _install_args("--config-file", str(config_file), "--client-id", "cid", "--client-secret", "sec")
    )

    assert rc == 1
    assert "not valid JSON" in capsys.readouterr().err
    assert config_file.read_text() == "{not json"


def test_mcp_install_app_and_config_file_are_exclusive():
    with pytest.raises(SystemExit):
        _install_args("--app", "claude-code", "--config-file", "mcp.json")


def test_main_dispatches_mcp_install():
    with patch.object(cli, "cmd_mcp_install", return_value=0) as cmd, patch("sys.argv", ["dataquery", "mcp-install"]):
        assert cli.main() == 0
    cmd.assert_called_once()


def _stub_token_manager(*headers, bearer=False):
    """TokenManager stand-in handing out ``headers`` in order; clear_token drops the current one."""
    issued = iter(headers)
    tm = MagicMock()
    tm.config.has_bearer_token = bearer
    tm.current_token = None

    async def get_valid_token():
        if tm.current_token is None:
            header = next(issued)
            tm.current_token = MagicMock(**{"to_authorization_header.return_value": header})
        return tm.current_token.to_authorization_header()

    def clear_token():
        tm.current_token = None

    tm.get_valid_token = AsyncMock(side_effect=get_valid_token)
    tm.clear_token = MagicMock(side_effect=clear_token)
    return tm


async def _send_through_authe_auth(tm, statuses):
    """POST once through ``_authe_auth``; return (final status, Authorization headers seen)."""
    httpx = pytest.importorskip("httpx")
    seen = []
    replies = iter(statuses)

    def handler(request):
        seen.append(request.headers["Authorization"])
        return httpx.Response(next(replies))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler), auth=cli._authe_auth(tm)) as client:
        resp = await client.post("https://mcp.example.com/mcp", json={"jsonrpc": "2.0"})
    return resp.status_code, seen


@pytest.mark.asyncio
async def test_mcp_connect_auth_replaces_rejected_token_and_retries():
    """A 401 on a locally-valid (e.g. stale cached) token fetches a new one and retries once."""
    tm = _stub_token_manager("Bearer stale", "Bearer fresh")

    status, seen = await _send_through_authe_auth(tm, [401, 200])

    assert status == 200
    assert seen == ["Bearer stale", "Bearer fresh"]
    tm.clear_token.assert_called_once()


@pytest.mark.asyncio
async def test_mcp_connect_auth_retries_only_once():
    tm = _stub_token_manager("Bearer a", "Bearer b", "Bearer c")

    status, seen = await _send_through_authe_auth(tm, [401, 401])

    assert status == 401
    assert seen == ["Bearer a", "Bearer b"]


@pytest.mark.asyncio
async def test_mcp_connect_auth_does_not_retry_static_bearer_token():
    """A configured DATAQUERY_BEARER_TOKEN cannot be refreshed, so a 401 is final."""
    tm = _stub_token_manager("Bearer static", bearer=True)

    status, seen = await _send_through_authe_auth(tm, [401])

    assert status == 401
    assert seen == ["Bearer static"]
    tm.clear_token.assert_not_called()


async def _connect_and_capture_config(args):
    """Run cmd_mcp_connect against a stub proxy; return the config its TokenManager got."""
    from dataquery.transport import auth

    with patch.object(auth, "TokenManager", wraps=auth.TokenManager) as token_manager:
        await _connect_and_capture_url(args)
    return token_manager.call_args.args[0]


@pytest.mark.asyncio
async def test_mcp_connect_caches_token_under_user_config_dir(mcp_env, tmp_path, monkeypatch):
    """The token cache is independent of the launch directory and keyed by credentials."""
    pytest.importorskip("fastmcp")
    monkeypatch.chdir(tmp_path)

    first = await _connect_and_capture_config(_mcp_args("--client-id", "cid-1", "--client-secret", "sec"))
    second = await _connect_and_capture_config(_mcp_args("--client-id", "cid-2"))

    tokens_dir = mcp_env.parent / "tokens"
    assert first.token_storage_enabled
    assert Path(first.token_storage_dir).parent == tokens_dir
    assert Path(second.token_storage_dir).parent == tokens_dir
    assert first.token_storage_dir != second.token_storage_dir
    assert not (tmp_path / "downloads").exists()


@pytest.mark.asyncio
async def test_mcp_connect_honors_explicit_token_storage_dir(mcp_env, tmp_path):
    pytest.importorskip("fastmcp")
    os.environ["DATAQUERY_TOKEN_STORAGE_DIR"] = str(tmp_path / "mine")
    os.environ["DATAQUERY_TOKEN_STORAGE_ENABLED"] = "true"

    config = await _connect_and_capture_config(_mcp_args("--client-id", "cid", "--client-secret", "sec"))

    assert config.token_storage_dir == str(tmp_path / "mine")
