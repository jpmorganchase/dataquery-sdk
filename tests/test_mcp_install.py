"""Tests for ``dataquery.mcp_install``: the server entry, JSON config merging and Claude Code registration.

Nothing here touches a real MCP app: config files live under ``tmp_path`` and
the ``claude`` CLI is stubbed.
"""

import json
import os
import stat
import subprocess
import sys
import tomllib
from pathlib import Path
from unittest.mock import patch

import pytest

from dataquery import mcp_install
from dataquery.types.exceptions import ConfigurationError

ENTRY = {"command": "/venv/bin/dataquery", "args": ["mcp-connect"]}


class TestServerEntry:
    def test_uses_the_script_next_to_the_interpreter(self, tmp_path, monkeypatch):
        bin_dir = tmp_path / "venv" / "bin"
        bin_dir.mkdir(parents=True)
        script = bin_dir / ("dataquery.exe" if os.name == "nt" else "dataquery")
        script.write_text("")
        monkeypatch.setattr(sys, "executable", str(bin_dir / "python"))
        assert mcp_install.server_entry() == {"command": str(script), "args": ["mcp-connect"]}

    def test_falls_back_to_the_module_without_a_script(self, tmp_path, monkeypatch):
        python = str(tmp_path / "python")
        monkeypatch.setattr(sys, "executable", python)
        assert mcp_install.server_entry() == {"command": python, "args": ["-m", "dataquery.cli", "mcp-connect"]}

    def test_url_is_passed_to_mcp_connect(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sys, "executable", str(tmp_path / "python"))
        entry = mcp_install.server_entry("https://uat.example.com/mcp")
        assert entry["args"][-3:] == ["mcp-connect", "--url", "https://uat.example.com/mcp"]


class TestAppConfigPaths:
    def test_app_data_dir_on_macos(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sys, "platform", "darwin")
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        assert mcp_install._app_data_dir() == tmp_path / "Library" / "Application Support"

    def test_app_data_dir_on_windows_is_appdata(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sys, "platform", "win32")
        monkeypatch.setenv("APPDATA", str(tmp_path / "Roaming"))
        assert mcp_install._app_data_dir() == tmp_path / "Roaming"

    def test_app_data_dir_on_linux_is_xdg_config_home(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "cfg"))
        assert mcp_install._app_data_dir() == tmp_path / "cfg"

    @pytest.mark.parametrize(
        "app, base, parts",
        [
            ("claude-desktop", "app_data", ("Claude", "claude_desktop_config.json")),
            ("vscode", "app_data", ("Code", "User", "mcp.json")),
            ("cursor", "home", (".cursor", "mcp.json")),
        ],
    )
    def test_json_app_paths(self, mcp_app_dirs, app, base, parts):
        home, app_data = mcp_app_dirs
        root = app_data if base == "app_data" else home
        assert mcp_install.JSON_APPS[app].config_path() == root.joinpath(*parts)

    def test_codex_config_path(self, mcp_app_dirs):
        home, _ = mcp_app_dirs
        assert mcp_install.codex_config_path() == home / ".codex" / "config.toml"


class TestAddToConfigFile:
    def test_creates_the_file_and_its_directory(self, tmp_path):
        path = tmp_path / "Claude" / "claude_desktop_config.json"
        assert mcp_install.add_to_config_file(path, "dataquery", ENTRY) is False
        assert json.loads(path.read_text()) == {"mcpServers": {"dataquery": ENTRY}}

    def test_empty_file_is_treated_as_new(self, tmp_path):
        path = tmp_path / "config.json"
        path.write_text("")
        mcp_install.add_to_config_file(path, "dataquery", ENTRY)
        assert json.loads(path.read_text()) == {"mcpServers": {"dataquery": ENTRY}}

    def test_keeps_everything_else_and_replaces_its_own_entry(self, tmp_path):
        path = tmp_path / "config.json"
        path.write_text(
            json.dumps(
                {
                    "globalShortcut": "Ctrl+Space",
                    "mcpServers": {
                        "other": {"command": "other-mcp"},
                        "dataquery": {
                            "command": "dataquery",
                            "args": ["mcp-connect", "--save-credentials"],
                            "env": {"DATAQUERY_CLIENT_SECRET": "s3cret"},
                        },
                    },
                }
            )
        )
        assert mcp_install.add_to_config_file(path, "dataquery", ENTRY) is True
        config = json.loads(path.read_text())
        assert config["globalShortcut"] == "Ctrl+Space"
        assert config["mcpServers"]["other"] == {"command": "other-mcp"}
        # The old entry goes whole, so a secret in its env block leaves the file.
        assert config["mcpServers"]["dataquery"] == ENTRY
        assert "s3cret" not in path.read_text()

    @pytest.mark.parametrize("content", ["{not json", "[]", '{"mcpServers": []}'])
    def test_unusable_file_is_left_untouched(self, tmp_path, content):
        path = tmp_path / "config.json"
        path.write_text(content)
        with pytest.raises(ConfigurationError):
            mcp_install.add_to_config_file(path, "dataquery", ENTRY)
        assert path.read_text() == content
        assert list(tmp_path.iterdir()) == [path]

    def test_failed_write_keeps_the_original_and_no_temp_file(self, tmp_path):
        path = tmp_path / "config.json"
        path.write_text("{}")
        with patch("dataquery.mcp_install.os.replace", side_effect=OSError("disk full")):
            with pytest.raises(OSError, match="disk full"):
                mcp_install.add_to_config_file(path, "dataquery", ENTRY)
        assert path.read_text() == "{}"
        assert list(tmp_path.iterdir()) == [path]

    @pytest.mark.skipif(os.name == "nt", reason="POSIX permissions")
    def test_keeps_the_files_permissions(self, tmp_path):
        path = tmp_path / "config.json"
        path.write_text("{}")
        path.chmod(0o640)
        mcp_install.add_to_config_file(path, "dataquery", ENTRY)
        assert stat.S_IMODE(path.stat().st_mode) == 0o640

    @pytest.mark.skipif(os.name == "nt", reason="POSIX permissions")
    def test_new_file_is_owner_only(self, tmp_path):
        path = tmp_path / "config.json"
        mcp_install.add_to_config_file(path, "dataquery", ENTRY)
        assert stat.S_IMODE(path.stat().st_mode) == 0o600

    @pytest.mark.skipif(os.name == "nt", reason="symlinks need privileges on Windows")
    def test_writes_through_a_symlink(self, tmp_path):
        real = tmp_path / "dotfiles" / "claude.json"
        real.parent.mkdir()
        real.write_text("{}")
        link = tmp_path / "claude_desktop_config.json"
        link.symlink_to(real)
        mcp_install.add_to_config_file(link, "dataquery", ENTRY)
        assert link.is_symlink()
        assert json.loads(real.read_text())["mcpServers"]["dataquery"] == ENTRY


class TestJsonApps:
    def test_claude_desktop_entry_is_untyped_under_mcp_servers(self, mcp_app_dirs):
        path, replaced = mcp_install.JSON_APPS["claude-desktop"].add("dataquery", ENTRY)
        assert replaced is False
        assert json.loads(path.read_text()) == {"mcpServers": {"dataquery": ENTRY}}

    def test_cursor_entry_is_typed_under_mcp_servers(self, mcp_app_dirs):
        path, _ = mcp_install.JSON_APPS["cursor"].add("dataquery", ENTRY)
        assert json.loads(path.read_text()) == {"mcpServers": {"dataquery": {"type": "stdio", **ENTRY}}}

    def test_vscode_entry_is_typed_under_servers_and_keeps_inputs(self, mcp_app_dirs):
        path = mcp_install.JSON_APPS["vscode"].config_path()
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({"servers": {"other": {"command": "x"}}, "inputs": [{"id": "key"}]}))

        _, replaced = mcp_install.JSON_APPS["vscode"].add("dataquery", ENTRY)

        assert replaced is False
        assert json.loads(path.read_text()) == {
            "servers": {"other": {"command": "x"}, "dataquery": {"type": "stdio", **ENTRY}},
            "inputs": [{"id": "key"}],
        }


class TestAddToCodex:
    def _config(self, mcp_app_dirs) -> Path:
        home, _ = mcp_app_dirs
        return home / ".codex" / "config.toml"

    def test_creates_the_config(self, mcp_app_dirs):
        path, replaced = mcp_install.add_to_codex("dataquery", ENTRY)
        assert path == self._config(mcp_app_dirs)
        assert replaced is False
        assert tomllib.loads(path.read_text()) == {"mcp_servers": {"dataquery": ENTRY}}

    def test_appends_and_keeps_the_rest_byte_for_byte(self, mcp_app_dirs):
        path = self._config(mcp_app_dirs)
        path.parent.mkdir(parents=True)
        original = 'model = "gpt-5"  # my default\n\n[mcp_servers.other]\ncommand = "other-mcp"\n'
        path.write_text(original)

        mcp_install.add_to_codex("dataquery", ENTRY)

        text = path.read_text()
        assert text.startswith(original)
        config = tomllib.loads(text)
        assert config["model"] == "gpt-5"
        assert config["mcp_servers"] == {"other": {"command": "other-mcp"}, "dataquery": ENTRY}

    def test_rerun_replaces_only_its_own_block(self, mcp_app_dirs):
        path = self._config(mcp_app_dirs)
        path.parent.mkdir(parents=True)
        path.write_text('model = "gpt-5"\n')
        mcp_install.add_to_codex("dataquery", ENTRY)
        updated = {"command": ENTRY["command"], "args": ["mcp-connect", "--url", "https://uat/mcp"]}

        _, replaced = mcp_install.add_to_codex("dataquery", updated)

        text = path.read_text()
        assert replaced is True
        assert text.count("[mcp_servers.dataquery]") == 1
        assert tomllib.loads(text) == {"model": "gpt-5", "mcp_servers": {"dataquery": updated}}

    def test_leaves_a_server_it_did_not_add_alone(self, mcp_app_dirs):
        path = self._config(mcp_app_dirs)
        path.parent.mkdir(parents=True)
        original = '[mcp_servers.dataquery]\ncommand = "hand-written"\n'
        path.write_text(original)
        with pytest.raises(ConfigurationError, match="didn't add"):
            mcp_install.add_to_codex("dataquery", ENTRY)
        assert path.read_text() == original

    @pytest.mark.parametrize(
        "content",
        [
            "[broken",  # not TOML at all
            'mcp_servers = { other = { command = "x" } }\n',  # inline table: can't take another server
        ],
    )
    def test_never_writes_a_config_it_cannot_extend_cleanly(self, mcp_app_dirs, content):
        path = self._config(mcp_app_dirs)
        path.parent.mkdir(parents=True)
        path.write_text(content)
        with pytest.raises(ConfigurationError):
            mcp_install.add_to_codex("dataquery", ENTRY)
        assert path.read_text() == content

    def test_windows_style_command_round_trips(self, mcp_app_dirs):
        entry = {"command": "C:\\Users\\me\\venv\\Scripts\\dataquery.exe", "args": ["mcp-connect"]}
        path, _ = mcp_install.add_to_codex("dataquery", entry)
        assert tomllib.loads(path.read_text())["mcp_servers"]["dataquery"] == entry


def _done(returncode=0, stderr=""):
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout="", stderr=stderr)


class TestAddToClaudeCode:
    def test_missing_cli_explains_the_manual_command(self):
        with patch("dataquery.mcp_install.shutil.which", return_value=None):
            with pytest.raises(ConfigurationError, match="claude mcp add-json -s user dataquery"):
                mcp_install.add_to_claude_code("dataquery", ENTRY)

    def test_replaces_the_user_scope_entry(self):
        with (
            patch("dataquery.mcp_install.shutil.which", return_value="/bin/claude"),
            patch("dataquery.mcp_install.subprocess.run", return_value=_done()) as run,
        ):
            mcp_install.add_to_claude_code("dataquery", ENTRY)
        remove, add = (call.args[0] for call in run.call_args_list)
        assert remove == ["/bin/claude", "mcp", "remove", "-s", "user", "dataquery"]
        assert add[:6] == ["/bin/claude", "mcp", "add-json", "-s", "user", "dataquery"]
        assert json.loads(add[6]) == {"type": "stdio", **ENTRY}

    def test_add_failure_is_reported(self):
        results = [_done(1, "No MCP server found"), _done(1, "Invalid configuration")]
        with (
            patch("dataquery.mcp_install.shutil.which", return_value="/bin/claude"),
            patch("dataquery.mcp_install.subprocess.run", side_effect=results),
        ):
            with pytest.raises(ConfigurationError, match="Invalid configuration"):
                mcp_install.add_to_claude_code("dataquery", ENTRY)

    def test_hung_cli_is_reported(self):
        with (
            patch("dataquery.mcp_install.shutil.which", return_value="/bin/claude"),
            patch("dataquery.mcp_install.subprocess.run", side_effect=subprocess.TimeoutExpired("claude", 60)),
        ):
            with pytest.raises(ConfigurationError, match="'claude mcp remove' failed"):
                mcp_install.add_to_claude_code("dataquery", ENTRY)
