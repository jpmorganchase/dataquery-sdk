import argparse
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dataquery import cli


def _parser():
    return cli.create_parser()

#
# def test_cli_no_command_prints_help(capsys):
#     parser = _parser()
#     # Directly call main to hit the no-command branch
#     with patch.object(cli, "create_parser", return_value=parser):
#         with patch("sys.argv", ["dataquery"]):
#             rc = cli.main()
#     captured = capsys.readouterr()
#     assert rc == 1
#     assert (
#         "Command Line Interface" in captured.out or "Available commands" in captured.out
#     )


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


# @pytest.mark.asyncio
# async def test_cli_download_single_json(monkeypatch, tmp_path, capsys):
#     parser = _parser()
#     dest = tmp_path / "out"
#     args = parser.parse_args(["download", "--file-group-id", "FG", "--file-datetime", "20240101", "--destination", str(dest), "--json"])  # type: ignore[arg-type]

#     fake_result = MagicMock()
#     fake_result.model_dump = lambda: {"status": "completed", "local_path": str(dest)}
#     fake_result.status.value = "completed"

#     fake_dq = MagicMock()
#     fake_dq.__aenter__ = AsyncMock(return_value=fake_dq)
#     fake_dq.__aexit__ = AsyncMock(return_value=None)
#     fake_dq.download_file_async = AsyncMock(return_value=fake_result)
#     monkeypatch.setattr(cli, "DataQuery", MagicMock(return_value=fake_dq))

#     rc = await cli.cmd_download(args)
#     out = capsys.readouterr().out
#     assert rc == 0
    # assert str(dest) in out


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
    fake_dq.start_auto_download_async = AsyncMock(return_value=_Mgr())
    monkeypatch.setattr(cli, "DataQuery", MagicMock(return_value=fake_dq))

    async def boom(_):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli.asyncio, "sleep", boom)

    rc = await cli.cmd_download(args)
    assert rc == 0
