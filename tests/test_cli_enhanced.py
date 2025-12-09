import argparse
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dataquery.cli import cmd_download, cmd_download_group


@pytest.mark.asyncio
async def test_cmd_download_with_performance_args():
    """Test download command with performance arguments."""
    args = argparse.Namespace(
        command="download",
        file_group_id="test_file",
        file_datetime="20240101",
        destination="/tmp/test",
        watch=False,
        group_id=None,
        json=False,
        num_parts=10,
        chunk_size=8192,
        env_file=None,
    )

    mock_dq_instance = AsyncMock()
    mock_dq_instance.download_file_async.return_value = MagicMock(local_path="/tmp/test/file")

    with patch("dataquery.cli.DataQuery") as MockDQ:
        MockDQ.return_value.__aenter__.return_value = mock_dq_instance
        
        await cmd_download(args)
        
        mock_dq_instance.download_file_async.assert_called_once()
        call_args = mock_dq_instance.download_file_async.call_args
        assert call_args[0] == ("test_file", "20240101")
        assert call_args[1]["num_parts"] == 10
        assert call_args[1]["options"].chunk_size == 8192


@pytest.mark.asyncio
async def test_cmd_download_group():
    """Test download-group command."""
    args = argparse.Namespace(
        command="download-group",
        group_id="test_group",
        start_date="20240101",
        end_date="20240131",
        destination="/tmp/downloads",
        max_concurrent=5,
        num_parts=4,
        json=True,
        env_file=None,
    )

    mock_dq_instance = AsyncMock()
    mock_dq_instance.run_group_download_async.return_value = {
        "successful": 10,
        "failed": 0,
        "results": []
    }

    with patch("dataquery.cli.DataQuery") as MockDQ:
        MockDQ.return_value.__aenter__.return_value = mock_dq_instance
        
        await cmd_download_group(args)
        
        mock_dq_instance.run_group_download_async.assert_called_once_with(
            group_id="test_group",
            start_date="20240101",
            end_date="20240131",
            destination_dir="/tmp/downloads",
            max_concurrent=5,
            num_parts=4
        )
