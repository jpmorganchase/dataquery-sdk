import pytest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from dataquery.dataquery import DataQuery


@pytest.mark.asyncio
async def test_run_group_download_parallel_async_filters_availability(monkeypatch):
    """Ensure only entries with is-available True are queued for download."""
    # Provide minimal valid client config via env bypass using patch
    with patch('dataquery.dataquery.EnvConfig.validate_config'):
        with patch('dataquery.dataquery.DataQueryClient') as mock_client_cls:
            mock_client = mock_client_cls.return_value
            # Ensure awaited connect/close work
            mock_client.connect = AsyncMock(return_value=None)
            mock_client.close = AsyncMock(return_value=None)
            # Mixed availability
            available = [
                {"file_group_id": "a", "file_datetime": "20240101", "is-available": True},
                {"file_group_id": "b", "file_datetime": "20240102", "is-available": False},
                {"file_group_id": "c", "file_datetime": "20240103"},
                {"file_group_id": "d", "file_datetime": "20240104", "is_available": True},
            ]
            mock_client.list_available_files_async = AsyncMock(return_value=available)

            async def ok_dl(file_group_id, *args, **kwargs):  # noqa: ARG001
                status_obj = type("S", (), {"value": "completed"})()
                class R:  # minimal result object instance
                    pass
                r = R()
                r.status = status_obj
                r.file_group_id = file_group_id
                return r

            # Mock rate limiter configuration
            mock_rate_limiter = AsyncMock()
            mock_rate_limiter.config.requests_per_minute = 100
            mock_rate_limiter.config.burst_capacity = 20
            mock_rate_limiter.config.enable_queuing = False
            mock_rate_limiter.config.max_queue_size = 100
            mock_client.rate_limiter = mock_rate_limiter
            
            # Mock download_file_async to return successful results
            mock_download_result = type('DownloadResult', (), {
                'file_group_id': 'test_file',
                'group_id': 'test_group',
                'local_path': Path('./test_file.bin'),
                'file_size': 1024,
                'download_time': 1.0,
                'bytes_downloaded': 1024,
                'speed_mbps': 1.0,
                'status': type('Status', (), {'value': 'completed'})(),
                'error_message': None
            })()
            
            mock_client.download_file_async = AsyncMock(return_value=mock_download_result)
            
            # Mock HTTP request to trigger fallback to download_file_async
            # Return a response without content-range header to trigger fallback
            mock_response = AsyncMock()
            mock_response.headers = {
                'content-length': '1024',
                'content-disposition': 'attachment; filename="file.bin"'
                # No content-range header to trigger fallback
            }
            
            mock_context_manager = AsyncMock()
            mock_context_manager.__aenter__.return_value = mock_response
            mock_client._enter_request_cm = AsyncMock(return_value=mock_context_manager)
            mock_client._handle_response = AsyncMock(return_value=None)
            mock_client._build_files_api_url.return_value = "https://api.example.com/group/file/download"
            
            # Build a real DataQuery but with client mocked
            async with DataQuery() as dq:
                report = await dq.run_group_download_parallel_async(
                    group_id="G",
                    start_date="20240101",
                    end_date="20240131",
                    max_concurrent=2,
                    num_parts=2,
                )

        # Only two entries are available True
        assert report["total_files"] == 2
        assert report["successful_downloads"] == 2
        assert report["failed_downloads"] == 0
        assert report["success_rate"] == 100.0


@pytest.mark.asyncio
async def test_run_group_download_async_filters_availability(monkeypatch):
    with patch('dataquery.dataquery.EnvConfig.validate_config'):
        with patch('dataquery.dataquery.DataQueryClient') as mock_client_cls:
            mock_client = mock_client_cls.return_value
            mock_client.connect = AsyncMock(return_value=None)
            mock_client.close = AsyncMock(return_value=None)
            available = [
                {"file_group_id": "x", "file_datetime": "20240101", "is_available": True},
                {"file_group_id": "y", "file_datetime": "20240102", "is-available": False},
            ]
            mock_client.list_available_files_async = AsyncMock(return_value=available)

            async def ok_dl(file_group_id, *args, **kwargs):  # noqa: ARG001
                status_obj = type("S", (), {"value": "completed"})()
                class R:
                    pass
                r = R()
                r.status = status_obj
                r.file_group_id = file_group_id
                return r

            mock_client.download_file_async = ok_dl

            async with DataQuery() as dq:
                report = await dq.run_group_download_async(
                    group_id="G",
                    start_date="20240101",
                    end_date="20240131",
                )

        assert report["total_files"] == 1
        assert report["successful_downloads"] == 1
        assert report["failed_downloads"] == 0

