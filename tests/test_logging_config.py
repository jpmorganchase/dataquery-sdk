import logging
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, mock_open, patch

import pytest

from dataquery.logging_config import (
    LogFormat,
    LoggingConfig,
    LoggingManager,
    LogLevel,
    PerformanceLogger,
    RequestResponseLogger,
    StructuredLogger,
    create_logging_config,
    create_logging_manager,
)


class TestLogLevel:
    """Test LogLevel enum."""

    def test_log_level_values(self):
        """Test LogLevel enum values."""
        assert LogLevel.DEBUG == "DEBUG"
        assert LogLevel.INFO == "INFO"
        assert LogLevel.WARNING == "WARNING"
        assert LogLevel.ERROR == "ERROR"
        assert LogLevel.CRITICAL == "CRITICAL"


class TestLogFormat:
    """Test LogFormat enum."""

    def test_log_format_values(self):
        """Test LogFormat enum values."""
        assert LogFormat.JSON == "json"
        assert LogFormat.CONSOLE == "console"
        assert LogFormat.SIMPLE == "simple"


class TestLoggingConfig:
    """Test LoggingConfig dataclass."""

    def test_logging_config_defaults(self):
        """Test LoggingConfig with default values."""
        config = LoggingConfig()

        assert config.level == LogLevel.INFO
        assert config.format == LogFormat.JSON
        assert config.enable_console is True
        assert config.enable_file is False
        assert config.log_file is None
        assert config.max_file_size == 10 * 1024 * 1024  # 10MB
        assert config.backup_count == 5
        assert config.enable_request_logging is False
        assert config.enable_performance_logging is True
        assert config.enable_metrics is True
        assert config.include_timestamps is True
        assert config.include_process_info is True
        assert config.include_thread_info is True
        assert config.custom_processors == []
        assert config.log_correlation_id is True

    def test_logging_config_custom_values(self):
        """Test LoggingConfig with custom values."""
        log_file = Path("/tmp/test.log")
        config = LoggingConfig(
            level=LogLevel.DEBUG,
            format=LogFormat.CONSOLE,
            enable_console=False,
            enable_file=True,
            log_file=log_file,
            max_file_size=1024 * 1024,  # 1MB
            backup_count=3,
            enable_request_logging=True,
            enable_performance_logging=False,
            enable_metrics=False,
            include_timestamps=False,
            include_process_info=False,
            include_thread_info=False,
            custom_processors=[Mock()],
            log_correlation_id=False,
        )

        assert config.level == LogLevel.DEBUG
        assert config.format == LogFormat.CONSOLE
        assert config.enable_console is False
        assert config.enable_file is True
        assert config.log_file == log_file
        assert config.max_file_size == 1024 * 1024
        assert config.backup_count == 3
        assert config.enable_request_logging is True
        assert config.enable_performance_logging is False
        assert config.enable_metrics is False
        assert config.include_timestamps is False
        assert config.include_process_info is False
        assert config.include_thread_info is False
        assert len(config.custom_processors) == 1
        assert config.log_correlation_id is False


class TestRequestResponseLogger:
    """Test RequestResponseLogger class."""

    @pytest.fixture
    def config(self):
        """Create a test configuration."""
        return LoggingConfig(enable_request_logging=True)

    @pytest.fixture
    def logger(self, config):
        """Create a test logger."""
        return RequestResponseLogger(config)

    def test_request_response_logger_initialization(self, logger, config):
        """Test RequestResponseLogger initialization."""
        assert logger.config == config
        assert logger.logger is not None

    def test_log_request_enabled(self, logger):
        """Test logging request when enabled."""
        headers = {"Authorization": "Bearer token", "Content-Type": "application/json"}
        body = '{"test": "data"}'

        with patch.object(logger.logger, "info") as mock_info:
            logger.log_request(
                "POST", "https://api.example.com/test", headers, body, "corr-123"
            )

            mock_info.assert_called_once()
            call_args = mock_info.call_args[1]
            assert call_args["event_type"] == "http_request"
            assert call_args["method"] == "POST"
            assert call_args["url"] == "https://api.example.com/test"
            assert call_args["correlation_id"] == "corr-123"

    def test_log_request_disabled(self):
        """Test logging request when disabled."""
        config = LoggingConfig(enable_request_logging=False)
        logger = RequestResponseLogger(config)

        with patch.object(logger.logger, "info") as mock_info:
            logger.log_request("GET", "https://api.example.com/test", {})

            mock_info.assert_not_called()

    def test_log_response_enabled(self, logger):
        """Test logging response when enabled."""
        headers = {"Content-Type": "application/json"}
        body = '{"result": "success"}'

        with patch.object(logger.logger, "info") as mock_info:
            logger.log_response(200, headers, body, 1.5, "corr-123")

            mock_info.assert_called_once()
            call_args = mock_info.call_args[1]
            assert call_args["event_type"] == "http_response"
            assert call_args["status_code"] == 200
            assert call_args["duration_ms"] == 1500.0
            assert call_args["correlation_id"] == "corr-123"

    def test_log_response_disabled(self):
        """Test logging response when disabled."""
        config = LoggingConfig(enable_request_logging=False)
        logger = RequestResponseLogger(config)

        with patch.object(logger.logger, "info") as mock_info:
            logger.log_response(200, {}, "body", 1.0)

            mock_info.assert_not_called()

    def test_sanitize_headers(self, logger):
        """Test header sanitization."""
        headers = {
            "Authorization": "Bearer secret-token",
            "Content-Type": "application/json",
            "User-Agent": "test-agent",
        }

        sanitized = logger._sanitize_headers(headers)

        assert sanitized["Authorization"] == "***"
        assert sanitized["Content-Type"] == "application/json"
        assert sanitized["User-Agent"] == "test-agent"

    def test_truncate_body(self, logger):
        """Test body truncation."""
        long_body = "x" * 2000
        truncated = logger._truncate_body(long_body, max_length=1000)

        assert len(truncated) == 1015  # 1000 + len("... [truncated]")
        assert truncated.endswith("... [truncated]")

        # Test short body
        short_body = "short"
        result = logger._truncate_body(short_body, max_length=1000)
        assert result == short_body


class TestPerformanceLogger:
    """Test PerformanceLogger class."""

    @pytest.fixture
    def config(self):
        """Create a test configuration."""
        return LoggingConfig(enable_performance_logging=True)

    @pytest.fixture
    def logger(self, config):
        """Create a test logger."""
        return PerformanceLogger(config)

    def test_performance_logger_initialization(self, logger, config):
        """Test PerformanceLogger initialization."""
        assert logger.config == config
        assert logger.logger is not None

    def test_log_operation_start(self, logger):
        """Test logging operation start."""
        with patch.object(logger.logger, "debug") as mock_debug:
            logger.log_operation_start("download_file", file_id="123", size=1024)

            mock_debug.assert_called_once()
            call_args = mock_debug.call_args[1]
            assert call_args["operation"] == "download_file"
            assert call_args["file_id"] == "123"
            assert call_args["size"] == 1024

    def test_log_operation_end(self, logger):
        """Test logging operation end."""
        # First log operation start to populate metrics
        logger.log_operation_start("download_file", file_id="123", size=1024)

        with patch.object(logger.logger, "info") as mock_info:
            logger.log_operation_end(
                "download_file", 2.5, True, file_id="123", bytes_downloaded=1024
            )

            mock_info.assert_called_once()
            call_args = mock_info.call_args[1]
            assert call_args["operation"] == "download_file"
            assert call_args["duration_ms"] == 2500.0
            assert call_args["success"] is True
            assert call_args["file_id"] == "123"
            assert call_args["bytes_downloaded"] == 1024

    def test_log_metric(self, logger):
        """Test logging metric."""
        with patch.object(logger.logger, "info") as mock_info:
            logger.log_metric("download_speed", 1024.0, "bytes/s", file_id="123")

            mock_info.assert_called_once()
            call_args = mock_info.call_args[1]
            assert call_args["metric_name"] == "download_speed"
            assert call_args["value"] == 1024.0
            assert call_args["unit"] == "bytes/s"
            assert call_args["file_id"] == "123"


class TestStructuredLogger:
    """Test StructuredLogger class."""

    @pytest.fixture
    def config(self):
        """Create a test configuration."""
        return LoggingConfig(
            level=LogLevel.INFO, format=LogFormat.JSON, enable_console=True
        )

    @pytest.fixture
    def logger(self, config):
        """Create a test logger."""
        return StructuredLogger(config)

    def test_structured_logger_initialization(self, logger, config):
        """Test StructuredLogger initialization."""
        assert logger.config == config

    def test_setup_logging(self, logger):
        """Test logging setup."""
        with patch("structlog.configure") as mock_configure:
            logger._setup_logging()
            mock_configure.assert_called_once()

    def test_add_correlation_id(self, logger):
        """Test adding correlation ID."""
        mock_logger = Mock()
        mock_event_dict = {}

        result = logger._add_correlation_id(mock_logger, "test_method", mock_event_dict)

        # The method is currently a stub, so it just returns the event_dict unchanged
        assert result == mock_event_dict

    def test_get_logger(self, logger):
        """Test getting logger."""
        with patch("structlog.get_logger") as mock_get_logger:
            mock_logger_instance = Mock()
            mock_get_logger.return_value = mock_logger_instance

            result = logger.get_logger("test_module")

            mock_get_logger.assert_called_with("test_module")
            assert result == mock_logger_instance


class TestLoggingManager:
    """Test LoggingManager class."""

    @pytest.fixture
    def config(self):
        """Create a test configuration."""
        return LoggingConfig(
            level=LogLevel.INFO,
            format=LogFormat.JSON,
            enable_console=True,
            enable_file=False,
        )

    @pytest.fixture
    def manager(self, config):
        """Create a test manager."""
        return LoggingManager(config)

    def test_logging_manager_initialization(self, manager, config):
        """Test LoggingManager initialization."""
        assert manager.config == config
        assert manager.request_logger is not None
        assert manager.performance_logger is not None
        assert manager.structured_logger is not None

    def test_setup_file_logging(self, manager):
        """Test file logging setup."""
        manager.config.enable_file = True
        # Use temp directory that works on all platforms
        temp_dir = Path(tempfile.gettempdir())
        manager.config.log_file = temp_dir / "test.log"

        with patch("logging.handlers.RotatingFileHandler") as mock_handler:
            with patch("logging.getLogger") as mock_get_logger:
                mock_logger = Mock()
                mock_get_logger.return_value = mock_logger

                manager._setup_file_logging()

                mock_handler.assert_called_once()
                mock_get_logger.assert_called_once()

    def test_get_logger(self, manager):
        """Test getting logger."""
        with patch.object(manager.structured_logger, "get_logger") as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger

            result = manager.get_logger("test_module")

            mock_get_logger.assert_called_with("test_module")
            assert result == mock_logger

    def test_log_request(self, manager):
        """Test logging request."""
        with patch.object(manager.request_logger, "log_request") as mock_log_request:
            manager.log_request(
                "GET", "https://api.example.com", {}, "body", "corr-123"
            )

            mock_log_request.assert_called_once_with(
                "GET", "https://api.example.com", {}, "body", "corr-123"
            )

    def test_log_response(self, manager):
        """Test logging response."""
        with patch.object(manager.request_logger, "log_response") as mock_log_response:
            manager.log_response(200, {}, "body", 1.5, "corr-123")

            mock_log_response.assert_called_once_with(200, {}, "body", 1.5, "corr-123")

    def test_log_operation_start(self, manager):
        """Test logging operation start."""
        with patch.object(
            manager.performance_logger, "log_operation_start"
        ) as mock_log_start:
            manager.log_operation_start("test_op", param="value")

            mock_log_start.assert_called_once_with("test_op", param="value")

    def test_log_operation_end(self, manager):
        """Test logging operation end."""
        with patch.object(
            manager.performance_logger, "log_operation_end"
        ) as mock_log_end:
            manager.log_operation_end("test_op", 2.0, True, param="value")

            mock_log_end.assert_called_once_with("test_op", 2.0, True, param="value")

    def test_log_metric(self, manager):
        """Test logging metric."""
        with patch.object(manager.performance_logger, "log_metric") as mock_log_metric:
            manager.log_metric("test_metric", 100.0, "units", param="value")

            mock_log_metric.assert_called_once_with(
                "test_metric", 100.0, "units", param="value"
            )


class TestCreateLoggingConfig:
    """Test create_logging_config function."""

    def test_create_logging_config_defaults(self):
        """Test creating config with default values."""
        config = create_logging_config()

        assert config.level == LogLevel.INFO
        assert config.format == LogFormat.JSON
        assert config.enable_console is True
        assert config.enable_file is False
        assert config.log_file is None
        assert config.enable_request_logging is False
        assert config.enable_performance_logging is True

    def test_create_logging_config_custom_values(self):
        """Test creating config with custom values."""
        log_file = Path("/tmp/custom.log")
        config = create_logging_config(
            level=LogLevel.DEBUG,
            format=LogFormat.CONSOLE,
            enable_console=False,
            enable_file=True,
            log_file=log_file,
            enable_request_logging=True,
            enable_performance_logging=False,
        )

        assert config.level == LogLevel.DEBUG
        assert config.format == LogFormat.CONSOLE
        assert config.enable_console is False
        assert config.enable_file is True
        assert config.log_file == log_file
        assert config.enable_request_logging is True
        assert config.enable_performance_logging is False


class TestCreateLoggingManager:
    """Test create_logging_manager function."""

    def test_create_logging_manager(self):
        """Test creating logging manager."""
        config = LoggingConfig()

        with patch("dataquery.logging_config.LoggingManager") as mock_manager_class:
            mock_manager = Mock()
            mock_manager_class.return_value = mock_manager

            result = create_logging_manager(config)

            mock_manager_class.assert_called_once_with(config)
            assert result == mock_manager


class TestLoggingIntegration:
    """Integration tests for logging."""

    def test_full_logging_workflow(self):
        """Test a complete logging workflow."""
        config = LoggingConfig(
            level=LogLevel.DEBUG,
            format=LogFormat.JSON,
            enable_console=True,
            enable_request_logging=True,
            enable_performance_logging=True,
        )

        manager = LoggingManager(config)

        # Test request/response logging
        with patch.object(manager.request_logger, "log_request") as mock_log_request:
            with patch.object(
                manager.request_logger, "log_response"
            ) as mock_log_response:
                manager.log_request(
                    "POST", "https://api.example.com/test", {}, "body", "corr-123"
                )
                manager.log_response(200, {}, "response", 1.5, "corr-123")

                mock_log_request.assert_called_once()
                mock_log_response.assert_called_once()

        # Test performance logging
        with patch.object(
            manager.performance_logger, "log_operation_start"
        ) as mock_log_start:
            with patch.object(
                manager.performance_logger, "log_operation_end"
            ) as mock_log_end:
                manager.log_operation_start("download", file_id="123")
                manager.log_operation_end("download", 2.0, True, file_id="123")

                mock_log_start.assert_called_once()
                mock_log_end.assert_called_once()

        # Test metric logging
        with patch.object(manager.performance_logger, "log_metric") as mock_log_metric:
            manager.log_metric("speed", 1024.0, "bytes/s")
            mock_log_metric.assert_called_once()

    def test_file_logging_setup(self):
        """Test file logging setup."""
        with tempfile.NamedTemporaryFile(suffix=".log") as temp_file:
            config = LoggingConfig(
                enable_file=True,
                log_file=Path(temp_file.name),
                max_file_size=1024,
                backup_count=2,
            )

            manager = LoggingManager(config)

            # Should not raise any exceptions
            assert manager.config.enable_file is True
            assert manager.config.log_file == Path(temp_file.name)
