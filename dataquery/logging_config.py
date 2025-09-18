"""
Enhanced logging configuration for the DATAQUERY SDK.

Provides structured logging, performance metrics, request/response logging,
and configurable log levels and formats.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional

import structlog

logger = structlog.get_logger(__name__)


class LogLevel(str, Enum):
    """Log levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogFormat(str, Enum):
    """Log formats."""

    JSON = "json"
    CONSOLE = "console"
    SIMPLE = "simple"


@dataclass
class LoggingConfig:
    """Configuration for logging."""

    level: LogLevel = LogLevel.INFO
    format: LogFormat = LogFormat.JSON
    enable_console: bool = True
    enable_file: bool = False
    log_file: Optional[Path] = None
    max_file_size: int = 10 * 1024 * 1024  # 10MB
    backup_count: int = 5
    enable_request_logging: bool = False
    enable_performance_logging: bool = True
    enable_metrics: bool = True
    include_timestamps: bool = True
    include_process_info: bool = True
    include_thread_info: bool = True
    custom_processors: list = field(default_factory=list)
    log_correlation_id: bool = True


class RequestResponseLogger:
    """Log HTTP requests and responses."""

    def __init__(self, config: LoggingConfig):
        self.config = config
        self.logger = structlog.get_logger(__name__)

    def log_request(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        body: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ):
        """Log HTTP request."""
        if not self.config.enable_request_logging:
            return

        log_data = {
            "event_type": "http_request",
            "method": method,
            "url": url,
            "headers": self._sanitize_headers(headers),
            "timestamp": datetime.now().isoformat(),
        }

        if body:
            log_data["body"] = self._truncate_body(body)

        if correlation_id:
            log_data["correlation_id"] = correlation_id

        self.logger.info("HTTP Request", **log_data)

    def log_response(
        self,
        status_code: int,
        headers: Dict[str, str],
        body: Optional[str] = None,
        duration: float = 0.0,
        correlation_id: Optional[str] = None,
    ):
        """Log HTTP response."""
        if not self.config.enable_request_logging:
            return

        log_data = {
            "event_type": "http_response",
            "status_code": status_code,
            "headers": self._sanitize_headers(headers),
            "duration_ms": round(duration * 1000, 2),
            "timestamp": datetime.now().isoformat(),
        }

        if body:
            log_data["body"] = self._truncate_body(body)

        if correlation_id:
            log_data["correlation_id"] = correlation_id

        # Use appropriate log level based on status code
        if status_code >= 500:
            self.logger.error("HTTP Response", **log_data)
        elif status_code >= 400:
            self.logger.warning("HTTP Response", **log_data)
        else:
            self.logger.info("HTTP Response", **log_data)

    def _sanitize_headers(self, headers: Dict[str, str]) -> Dict[str, str]:
        """Sanitize headers for logging."""
        sanitized = {}
        sensitive_headers = {"authorization", "cookie", "x-api-key"}

        for key, value in headers.items():
            if key.lower() in sensitive_headers:
                sanitized[key] = "***"
            else:
                sanitized[key] = value

        return sanitized

    def _truncate_body(self, body: str, max_length: int = 1000) -> str:
        """Truncate body for logging."""
        if len(body) <= max_length:
            return body
        return body[:max_length] + "... [truncated]"


class PerformanceLogger:
    """Log performance metrics."""

    def __init__(self, config: LoggingConfig):
        self.config = config
        self.logger = structlog.get_logger(__name__)
        self.metrics: Dict[str, Any] = {}

    def log_operation_start(self, operation: str, **kwargs):
        """Log start of operation."""
        if not self.config.enable_performance_logging:
            return

        self.metrics[operation] = {"start_time": datetime.now(), "kwargs": kwargs}

        self.logger.debug("Operation started", operation=operation, **kwargs)

    def log_operation_end(
        self, operation: str, duration: float, success: bool = True, **kwargs
    ):
        """Log end of operation."""
        if not self.config.enable_performance_logging:
            return

        if operation in self.metrics:
            start_time = self.metrics[operation]["start_time"]
            total_duration = (datetime.now() - start_time).total_seconds()

            log_data = {
                "operation": operation,
                "duration_ms": round(duration * 1000, 2),
                "total_duration_ms": round(total_duration * 1000, 2),
                "success": success,
                **kwargs,
            }

            if success:
                self.logger.info("Operation completed", **log_data)
            else:
                self.logger.warning("Operation failed", **log_data)

            # Clean up
            del self.metrics[operation]

    def log_metric(self, name: str, value: float, unit: str = "", **kwargs):
        """Log a performance metric."""
        if not self.config.enable_metrics:
            return

        self.logger.info(
            "Performance metric", metric_name=name, value=value, unit=unit, **kwargs
        )


class StructuredLogger:
    """Enhanced structured logger with correlation IDs and context."""

    def __init__(self, config: LoggingConfig):
        self.config = config
        self._setup_logging()

    def _setup_logging(self):
        """Setup structured logging configuration."""
        processors = [
            *self.config.custom_processors,
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
        ]

        if self.config.include_timestamps:
            processors.append(structlog.processors.TimeStamper(fmt="iso"))

        # Minimal and safe error processors
        processors.extend(
            [
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
            ]
        )

        if self.config.log_correlation_id:
            processors.append(self._add_correlation_id)

        if self.config.format == LogFormat.JSON:
            processors.append(structlog.processors.JSONRenderer())
        elif self.config.format == LogFormat.CONSOLE:
            processors.append(structlog.dev.ConsoleRenderer())
        else:
            processors.append(structlog.dev.ConsoleRenderer(colors=False))

        structlog.configure(
            processors=processors,
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )

    def _add_correlation_id(self, logger, method_name, event_dict):
        """Add correlation ID to log entries."""
        # This would be implemented to add correlation IDs
        # from context or generate new ones
        return event_dict

    def get_logger(self, name: Optional[str] = None) -> structlog.BoundLogger:
        """Get a structured logger."""
        return structlog.get_logger(name)


class LoggingManager:
    """Main logging manager for the DATAQUERY SDK."""

    def __init__(self, config: LoggingConfig):
        self.config = config
        self.structured_logger = StructuredLogger(config)
        self.request_logger = RequestResponseLogger(config)
        self.performance_logger = PerformanceLogger(config)

        # Setup file logging if enabled
        if config.enable_file and config.log_file:
            self._setup_file_logging()

        logger.info(
            "Logging manager initialized",
            level=config.level.value,
            format=config.format.value,
            enable_request_logging=config.enable_request_logging,
        )

    def _setup_file_logging(self):
        """Setup file logging with rotation."""
        try:
            from logging.handlers import RotatingFileHandler

            # Check if log_file is set
            if not self.config.log_file:
                logger.warning("File logging enabled but no log file specified")
                return

            # Ensure log directory exists
            self.config.log_file.parent.mkdir(parents=True, exist_ok=True)

            # Create rotating file handler
            handler = RotatingFileHandler(
                str(self.config.log_file),
                maxBytes=self.config.max_file_size,
                backupCount=self.config.backup_count,
            )

            # Set formatter
            if self.config.format == LogFormat.JSON:
                formatter = logging.Formatter("%(message)s")
            else:
                formatter = logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )

            handler.setFormatter(formatter)

            # Add to root logger
            root_logger = logging.getLogger()
            root_logger.addHandler(handler)
            root_logger.setLevel(getattr(logging, self.config.level.value))

            logger.info(
                "File logging configured",
                log_file=str(self.config.log_file),
                max_size=self.config.max_file_size,
                backup_count=self.config.backup_count,
            )

        except Exception as e:
            logger.warning("Failed to setup file logging", error=str(e))

    def get_logger(self, name: Optional[str] = None) -> structlog.BoundLogger:
        """Get a logger instance."""
        return self.structured_logger.get_logger(name)

    def log_request(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        body: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ):
        """Log HTTP request."""
        self.request_logger.log_request(method, url, headers, body, correlation_id)

    def log_response(
        self,
        status_code: int,
        headers: Dict[str, str],
        body: Optional[str] = None,
        duration: float = 0.0,
        correlation_id: Optional[str] = None,
    ):
        """Log HTTP response."""
        self.request_logger.log_response(
            status_code, headers, body, duration, correlation_id
        )

    def log_operation_start(self, operation: str, **kwargs):
        """Log operation start."""
        self.performance_logger.log_operation_start(operation, **kwargs)

    def log_operation_end(
        self, operation: str, duration: float, success: bool = True, **kwargs
    ):
        """Log operation end."""
        self.performance_logger.log_operation_end(
            operation, duration, success, **kwargs
        )

    def log_metric(self, name: str, value: float, unit: str = "", **kwargs):
        """Log performance metric."""
        self.performance_logger.log_metric(name, value, unit, **kwargs)


def create_logging_config(
    level: LogLevel = LogLevel.INFO,
    format: LogFormat = LogFormat.JSON,
    enable_console: bool = True,
    enable_file: bool = False,
    log_file: Optional[Path] = None,
    enable_request_logging: bool = False,
    enable_performance_logging: bool = True,
) -> LoggingConfig:
    """
    Create logging configuration.

    Args:
        level: Log level
        format: Log format
        enable_console: Whether to enable console logging
        enable_file: Whether to enable file logging
        log_file: Path to log file
        enable_request_logging: Whether to log HTTP requests/responses
        enable_performance_logging: Whether to log performance metrics

    Returns:
        Logging configuration
    """
    return LoggingConfig(
        level=level,
        format=format,
        enable_console=enable_console,
        enable_file=enable_file,
        log_file=log_file,
        enable_request_logging=enable_request_logging,
        enable_performance_logging=enable_performance_logging,
    )


def create_logging_manager(config: LoggingConfig) -> LoggingManager:
    """
    Create a logging manager with the specified configuration.

    Args:
        config: Logging configuration

    Returns:
        Configured logging manager
    """
    return LoggingManager(config)
