"""
Custom exceptions for the DATAQUERY SDK.
"""

from typing import Any, Dict, Optional


class DataQueryError(Exception):
    """Base exception for all DATAQUERY SDK errors."""

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.details:
            return f"{self.message} - Details: {self.details}"
        return self.message


class AuthenticationError(DataQueryError):
    """Raised when authentication fails."""

    def __init__(
        self,
        message: str = "Authentication failed",
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message, details)


class ValidationError(DataQueryError):
    """Raised when input validation fails."""

    def __init__(
        self,
        message: str = "Validation failed",
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message, details)


class NotFoundError(DataQueryError):
    """Raised when a requested resource is not found."""

    def __init__(self, resource_type: str, resource_id: str, message: Optional[str] = None):
        if message is None:
            message = f"{resource_type} not found: {resource_id}"
        super().__init__(message, {"resource_type": resource_type, "resource_id": resource_id})


class RateLimitError(DataQueryError):
    """Raised when rate limits are exceeded."""

    def __init__(self, message: str = "Rate limit exceeded", retry_after: Optional[int] = None):
        details = {"retry_after": retry_after} if retry_after else {}
        super().__init__(message, details)


class NetworkError(DataQueryError):
    """Raised when network-related errors occur."""

    def __init__(self, message: str = "Network error occurred", status_code: Optional[int] = None):
        details = {"status_code": status_code} if status_code else {}
        super().__init__(message, details)


class ConfigurationError(DataQueryError):
    """Raised when configuration is invalid or missing."""

    def __init__(
        self,
        message: str = "Configuration error",
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message, details)


class DownloadError(DataQueryError):
    """Raised when file download fails."""

    def __init__(
        self,
        file_group_id: str,
        group_id: str,
        message: str = "Download failed",
        details: Optional[Dict[str, Any]] = None,
    ):
        if details is None:
            details = {}
        details.update({"file_group_id": file_group_id, "group_id": group_id})
        super().__init__(message, details)


class AvailabilityError(DataQueryError):
    """Raised when checking file availability fails."""

    def __init__(
        self,
        file_group_id: str,
        group_id: str,
        message: str = "Availability check failed",
        details: Optional[Dict[str, Any]] = None,
    ):
        if details is None:
            details = {}
        details.update({"file_group_id": file_group_id, "group_id": group_id})
        super().__init__(message, details)


class GroupNotFoundError(NotFoundError):
    """Raised when a group is not found."""

    def __init__(self, group_id: str):
        super().__init__("Group", group_id)


class FileNotFoundInGroupError(NotFoundError):
    """Raised when a file is not found within a group."""

    def __init__(self, file_group_id: str, group_id: str):
        super().__init__("File", file_group_id, f"File {file_group_id} not found in group {group_id}")


# Deprecated alias — avoids shadowing the built-in FileNotFoundError
FileNotFoundError = FileNotFoundInGroupError


class DateRangeError(ValidationError):
    """Raised when date range validation fails."""

    def __init__(self, start_date: str, end_date: str, message: Optional[str] = None):
        if message is None:
            message = f"Invalid date range: {start_date} to {end_date}"
        super().__init__(message, {"start_date": start_date, "end_date": end_date})


class FileTypeError(ValidationError):
    """Raised when file type validation fails."""

    def __init__(self, file_type: str, allowed_types: Optional[list] = None):
        message = f"Invalid file type: {file_type}"
        if allowed_types:
            message += f". Allowed types: {', '.join(allowed_types)}"
        super().__init__(message, {"file_type": file_type, "allowed_types": allowed_types})


class WorkflowError(DataQueryError):
    """Raised when workflow operations fail."""

    def __init__(
        self,
        workflow_name: str,
        message: str = "Workflow failed",
        details: Optional[Dict[str, Any]] = None,
    ):
        if details is None:
            details = {}
        details["workflow_name"] = workflow_name
        super().__init__(message, details)
