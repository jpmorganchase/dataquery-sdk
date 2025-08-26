"""Tests for custom exceptions."""

import pytest
from dataquery.exceptions import (
    DataQueryError, AuthenticationError, ValidationError, NotFoundError,
    RateLimitError, NetworkError, ConfigurationError, DownloadError,
    AvailabilityError, GroupNotFoundError, FileNotFoundError, DateRangeError,
    FileTypeError, WorkflowError
)


class TestDataQueryError:
    """Test base DataQueryError class."""

    def test_data_query_error_basic(self):
        """Test basic DataQueryError creation."""
        error = DataQueryError("Test error")
        assert str(error) == "Test error"
        assert error.details == {}

    def test_data_query_error_with_details(self):
        """Test DataQueryError with details."""
        details = {"field": "test_field", "value": "test_value"}
        error = DataQueryError("Test error", details)
        assert "Test error - Details: {'field': 'test_field', 'value': 'test_value'}" in str(error)
        assert error.details == details


class TestAuthenticationError:
    """Test AuthenticationError class."""

    def test_authentication_error_basic(self):
        """Test basic AuthenticationError creation."""
        error = AuthenticationError()
        assert str(error) == "Authentication failed"

    def test_authentication_error_with_message(self):
        """Test AuthenticationError with custom message."""
        error = AuthenticationError("Custom auth error")
        assert str(error) == "Custom auth error"


class TestValidationError:
    """Test ValidationError class."""

    def test_validation_error_basic(self):
        """Test basic ValidationError creation."""
        error = ValidationError()
        assert str(error) == "Validation failed"

    def test_validation_error_with_message(self):
        """Test ValidationError with custom message."""
        error = ValidationError("Custom validation error")
        assert str(error) == "Custom validation error"


class TestNotFoundError:
    """Test NotFoundError class."""

    def test_not_found_error_basic(self):
        """Test basic NotFoundError creation."""
        error = NotFoundError("Resource", "test_id")
        assert "Resource not found: test_id" in str(error)
        assert error.details["resource_type"] == "Resource"
        assert error.details["resource_id"] == "test_id"

    def test_not_found_error_with_custom_message(self):
        """Test NotFoundError with custom message."""
        error = NotFoundError("Resource", "test_id", "Custom not found message")
        assert "Custom not found message - Details: {'resource_type': 'Resource', 'resource_id': 'test_id'}" in str(error)


class TestRateLimitError:
    """Test RateLimitError class."""

    def test_rate_limit_error_basic(self):
        """Test basic RateLimitError creation."""
        error = RateLimitError()
        assert str(error) == "Rate limit exceeded"

    def test_rate_limit_error_with_retry_after(self):
        """Test RateLimitError with retry_after."""
        error = RateLimitError("Rate limit exceeded", retry_after=60)
        assert "Rate limit exceeded - Details: {'retry_after': 60}" in str(error)
        assert error.details["retry_after"] == 60


class TestNetworkError:
    """Test NetworkError class."""

    def test_network_error_basic(self):
        """Test basic NetworkError creation."""
        error = NetworkError()
        assert str(error) == "Network error occurred"

    def test_network_error_with_status_code(self):
        """Test NetworkError with status code."""
        error = NetworkError("Network connection failed", status_code=500)
        assert "Network connection failed - Details: {'status_code': 500}" in str(error)
        assert error.details["status_code"] == 500


class TestConfigurationError:
    """Test ConfigurationError class."""

    def test_configuration_error_basic(self):
        """Test basic ConfigurationError creation."""
        error = ConfigurationError()
        assert str(error) == "Configuration error"

    def test_configuration_error_with_message(self):
        """Test ConfigurationError with custom message."""
        error = ConfigurationError("Custom config error")
        assert str(error) == "Custom config error"


class TestDownloadError:
    """Test DownloadError class."""

    def test_download_error_basic(self):
        """Test basic DownloadError creation."""
        error = DownloadError("test_file", "test_group")
        assert "Download failed - Details: {'file_group_id': 'test_file', 'group_id': 'test_group'}" in str(error)
        assert error.details["file_group_id"] == "test_file"
        assert error.details["group_id"] == "test_group"

    def test_download_error_with_custom_message(self):
        """Test DownloadError with custom message."""
        error = DownloadError("test_file", "test_group", "Custom download error")
        assert "Custom download error - Details: {'file_group_id': 'test_file', 'group_id': 'test_group'}" in str(error)


class TestAvailabilityError:
    """Test AvailabilityError class."""

    def test_availability_error_basic(self):
        """Test basic AvailabilityError creation."""
        error = AvailabilityError("test_file", "test_group")
        assert "Availability check failed - Details: {'file_group_id': 'test_file', 'group_id': 'test_group'}" in str(error)
        assert error.details["file_group_id"] == "test_file"
        assert error.details["group_id"] == "test_group"

    def test_availability_error_with_custom_message(self):
        """Test AvailabilityError with custom message."""
        error = AvailabilityError("test_file", "test_group", "Custom availability error")
        assert "Custom availability error - Details: {'file_group_id': 'test_file', 'group_id': 'test_group'}" in str(error)


class TestGroupNotFoundError:
    """Test GroupNotFoundError class."""

    def test_group_not_found_error_basic(self):
        """Test basic GroupNotFoundError creation."""
        error = GroupNotFoundError("test_group")
        assert "Group not found: test_group" in str(error)
        assert error.details["resource_type"] == "Group"
        assert error.details["resource_id"] == "test_group"

    def test_group_not_found_error_with_group_id(self):
        """Test GroupNotFoundError with group ID."""
        error = GroupNotFoundError("group123")
        assert "Group not found: group123" in str(error)
        assert error.details["resource_id"] == "group123"


class TestFileNotFoundError:
    """Test FileNotFoundError class."""

    def test_file_not_found_error_basic(self):
        """Test basic FileNotFoundError creation."""
        error = FileNotFoundError("test_file", "test_group")
        assert "File test_file not found in group test_group" in str(error)
        assert error.details["resource_type"] == "File"
        assert error.details["resource_id"] == "test_file"

    def test_file_not_found_error_with_file_info(self):
        """Test FileNotFoundError with file info."""
        error = FileNotFoundError("file123", "group456")
        assert "File file123 not found in group group456" in str(error)
        assert error.details["resource_id"] == "file123"


class TestDateRangeError:
    """Test DateRangeError class."""

    def test_date_range_error_basic(self):
        """Test basic DateRangeError creation."""
        error = DateRangeError("20240101", "20241231")
        assert "Invalid date range: 20240101 to 20241231" in str(error)
        assert error.details["start_date"] == "20240101"
        assert error.details["end_date"] == "20241231"

    def test_date_range_error_with_custom_message(self):
        """Test DateRangeError with custom message."""
        error = DateRangeError("20240101", "20241231", "Custom date range error")
        assert "Custom date range error - Details: {'start_date': '20240101', 'end_date': '20241231'}" in str(error)


class TestFileTypeError:
    """Test FileTypeError class."""

    def test_file_type_error_basic(self):
        """Test basic FileTypeError creation."""
        error = FileTypeError("xyz")
        assert "Invalid file type: xyz" in str(error)
        assert error.details["file_type"] == "xyz"

    def test_file_type_error_with_allowed_types(self):
        """Test FileTypeError with allowed types."""
        error = FileTypeError("xyz", ["csv", "parquet"])
        assert "Invalid file type: xyz. Allowed types: csv, parquet" in str(error)
        assert error.details["file_type"] == "xyz"
        assert error.details["allowed_types"] == ["csv", "parquet"]


class TestWorkflowError:
    """Test WorkflowError class."""

    def test_workflow_error_basic(self):
        """Test basic WorkflowError creation."""
        error = WorkflowError("test_workflow")
        assert "Workflow failed" in str(error)
        assert error.details["workflow_name"] == "test_workflow"

    def test_workflow_error_with_custom_message(self):
        """Test WorkflowError with custom message."""
        error = WorkflowError("download_workflow", "Custom workflow error")
        assert "Custom workflow error - Details: {'workflow_name': 'download_workflow'}" in str(error)
        assert error.details["workflow_name"] == "download_workflow"


class TestExceptionHierarchy:
    """Test exception inheritance hierarchy."""

    def test_exception_hierarchy(self):
        """Test that exceptions inherit correctly."""
        exceptions = [
            AuthenticationError("test"),
            ValidationError("test"),
            NotFoundError("Resource", "test_id"),
            RateLimitError("test"),
            NetworkError("test"),
            ConfigurationError("test"),
            DownloadError("test_file", "test_group"),
            AvailabilityError("test_file", "test_group"),
            GroupNotFoundError("test_group"),
            FileNotFoundError("test_file", "test_group"),
            DateRangeError("20240101", "20241231"),
            FileTypeError("xyz"),
            WorkflowError("test_workflow")
        ]
        
        for exc in exceptions:
            assert isinstance(exc, DataQueryError)

    def test_specific_inheritance(self):
        """Test specific inheritance relationships."""
        group_error = GroupNotFoundError("test_group")
        assert isinstance(group_error, NotFoundError)
        assert isinstance(group_error, DataQueryError)
        
        file_error = FileNotFoundError("test_file", "test_group")
        assert isinstance(file_error, NotFoundError)
        assert isinstance(file_error, DataQueryError)
        
        date_error = DateRangeError("20240101", "20241231")
        assert isinstance(date_error, ValidationError)
        assert isinstance(date_error, DataQueryError)
        
        file_type_error = FileTypeError("xyz")
        assert isinstance(file_type_error, ValidationError)
        assert isinstance(file_type_error, DataQueryError)


class TestExceptionStringRepresentation:
    """Test exception string representation."""

    def test_basic_exception_str(self):
        """Test basic exception string representation."""
        error = DataQueryError("Test error")
        assert str(error) == "Test error"

    def test_exception_str_with_details(self):
        """Test exception string representation with details."""
        error = DataQueryError("Test error", {"key": "value"})
        assert "Test error - Details: {'key': 'value'}" in str(error)

    def test_specific_exception_str(self):
        """Test specific exception string representation."""
        not_found_error = NotFoundError("Resource", "test")
        assert "Resource not found: test" in str(not_found_error)
        
        rate_limit_error = RateLimitError("Rate limit exceeded", retry_after=30)
        assert "Rate limit exceeded - Details: {'retry_after': 30}" in str(rate_limit_error)


class TestExceptionAttributes:
    """Test exception attributes."""

    def test_not_found_error_attributes(self):
        """Test NotFoundError attributes."""
        error = NotFoundError("Resource", "test_id")
        assert error.details["resource_type"] == "Resource"
        assert error.details["resource_id"] == "test_id"

    def test_rate_limit_error_attributes(self):
        """Test RateLimitError attributes."""
        error = RateLimitError("Rate limit exceeded", retry_after=30)
        assert error.details["retry_after"] == 30

    def test_network_error_attributes(self):
        """Test NetworkError attributes."""
        error = NetworkError("Network error", status_code=500)
        assert error.details["status_code"] == 500

    def test_configuration_error_attributes(self):
        """Test ConfigurationError attributes."""
        error = ConfigurationError("Config error")
        assert error.message == "Config error"

    def test_download_error_attributes(self):
        """Test DownloadError attributes."""
        error = DownloadError("file1", "group1")
        assert error.details["file_group_id"] == "file1"
        assert error.details["group_id"] == "group1"

    def test_availability_error_attributes(self):
        """Test AvailabilityError attributes."""
        error = AvailabilityError("file1", "group1")
        assert error.details["file_group_id"] == "file1"
        assert error.details["group_id"] == "group1"

    def test_date_range_error_attributes(self):
        """Test DateRangeError attributes."""
        error = DateRangeError("20240101", "20241231")
        assert error.details["start_date"] == "20240101"
        assert error.details["end_date"] == "20241231"

    def test_file_type_error_attributes(self):
        """Test FileTypeError attributes."""
        error = FileTypeError("xyz", ["csv", "parquet"])
        assert error.details["file_type"] == "xyz"
        assert error.details["allowed_types"] == ["csv", "parquet"]

    def test_workflow_error_attributes(self):
        """Test WorkflowError attributes."""
        error = WorkflowError("test_workflow", "Workflow error")
        assert error.details["workflow_name"] == "test_workflow"
        assert error.message == "Workflow error" 