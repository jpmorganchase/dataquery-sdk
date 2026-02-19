"""
Targeted tests to improve client.py utility functions coverage.
"""

from pathlib import Path
from unittest.mock import Mock, patch

import aiohttp
import pytest

from dataquery.client import (
    format_file_size,
    get_filename_from_response,
    validate_attributes_list,
    validate_date_format,
    validate_file_datetime,
    validate_instruments_list,
)
from dataquery.exceptions import ValidationError
from dataquery.utils import parse_content_disposition, validate_required_param


class TestFormatFileSize:
    """Test format_file_size utility function."""

    def test_format_zero_bytes(self):
        """Test formatting zero bytes."""
        assert format_file_size(0) == "0 B"

    def test_format_bytes(self):
        """Test formatting small byte values."""
        assert format_file_size(512) == "512.00 B"
        assert format_file_size(1023) == "1023.00 B"

    def test_format_kilobytes(self):
        """Test formatting kilobyte values."""
        assert format_file_size(1024) == "1.00 KB"
        assert format_file_size(2048) == "2.00 KB"
        assert format_file_size(1536) == "1.50 KB"

    def test_format_megabytes(self):
        """Test formatting megabyte values."""
        assert format_file_size(1024 * 1024) == "1.00 MB"
        assert format_file_size(1024 * 1024 * 2.5) == "2.50 MB"

    def test_format_gigabytes(self):
        """Test formatting gigabyte values."""
        assert format_file_size(1024 * 1024 * 1024) == "1.00 GB"
        assert format_file_size(1024 * 1024 * 1024 * 3) == "3.00 GB"

    def test_format_terabytes(self):
        """Test formatting terabyte values."""
        assert format_file_size(1024 * 1024 * 1024 * 1024) == "1.00 TB"

    def test_format_very_large(self):
        """Test formatting very large values stays in TB."""
        very_large = 1024 * 1024 * 1024 * 1024 * 5
        result = format_file_size(very_large)
        assert "TB" in result
        assert "5.00 TB" == result


class TestParseContentDisposition:
    """Test parse_content_disposition utility function."""

    def test_parse_none_header(self):
        """Test parsing None header."""
        assert parse_content_disposition(None) is None
        assert parse_content_disposition("") is None

    def test_parse_filename_with_quotes(self):
        """Test parsing filename with quotes."""
        header = 'attachment; filename="test_file.csv"'
        assert parse_content_disposition(header) == "test_file.csv"

    def test_parse_filename_without_quotes(self):
        """Test parsing filename without quotes."""
        header = "attachment; filename=test_file.csv"
        assert parse_content_disposition(header) == "test_file.csv"

    def test_parse_filename_star_utf8(self):
        """Test parsing filename* with UTF-8 encoding."""
        header = "attachment; filename*=UTF-8''test%20file.csv"
        assert parse_content_disposition(header) == "test file.csv"

    def test_parse_filename_star_without_utf8(self):
        """Test parsing filename* without UTF-8 prefix."""
        header = "attachment; filename*=test%20file.csv"
        assert parse_content_disposition(header) == "test file.csv"

    def test_parse_complex_header(self):
        """Test parsing complex content disposition header."""
        header = "attachment; filename=\"data.csv\"; filename*=UTF-8''data%20file.csv"
        # Should prefer filename* over filename
        assert parse_content_disposition(header) == "data file.csv"

    def test_parse_no_filename(self):
        """Test parsing header without filename."""
        header = "attachment"
        assert parse_content_disposition(header) is None

    def test_parse_url_encoded_filename(self):
        """Test parsing URL-encoded filename."""
        header = 'attachment; filename="test%2Bfile.csv"'
        assert parse_content_disposition(header) == "test+file.csv"


class TestGetFilenameFromResponse:
    """Test get_filename_from_response utility function."""

    def test_filename_from_content_disposition(self):
        """Test extracting filename from Content-Disposition header."""
        response = Mock(spec=aiohttp.ClientResponse)
        response.headers = {"content-disposition": 'attachment; filename="data_2024.csv"'}

        filename = get_filename_from_response(response, "test-group", "20240101")
        assert filename == "data_2024.csv"

    def test_filename_fallback_with_datetime(self):
        """Test fallback filename generation with datetime."""
        response = Mock(spec=aiohttp.ClientResponse)
        response.headers = {}

        filename = get_filename_from_response(response, "economic-data", "20240101")
        assert filename == "economic-data_20240101.bin"

    def test_filename_fallback_without_datetime(self):
        """Test fallback filename generation without datetime."""
        response = Mock(spec=aiohttp.ClientResponse)
        response.headers = {}

        filename = get_filename_from_response(response, "market-data", None)
        assert filename == "market-data.bin"

    def test_filename_with_content_type_csv(self):
        """Test filename generation with CSV content type."""
        response = Mock(spec=aiohttp.ClientResponse)
        response.headers = {"content-type": "text/csv"}

        filename = get_filename_from_response(response, "data", "20240101")
        assert filename == "data_20240101.csv"

    def test_filename_with_content_type_json(self):
        """Test filename generation with JSON content type."""
        response = Mock(spec=aiohttp.ClientResponse)
        response.headers = {"content-type": "application/json"}

        filename = get_filename_from_response(response, "api-data", None)
        assert filename == "api-data.json"

    def test_filename_with_content_type_zip(self):
        """Test filename generation with ZIP content type."""
        response = Mock(spec=aiohttp.ClientResponse)
        response.headers = {"content-type": "application/zip"}

        filename = get_filename_from_response(response, "archive", "20240101")
        assert filename == "archive_20240101.zip"

    def test_filename_with_content_type_parameters(self):
        """Test filename generation with content type parameters."""
        response = Mock(spec=aiohttp.ClientResponse)
        response.headers = {"content-type": "text/csv; charset=utf-8"}

        filename = get_filename_from_response(response, "data", None)
        assert filename == "data.csv"

    def test_filename_with_unknown_content_type(self):
        """Test filename generation with unknown content type."""
        response = Mock(spec=aiohttp.ClientResponse)
        response.headers = {"content-type": "application/unknown"}

        filename = get_filename_from_response(response, "data", None)
        assert filename == "data.bin"


class TestValidateFileDateTime:
    """Test validate_file_datetime utility function."""

    def test_validate_empty_datetime(self):
        """Test validating empty datetime."""
        validate_file_datetime("")  # Should not raise
        validate_file_datetime(None)  # Should not raise

    def test_validate_yyyymmdd_format(self):
        """Test validating YYYYMMDD format."""
        validate_file_datetime("20240101")  # Should not raise
        validate_file_datetime("20231231")  # Should not raise

    def test_validate_yyyymmddthhmm_format(self):
        """Test validating YYYYMMDDTHHMM format."""
        validate_file_datetime("20240101T1030")  # Should not raise
        validate_file_datetime("20231225T2359")  # Should not raise

    def test_validate_yyyymmddthhmmss_format(self):
        """Test validating YYYYMMDDTHHMMSS format."""
        validate_file_datetime("20240101T103045")  # Should not raise
        validate_file_datetime("20231225T235959")  # Should not raise

    def test_validate_invalid_formats(self):
        """Test validating invalid formats."""
        invalid_formats = [
            "2024-01-01",  # Wrong separator
            "240101",  # Year too short
            "202401011",  # Too many digits
            "20240101T10",  # Incomplete time
            "invalid",  # Non-numeric
        ]

        for invalid_format in invalid_formats:
            with pytest.raises(ValueError):
                validate_file_datetime(invalid_format)


class TestValidateDateFormat:
    """Test validate_date_format utility function."""

    def test_validate_empty_date(self):
        """Test validating empty date."""
        validate_date_format("", "start_date")  # Should not raise
        validate_date_format(None, "end_date")  # Should not raise

    def test_validate_yyyymmdd_date(self):
        """Test validating YYYYMMDD date format."""
        validate_date_format("20240101", "start_date")  # Should not raise
        validate_date_format("20231231", "end_date")  # Should not raise

    def test_validate_today_format(self):
        """Test validating TODAY format."""
        validate_date_format("TODAY", "start_date")  # Should not raise

    def test_validate_today_minus_formats(self):
        """Test validating TODAY-Nx formats."""
        valid_formats = [
            "TODAY-1D",  # Days
            "TODAY-5W",  # Weeks
            "TODAY-3M",  # Months
            "TODAY-1Y",  # Years
            "TODAY-30D",  # Multiple digits
        ]

        for valid_format in valid_formats:
            validate_date_format(valid_format, "test_date")  # Should not raise

    def test_validate_invalid_date_formats(self):
        """Test validating invalid date formats."""
        invalid_formats = [
            "2024-01-01",  # Wrong separator
            "TODAY-",  # Incomplete relative
            "TODAY-1",  # Missing unit
            "TODAY-1X",  # Invalid unit
            "TOMORROW",  # Invalid keyword
            "invalid",  # Random string
        ]

        for invalid_format in invalid_formats:
            with pytest.raises(ValidationError):
                validate_date_format(invalid_format, "test_date")


class TestValidateRequiredParam:
    """Test validate_required_param utility function."""

    def test_validate_valid_string(self):
        """Test validating valid string parameter."""
        validate_required_param("valid_value", "param_name")  # Should not raise
        validate_required_param("  spaces  ", "param_name")  # Should not raise

    def test_validate_valid_non_string(self):
        """Test validating valid non-string parameter."""
        validate_required_param(123, "param_name")  # Should not raise
        validate_required_param([], "param_name")  # Should not raise
        validate_required_param({}, "param_name")  # Should not raise

    def test_validate_none_param(self):
        """Test validating None parameter."""
        with pytest.raises(ValidationError) as exc_info:
            validate_required_param(None, "test_param")
        assert "Required parameter 'test_param' cannot be empty" in str(exc_info.value)

    def test_validate_empty_string(self):
        """Test validating empty string parameter."""
        with pytest.raises(ValidationError) as exc_info:
            validate_required_param("", "test_param")
        assert "Required parameter 'test_param' cannot be empty" in str(exc_info.value)

    def test_validate_whitespace_string(self):
        """Test validating whitespace-only string parameter."""
        with pytest.raises(ValidationError) as exc_info:
            validate_required_param("   ", "test_param")
        assert "Required parameter 'test_param' cannot be empty" in str(exc_info.value)


class TestValidateInstrumentsList:
    """Test validate_instruments_list utility function."""

    def test_validate_valid_instruments_list(self):
        """Test validating valid instruments list."""
        instruments = ["AAPL", "GOOGL", "MSFT"]
        validate_instruments_list(instruments)  # Should not raise

    def test_validate_single_instrument(self):
        """Test validating single instrument list."""
        instruments = ["AAPL"]
        validate_instruments_list(instruments)  # Should not raise

    def test_validate_max_instruments(self):
        """Test validating maximum allowed instruments (20)."""
        instruments = [f"INST{i:02d}" for i in range(20)]  # Exactly 20
        validate_instruments_list(instruments)  # Should not raise

    def test_validate_empty_instruments_list(self):
        """Test validating empty instruments list."""
        with pytest.raises(ValidationError) as exc_info:
            validate_instruments_list([])
        assert "must be a non-empty list" in str(exc_info.value)

    def test_validate_none_instruments_list(self):
        """Test validating None instruments list."""
        with pytest.raises(ValidationError) as exc_info:
            validate_instruments_list(None)
        assert "must be a non-empty list" in str(exc_info.value)

    def test_validate_non_list_instruments(self):
        """Test validating non-list instruments."""
        with pytest.raises(ValidationError) as exc_info:
            validate_instruments_list("not_a_list")
        assert "must be a non-empty list" in str(exc_info.value)

    def test_validate_too_many_instruments(self):
        """Test validating too many instruments (>20)."""
        instruments = [f"INST{i:02d}" for i in range(21)]  # 21 instruments
        with pytest.raises(ValidationError) as exc_info:
            validate_instruments_list(instruments)
        assert "Maximum 20 instrument IDs are supported" in str(exc_info.value)

    def test_validate_empty_instrument_string(self):
        """Test validating list with empty instrument string."""
        instruments = ["AAPL", "", "GOOGL"]
        with pytest.raises(ValidationError) as exc_info:
            validate_instruments_list(instruments)
        assert "All instrument IDs must be non-empty strings" in str(exc_info.value)

    def test_validate_whitespace_instrument_string(self):
        """Test validating list with whitespace-only instrument string."""
        instruments = ["AAPL", "   ", "GOOGL"]
        with pytest.raises(ValidationError) as exc_info:
            validate_instruments_list(instruments)
        assert "All instrument IDs must be non-empty strings" in str(exc_info.value)

    def test_validate_non_string_instrument(self):
        """Test validating list with non-string instrument."""
        instruments = ["AAPL", 123, "GOOGL"]
        with pytest.raises(ValidationError) as exc_info:
            validate_instruments_list(instruments)
        assert "All instrument IDs must be non-empty strings" in str(exc_info.value)


class TestValidateAttributesList:
    """Test validate_attributes_list utility function."""

    def test_validate_valid_attributes_list(self):
        """Test validating valid attributes list."""
        attributes = ["price", "volume", "market_cap"]
        validate_attributes_list(attributes)  # Should not raise

    def test_validate_single_attribute(self):
        """Test validating single attribute list."""
        attributes = ["price"]
        validate_attributes_list(attributes)  # Should not raise

    def test_validate_empty_attributes_list(self):
        """Test validating empty attributes list."""
        with pytest.raises(ValidationError) as exc_info:
            validate_attributes_list([])
        assert "Attributes list cannot be empty" in str(exc_info.value)

    def test_validate_none_attributes_list(self):
        """Test validating None attributes list."""
        with pytest.raises(ValidationError) as exc_info:
            validate_attributes_list(None)
        assert "Attributes list cannot be empty" in str(exc_info.value)

    def test_validate_non_list_attributes(self):
        """Test validating non-list attributes."""
        with pytest.raises(ValidationError) as exc_info:
            validate_attributes_list("not_a_list")
        assert "Attributes list cannot be empty" in str(exc_info.value)

    def test_validate_empty_attribute_string(self):
        """Test validating list with empty attribute string."""
        attributes = ["price", "", "volume"]
        with pytest.raises(ValidationError) as exc_info:
            validate_attributes_list(attributes)
        assert "All attribute IDs must be non-empty strings" in str(exc_info.value)

    def test_validate_whitespace_attribute_string(self):
        """Test validating list with whitespace-only attribute string."""
        attributes = ["price", "   ", "volume"]
        with pytest.raises(ValidationError) as exc_info:
            validate_attributes_list(attributes)
        assert "All attribute IDs must be non-empty strings" in str(exc_info.value)

    def test_validate_non_string_attribute(self):
        """Test validating list with non-string attribute."""
        attributes = ["price", 456, "volume"]
        with pytest.raises(ValidationError) as exc_info:
            validate_attributes_list(attributes)
        assert "All attribute IDs must be non-empty strings" in str(exc_info.value)


class TestUtilityFunctionsIntegration:
    """Integration tests for utility functions."""

    def test_file_size_and_filename_integration(self):
        """Test file size formatting and filename extraction work together."""
        # Test file size formatting
        size = 1024 * 1024 * 2.5  # 2.5 MB
        formatted_size = format_file_size(int(size))
        assert "2.50 MB" == formatted_size

        # Test filename extraction
        response = Mock(spec=aiohttp.ClientResponse)
        response.headers = {
            "content-disposition": 'attachment; filename="large_data.csv"',
            "content-type": "text/csv",
        }

        filename = get_filename_from_response(response, "data", "20240101")
        assert filename == "large_data.csv"

    def test_validation_functions_integration(self):
        """Test validation functions work together."""
        # Test date validation
        validate_date_format("20240101", "start_date")
        validate_file_datetime("20240101T103045")

        # Test parameter validation
        validate_required_param("test_value", "param")
        validate_instruments_list(["AAPL", "GOOGL"])
        validate_attributes_list(["price", "volume"])

        # All should pass without errors

    def test_content_disposition_edge_cases(self):
        """Test content disposition parsing edge cases."""
        # Test case insensitive matching
        header = 'attachment; FILENAME="test.csv"'
        assert parse_content_disposition(header) == "test.csv"

        # Test with extra whitespace
        header = '  attachment  ;  filename="test.csv"  '
        assert parse_content_disposition(header) == "test.csv"

        # Test with multiple parameters
        header = 'attachment; size=1024; filename="test.csv"; charset=utf-8'
        assert parse_content_disposition(header) == "test.csv"
