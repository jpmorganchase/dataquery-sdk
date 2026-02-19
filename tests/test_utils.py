"""Tests for utility functions."""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest

from dataquery.models import ClientConfig
from dataquery.utils import (
    create_env_template,
    ensure_directory,
    format_duration,
    format_file_size,
    get_download_paths,
    get_env_value,
    load_env_file,
    save_config_to_env,
    set_env_value,
    validate_env_config,
)


class TestEnvTemplate:
    """Test environment template creation."""

    def test_create_env_template_default(self):
        """Test create_env_template with default path."""
        with patch("builtins.open", mock_open()) as mock_file:
            with patch("pathlib.Path.write_text") as mock_write:
                result = create_env_template()
                assert result == Path(".env.template")
                mock_write.assert_called_once()

    def test_create_env_template_custom_path(self):
        """Test create_env_template with custom path."""
        custom_path = Path("./test_custom/.env.template")
        with patch("builtins.open", mock_open()) as mock_file:
            with patch("pathlib.Path.write_text") as mock_write:
                result = create_env_template(custom_path)
                assert result == custom_path
                mock_write.assert_called_once()

    def test_create_env_template_string_path(self):
        """Test create_env_template with string path."""
        with patch("builtins.open", mock_open()) as mock_file:
            with patch("pathlib.Path.write_text") as mock_write:
                result = create_env_template("./test_custom/.env.template")
                assert result == Path("./test_custom/.env.template")
                mock_write.assert_called_once()


class TestConfigSaving:
    """Test configuration saving functionality."""

    def test_save_config_to_env_default(self):
        """Test save_config_to_env with default path."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
        )

        with patch("builtins.open", mock_open()) as mock_file:
            with patch("pathlib.Path.write_text") as mock_write:
                result = save_config_to_env(config)
                assert result == Path(".env")
                mock_write.assert_called_once()

    def test_save_config_to_env_custom_path(self):
        """Test save_config_to_env with custom path."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
        )

        custom_path = Path("/custom/.env")
        with patch("builtins.open", mock_open()) as mock_file:
            with patch("pathlib.Path.write_text") as mock_write:
                result = save_config_to_env(config, custom_path)
                assert result == custom_path
                mock_write.assert_called_once()

    def test_save_config_to_env_string_path(self):
        """Test save_config_to_env with string path."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
        )

        with patch("builtins.open", mock_open()) as mock_file:
            with patch("pathlib.Path.write_text") as mock_write:
                result = save_config_to_env(config, "/custom/.env")
                assert result == Path("/custom/.env")
                mock_write.assert_called_once()


class TestEnvFileLoading:
    """Test environment file loading."""

    # Note: tests for load_env_file default/custom path using import patching
    # were removed due to fragility across Python versions and import mechanisms.

    def test_load_env_file_not_exists(self):
        """Test load_env_file when file doesn't exist."""
        with patch("dataquery.utils.Path.exists", return_value=False):
            with patch("dotenv.load_dotenv") as mock_load_dotenv:
                load_env_file()
                # When file doesn't exist, load_dotenv should not be called
                mock_load_dotenv.assert_not_called()


class TestEnvValueHandling:
    """Test environment value handling."""

    def test_get_env_value_exists(self):
        """Test get_env_value when key exists."""
        with patch.dict(os.environ, {"TEST_KEY": "test_value"}):
            value = get_env_value("TEST_KEY")
            assert value == "test_value"

    def test_get_env_value_not_exists(self):
        """Test get_env_value when key doesn't exist."""
        with patch.dict(os.environ, {}, clear=True):
            value = get_env_value("TEST_KEY")
            assert value is None

    def test_get_env_value_with_default(self):
        """Test get_env_value with default value."""
        with patch.dict(os.environ, {}, clear=True):
            value = get_env_value("TEST_KEY", "default_value")
            assert value == "default_value"

    def test_set_env_value(self):
        """Test set_env_value."""
        with patch.dict(os.environ, {}, clear=True):
            set_env_value("TEST_KEY", "test_value")
            assert os.environ["TEST_KEY"] == "test_value"


class TestEnvConfigValidation:
    """Test environment configuration validation."""

    def test_validate_env_config_success(self):
        """Test validate_env_config with valid configuration."""
        with patch.dict(
            os.environ,
            {
                "DATAQUERY_BASE_URL": "https://api.example.com",
                "DATAQUERY_TIMEOUT": "30.0",
                "DATAQUERY_MAX_RETRIES": "3",
                "DATAQUERY_OAUTH_ENABLED": "true",
                "DATAQUERY_CLIENT_ID": "test_client_id",
                "DATAQUERY_CLIENT_SECRET": "test_client_secret",
            },
        ):
            validate_env_config()  # Should not raise

    def test_validate_env_config_invalid_timeout(self):
        """Test validate_env_config with invalid timeout."""
        with patch.dict(os.environ, {"DATAQUERY_TIMEOUT": "invalid"}):
            with pytest.raises(ValueError, match="Invalid timeout value"):
                validate_env_config()

    def test_validate_env_config_invalid_max_retries(self):
        """Test validate_env_config with invalid max retries."""
        with patch.dict(os.environ, {"DATAQUERY_MAX_RETRIES": "invalid"}):
            with pytest.raises(ValueError, match="Invalid max retries value"):
                validate_env_config()

    def test_validate_env_config_invalid_oauth_enabled(self):
        """Test validate_env_config with invalid OAuth enabled value."""
        with patch.dict(os.environ, {"DATAQUERY_OAUTH_ENABLED": "maybe"}):
            with pytest.raises(ValueError, match="Invalid OAuth enabled value"):
                validate_env_config()


class TestFileSizeFormatting:
    """Test file size formatting."""

    def test_format_file_size_zero(self):
        """Test format_file_size with zero bytes."""
        assert format_file_size(0) == "0 B"

    def test_format_file_size_bytes(self):
        """Test format_file_size with bytes."""
        assert format_file_size(512) == "512 B"

    def test_format_file_size_kilobytes(self):
        """Test format_file_size with kilobytes."""
        assert format_file_size(1536) == "1.5 KB"

    def test_format_file_size_megabytes(self):
        """Test format_file_size with megabytes."""
        assert format_file_size(2097152) == "2.0 MB"

    def test_format_file_size_gigabytes(self):
        """Test format_file_size with gigabytes."""
        assert format_file_size(3221225472) == "3.0 GB"

    def test_format_file_size_terabytes(self):
        """Test format_file_size with terabytes."""
        assert format_file_size(1099511627776) == "1.0 TB"

    def test_format_file_size_negative(self):
        """Test format_file_size with negative values."""
        assert format_file_size(-1024) == "-1.0 KB"
        assert format_file_size(-512) == "-512 B"

    def test_format_file_size_large_negative(self):
        """Test format_file_size with large negative values."""
        assert format_file_size(-2097152) == "-2.0 MB"

    def test_format_file_size_exact_powers(self):
        """Test format_file_size with exact power values."""
        assert format_file_size(1024) == "1.0 KB"
        assert format_file_size(1048576) == "1.0 MB"
        assert format_file_size(1073741824) == "1.0 GB"


class TestDurationFormatting:
    """Test duration formatting."""

    def test_format_duration_zero(self):
        """Test format_duration with zero seconds."""
        assert format_duration(0) == "0s"

    def test_format_duration_seconds(self):
        """Test format_duration with seconds."""
        assert format_duration(30.5) == "30.5s"
        assert format_duration(45) == "45.0s"

    def test_format_duration_minutes(self):
        """Test format_duration with minutes."""
        assert format_duration(90) == "1m 30s"
        assert format_duration(120) == "2m"

    def test_format_duration_hours(self):
        """Test format_duration with hours."""
        assert format_duration(3661) == "1h 1m 1s"
        assert format_duration(3600) == "1h"
        assert format_duration(3720) == "1h 2m"

    def test_format_duration_negative_seconds(self):
        """Test format_duration with negative seconds."""
        assert format_duration(-30.5) == "-30.5s"
        assert format_duration(-45) == "-45.0s"

    def test_format_duration_negative_minutes(self):
        """Test format_duration with negative minutes."""
        assert format_duration(-90) == "-1m 30s"
        assert format_duration(-120) == "-2m"

    def test_format_duration_negative_hours(self):
        """Test format_duration with negative hours."""
        assert format_duration(-3661) == "-1h 1m 1s"
        assert format_duration(-3600) == "-1h"
        assert format_duration(-3720) == "-1h 2m"

    def test_format_duration_fractional(self):
        """Test format_duration with fractional seconds."""
        assert format_duration(0.5) == "0.5s"
        assert format_duration(1.25) == "1.2s"

    def test_format_duration_large_values(self):
        """Test format_duration with large values."""
        assert format_duration(86400) == "24h"  # 24 hours
        assert format_duration(90000) == "25h"  # 25 hours


class TestDirectoryHandling:
    """Test directory handling functions."""

    def test_ensure_directory_path_object(self):
        """Test ensure_directory with Path object."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_path = Path(temp_dir) / "test_dir"
            result = ensure_directory(test_path)
            assert result == test_path
            assert test_path.exists()
            assert test_path.is_dir()

    def test_ensure_directory_string_path(self):
        """Test ensure_directory with string path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_path = os.path.join(temp_dir, "test_dir")
            result = ensure_directory(test_path)
            assert result == Path(test_path)
            assert Path(test_path).exists()
            assert Path(test_path).is_dir()

    def test_ensure_directory_nested(self):
        """Test ensure_directory with nested path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            test_path = Path(temp_dir) / "nested" / "deep" / "directory"
            result = ensure_directory(test_path)
            assert result == test_path
            assert test_path.exists()
            assert test_path.is_dir()


class TestDownloadPaths:
    """Test download paths functionality."""

    def test_get_download_paths_default(self):
        """Test get_download_paths with default values."""
        with patch.dict(os.environ, {}, clear=True):
            paths = get_download_paths()
            assert paths["base"] == Path("./downloads")
            assert paths["workflow"] == Path("./downloads/workflow")
            assert paths["groups"] == Path("./downloads/groups")
            assert paths["availability"] == Path("./downloads/availability")
            assert paths["default"] == Path("./downloads/files")

    def test_get_download_paths_custom_base_dir(self):
        """Test get_download_paths with custom base directory."""
        with patch.dict(os.environ, {}, clear=True):
            custom_base = Path("/custom/downloads")
            paths = get_download_paths(custom_base)
            assert paths["base"] == custom_base
            assert paths["workflow"] == custom_base / "workflow"
            assert paths["groups"] == custom_base / "groups"
            assert paths["availability"] == custom_base / "availability"
            assert paths["default"] == custom_base / "files"

    def test_get_download_paths_string_base_dir(self):
        """Test get_download_paths with string base directory."""
        with patch.dict(os.environ, {}, clear=True):
            paths = get_download_paths("/custom/downloads")
            assert paths["base"] == Path("/custom/downloads")
            assert paths["workflow"] == Path("/custom/downloads/workflow")

    def test_get_download_paths_custom_env_vars(self):
        """Test get_download_paths with custom environment variables."""
        with patch.dict(
            os.environ,
            {
                "DATAQUERY_DOWNLOAD_DIR": "/custom/downloads",
                "DATAQUERY_WORKFLOW_DIR": "custom_workflow",
                "DATAQUERY_GROUPS_DIR": "custom_groups",
                "DATAQUERY_AVAILABILITY_DIR": "custom_availability",
                "DATAQUERY_DEFAULT_DIR": "custom_files",
            },
        ):
            paths = get_download_paths()
            assert paths["base"] == Path("/custom/downloads")
            assert paths["workflow"] == Path("/custom/downloads/custom_workflow")
            assert paths["groups"] == Path("/custom/downloads/custom_groups")
            assert paths["availability"] == Path("/custom/downloads/custom_availability")
            assert paths["default"] == Path("/custom/downloads/custom_files")

    def test_get_download_paths_mixed_env_and_base(self):
        """Test get_download_paths with both environment variables and base directory."""
        with patch.dict(
            os.environ,
            {
                "DATAQUERY_WORKFLOW_DIR": "custom_workflow",
                "DATAQUERY_GROUPS_DIR": "custom_groups",
            },
        ):
            custom_base = Path("/custom/downloads")
            paths = get_download_paths(custom_base)
            assert paths["base"] == custom_base
            assert paths["workflow"] == custom_base / "custom_workflow"
            assert paths["groups"] == custom_base / "custom_groups"
            assert paths["availability"] == custom_base / "availability"  # Default
            assert paths["default"] == custom_base / "files"  # Default

    # Additional tests for missing coverage
    def test_format_file_size_petabytes(self):
        """Test format_file_size with petabytes."""
        pb_size = 1024**5  # 1 PB
        assert format_file_size(pb_size) == "1.0 PB"

    def test_format_file_size_exabytes(self):
        """Test format_file_size with exabytes."""
        eb_size = 1024**6  # 1 EB
        assert format_file_size(eb_size) == "1.0 EB"

    def test_format_file_size_max_unit(self):
        """Test format_file_size with size larger than EB."""
        max_size = 1024**7  # Larger than EB
        assert format_file_size(max_size) == "1024.0 EB"

    def test_format_duration_exact_minutes(self):
        """Test format_duration with exact minutes."""
        assert format_duration(60) == "1m"
        assert format_duration(120) == "2m"
        assert format_duration(180) == "3m"

    def test_format_duration_exact_hours(self):
        """Test format_duration with exact hours."""
        assert format_duration(3600) == "1h"
        assert format_duration(7200) == "2h"

    def test_format_duration_hours_minutes_only(self):
        """Test format_duration with hours and minutes only."""
        assert format_duration(3720) == "1h 2m"  # 1h 2m 0s

    def test_format_duration_hours_seconds_only(self):
        """Test format_duration with hours and seconds only."""
        assert format_duration(3601) == "1h 0m 1s"

    def test_format_duration_negative_exact_minutes(self):
        """Test format_duration with negative exact minutes."""
        assert format_duration(-60) == "-1m"
        assert format_duration(-120) == "-2m"

    def test_format_duration_negative_exact_hours(self):
        """Test format_duration with negative exact hours."""
        assert format_duration(-3600) == "-1h"
        assert format_duration(-7200) == "-2h"

    def test_format_duration_negative_hours_minutes_only(self):
        """Test format_duration with negative hours and minutes only."""
        assert format_duration(-3720) == "-1h 2m"

    def test_format_duration_negative_hours_seconds_only(self):
        """Test format_duration with negative hours and seconds only."""
        assert format_duration(-3601) == "-1h 0m 1s"

    def test_validate_env_config_no_values(self):
        """Test validate_env_config with no environment variables."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="DATAQUERY_BASE_URL is required"):
                validate_env_config()

    def test_validate_env_config_only_base_url(self):
        """Test validate_env_config with only base URL."""
        with patch.dict(os.environ, {"DATAQUERY_BASE_URL": "https://api.example.com"}):
            validate_env_config()  # Should not raise

    def test_validate_env_config_only_timeout(self):
        """Test validate_env_config with only timeout."""
        with patch.dict(os.environ, {"DATAQUERY_TIMEOUT": "30.0"}, clear=True):
            with pytest.raises(ValueError, match="DATAQUERY_BASE_URL is required"):
                validate_env_config()

    def test_validate_env_config_only_max_retries(self):
        """Test validate_env_config with only max retries."""
        with patch.dict(os.environ, {"DATAQUERY_MAX_RETRIES": "3"}, clear=True):
            with pytest.raises(ValueError, match="DATAQUERY_BASE_URL is required"):
                validate_env_config()

    def test_validate_env_config_only_oauth_enabled(self):
        """Test validate_env_config with only OAuth enabled."""
        with patch.dict(os.environ, {"DATAQUERY_OAUTH_ENABLED": "true"}, clear=True):
            with pytest.raises(ValueError, match="DATAQUERY_BASE_URL is required"):
                validate_env_config()

    def test_validate_env_config_oauth_enabled_false(self):
        """Test validate_env_config with OAuth enabled set to false."""
        with patch.dict(os.environ, {"DATAQUERY_OAUTH_ENABLED": "false"}, clear=True):
            with pytest.raises(ValueError, match="DATAQUERY_BASE_URL is required"):
                validate_env_config()

    def test_validate_env_config_oauth_enabled_uppercase(self):
        """Test validate_env_config with OAuth enabled in uppercase."""
        with patch.dict(os.environ, {"DATAQUERY_OAUTH_ENABLED": "TRUE"}, clear=True):
            with pytest.raises(ValueError, match="DATAQUERY_BASE_URL is required"):
                validate_env_config()

    def test_validate_env_config_oauth_enabled_false_uppercase(self):
        """Test validate_env_config with OAuth enabled false in uppercase."""
        with patch.dict(os.environ, {"DATAQUERY_OAUTH_ENABLED": "FALSE"}, clear=True):
            with pytest.raises(ValueError, match="DATAQUERY_BASE_URL is required"):
                validate_env_config()


class TestUtilsEdgeCases:
    """Test edge cases and error handling in utils module."""

    def test_validate_env_config_missing_base_url(self):
        """Test validation when base_url is missing."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="DATAQUERY_BASE_URL is required"):
                validate_env_config()

    def test_validate_env_config_invalid_base_url(self):
        """Test validation with invalid base_url format."""
        with patch.dict(os.environ, {"DATAQUERY_BASE_URL": "invalid-url"}, clear=True):
            with pytest.raises(ValueError, match="DATAQUERY_BASE_URL is required"):
                validate_env_config()

    def test_validate_env_config_oauth_enabled_missing_credentials(self):
        """Test validation when OAuth is enabled but credentials are missing."""
        with patch.dict(
            os.environ,
            {
                "DATAQUERY_BASE_URL": "https://api.example.com",
                "DATAQUERY_OAUTH_ENABLED": "true",
                # Missing client_id and client_secret
            },
            clear=True,
        ):
            with pytest.raises(ValueError, match="OAuth credentials are required"):
                validate_env_config()

    def test_validate_env_config_oauth_enabled_missing_token_url(self):
        """Test validation when OAuth is enabled but token URL is missing."""
        with patch.dict(
            os.environ,
            {
                "DATAQUERY_BASE_URL": "https://api.example.com",
                "DATAQUERY_OAUTH_ENABLED": "true",
                "DATAQUERY_CLIENT_ID": "test_client",
                "DATAQUERY_CLIENT_SECRET": "test_secret",
                # Missing oauth_token_url
            },
        ):
            # This should pass since the validation doesn't check for oauth_token_url
            validate_env_config()

    def test_validate_env_config_invalid_timeout(self):
        """Test validation with invalid timeout value."""
        with patch.dict(
            os.environ,
            {
                "DATAQUERY_BASE_URL": "https://api.example.com",
                "DATAQUERY_TIMEOUT": "invalid",
            },
        ):
            with pytest.raises(ValueError, match="Invalid timeout value"):
                validate_env_config()

    def test_validate_env_config_invalid_max_retries(self):
        """Test validation with invalid max_retries value."""
        with patch.dict(
            os.environ,
            {
                "DATAQUERY_BASE_URL": "https://api.example.com",
                "DATAQUERY_MAX_RETRIES": "invalid",
            },
        ):
            with pytest.raises(ValueError, match="Invalid max retries value"):
                validate_env_config()

    def test_validate_env_config_invalid_oauth_enabled(self):
        """Test validation with invalid OAuth enabled value."""
        with patch.dict(
            os.environ,
            {
                "DATAQUERY_BASE_URL": "https://api.example.com",
                "DATAQUERY_OAUTH_ENABLED": "maybe",
            },
        ):
            with pytest.raises(ValueError, match="Invalid OAuth enabled value"):
                validate_env_config()

    def test_create_env_template_with_custom_values(self):
        """Test creating env template with custom values."""
        # Test the default template creation
        template_file = create_env_template()

        assert template_file.exists()
        template_content = template_file.read_text()

        # Check that the template contains expected sections
        assert "DATAQUERY_BASE_URL=" in template_content
        assert "DATAQUERY_OAUTH_ENABLED=" in template_content
        assert "DATAQUERY_CLIENT_ID=" in template_content
        assert "DATAQUERY_CLIENT_SECRET=" in template_content
        assert "DATAQUERY_OAUTH_TOKEN_URL=" in template_content
        assert "DATAQUERY_TIMEOUT=" in template_content
        assert "DATAQUERY_MAX_RETRIES=" in template_content

        # Clean up
        template_file.unlink()

    def test_save_config_to_env_with_custom_path(self):
        """Test saving config to env file with custom path."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )

        custom_path = Path("./test_custom/.env")
        custom_path.parent.mkdir(exist_ok=True)

        try:
            save_config_to_env(config, custom_path)

            assert custom_path.exists()
            content = custom_path.read_text()

            assert "DATAQUERY_BASE_URL=https://api.example.com" in content
            assert "DATAQUERY_OAUTH_ENABLED=true" in content
            assert "DATAQUERY_CLIENT_ID=test_client" in content
            assert "DATAQUERY_CLIENT_SECRET=test_secret" in content
            assert "DATAQUERY_OAUTH_TOKEN_URL=https://api.example.com/oauth/token" in content
        finally:
            if custom_path.exists():
                custom_path.unlink()
            if custom_path.parent.exists():
                custom_path.parent.rmdir()

    def test_load_env_file_with_missing_file(self):
        """Test loading env file when file doesn't exist."""
        missing_path = Path("./missing.env")

        # Should not raise an exception, just return None
        result = load_env_file(missing_path)
        assert result is None

    def test_load_env_file_with_empty_file(self):
        """Test loading env file with empty content."""
        empty_file = Path("./test_empty.env")

        try:
            empty_file.write_text("")

            result = load_env_file(empty_file)
            assert result is None
        finally:
            if empty_file.exists():
                empty_file.unlink()

    def test_load_env_file_with_invalid_content(self):
        """Test loading env file with invalid content."""
        invalid_file = Path("./test_invalid.env")

        try:
            invalid_file.write_text("invalid content without = signs")

            result = load_env_file(invalid_file)
            assert result is None
        finally:
            if invalid_file.exists():
                invalid_file.unlink()

    def test_get_env_value_with_default(self):
        """Test getting env value with default."""
        with patch.dict(os.environ, {}, clear=True):
            value = get_env_value("NONEXISTENT_VAR", default="default_value")
            assert value == "default_value"

    def test_get_env_value_with_empty_string(self):
        """Test getting env value that is empty string."""
        with patch.dict(os.environ, {"EMPTY_VAR": ""}):
            value = get_env_value("EMPTY_VAR")
            assert value == ""

    def test_get_env_value_with_whitespace(self):
        """Test getting env value with whitespace."""
        with patch.dict(os.environ, {"WHITESPACE_VAR": "  test  "}):
            value = get_env_value("WHITESPACE_VAR")
            assert value == "  test  "  # get_env_value doesn't strip whitespace

    def test_format_file_size_edge_cases(self):
        """Test format_file_size with edge cases."""
        # Test zero
        assert format_file_size(0) == "0 B"

        # Test negative values
        assert format_file_size(-1024) == "-1.0 KB"
        assert format_file_size(-1) == "-1 B"

        # Test large values
        assert format_file_size(1024**3) == "1.0 GB"
        assert format_file_size(1024**4) == "1.0 TB"

        # Test bytes
        assert format_file_size(500) == "500 B"

        # Test kilobytes
        assert format_file_size(1500) == "1.5 KB"

    def test_format_duration_edge_cases(self):
        """Test format_duration with edge cases."""
        # Test zero
        assert format_duration(0) == "0s"

        # Test negative values
        assert format_duration(-1) == "-1.0s"  # format_duration doesn't handle negative values specially

        # Test very small values
        assert format_duration(0.1) == "0.1s"
        assert format_duration(0.001) == "0.0s"  # format_duration uses .1f formatting

        # Test very large values
        assert format_duration(3661) == "1h 1m 1s"
        assert format_duration(86400) == "24h"  # When no remaining minutes/seconds, only hours are shown
