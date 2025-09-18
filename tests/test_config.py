"""Tests for configuration module."""

import os
import tempfile
from pathlib import Path
from unittest.mock import mock_open, patch

import pytest

from dataquery.config import EnvConfig
from dataquery.exceptions import ConfigurationError
from dataquery.models import ClientConfig


class TestEnvConfig:
    """Test EnvConfig class."""

    def test_env_config_class_methods(self):
        """Test EnvConfig class methods."""
        # Test get_env_var
        with patch.dict(os.environ, {"DATAQUERY_BASE_URL": "https://api.example.com"}):
            value = EnvConfig.get_env_var("BASE_URL")
            assert value == "https://api.example.com"

    def test_env_config_get_bool(self):
        """Test get_bool method."""
        with patch.dict(os.environ, {"DATAQUERY_OAUTH_ENABLED": "true"}):
            value = EnvConfig.get_bool("OAUTH_ENABLED")
            assert value is True

    def test_env_config_get_bool_false_values(self):
        """Test get_bool method with false values."""
        false_values = ["false", "0", "no", "off", "FALSE", "False"]
        for value in false_values:
            with patch.dict(os.environ, {"DATAQUERY_TEST": value}):
                result = EnvConfig.get_bool("TEST")
                assert result is False

    def test_env_config_get_bool_none_value(self):
        """Test get_bool method with None value."""
        with patch.dict(os.environ, {"DATAQUERY_TEST": ""}):
            result = EnvConfig.get_bool("TEST")
            assert result is False

    def test_env_config_get_int(self):
        """Test get_int method."""
        with patch.dict(os.environ, {"DATAQUERY_MAX_RETRIES": "5"}):
            value = EnvConfig.get_int("MAX_RETRIES")
            assert value == 5

    def test_env_config_get_int_invalid_value(self):
        """Test get_int method with invalid value."""
        with patch.dict(os.environ, {"DATAQUERY_MAX_RETRIES": "invalid"}):
            with pytest.raises(ConfigurationError, match="Invalid integer value"):
                EnvConfig.get_int("MAX_RETRIES")

    def test_env_config_get_int_none_value(self):
        """Test get_int method with None value."""
        with patch.dict(os.environ, {"DATAQUERY_MAX_RETRIES": ""}):
            value = EnvConfig.get_int("MAX_RETRIES")
            assert value == 0

    def test_env_config_get_float(self):
        """Test get_float method."""
        with patch.dict(os.environ, {"DATAQUERY_TIMEOUT": "30.5"}):
            value = EnvConfig.get_float("TIMEOUT")
            assert value == 30.5

    def test_env_config_get_float_invalid_value(self):
        """Test get_float method with invalid value."""
        with patch.dict(os.environ, {"DATAQUERY_TIMEOUT": "invalid"}):
            with pytest.raises(ConfigurationError, match="Invalid float value"):
                EnvConfig.get_float("TIMEOUT")

    def test_env_config_get_float_none_value(self):
        """Test get_float method with None value."""
        with patch.dict(os.environ, {"DATAQUERY_TIMEOUT": ""}):
            value = EnvConfig.get_float("TIMEOUT")
            assert value == 0.0

    def test_env_config_get_path(self):
        """Test get_path method."""
        with patch.dict(os.environ, {"DATAQUERY_DOWNLOAD_DIR": "/custom/downloads"}):
            value = EnvConfig.get_path("DOWNLOAD_DIR")
            assert value == Path("/custom/downloads")

    def test_env_config_get_path_none_value(self):
        """Test get_path method with None value."""
        with patch.dict(os.environ, {"DATAQUERY_DOWNLOAD_DIR": ""}):
            value = EnvConfig.get_path("DOWNLOAD_DIR")
            assert value == Path(".")

    def test_env_config_load_env_file(self):
        """Test load_env_file method."""
        with patch("dataquery.config.load_dotenv") as mock_load_dotenv:
            with patch("pathlib.Path.exists", return_value=True):
                EnvConfig.load_env_file()
                mock_load_dotenv.assert_called_once_with(Path(".env"))

    def test_env_config_load_env_file_custom_path(self):
        """Test load_env_file method with custom path."""
        custom_path = Path("/custom/.env")
        with patch("dataquery.config.load_dotenv") as mock_load_dotenv:
            with patch("pathlib.Path.exists", return_value=True):
                EnvConfig.load_env_file(custom_path)
                mock_load_dotenv.assert_called_once_with(custom_path)

    def test_env_config_load_env_file_not_exists(self):
        """Test load_env_file method when file doesn't exist."""
        with patch("dataquery.config.load_dotenv") as mock_load_dotenv:
            with patch("pathlib.Path.exists", return_value=False):
                EnvConfig.load_env_file()
                mock_load_dotenv.assert_not_called()

    def test_env_config_create_client_config(self):
        """Test create_client_config method."""
        with patch.dict(
            os.environ,
            {
                "DATAQUERY_BASE_URL": "https://api.example.com",
                "DATAQUERY_OAUTH_ENABLED": "true",
                "DATAQUERY_CLIENT_ID": "test_client",
                "DATAQUERY_CLIENT_SECRET": "test_secret",
            },
        ):
            config = EnvConfig.create_client_config()
            assert config.base_url == "https://api.example.com"
            assert config.oauth_enabled is True
            assert config.client_id == "test_client"
            assert config.client_secret == "test_secret"

    def test_env_config_create_client_config_missing_base_url(self):
        """Test create_client_config method with missing base URL."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(
                ConfigurationError,
                match="DATAQUERY_BASE_URL environment variable is required",
            ):
                EnvConfig.create_client_config()

    def test_env_config_create_client_config_auto_generate_oauth_url(self):
        """Test create_client_config method auto-generating OAuth URL."""
        with patch.dict(
            os.environ,
            {
                "DATAQUERY_BASE_URL": "https://api.example.com",
                "DATAQUERY_OAUTH_ENABLED": "true",
                "DATAQUERY_CLIENT_ID": "test_client",
                "DATAQUERY_CLIENT_SECRET": "test_secret",
            },
            clear=True,
        ):
            # Explicitly remove OAuth_TOKEN_URL to ensure auto-generation
            if "DATAQUERY_OAUTH_TOKEN_URL" in os.environ:
                del os.environ["DATAQUERY_OAUTH_TOKEN_URL"]
            config = EnvConfig.create_client_config()
            assert config.oauth_token_url == "https://api.example.com/oauth/token"

    def test_env_config_create_client_config_with_custom_grant_type(self):
        """Test create_client_config method with custom grant type."""
        with patch.dict(
            os.environ,
            {
                "DATAQUERY_BASE_URL": "https://api.example.com",
                "DATAQUERY_GRANT_TYPE": "password",
            },
        ):
            config = EnvConfig.create_client_config()
            assert config.grant_type == "password"

    def test_env_config_get_download_options(self):
        """Test get_download_options method."""
        options = EnvConfig.get_download_options()
        assert isinstance(options, dict)
        assert "enable_range_requests" in options
        assert "show_progress" in options

    def test_env_config_get_batch_download_options(self):
        """Test get_batch_download_options method."""
        options = EnvConfig.get_batch_download_options()
        assert isinstance(options, dict)
        assert "max_concurrent_downloads" in options
        assert "batch_size" in options

    def test_env_config_get_workflow_paths(self):
        """Test get_workflow_paths method."""
        paths = EnvConfig.get_workflow_paths()
        assert isinstance(paths, dict)
        assert "workflow" in paths
        assert "groups" in paths
        assert "availability" in paths
        assert "default" in paths

    def test_env_config_get_workflow_paths_custom_base_dir(self):
        """Test get_workflow_paths method with custom base directory."""
        with patch.dict(os.environ, {"DATAQUERY_DOWNLOAD_DIR": "/custom/downloads"}):
            paths = EnvConfig.get_workflow_paths()
            assert paths["base"] == Path("/custom/downloads")
            assert paths["workflow"] == Path("/custom/downloads/workflow")

    def test_env_config_get_token_storage_config(self):
        """Test get_token_storage_config method."""
        config = EnvConfig.get_token_storage_config()
        assert isinstance(config, dict)
        assert "enabled" in config
        assert "directory" in config

    def test_env_config_validate_config(self):
        """Test validate_config method."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )

        # Should not raise exception
        EnvConfig.validate_config(config)

    def test_env_config_validate_config_missing_base_url(self):
        """Test validate_config with missing base URL."""
        # Use a valid URL format but empty string to trigger our validation
        config = ClientConfig(
            base_url="https://api.example.com",  # Use valid URL to pass Pydantic validation
            oauth_enabled=False,
            bearer_token="test_token",
        )

        # Mock the base_url to be empty after creation to test our validation
        config.base_url = ""

        with pytest.raises(ConfigurationError, match="BASE_URL is required"):
            EnvConfig.validate_config(config)

    def test_env_config_validate_config_oauth_enabled_missing_credentials(self):
        """Test validate_config with OAuth enabled but missing credentials."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id=None,
            client_secret=None,
            oauth_token_url="https://api.example.com/oauth/token",
        )

        with pytest.raises(ConfigurationError, match="CLIENT_ID is required"):
            EnvConfig.validate_config(config)

    def test_env_config_validate_config_oauth_enabled_missing_client_secret(self):
        """Test validate_config with OAuth enabled but missing client secret."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret=None,
            oauth_token_url="https://api.example.com/oauth/token",
        )

        with pytest.raises(ConfigurationError, match="CLIENT_SECRET is required"):
            EnvConfig.validate_config(config)

    def test_env_config_validate_config_oauth_enabled_missing_token_url(self):
        """Test validate_config with OAuth enabled but missing token URL."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",  # Set valid URL first
        )

        # Mock the oauth_token_url to be None after creation
        config.oauth_token_url = None

        with pytest.raises(ConfigurationError, match="OAUTH_TOKEN_URL is required"):
            EnvConfig.validate_config(config)

    def test_env_config_validate_config_no_auth_method(self):
        """Test validate_config with no authentication method."""
        config = ClientConfig(
            base_url="https://api.example.com", oauth_enabled=False, bearer_token=None
        )

        with pytest.raises(
            ConfigurationError,
            match="Either OAuth credentials or Bearer token must be configured",
        ):
            EnvConfig.validate_config(config)

    def test_env_config_validate_config_invalid_timeout(self):
        """Test validate_config with invalid timeout."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=False,
            bearer_token="test_token",
            timeout=-1,
        )

        with pytest.raises(ConfigurationError, match="TIMEOUT must be positive"):
            EnvConfig.validate_config(config)

    def test_env_config_validate_config_invalid_max_retries(self):
        """Test validate_config with invalid max retries."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=False,
            bearer_token="test_token",
            max_retries=-1,
        )

        with pytest.raises(
            ConfigurationError, match="MAX_RETRIES must be non-negative"
        ):
            EnvConfig.validate_config(config)

    def test_env_config_validate_config_invalid_retry_delay(self):
        """Test validate_config with invalid retry delay."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=False,
            bearer_token="test_token",
            retry_delay=-1,
        )

        with pytest.raises(
            ConfigurationError, match="RETRY_DELAY must be non-negative"
        ):
            EnvConfig.validate_config(config)

    def test_env_config_validate_config_invalid_pool_connections(self):
        """Test validate_config with invalid pool connections."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=False,
            bearer_token="test_token",
            pool_connections=0,
        )

        with pytest.raises(
            ConfigurationError, match="POOL_CONNECTIONS must be positive"
        ):
            EnvConfig.validate_config(config)

    def test_env_config_validate_config_invalid_pool_maxsize(self):
        """Test validate_config with invalid pool maxsize."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=False,
            bearer_token="test_token",
            pool_maxsize=0,
        )

        with pytest.raises(ConfigurationError, match="POOL_MAXSIZE must be positive"):
            EnvConfig.validate_config(config)

    def test_env_config_validate_config_invalid_requests_per_minute(self):
        """Test validate_config with invalid requests per minute."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=False,
            bearer_token="test_token",
            requests_per_minute=0,
        )

        with pytest.raises(
            ConfigurationError, match="REQUESTS_PER_MINUTE must be positive"
        ):
            EnvConfig.validate_config(config)

    def test_env_config_validate_config_invalid_burst_capacity(self):
        """Test validate_config with invalid burst capacity."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=False,
            bearer_token="test_token",
            burst_capacity=0,
        )

        with pytest.raises(ConfigurationError, match="BURST_CAPACITY must be positive"):
            EnvConfig.validate_config(config)

    def test_env_config_create_env_template(self):
        """Test create_env_template method."""
        with tempfile.TemporaryDirectory() as temp_dir:
            template_path = Path(temp_dir) / ".env.template"
            result = EnvConfig.create_env_template(template_path)

            assert result == template_path
            assert template_path.exists()

            # Check content
            with open(template_path, "r") as f:
                content = f.read()

            assert "BASE_URL=" in content
            assert "OAUTH_ENABLED=" in content
            assert "CLIENT_ID=" in content

    def test_env_config_create_env_template_default_path(self):
        """Test create_env_template method with default path."""
        with patch("builtins.open", mock_open()) as mock_file:
            with patch("pathlib.Path.write_text") as mock_write:
                result = EnvConfig.create_env_template()
                assert result == Path(".env.template")
                mock_write.assert_called_once()

    def test_env_config_create_env_template_string_path(self):
        """Test create_env_template method with string path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            template_path = str(Path(temp_dir) / ".env.template")
            result = EnvConfig.create_env_template(template_path)

            assert result == Path(template_path)
            assert Path(template_path).exists()

    def test_env_config_get_all_env_vars(self):
        """Test get_all_env_vars method."""
        # Mock the environment to return some values
        with patch.dict(
            os.environ,
            {
                "DATAQUERY_BASE_URL": "https://api.example.com",
                "DATAQUERY_OAUTH_ENABLED": "true",
            },
        ):
            vars_dict = EnvConfig.get_all_env_vars()
            assert isinstance(vars_dict, dict)
            # The method returns keys without the prefix
            assert "BASE_URL" in vars_dict
            assert "OAUTH_ENABLED" in vars_dict

    def test_env_config_get_all_env_vars_empty(self):
        """Test get_all_env_vars method with no environment variables."""
        with patch.dict(os.environ, {}, clear=True):
            vars_dict = EnvConfig.get_all_env_vars()
            assert isinstance(vars_dict, dict)
            assert len(vars_dict) == 0

    def test_env_config_mask_secrets(self):
        """Test mask_secrets method."""
        config_dict = {
            "base_url": "https://api.example.com",
            "client_secret": "secret123",
            "bearer_token": "token123",
            "oauth_token_url": "https://api.example.com/oauth/token",
            # "scope": "data.read",
            "proxy_password": "pass123",  # This is not masked by the current implementation
        }

        masked = EnvConfig.mask_secrets(config_dict)

        assert masked["base_url"] == "https://api.example.com"
        assert masked["client_secret"] == "***"
        assert masked["bearer_token"] == "***"
        assert masked["oauth_token_url"] == "***"
        # scope removed
        # proxy_password is not in the sensitive_keys list, so it's not masked
        assert masked["proxy_password"] == "pass123"

    def test_env_config_mask_secrets_empty_values(self):
        """Test mask_secrets method with empty values."""
        config_dict = {"client_secret": "", "bearer_token": None, "oauth_token_url": ""}

        masked = EnvConfig.mask_secrets(config_dict)

        assert masked["client_secret"] == ""
        assert masked["bearer_token"] is None
        assert masked["oauth_token_url"] == ""

    def test_env_config_mask_secrets_no_sensitive_keys(self):
        """Test mask_secrets method with no sensitive keys."""
        config_dict = {
            "base_url": "https://api.example.com",
            "timeout": "30.0",
            "max_retries": "3",
        }

        masked = EnvConfig.mask_secrets(config_dict)

        assert masked == config_dict  # No changes should be made
