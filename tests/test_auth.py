"""Tests for authentication module."""

import json
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, mock_open, patch

import pytest

from dataquery.auth import OAuthManager, TokenManager
from dataquery.exceptions import AuthenticationError, ConfigurationError
from dataquery.models import ClientConfig, OAuthToken, TokenResponse


class TestOAuthManager:
    """Test OAuthManager class."""

    def test_oauth_manager_initialization(self):
        """Test OAuthManager initialization."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )

        oauth_manager = OAuthManager(config)
        assert oauth_manager.config == config
        assert oauth_manager.token_manager is not None

    @pytest.mark.asyncio
    async def test_oauth_manager_authenticate(self):
        """Test OAuthManager authenticate method."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )

        oauth_manager = OAuthManager(config)

        with patch.object(
            oauth_manager.token_manager, "get_valid_token"
        ) as mock_get_token:
            mock_get_token.return_value = "Bearer test_token"

            token = await oauth_manager.authenticate()
            assert token == "Bearer test_token"
            mock_get_token.assert_called_once()

    @pytest.mark.asyncio
    async def test_oauth_manager_authenticate_failure(self):
        """Test OAuthManager authenticate method when token manager returns None."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )

        oauth_manager = OAuthManager(config)

        with patch.object(
            oauth_manager.token_manager, "get_valid_token"
        ) as mock_get_token:
            mock_get_token.return_value = None

            with pytest.raises(
                AuthenticationError, match="Failed to obtain valid authentication token"
            ):
                await oauth_manager.authenticate()

    @pytest.mark.asyncio
    async def test_oauth_manager_get_headers(self):
        """Test OAuthManager get_headers method."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )

        oauth_manager = OAuthManager(config)

        with patch.object(oauth_manager, "authenticate") as mock_authenticate:
            mock_authenticate.return_value = "Bearer test_token"

            headers = await oauth_manager.get_headers()
            assert headers == {"Authorization": "Bearer test_token"}
            mock_authenticate.assert_called_once()

    def test_oauth_manager_is_authenticated_with_oauth(self):
        """Test OAuthManager is_authenticated method with OAuth."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )

        oauth_manager = OAuthManager(config)
        assert oauth_manager.is_authenticated() is True

    def test_oauth_manager_is_authenticated_with_bearer(self):
        """Test OAuthManager is_authenticated method with bearer token."""
        config = ClientConfig(
            base_url="https://api.example.com", bearer_token="test_bearer_token"
        )

        oauth_manager = OAuthManager(config)
        assert oauth_manager.is_authenticated() is True

    def test_oauth_manager_is_authenticated_no_auth(self):
        """Test OAuthManager is_authenticated method with no auth."""
        config = ClientConfig(base_url="https://api.example.com")

        oauth_manager = OAuthManager(config)
        assert oauth_manager.is_authenticated() is False

    def test_oauth_manager_get_auth_info(self):
        """Test OAuthManager get_auth_info method."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )

        oauth_manager = OAuthManager(config)

        with patch.object(
            oauth_manager.token_manager, "get_token_info"
        ) as mock_get_info:
            mock_get_info.return_value = {
                "status": "valid",
                "token_type": "Bearer",
                "issued_at": "2023-12-31T23:59:59",
                "expires_at": "2024-01-01T00:59:59",
                "is_expired": False,
                "has_refresh_token": True,
            }

            auth_info = oauth_manager.get_auth_info()
            assert auth_info["oauth_enabled"] is True
            assert auth_info["has_oauth_credentials"] is True
            assert auth_info["has_bearer_token"] is False
            assert auth_info["oauth_token_url"] == "https://api.example.com/oauth/token"
            assert auth_info["grant_type"] == "client_credentials"
            assert auth_info["token_info"]["status"] == "valid"
            mock_get_info.assert_called_once()

    def test_oauth_manager_get_auth_info_bearer(self):
        """Test OAuthManager get_auth_info method with bearer token."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=False,
            bearer_token="test_bearer_token",
            oauth_token_url=None,
        )

        oauth_manager = OAuthManager(config)
        auth_info = oauth_manager.get_auth_info()
        assert auth_info["oauth_enabled"] is False
        assert auth_info["has_oauth_credentials"] is False
        assert auth_info["has_bearer_token"] is True
        assert auth_info["oauth_token_url"] is None
        assert auth_info["grant_type"] == "client_credentials"

    @pytest.mark.asyncio
    async def test_oauth_manager_test_authentication_success(self):
        """Test OAuthManager test_authentication method with success."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )

        oauth_manager = OAuthManager(config)

        with patch.object(oauth_manager, "authenticate") as mock_authenticate:
            mock_authenticate.return_value = "Bearer test_token"

            result = await oauth_manager.test_authentication()
            assert result is True
            mock_authenticate.assert_called_once()

    @pytest.mark.asyncio
    async def test_oauth_manager_test_authentication_failure(self):
        """Test OAuthManager test_authentication method with failure."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )

        oauth_manager = OAuthManager(config)

        with patch.object(oauth_manager, "authenticate") as mock_authenticate:
            mock_authenticate.side_effect = AuthenticationError("Auth failed")

            result = await oauth_manager.test_authentication()
            assert result is False
            mock_authenticate.assert_called_once()

    def test_oauth_manager_clear_authentication(self):
        """Test OAuthManager clear_authentication method."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )

        oauth_manager = OAuthManager(config)

        with patch.object(oauth_manager.token_manager, "clear_token") as mock_clear:
            oauth_manager.clear_authentication()
            mock_clear.assert_called_once()


class TestTokenManager:
    """Test TokenManager class."""

    def test_token_manager_initialization(self):
        """Test TokenManager initialization."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
            download_dir="./downloads",
        )

        token_manager = TokenManager(config)
        assert token_manager.config == config
        assert token_manager.current_token is None
        assert token_manager.token_file is not None

    def test_token_manager_initialization_without_download_dir(self):
        """Test TokenManager initialization without download directory."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
            download_dir="",
        )

        token_manager = TokenManager(config)
        assert token_manager.config == config
        assert token_manager.current_token is None
        assert token_manager.token_file is None

    @pytest.mark.asyncio
    async def test_token_manager_get_valid_token_bearer(self):
        """Test TokenManager get_valid_token method with bearer token."""
        config = ClientConfig(
            base_url="https://api.example.com", bearer_token="test_bearer_token"
        )

        token_manager = TokenManager(config)
        token = await token_manager.get_valid_token()
        assert token == "Bearer test_bearer_token"

    @pytest.mark.asyncio
    async def test_token_manager_get_valid_token_none(self):
        """Test TokenManager get_valid_token method with no auth."""
        config = ClientConfig(base_url="https://api.example.com")

        token_manager = TokenManager(config)
        token = await token_manager.get_valid_token()
        assert token is None

    @pytest.mark.asyncio
    async def test_token_manager_get_valid_token_oauth_no_credentials(self):
        """Test TokenManager get_valid_token method with OAuth but no credentials."""
        config = ClientConfig(base_url="https://api.example.com", oauth_enabled=True)

        token_manager = TokenManager(config)

        with patch("dataquery.auth.logger") as mock_logger:
            token = await token_manager.get_valid_token()
            assert token is None
            mock_logger.warning.assert_called_once()

    @pytest.mark.asyncio
    async def test_token_manager_get_valid_token_oauth_with_credentials(self):
        """Test TokenManager get_valid_token method with OAuth credentials."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )

        token_manager = TokenManager(config)

        with patch.object(token_manager, "_get_new_token") as mock_get_new:
            mock_token = MagicMock()
            mock_token.to_authorization_header.return_value = "Bearer test_token"
            mock_get_new.return_value = mock_token

            # Set the current_token directly since _get_new_token sets it
            token_manager.current_token = mock_token

            token = await token_manager.get_valid_token()
            assert token == "Bearer test_token"
            mock_get_new.assert_called_once()

    @pytest.mark.asyncio
    async def test_token_manager_get_valid_token_with_expiring_token(self):
        """Test TokenManager get_valid_token method with expiring token."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
            token_refresh_threshold=300,
        )

        token_manager = TokenManager(config)

        # Create a token that's expiring soon
        mock_token = MagicMock()
        mock_token.is_expired = False
        mock_token.is_expiring_soon.return_value = True
        mock_token.to_authorization_header.return_value = "Bearer test_token"
        token_manager.current_token = mock_token

        with patch.object(token_manager, "_refresh_token") as mock_refresh:
            mock_refresh.return_value = mock_token

            token = await token_manager.get_valid_token()
            assert token == "Bearer test_token"
            mock_refresh.assert_called_once()

    @pytest.mark.asyncio
    async def test_token_manager_get_valid_token_with_expired_token(self):
        """Test TokenManager get_valid_token method with expired token."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )

        token_manager = TokenManager(config)

        # Create an expired token
        mock_token = MagicMock()
        mock_token.is_expired = True
        token_manager.current_token = mock_token

        with patch.object(token_manager, "_get_new_token") as mock_get_new:
            mock_get_new.return_value = mock_token
            mock_token.to_authorization_header.return_value = "Bearer test_token"

            token = await token_manager.get_valid_token()
            assert token == "Bearer test_token"
            mock_get_new.assert_called_once()

    @pytest.mark.asyncio
    async def test_token_manager_get_new_token_no_token_url(self):
        """Test TokenManager _get_new_token method with no token URL."""
        config = ClientConfig(
            base_url="",  # Empty base URL to prevent auto-generation
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url=None,
        )

        token_manager = TokenManager(config)

        with pytest.raises(ConfigurationError, match="OAuth token URL not configured"):
            await token_manager._get_new_token()

    @pytest.mark.asyncio
    async def test_token_manager_get_new_token_no_credentials(self):
        """Test TokenManager _get_new_token method with no credentials."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            oauth_token_url="https://api.example.com/oauth/token",
        )

        token_manager = TokenManager(config)

        with pytest.raises(
            ConfigurationError,
            match="client_id and client_secret are required for OAuth",
        ):
            await token_manager._get_new_token()

    @pytest.mark.asyncio
    async def test_token_manager_get_new_token_exception(self):
        """Test TokenManager _get_new_token method with exception."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )

        token_manager = TokenManager(config)

        with patch("aiohttp.ClientSession", side_effect=Exception("Network error")):
            with pytest.raises(
                AuthenticationError, match="Failed to get OAuth token: Network error"
            ):
                await token_manager._get_new_token()

    @pytest.mark.asyncio
    async def test_token_manager_refresh_token_no_refresh_token(self):
        """Test TokenManager _refresh_token method with no refresh token."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )

        token_manager = TokenManager(config)

        # Create a token without refresh token
        mock_token = MagicMock()
        mock_token.refresh_token = None
        token_manager.current_token = mock_token

        with patch.object(token_manager, "_get_new_token") as mock_get_new:
            mock_get_new.return_value = mock_token

            token = await token_manager._refresh_token()
            assert token == mock_token
            mock_get_new.assert_called_once()

    @pytest.mark.asyncio
    async def test_token_manager_refresh_token_no_current_token(self):
        """Test TokenManager _refresh_token method with no current token."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )

        token_manager = TokenManager(config)
        token_manager.current_token = None

        with patch.object(token_manager, "_get_new_token") as mock_get_new:
            mock_token = MagicMock()
            mock_get_new.return_value = mock_token

            token = await token_manager._refresh_token()
            assert token == mock_token
            mock_get_new.assert_called_once()

    @pytest.mark.asyncio
    async def test_token_manager_refresh_token_failure_fallback(self):
        """Test TokenManager _refresh_token method with failure and fallback."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )

        token_manager = TokenManager(config)

        # Create a token with refresh token
        mock_token = MagicMock()
        mock_token.refresh_token = "test_refresh_token"
        token_manager.current_token = mock_token

        mock_response = MagicMock()
        mock_response.status = 400
        mock_response.text.return_value = "Invalid refresh token"

        mock_session = AsyncMock()
        mock_session.__aenter__.return_value.post.return_value.__aenter__.return_value = (
            mock_response
        )

        with patch("aiohttp.ClientSession", return_value=mock_session):
            with patch.object(token_manager, "_get_new_token") as mock_get_new:
                mock_get_new.return_value = mock_token

                token = await token_manager._refresh_token()
                assert token == mock_token
                mock_get_new.assert_called_once()

    @pytest.mark.asyncio
    async def test_token_manager_refresh_token_exception_fallback(self):
        """Test TokenManager _refresh_token method with exception and fallback."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )

        token_manager = TokenManager(config)

        # Create a token with refresh token
        mock_token = MagicMock()
        mock_token.refresh_token = "test_refresh_token"
        token_manager.current_token = mock_token

        with patch("aiohttp.ClientSession", side_effect=Exception("Network error")):
            with patch.object(token_manager, "_get_new_token") as mock_get_new:
                mock_get_new.return_value = mock_token

                token = await token_manager._refresh_token()
                assert token == mock_token
                mock_get_new.assert_called_once()

    @pytest.mark.asyncio
    async def test_token_manager_refresh_token_no_token_url(self):
        """Test TokenManager _refresh_token method with no token URL."""
        config = ClientConfig(
            base_url="",  # Empty base URL to prevent auto-generation
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url=None,
        )

        token_manager = TokenManager(config)

        # Create a token with refresh token
        mock_token = MagicMock()
        mock_token.refresh_token = "test_refresh_token"
        token_manager.current_token = mock_token

        with pytest.raises(ConfigurationError, match="OAuth token URL not configured"):
            await token_manager._refresh_token()

    @pytest.mark.asyncio
    async def test_token_manager_load_token_success(self):
        """Test TokenManager _load_token method with success."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
            download_dir="./downloads",
        )

        token_manager = TokenManager(config)

        # Create token data
        token_data = {
            "access_token": "test_access_token",
            "token_type": "Bearer",
            "expires_at": (datetime.now() + timedelta(hours=1)).isoformat(),
            "refresh_token": "test_refresh_token",
        }

        with patch("pathlib.Path.exists", return_value=True):
            with patch("builtins.open", mock_open(read_data=json.dumps(token_data))):
                await token_manager._load_token()

                assert token_manager.current_token is not None
                assert token_manager.current_token.access_token == "test_access_token"

    @pytest.mark.asyncio
    async def test_token_manager_load_token_file_not_exists(self):
        """Test TokenManager _load_token method when file doesn't exist."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
            download_dir="./downloads",
        )

        token_manager = TokenManager(config)

        with patch("pathlib.Path.exists", return_value=False):
            await token_manager._load_token()
            assert token_manager.current_token is None

    @pytest.mark.asyncio
    async def test_token_manager_load_token_expired(self):
        """Test TokenManager _load_token method with expired token."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
            download_dir="./downloads",
        )

        token_manager = TokenManager(config)

        # Create expired token data - use expires_in instead of expires_at
        # Set issued_at to 2 hours ago and expires_in to 1 hour to make it expired
        issued_at = (datetime.now() - timedelta(hours=2)).isoformat()
        token_data = {
            "access_token": "test_access_token",
            "token_type": "Bearer",
            "expires_in": 3600,  # 1 hour in seconds
            "issued_at": issued_at,
            "refresh_token": "test_refresh_token",
        }

        with patch("pathlib.Path.exists", return_value=True):
            with patch("builtins.open", mock_open(read_data=json.dumps(token_data))):
                await token_manager._load_token()
                # The token should be loaded but then set to None because it's expired
                assert token_manager.current_token is None

    @pytest.mark.asyncio
    async def test_token_manager_load_token_exception(self):
        """Test TokenManager _load_token method with exception."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
            download_dir="./downloads",
        )

        token_manager = TokenManager(config)

        with patch("pathlib.Path.exists", return_value=True):
            with patch("builtins.open", side_effect=Exception("File read error")):
                await token_manager._load_token()
                assert token_manager.current_token is None

    @pytest.mark.asyncio
    async def test_token_manager_save_token(self):
        """Test TokenManager _save_token method."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
            download_dir="./downloads",
        )

        token_manager = TokenManager(config)

        # Create a token
        mock_token = MagicMock()
        mock_token.access_token = "test_access_token"
        mock_token.token_type = "Bearer"
        mock_token.expires_at = datetime.now() + timedelta(hours=1)
        mock_token.refresh_token = "test_refresh_token"
        token_manager.current_token = mock_token

        with patch("pathlib.Path.mkdir") as mock_mkdir:
            with patch("builtins.open", mock_open()) as mock_file:
                await token_manager._save_token()

                mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
                mock_file.assert_called_once()

    @pytest.mark.asyncio
    async def test_token_manager_save_token_no_token(self):
        """Test TokenManager _save_token method with no token."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
            download_dir="./downloads",
        )

        token_manager = TokenManager(config)
        token_manager.current_token = None

        with patch("pathlib.Path.mkdir") as mock_mkdir:
            with patch("builtins.open", mock_open()) as mock_file:
                await token_manager._save_token()

                mock_mkdir.assert_not_called()
                mock_file.assert_not_called()

    @pytest.mark.asyncio
    async def test_token_manager_save_token_exception(self):
        """Test TokenManager _save_token method with exception."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
            download_dir="./downloads",
        )

        token_manager = TokenManager(config)

        # Create a token
        mock_token = MagicMock()
        mock_token.access_token = "test_access_token"
        mock_token.token_type = "Bearer"
        mock_token.expires_at = datetime.now() + timedelta(hours=1)
        mock_token.refresh_token = "test_refresh_token"
        token_manager.current_token = mock_token

        with patch("pathlib.Path.mkdir") as mock_mkdir:
            mock_mkdir.side_effect = Exception("Directory creation failed")

            # Should not raise exception
            await token_manager._save_token()

    def test_token_manager_get_token_info_no_token(self):
        """Test TokenManager get_token_info method with no token."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )

        token_manager = TokenManager(config)
        token_manager.current_token = None

        info = token_manager.get_token_info()
        assert info["status"] == "no_token"

    def test_token_manager_get_token_info_with_token(self):
        """Test TokenManager get_token_info method with token."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )

        token_manager = TokenManager(config)

        # Create a valid token
        mock_token = MagicMock()
        mock_token.is_expired = False
        mock_token.expires_at = datetime.now() + timedelta(hours=1)
        mock_token.token_type = "Bearer"
        mock_token.issued_at = datetime.now()
        mock_token.status.value = "valid"
        mock_token.refresh_token = "test_refresh_token"
        token_manager.current_token = mock_token

        info = token_manager.get_token_info()
        assert info["status"] == "valid"
        assert info["token_type"] == "Bearer"
        assert info["is_expired"] is False
        assert info["has_refresh_token"] is True

    def test_token_manager_clear_token(self):
        """Test TokenManager clear_token method."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
            download_dir="./downloads",
        )

        token_manager = TokenManager(config)

        # Set a token
        mock_token = MagicMock()
        token_manager.current_token = mock_token

        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.unlink") as mock_unlink:
                token_manager.clear_token()

                assert token_manager.current_token is None
                mock_unlink.assert_called_once()

    def test_token_manager_clear_token_no_file(self):
        """Test TokenManager clear_token method when file doesn't exist."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
            download_dir="./downloads",
        )

        token_manager = TokenManager(config)

        # Set a token
        mock_token = MagicMock()
        token_manager.current_token = mock_token

        with patch("pathlib.Path.exists", return_value=False):
            with patch("pathlib.Path.unlink") as mock_unlink:
                token_manager.clear_token()

                assert token_manager.current_token is None
                mock_unlink.assert_not_called()

    def test_token_manager_clear_token_exception(self):
        """Test TokenManager clear_token method with exception."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
            download_dir="./downloads",
        )

        token_manager = TokenManager(config)

        # Set a token
        mock_token = MagicMock()
        token_manager.current_token = mock_token

        with patch("pathlib.Path.exists", return_value=True):
            with patch(
                "pathlib.Path.unlink", side_effect=Exception("File deletion failed")
            ):
                # Should not raise exception
                token_manager.clear_token()
                assert token_manager.current_token is None

    # Additional tests for missing coverage
    @pytest.mark.asyncio
    async def test_token_manager_get_valid_token_oauth_no_oauth_enabled(self):
        """Test TokenManager get_valid_token method with OAuth disabled."""
        config = ClientConfig(base_url="https://api.example.com", oauth_enabled=False)

        token_manager = TokenManager(config)
        token = await token_manager.get_valid_token()
        assert token is None

    @pytest.mark.asyncio
    async def test_token_manager_get_valid_token_oauth_get_new_token_failure(self):
        """Test TokenManager get_valid_token method when _get_new_token returns None."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )

        token_manager = TokenManager(config)

        with (
            patch.object(token_manager, "_load_token") as mock_load,
            patch.object(token_manager, "_get_new_token") as mock_get_new,
        ):
            mock_load.return_value = None
            mock_get_new.return_value = None

            token = await token_manager.get_valid_token()
            assert token is None

    @pytest.mark.asyncio
    async def test_token_manager_refresh_token_failure_response(self):
        """Test TokenManager _refresh_token method with failure response."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )

        token_manager = TokenManager(config)

        # Create a token with refresh token
        mock_token = MagicMock()
        mock_token.refresh_token = "test_refresh_token"
        token_manager.current_token = mock_token

        mock_response = MagicMock()
        mock_response.status = 400
        mock_response.text.return_value = "Invalid refresh token"

        mock_session = AsyncMock()
        mock_session.__aenter__.return_value.post.return_value.__aenter__.return_value = (
            mock_response
        )

        with patch("aiohttp.ClientSession", return_value=mock_session):
            with patch.object(token_manager, "_get_new_token") as mock_get_new:
                mock_get_new.return_value = mock_token

                token = await token_manager._refresh_token()
                assert token == mock_token
                mock_get_new.assert_called_once()

    @pytest.mark.asyncio
    async def test_token_manager_refresh_token_exception_response(self):
        """Test TokenManager _refresh_token method with exception during request."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )

        token_manager = TokenManager(config)

        # Create a token with refresh token
        mock_token = MagicMock()
        mock_token.refresh_token = "test_refresh_token"
        token_manager.current_token = mock_token

        with patch("aiohttp.ClientSession", side_effect=Exception("Network error")):
            with patch.object(token_manager, "_get_new_token") as mock_get_new:
                mock_get_new.return_value = mock_token

                token = await token_manager._refresh_token()
                assert token == mock_token
                mock_get_new.assert_called_once()

    def test_token_manager_get_token_info_expired_token(self):
        """Test TokenManager get_token_info method with expired token."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="test_client",
            client_secret="test_secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )

        token_manager = TokenManager(config)

        # Create an expired token
        mock_token = MagicMock()
        mock_token.is_expired = True
        mock_token.expires_at = datetime.now() - timedelta(hours=1)
        mock_token.token_type = "Bearer"
        mock_token.issued_at = datetime.now()
        mock_token.status.value = "expired"
        mock_token.refresh_token = None
        token_manager.current_token = mock_token

        info = token_manager.get_token_info()
        assert info["status"] == "expired"
        assert info["token_type"] == "Bearer"
        assert info["is_expired"] is True
