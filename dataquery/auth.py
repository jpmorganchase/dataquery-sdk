"""
Authentication module for the DATAQUERY SDK.

Provides OAuth token management and Bearer token authentication.
"""

import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import aiohttp
import structlog

from .exceptions import AuthenticationError, ConfigurationError, DataQueryError
from .models import ClientConfig, OAuthToken, TokenRequest, TokenResponse

logger = structlog.get_logger(__name__)


class TokenManager:
    """Manages OAuth tokens and Bearer token authentication."""

    def __init__(self, config: ClientConfig):
        self.config = config
        self.current_token: Optional[OAuthToken] = None
        self.token_file: Optional[Path] = None
        self._setup_token_storage()

    def _setup_token_storage(self):
        """Setup token storage file."""
        # Prefer explicit token_storage_dir only when token storage is enabled,
        # else fallback to download_dir/.tokens if download_dir is provided.
        base_dir: Optional[Path] = None
        token_storage_enabled = bool(
            getattr(self.config, "token_storage_enabled", False)
        )
        token_storage_dir = getattr(self.config, "token_storage_dir", None)
        if token_storage_enabled and token_storage_dir:
            base_dir = Path(token_storage_dir)
        elif getattr(self.config, "download_dir", None):
            # Use hidden .tokens folder under download_dir only if download_dir is configured
            try:
                if str(self.config.download_dir).strip():
                    base_dir = Path(self.config.download_dir) / ".tokens"
            except Exception:
                base_dir = None

        if base_dir:
            base_dir.mkdir(parents=True, exist_ok=True)
            self.token_file = base_dir / "oauth_token.json"
        else:
            self.token_file = None

    async def get_valid_token(self) -> Optional[str]:
        """
        Get a valid access token for API requests.

        Returns:
            Bearer token string or None if no valid token available
        """
        # Check if we have a static bearer token
        if self.config.has_bearer_token:
            return f"Bearer {self.config.bearer_token}"

        # Check if OAuth is enabled and credentials are available
        if not self.config.has_oauth_credentials:
            logger.warning("No OAuth credentials or bearer token configured")
            return None

        # Load existing token
        if not self.current_token:
            await self._load_token()

        # Check if current token is valid
        if self.current_token and not self.current_token.is_expired:
            # Check if token is expiring soon
            if self.current_token.is_expiring_soon(self.config.token_refresh_threshold):
                logger.info("Token expiring soon, refreshing...")
                await self._refresh_token()

            return self.current_token.to_authorization_header()

        # Token is expired or invalid, get new token
        logger.info("Getting new OAuth token...")
        await self._get_new_token()

        if self.current_token:
            return self.current_token.to_authorization_header()

        return None

    async def _get_new_token(self) -> Optional[OAuthToken]:
        """Get a new OAuth token from the server."""
        if not self.config.oauth_token_url:
            raise ConfigurationError("OAuth token URL not configured")

        # Validate required fields
        if not self.config.client_id or not self.config.client_secret:
            raise ConfigurationError(
                "client_id and client_secret are required for OAuth"
            )

        # Create token request
        token_request = TokenRequest(
            grant_type=self.config.grant_type,
            client_id=self.config.client_id,
            client_secret=self.config.client_secret,
            # scope removed
            aud=getattr(self.config, "aud", None),
        )

        try:
            # Make token request
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.config.oauth_token_url,
                    data=token_request.to_dict(),
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout=aiohttp.ClientTimeout(
                        total=self.config.timeout,
                        connect=min(300.0, self.config.timeout * 0.5),
                        sock_read=min(300.0, self.config.timeout * 0.5),
                    ),
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        token_response = TokenResponse(**data)
                        self.current_token = token_response.to_oauth_token()

                        # Save token
                        await self._save_token()

                        logger.info(
                            "OAuth token obtained successfully",
                            expires_in=self.current_token.expires_in,
                        )
                        return self.current_token
                    else:
                        error_data = await response.text()
                        logger.error(
                            "Failed to get OAuth token",
                            status=response.status,
                            error=error_data,
                        )
                        raise AuthenticationError(
                            f"OAuth token request failed: {response.status}"
                        )

        except Exception as e:
            logger.error("Error getting OAuth token", error=str(e))
            raise AuthenticationError(f"Failed to get OAuth token: {e}")

    async def _refresh_token(self) -> Optional[OAuthToken]:
        """Refresh the current OAuth token."""
        if not self.current_token or not self.current_token.refresh_token:
            # No refresh token available, get new token
            return await self._get_new_token()

        try:
            # Create refresh request
            refresh_data = {
                "grant_type": "refresh_token",
                "refresh_token": self.current_token.refresh_token,
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
            }

            # Validate OAuth token URL
            if not self.config.oauth_token_url:
                raise ConfigurationError("OAuth token URL not configured")

            # Make refresh request
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.config.oauth_token_url,
                    data=refresh_data,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout=aiohttp.ClientTimeout(
                        total=self.config.timeout,
                        connect=min(300.0, self.config.timeout * 0.5),
                        sock_read=min(300.0, self.config.timeout * 0.5),
                    ),
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        token_response = TokenResponse(**data)
                        self.current_token = token_response.to_oauth_token()

                        # Save token
                        await self._save_token()

                        logger.info(
                            "OAuth token refreshed successfully",
                            expires_in=self.current_token.expires_in,
                        )
                        return self.current_token
                    else:
                        error_data = await response.text()
                        logger.error(
                            "Failed to refresh OAuth token",
                            status=response.status,
                            error=error_data,
                        )
                        # Fall back to getting new token
                        return await self._get_new_token()

        except Exception as e:
            logger.error("Error refreshing OAuth token", error=str(e))
            # Fall back to getting new token
            return await self._get_new_token()

    async def _load_token(self) -> Optional[OAuthToken]:
        """Load token from storage."""
        if not self.token_file or not self.token_file.exists():
            return None

        try:
            with open(self.token_file, "r") as f:
                token_data = json.load(f)

            # Convert timestamp back to datetime
            if "issued_at" in token_data:
                token_data["issued_at"] = datetime.fromisoformat(
                    token_data["issued_at"]
                )

            self.current_token = OAuthToken(**token_data)

            # Check if token is still valid
            if self.current_token.is_expired:
                logger.info("Stored token is expired")
                self.current_token = None
                return None

            logger.info(
                "Token loaded from storage", expires_at=self.current_token.expires_at
            )
            return self.current_token

        except Exception as e:
            logger.warning("Failed to load token from storage", error=str(e))
            return None

    async def _save_token(self) -> None:
        """Save token to storage."""
        if not self.token_file or not self.current_token:
            return

        try:
            token_data = self.current_token.model_dump()
            # Convert datetime to ISO string for JSON serialization
            if "issued_at" in token_data and token_data["issued_at"] is not None:
                token_data["issued_at"] = token_data["issued_at"].isoformat()

            # Ensure directory exists
            self.token_file.parent.mkdir(parents=True, exist_ok=True)

            # Write to temporary file first, then rename for atomic operation
            temp_file = self.token_file.with_suffix(".tmp")
            with open(temp_file, "w") as f:
                json.dump(token_data, f, indent=2)

            # Set restrictive permissions on the temp file (owner read/write only)
            import os

            os.chmod(temp_file, 0o600)

            # Atomic rename
            temp_file.replace(self.token_file)

            # Ensure the final file also has restrictive permissions
            os.chmod(self.token_file, 0o600)

            logger.debug("Token saved to storage with secure permissions")

        except Exception as e:
            logger.warning("Failed to save token to storage", error=str(e))
            # Clean up temp file if it exists
            temp_file = self.token_file.with_suffix(".tmp")
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except Exception:
                    pass

    def clear_token(self) -> None:
        """Clear the current token."""
        self.current_token = None
        if self.token_file and self.token_file.exists():
            try:
                self.token_file.unlink()
                logger.info("Token file removed")
            except Exception as e:
                logger.warning("Failed to remove token file", error=str(e))

    def get_token_info(self) -> Dict[str, Any]:
        """Get information about the current token."""
        if not self.current_token:
            return {"status": "no_token"}

        return {
            "status": self.current_token.status.value,
            "token_type": self.current_token.token_type,
            "issued_at": self.current_token.issued_at.isoformat() if self.current_token.issued_at else None,
            "expires_at": (
                self.current_token.expires_at.isoformat()
                if self.current_token.expires_at
                else None
            ),
            "is_expired": self.current_token.is_expired,
            "has_refresh_token": self.current_token.refresh_token is not None,
        }


class OAuthManager:
    """High-level OAuth management for the DATAQUERY SDK."""

    def __init__(self, config: ClientConfig):
        self.config = config
        self.token_manager = TokenManager(config)

    async def authenticate(self) -> str:
        """
        Authenticate and get a valid Bearer token.

        Returns:
            Bearer token string

        Raises:
            AuthenticationError: If authentication fails
        """
        token = await self.token_manager.get_valid_token()
        if not token:
            raise AuthenticationError("Failed to obtain valid authentication token")
        return token

    async def get_headers(self) -> Dict[str, str]:
        """
        Get headers with authentication token.

        Returns:
            Dictionary with Authorization header
        """
        token = await self.authenticate()
        return {"Authorization": token}

    def is_authenticated(self) -> bool:
        """Check if authentication is configured."""
        return self.config.has_bearer_token or self.config.has_oauth_credentials

    def get_auth_info(self) -> Dict[str, Any]:
        """Get authentication configuration information."""
        return {
            "oauth_enabled": self.config.oauth_enabled,
            "has_oauth_credentials": self.config.has_oauth_credentials,
            "has_bearer_token": self.config.has_bearer_token,
            "oauth_token_url": self.config.oauth_token_url,
            "grant_type": self.config.grant_type,
            "token_info": self.token_manager.get_token_info(),
        }

    def clear_authentication(self) -> None:
        """Clear all authentication data."""
        self.token_manager.clear_token()
        logger.info("Authentication data cleared")

    async def test_authentication(self) -> bool:
        """
        Test if authentication is working.

        Returns:
            True if authentication is successful
        """
        try:
            await self.authenticate()
            return True
        except Exception as e:
            logger.error("Authentication test failed", error=str(e))
            return False
