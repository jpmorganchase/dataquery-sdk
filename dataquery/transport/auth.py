"""Authentication module for the DATAQUERY SDK."""

import asyncio
import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import aiohttp
import structlog

from ..types.exceptions import AuthenticationError, ConfigurationError, NetworkError
from ..types.models import ClientConfig, OAuthToken, TokenRequest, TokenResponse

logger = structlog.get_logger(__name__)


def credential_fingerprint(config: ClientConfig) -> str:
    """Short hash identifying the credential set a token was issued for."""
    raw = "\n".join([config.client_id or "", config.oauth_token_url or "", getattr(config, "aud", None) or ""])
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def default_token_storage_dir(config: ClientConfig) -> Path:
    """Per-user, per-credential token cache: ``~/.dataquery/tokens/<fingerprint>``.

    Not relative to the working directory, so a token cached by one project,
    tool or test run is never picked up by a process using other credentials.
    """
    from ..config import EnvConfig

    return EnvConfig.user_config_dir() / "tokens" / credential_fingerprint(config)


class TokenManager:
    """Manages OAuth tokens and Bearer token authentication."""

    def __init__(self, config: ClientConfig):
        self.config = config
        self.current_token: Optional[OAuthToken] = None
        self.token_file: Optional[Path] = None
        # Single-flight lock around token acquisition so concurrent callers
        # don't stampede the token endpoint.
        self._token_lock: Optional[asyncio.Lock] = None
        self._setup_token_storage()

    def _get_token_lock(self) -> asyncio.Lock:
        """Return the token-acquisition lock, creating it on first use."""
        if self._token_lock is None:
            self._token_lock = asyncio.Lock()
        return self._token_lock

    def _setup_token_storage(self):
        """Setup token storage file.

        ``token_storage_enabled`` + ``token_storage_dir`` pick an explicit
        directory; otherwise OAuth tokens are cached per credential set under
        the user config dir. Bearer-token-only configs cache nothing.
        """
        self._fingerprint = credential_fingerprint(self.config)
        base_dir: Optional[Path] = None
        token_storage_enabled = bool(getattr(self.config, "token_storage_enabled", False))
        token_storage_dir = getattr(self.config, "token_storage_dir", None)
        if token_storage_enabled and token_storage_dir:
            base_dir = Path(token_storage_dir)
        elif self.config.has_oauth_credentials:
            base_dir = default_token_storage_dir(self.config)

        if base_dir:
            try:
                base_dir.mkdir(parents=True, exist_ok=True)
                os.chmod(base_dir, 0o700)
            except OSError as e:
                logger.warning("Token storage unavailable; tokens will not be cached", error=str(e))
                self.token_file = None
                return
            self.token_file = base_dir / "oauth_token.json"
        else:
            self.token_file = None

    async def get_valid_token(self) -> Optional[str]:
        """Get a valid access token for API requests."""
        if self.config.has_bearer_token:
            return f"Bearer {self.config.get_bearer_token()}"

        if not self.config.has_oauth_credentials:
            logger.warning("No OAuth credentials or bearer token configured")
            return None

        if not self.current_token:
            await self._load_token()

        if self.current_token and not self.current_token.is_expired:
            if self.current_token.is_expiring_soon(self.config.token_refresh_threshold):
                async with self._get_token_lock():
                    if (
                        self.current_token
                        and not self.current_token.is_expired
                        and self.current_token.is_expiring_soon(self.config.token_refresh_threshold)
                    ):
                        logger.info("Token expiring soon, refreshing...")
                        try:
                            await self._refresh_token()
                        except (AuthenticationError, NetworkError) as e:
                            # The current token still works until it expires;
                            # use it and try again on the next request.
                            if self.current_token.is_expired:
                                raise
                            logger.warning("Token refresh failed; using current token until expiry", error=str(e))

            if self.current_token:
                return self.current_token.to_authorization_header()

        async with self._get_token_lock():
            if not (self.current_token and not self.current_token.is_expired):
                logger.info("Getting new OAuth token...")
                await self._get_new_token()

        if self.current_token:
            return self.current_token.to_authorization_header()

        return None

    async def _get_new_token(self) -> Optional[OAuthToken]:
        """Get a new OAuth token from the server."""
        if not self.config.oauth_token_url:
            raise ConfigurationError("OAuth token URL not configured")

        if not self.config.client_id or not self.config.get_client_secret():
            raise ConfigurationError("client_id and client_secret are required for OAuth")

        token_request = TokenRequest(
            grant_type=self.config.grant_type,
            client_id=self.config.client_id,
            client_secret=self.config.client_secret,
            aud=getattr(self.config, "aud", None),
        )

        try:
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
                    **self.config.get_proxy_kwargs(),
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        token_response = TokenResponse(**data)
                        self.current_token = token_response.to_oauth_token()

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
                        raise AuthenticationError(f"OAuth token request failed: {response.status}")

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error("Network error getting OAuth token", error=str(e))
            raise NetworkError(f"Failed to get OAuth token: {e}") from e
        except Exception as e:
            logger.error("Error getting OAuth token", error=str(e))
            raise AuthenticationError(f"Failed to get OAuth token: {e}") from e

    async def _refresh_token(self) -> Optional[OAuthToken]:
        """Refresh the current OAuth token, falling back to a new client-credentials token."""
        if not self.current_token or not self.current_token.refresh_token:
            return await self._get_new_token()

        if not self.config.oauth_token_url:
            raise ConfigurationError("OAuth token URL not configured")

        refresh_data = {
            "grant_type": "refresh_token",
            "refresh_token": self.current_token.refresh_token,
            "client_id": self.config.client_id,
            "client_secret": self.config.get_client_secret(),
        }
        try:
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
                    **self.config.get_proxy_kwargs(),
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        token_response = TokenResponse(**data)
                        self.current_token = token_response.to_oauth_token()

                        await self._save_token()

                        logger.info(
                            "OAuth token refreshed successfully",
                            expires_in=self.current_token.expires_in,
                        )
                        return self.current_token
                    error_data = await response.text()
                    logger.error(
                        "Failed to refresh OAuth token",
                        status=response.status,
                        error=error_data,
                    )
        except Exception as e:
            logger.error("Error refreshing OAuth token", error=str(e))

        # Outside the try, so a failure here is raised once instead of
        # being caught and retried with a second token request.
        return await self._get_new_token()

    async def _load_token(self) -> Optional[OAuthToken]:
        """Load token from storage."""
        if not self.token_file or not self.token_file.exists():
            return None

        try:
            with open(self.token_file, "r") as f:
                token_data = json.load(f)

            if token_data.pop("credential_fingerprint", None) != self._fingerprint:
                # Issued for other credentials (or written by an older SDK
                # that did not record them): never send it.
                logger.info("Ignoring stored token issued for different credentials")
                return None

            if "issued_at" in token_data:
                token_data["issued_at"] = datetime.fromisoformat(token_data["issued_at"])

            token = OAuthToken(**token_data)
            if token.expires_at is None or token.is_expired:
                logger.info("Stored token is expired or has no expiry")
                return None

            self.current_token = token

            logger.info("Token loaded from storage", expires_at=self.current_token.expires_at)
            return self.current_token

        except Exception as e:
            logger.warning("Failed to load token from storage", error=str(e))
            return None

    async def _save_token(self) -> None:
        """Save token to storage."""
        if not self.token_file or not self.current_token:
            return
        if self.current_token.expires_in is None:
            # Another process could not tell when it goes stale; keep it in memory only.
            logger.debug("Token has no expires_in; not persisting it")
            return

        # Per-process temp name so concurrent writers never share (and clobber) one file.
        temp_file = self.token_file.with_name(f"{self.token_file.name}.{os.getpid()}.tmp")
        try:
            token_data = self.current_token.model_dump()
            if "issued_at" in token_data and token_data["issued_at"] is not None:
                token_data["issued_at"] = token_data["issued_at"].isoformat()
            token_data["credential_fingerprint"] = self._fingerprint

            self.token_file.parent.mkdir(parents=True, exist_ok=True)

            # Create the temp file with owner-only permissions from the start
            # (no TOCTOU window between open() and chmod()).
            flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
            fd = os.open(temp_file, flags, 0o600)
            try:
                with os.fdopen(fd, "w") as f:
                    json.dump(token_data, f, indent=2)
            except BaseException:
                try:
                    os.close(fd)
                except OSError:
                    pass
                raise

            temp_file.replace(self.token_file)

            logger.debug("Token saved to storage with secure permissions")

        except Exception as e:
            logger.warning("Failed to save token to storage", error=str(e))
            try:
                temp_file.unlink(missing_ok=True)
            except OSError as cleanup_err:
                logger.warning("Failed to remove temp token file", error=str(cleanup_err))

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
            "issued_at": (self.current_token.issued_at.isoformat() if self.current_token.issued_at else None),
            "expires_at": (self.current_token.expires_at.isoformat() if self.current_token.expires_at else None),
            "is_expired": self.current_token.is_expired,
            "has_refresh_token": self.current_token.refresh_token is not None,
        }


class OAuthManager:
    """High-level OAuth management for the DATAQUERY SDK."""

    def __init__(self, config: ClientConfig):
        self.config = config
        self.token_manager = TokenManager(config)

    async def authenticate(self) -> str:
        """Authenticate and get a valid Bearer token."""
        token = await self.token_manager.get_valid_token()
        if not token:
            raise AuthenticationError("Failed to obtain valid authentication token")
        return token

    async def get_headers(self) -> Dict[str, str]:
        """Get headers with authentication token."""
        token = await self.authenticate()
        return {"Authorization": token}

    async def force_refresh(self) -> Optional[str]:
        """Discard the cached token and fetch a new one."""
        self.token_manager.clear_token()
        return await self.token_manager.get_valid_token()

    async def refresh_after_rejection(self, rejected_header: str) -> Optional[str]:
        """Replace a token the API rejected (401) and return the new header.

        Only an OAuth token is replaced, and only if it is still the rejected
        one: when concurrent requests all get 401, the first discards it and
        the rest reuse the replacement instead of each fetching another.
        """
        if self.config.has_bearer_token or not self.config.has_oauth_credentials:
            return None
        tm = self.token_manager
        async with tm._get_token_lock():
            current = tm.current_token.to_authorization_header() if tm.current_token else None
            if current and current != rejected_header:
                return current
            logger.info("API rejected the cached OAuth token; fetching a new one")
            tm.clear_token()
        return await tm.get_valid_token()

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
        """Test if authentication is working."""
        try:
            await self.authenticate()
            return True
        except Exception as e:
            logger.error("Authentication test failed", error=str(e))
            return False
