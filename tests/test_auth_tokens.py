import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import pytest

from dataquery.transport.auth import OAuthManager, TokenManager, default_token_storage_dir
from dataquery.types.models import ClientConfig, OAuthToken, TokenResponse


def make_config(tmp_path: Path, oauth: bool = True) -> ClientConfig:
    return ClientConfig(
        base_url="https://api.example.com",
        oauth_enabled=oauth,
        client_id="cid" if oauth else None,
        client_secret="csec" if oauth else None,
        # scope removed
        timeout=5.0,
        download_dir=str(tmp_path),
    )


def test_token_storage_path_setup(tmp_path: Path):
    cfg = make_config(tmp_path)
    tm = TokenManager(cfg)
    # Default storage is per credential set under the user config dir, never
    # relative to the working directory or download_dir.
    assert tm.token_file is not None
    assert tm.token_file.name == "oauth_token.json"
    assert tm.token_file.parent == default_token_storage_dir(cfg)
    assert tmp_path not in tm.token_file.parents


@pytest.mark.asyncio
async def test_get_valid_token_with_bearer(tmp_path: Path):
    cfg = make_config(tmp_path, oauth=False)
    cfg.bearer_token = "BEAR"
    tm = TokenManager(cfg)
    token = await tm.get_valid_token()
    assert token == "Bearer BEAR"


@pytest.mark.asyncio
async def test_get_new_token_success_and_save_load(tmp_path: Path, monkeypatch):
    cfg = make_config(tmp_path, oauth=True)
    # Provide explicit token URL
    cfg.oauth_token_url = "https://auth.example.com/oauth/token"
    tm = TokenManager(cfg)

    fake_response_data = {
        "access_token": "abc",
        "token_type": "Bearer",
        "expires_in": 3600,
        # "scope": "data.read",
        "refresh_token": "r1",
    }

    class _Resp:
        status = 200

        async def json(self):
            return fake_response_data

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _Sess:
        def __init__(self):
            # Return a context-manager-like object (the response) directly
            self._resp = _Resp()

        def post(self, *args, **kwargs):
            return self._resp

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    with patch("aiohttp.ClientSession", return_value=_Sess()):
        tok = await tm._get_new_token()
        assert tok is not None
        assert tm.current_token is not None
        # Token should be saved to disk
        assert tm.token_file is not None and tm.token_file.exists()

        # Force reload
        tm.current_token = None
        loaded = await tm._load_token()
        assert loaded is not None


@pytest.mark.asyncio
async def test_refresh_token_fallback_to_new(tmp_path: Path, monkeypatch):
    cfg = make_config(tmp_path, oauth=True)
    cfg.oauth_token_url = "https://auth.example.com/oauth/token"
    tm = TokenManager(cfg)
    # No current token -> should call _get_new_token
    called = {"new": 0}

    async def fake_new():
        called["new"] += 1
        return OAuthToken(access_token="x", token_type="Bearer")

    with patch.object(tm, "_get_new_token", side_effect=fake_new):
        out = await tm._refresh_token()
        assert out is not None
        assert called["new"] == 1


@pytest.mark.asyncio
async def test_oauth_manager_headers_and_auth_info(tmp_path: Path):
    cfg = make_config(tmp_path, oauth=True)
    om = OAuthManager(cfg)

    # Stub token manager to avoid network
    om.token_manager.get_valid_token = AsyncMock(return_value="Bearer Z")

    headers = await om.get_headers()
    assert headers["Authorization"] == "Bearer Z"
    assert om.is_authenticated() is True
    info = om.get_auth_info()
    assert isinstance(info, dict)


# --- Token storage, rotation and expiry -------------------------------------


def _fresh_token(access="abc", expires_in=3600, age_seconds=0):
    from datetime import datetime, timedelta, timezone

    return OAuthToken(
        access_token=access,
        token_type="Bearer",
        expires_in=expires_in,
        issued_at=datetime.now(timezone.utc) - timedelta(seconds=age_seconds),
    )


def _cfg(tmp_path, client_id="cid"):
    cfg = make_config(tmp_path)
    cfg.client_id = client_id
    cfg.oauth_token_url = "https://auth.example.com/oauth/token"
    return cfg


@pytest.mark.asyncio
async def test_stored_token_is_scoped_to_its_credentials(tmp_path: Path):
    """A token saved for one credential set is never loaded for another."""
    tm_a = TokenManager(_cfg(tmp_path, "client-a"))
    tm_a.current_token = _fresh_token("token-a")
    await tm_a._save_token()

    tm_b = TokenManager(_cfg(tmp_path, "client-b"))
    assert tm_b.token_file != tm_a.token_file
    # Even when both are pointed at one explicit shared directory.
    tm_b.token_file = tm_a.token_file
    assert await tm_b._load_token() is None
    assert tm_b.current_token is None

    tm_a2 = TokenManager(_cfg(tmp_path, "client-a"))
    assert (await tm_a2._load_token()).access_token == "token-a"


@pytest.mark.asyncio
async def test_legacy_token_file_without_fingerprint_is_ignored(tmp_path: Path):
    import json

    tm = TokenManager(_cfg(tmp_path))
    tm.token_file.write_text(
        json.dumps({"access_token": "tok", "expires_in": 3600, "issued_at": "2099-01-01T00:00:00+00:00"})
    )
    assert await tm._load_token() is None


@pytest.mark.asyncio
async def test_token_without_expiry_is_not_persisted(tmp_path: Path):
    tm = TokenManager(_cfg(tmp_path))
    tm.clear_token()  # the per-credential cache is shared across tests
    tm.current_token = _fresh_token(expires_in=None)
    await tm._save_token()
    assert not tm.token_file.exists()


def test_short_lived_token_refreshes_at_half_life():
    # Old logic: threshold (300) > lifetime (120) meant "never refresh early".
    assert not _fresh_token(expires_in=120, age_seconds=30).is_expiring_soon(300)
    assert _fresh_token(expires_in=120, age_seconds=70).is_expiring_soon(300)


def test_token_counts_as_expired_just_before_expiry():
    # 3600s lifetime -> 30s safety margin.
    assert not _fresh_token(expires_in=3600, age_seconds=3560).is_expired
    assert _fresh_token(expires_in=3600, age_seconds=3575).is_expired


@pytest.mark.asyncio
async def test_failed_refresh_keeps_a_still_valid_token(tmp_path: Path):
    from dataquery.types.exceptions import NetworkError

    tm = TokenManager(_cfg(tmp_path))
    tm.current_token = _fresh_token("still-good", expires_in=3600, age_seconds=3400)  # expiring soon
    with patch.object(tm, "_refresh_token", AsyncMock(side_effect=NetworkError("token endpoint down"))):
        assert await tm.get_valid_token() == "Bearer still-good"


@pytest.mark.asyncio
async def test_refresh_fallback_requests_a_new_token_only_once(tmp_path: Path):
    from dataquery.types.exceptions import AuthenticationError

    tm = TokenManager(_cfg(tmp_path))
    tm.current_token = _fresh_token()
    tm.current_token.refresh_token = "r1"

    class _Resp:
        status = 400

        async def text(self):
            return "invalid_grant"

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

    class _Sess:
        def post(self, *a, **k):
            return _Resp()

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

    new_token = AsyncMock(side_effect=AuthenticationError("OAuth token request failed: 401"))
    with patch("aiohttp.ClientSession", return_value=_Sess()), patch.object(tm, "_get_new_token", new_token):
        with pytest.raises(AuthenticationError):
            await tm._refresh_token()
    assert new_token.await_count == 1


@pytest.mark.asyncio
async def test_refresh_after_rejection_replaces_only_the_rejected_token(tmp_path: Path):
    om = OAuthManager(_cfg(tmp_path))
    tm = om.token_manager
    tm.current_token = _fresh_token("rejected")
    await tm._save_token()

    async def mint():
        tm.current_token = _fresh_token("fresh")
        return tm.current_token

    with patch.object(tm, "_get_new_token", AsyncMock(side_effect=mint)) as new_token:
        assert await om.refresh_after_rejection("Bearer rejected") == "Bearer fresh"
        # A concurrent request that also sent the old token reuses the replacement.
        assert await om.refresh_after_rejection("Bearer rejected") == "Bearer fresh"
    assert new_token.await_count == 1


@pytest.mark.asyncio
async def test_refresh_after_rejection_leaves_static_bearer_alone(tmp_path: Path):
    cfg = make_config(tmp_path, oauth=False)
    cfg.bearer_token = "BEAR"
    assert await OAuthManager(cfg).refresh_after_rejection("Bearer BEAR") is None
