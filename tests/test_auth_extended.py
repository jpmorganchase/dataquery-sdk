from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from dataquery.auth import OAuthManager, TokenManager
from dataquery.models import ClientConfig, OAuthToken


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
    # Default storage should be under download_dir/.tokens
    assert tm.token_file is not None
    assert tm.token_file.name == "oauth_token.json"
    assert tm.token_file.parent.name == ".tokens"


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
