"""Regression tests for the reliability fixes:

- 429 responses raise inside the retry scope and are actually retried
- a server ``Retry-After`` is honored when timing the backoff
- ``_parse_retry_after`` handles seconds, HTTP-date, and garbage
- the adaptive rate-limit backoff grows from zero instead of staying stuck
- token-fetch transport failures are classified as ``NetworkError``
- token acquisition is single-flight under concurrency
"""

import asyncio
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest

from dataquery.core.client import DataQueryClient
from dataquery.transport.auth import TokenManager
from dataquery.transport.rate_limiter import EnhancedTokenBucketRateLimiter, RateLimitConfig
from dataquery.transport.retry import RetryConfig, RetryManager
from dataquery.types.exceptions import AuthenticationError, NetworkError, RateLimitError
from dataquery.types.models import ClientConfig, OAuthToken


def _client() -> DataQueryClient:
    return DataQueryClient(
        ClientConfig(
            base_url="https://api.example.com",
            context_path="/v2",
            client_id="cid",
            client_secret="secret",
            oauth_enabled=True,
        )
    )


def _token_manager() -> TokenManager:
    tm = TokenManager(
        ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            client_id="cid",
            client_secret="secret",
            oauth_token_url="https://api.example.com/oauth/token",
        )
    )
    # Keep tests hermetic: never read/write a persisted token on disk.
    tm.token_file = None
    return tm


# --------------------------------------------------------------------------- #
# _parse_retry_after
# --------------------------------------------------------------------------- #
def test_parse_retry_after_integer_seconds():
    assert DataQueryClient._parse_retry_after({"Retry-After": "30"}) == 30


def test_parse_retry_after_absent_returns_none():
    assert DataQueryClient._parse_retry_after({}) is None


def test_parse_retry_after_negative_returns_none():
    assert DataQueryClient._parse_retry_after({"Retry-After": "-5"}) is None


def test_parse_retry_after_garbage_returns_none():
    assert DataQueryClient._parse_retry_after({"Retry-After": "soon"}) is None


def test_parse_retry_after_http_date():
    future = datetime.now(timezone.utc) + timedelta(seconds=120)
    secs = DataQueryClient._parse_retry_after({"Retry-After": format_datetime(future)})
    assert secs is not None and 100 <= secs <= 121


# --------------------------------------------------------------------------- #
# _execute_request: 429 raises (retryable) inside the retry scope
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_execute_request_raises_ratelimit_on_429():
    client = _client()
    client.auth_manager = AsyncMock()
    client.auth_manager.get_headers = AsyncMock(return_value={"Authorization": "Bearer x"})
    client._ensure_connected = AsyncMock()

    resp = AsyncMock()
    resp.status = 429
    resp.headers = {"Retry-After": "7"}
    resp.text = AsyncMock(return_value="rate limited")
    client.session = AsyncMock()
    client.session.request = AsyncMock(return_value=resp)

    with pytest.raises(RateLimitError) as excinfo:
        await client._execute_request("GET", "https://api.example.com/v2/groups")

    assert excinfo.value.details.get("retry_after") == 7
    resp.text.assert_awaited()  # body drained so the connection can be reused


@pytest.mark.asyncio
async def test_execute_request_still_raises_networkerror_on_500():
    client = _client()
    client.auth_manager = AsyncMock()
    client.auth_manager.get_headers = AsyncMock(return_value={"Authorization": "Bearer x"})
    client._ensure_connected = AsyncMock()

    resp = AsyncMock()
    resp.status = 503
    resp.headers = {}
    resp.text = AsyncMock(return_value="boom")
    client.session = AsyncMock()
    client.session.request = AsyncMock(return_value=resp)

    with pytest.raises(NetworkError):
        await client._execute_request("GET", "https://api.example.com/v2/groups")


# --------------------------------------------------------------------------- #
# RetryManager honors Retry-After carried on the exception
# --------------------------------------------------------------------------- #
def test_retry_after_from_exception():
    mgr = RetryManager(RetryConfig())
    assert mgr._retry_after_from_exception(RateLimitError("x", retry_after=9)) == 9.0
    assert mgr._retry_after_from_exception(RateLimitError("x")) is None
    assert mgr._retry_after_from_exception(ValueError("no details")) is None


@pytest.mark.asyncio
async def test_retry_manager_honors_retry_after(monkeypatch):
    cfg = RetryConfig(
        max_retries=2,
        base_delay=0.01,
        max_delay=100.0,
        enable_circuit_breaker=False,
        retryable_exceptions=[RateLimitError],
    )
    mgr = RetryManager(cfg)

    slept: list = []

    async def fake_sleep(delay):
        slept.append(delay)

    monkeypatch.setattr("dataquery.transport.retry.asyncio.sleep", fake_sleep)

    calls = {"n": 0}

    async def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RateLimitError("rate limited", retry_after=5)
        return "ok"

    result = await mgr.execute_with_retry(flaky)
    assert result == "ok"
    # The 5s Retry-After wins over the tiny 0.01 exponential base.
    assert slept and slept[0] >= 5.0


# --------------------------------------------------------------------------- #
# Adaptive backoff now grows from zero
# --------------------------------------------------------------------------- #
def test_adaptive_backoff_grows_from_zero_and_caps():
    cfg = RateLimitConfig(
        requests_per_minute=300,
        burst_capacity=5,
        adaptive_rate_limiting=True,
        backoff_multiplier=1.5,
        max_backoff_seconds=60.0,
    )
    rl = EnhancedTokenBucketRateLimiter(cfg)
    assert rl.state.current_backoff == 0.0

    rl.handle_rate_limit_response({"Retry-After": "1"})
    first = rl.state.current_backoff
    assert first > 0.0  # the bug: this used to stay 0.0 forever

    rl.handle_rate_limit_response({"Retry-After": "1"})
    assert rl.state.current_backoff > first  # grows on repeated hits

    for _ in range(50):
        rl.handle_rate_limit_response({"Retry-After": "1"})
    assert rl.state.current_backoff <= 60.0  # capped at max_backoff_seconds


# --------------------------------------------------------------------------- #
# Auth: transport failures are NetworkError, generic failures are AuthError
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_get_new_token_connection_error_is_networkerror():
    tm = _token_manager()
    with patch("aiohttp.ClientSession", side_effect=aiohttp.ClientConnectionError("refused")):
        with pytest.raises(NetworkError, match="Failed to get OAuth token"):
            await tm._get_new_token()


@pytest.mark.asyncio
async def test_get_new_token_timeout_is_networkerror():
    tm = _token_manager()
    with patch("aiohttp.ClientSession", side_effect=asyncio.TimeoutError()):
        with pytest.raises(NetworkError):
            await tm._get_new_token()


@pytest.mark.asyncio
async def test_get_new_token_generic_error_is_autherror():
    tm = _token_manager()
    with patch("aiohttp.ClientSession", side_effect=Exception("weird")):
        with pytest.raises(AuthenticationError, match="Failed to get OAuth token: weird"):
            await tm._get_new_token()


# --------------------------------------------------------------------------- #
# Single-flight token acquisition
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_token_acquisition_is_single_flight():
    tm = _token_manager()
    calls = {"n": 0}

    async def fake_get_new():
        calls["n"] += 1
        await asyncio.sleep(0.01)  # widen the race window
        tm.current_token = OAuthToken(
            access_token="tok",
            token_type="Bearer",
            expires_in=3600,
            issued_at=datetime.now(),
        )
        return tm.current_token

    tm._get_new_token = fake_get_new

    tokens = await asyncio.gather(*[tm.get_valid_token() for _ in range(5)])

    assert all(t == "Bearer tok" for t in tokens)
    assert calls["n"] == 1  # five concurrent callers, one token fetch


def test_parse_retry_after_naive_http_date_treated_as_utc():
    # A non-compliant server may emit a Retry-After HTTP-date without a zone.
    # It must be read as UTC, not naive local time (which would skew by the
    # host's UTC offset and could wrongly clamp the delay to 0).
    future_naive = (datetime.now(timezone.utc) + timedelta(seconds=120)).strftime("%a, %d %b %Y %H:%M:%S")
    secs = DataQueryClient._parse_retry_after({"Retry-After": future_naive})
    assert secs is not None and 100 <= secs <= 121


def test_retry_after_from_exception_rejects_non_finite():
    mgr = RetryManager(RetryConfig())
    assert mgr._retry_after_from_exception(RateLimitError("x", retry_after=float("inf"))) is None


@pytest.mark.asyncio
async def test_persistent_ratelimit_exhausts_and_raises(monkeypatch):
    cfg = RetryConfig(
        max_retries=2,
        base_delay=0.0,
        enable_circuit_breaker=False,
        retryable_exceptions=[RateLimitError],
    )
    mgr = RetryManager(cfg)
    monkeypatch.setattr("dataquery.transport.retry.asyncio.sleep", AsyncMock())

    calls = {"n": 0}

    async def always_429():
        calls["n"] += 1
        raise RateLimitError("rate limited", retry_after=1)

    with pytest.raises(RateLimitError):
        await mgr.execute_with_retry(always_429)
    assert calls["n"] == 3  # max_retries + 1 attempts, then re-raised


@pytest.mark.asyncio
async def test_expiring_soon_refresh_is_single_flight():
    tm = _token_manager()
    # Valid but expiring soon: lifetime 600s, issued 350s ago → ~250s left,
    # under the default 300s refresh threshold (and 300 < 600 so the
    # short-lifetime short-circuit in is_expiring_soon doesn't apply).
    tm.current_token = OAuthToken(
        access_token="old",
        token_type="Bearer",
        expires_in=600,
        issued_at=datetime.now() - timedelta(seconds=350),
    )
    calls = {"n": 0}

    async def fake_refresh():
        calls["n"] += 1
        await asyncio.sleep(0.01)
        tm.current_token = OAuthToken(
            access_token="new",
            token_type="Bearer",
            expires_in=600,
            issued_at=datetime.now(),
        )
        return tm.current_token

    tm._refresh_token = fake_refresh

    tokens = await asyncio.gather(*[tm.get_valid_token() for _ in range(5)])
    assert calls["n"] == 1  # only one refresh despite five concurrent callers
    assert all(t == "Bearer new" for t in tokens)
