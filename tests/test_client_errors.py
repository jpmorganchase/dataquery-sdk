import json

import pytest

from dataquery.client import DataQueryClient
from dataquery.exceptions import (
    AuthenticationError,
    NetworkError,
    NotFoundError,
    RateLimitError,
    ValidationError,
)
from dataquery.models import ClientConfig, ErrorResponse


class Resp:
    def __init__(self, status, headers=None, body: str = ""):
        self.status = status
        self.headers = headers or {}
        self.url = "https://api.example.com/x"
        self._body = body

    async def text(self) -> str:  # mimic aiohttp.ClientResponse.text
        return self._body


@pytest.mark.asyncio
async def test_handle_response_error_mappings():
    cfg = ClientConfig(base_url="https://api.example.com", api_base_url="https://api.example.com")
    c = DataQueryClient(cfg)

    with pytest.raises(AuthenticationError):
        await c._handle_response(Resp(401))
    with pytest.raises(AuthenticationError):
        await c._handle_response(Resp(403))
    with pytest.raises(NotFoundError):
        await c._handle_response(Resp(404))
    with pytest.raises(RateLimitError):
        await c._handle_response(Resp(429, {"Retry-After": "1"}))
    with pytest.raises(NetworkError):
        await c._handle_response(Resp(500))
    with pytest.raises(ValidationError):
        await c._handle_response(Resp(400))


# ---------------------------------------------------------------------------
# v2 error envelope parsing
# ---------------------------------------------------------------------------


def test_parse_v2_error_flat_payload():
    err = DataQueryClient._parse_v2_error(json.dumps({"code": 4001, "description": "Bad input"}))
    assert isinstance(err, ErrorResponse)
    assert err.code == 4001
    assert err.description == "Bad input"


def test_parse_v2_error_wrapped_under_info():
    err = DataQueryClient._parse_v2_error(json.dumps({"info": {"code": 5001, "description": "Server boom"}}))
    assert err is not None and err.code == 5001 and err.description == "Server boom"


def test_parse_v2_error_wrapped_under_error():
    err = DataQueryClient._parse_v2_error(json.dumps({"error": {"code": "E_AUTH", "description": "Token expired"}}))
    assert err is not None and err.code == "E_AUTH" and err.description == "Token expired"


def test_parse_v2_error_array_under_errors():
    err = DataQueryClient._parse_v2_error(
        json.dumps({"errors": [{"code": 9, "description": "first"}, {"code": 10, "description": "second"}]})
    )
    assert err is not None and err.code == 9 and err.description == "first"


def test_parse_v2_error_returns_none_for_garbage():
    assert DataQueryClient._parse_v2_error("") is None
    assert DataQueryClient._parse_v2_error(None) is None
    assert DataQueryClient._parse_v2_error("not json") is None
    assert DataQueryClient._parse_v2_error(json.dumps({"foo": "bar"})) is None


@pytest.mark.asyncio
async def test_handle_response_400_propagates_code_and_description():
    cfg = ClientConfig(base_url="https://api.example.com", api_base_url="https://api.example.com")
    c = DataQueryClient(cfg)

    body = json.dumps({"code": 4002, "description": "Invalid date range"})
    with pytest.raises(ValidationError) as ei:
        await c._handle_response(Resp(400, body=body))

    err = ei.value
    assert err.details["code"] == 4002
    assert err.details["description"] == "Invalid date range"
    assert err.details["status_code"] == 400
    assert "[4002]" in str(err)
    assert "Invalid date range" in str(err)


@pytest.mark.asyncio
async def test_handle_response_500_propagates_v2_envelope():
    cfg = ClientConfig(base_url="https://api.example.com", api_base_url="https://api.example.com")
    c = DataQueryClient(cfg)

    body = json.dumps({"info": {"code": 5003, "description": "Backend timeout"}})
    with pytest.raises(NetworkError) as ei:
        await c._handle_response(Resp(503, body=body))

    err = ei.value
    assert err.details["code"] == 5003
    assert err.details["description"] == "Backend timeout"
    assert "Backend timeout" in str(err)


@pytest.mark.asyncio
async def test_handle_response_404_uses_code_as_resource_id():
    cfg = ClientConfig(base_url="https://api.example.com", api_base_url="https://api.example.com")
    c = DataQueryClient(cfg)

    body = json.dumps({"code": 404001, "description": "Group not found"})
    with pytest.raises(NotFoundError) as ei:
        await c._handle_response(Resp(404, body=body))

    err = ei.value
    assert err.details["resource_id"] == "404001"
    assert err.details["code"] == 404001
    assert err.details["description"] == "Group not found"


@pytest.mark.asyncio
async def test_handle_response_without_body_still_raises_status_only():
    cfg = ClientConfig(base_url="https://api.example.com", api_base_url="https://api.example.com")
    c = DataQueryClient(cfg)

    with pytest.raises(ValidationError) as ei:
        await c._handle_response(Resp(400, body=""))

    err = ei.value
    # No code/description in details when the body is unparseable.
    assert "code" not in err.details
    assert err.details["status_code"] == 400
