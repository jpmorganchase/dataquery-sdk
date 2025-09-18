import pytest

from dataquery.client import DataQueryClient
from dataquery.exceptions import (
    AuthenticationError,
    NetworkError,
    NotFoundError,
    RateLimitError,
    ValidationError,
)
from dataquery.models import ClientConfig


class Resp:
    def __init__(self, status, headers=None):
        self.status = status
        self.headers = headers or {}
        self.url = "https://api.example.com/x"


@pytest.mark.asyncio
async def test_handle_response_error_mappings():
    cfg = ClientConfig(
        base_url="https://api.example.com", api_base_url="https://api.example.com"
    )
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
