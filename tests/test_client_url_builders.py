from dataquery.core.client import DataQueryClient
from dataquery.types.models import ClientConfig


def test_url_builders_and_extract():
    cfg = ClientConfig(base_url="https://api.example.com", context_path="/ctx", files_base_url=None)
    client = DataQueryClient(cfg)
    # _build_api_url
    u1 = client._build_api_url("groups")
    assert u1 == "https://api.example.com/ctx/groups"
    # _build_files_api_url
    u2 = client._build_files_api_url("group/file/download")
    assert u2.startswith("https://api.example.com")
    # _extract_endpoint
    assert client._extract_endpoint(u2).endswith("group/file/download")
