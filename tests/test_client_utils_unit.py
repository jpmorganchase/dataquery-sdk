from dataquery.client import format_file_size, format_duration, DataQueryClient
from dataquery.models import ClientConfig


def test_format_file_size():
    assert format_file_size(0) == "0 B"
    assert format_file_size(1023) in ("1023 B", "1023.00 B")
    assert format_file_size(1024) in ("1.0 KB", "1.00 KB")
    assert format_file_size(1024 * 1024) in ("1.0 MB", "1.00 MB")


def test_format_duration():
    assert format_duration(0) in ("0s", "0.0s", "0.00s")
    assert format_duration(1).endswith("s")


def test_url_builders_and_extract():
    cfg = ClientConfig(base_url="https://api.example.com", context_path="/ctx")
    client = DataQueryClient(cfg)
    # _build_api_url
    u1 = client._build_api_url("groups")
    assert u1 == "https://api.example.com/ctx/groups"
    # _build_files_api_url
    u2 = client._build_files_api_url("group/file/download")
    assert u2.startswith("https://api.example.com")
    # _extract_endpoint
    assert client._extract_endpoint(u2).endswith("group/file/download")

