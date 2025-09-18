from types import SimpleNamespace

import pytest

from dataquery.client import DataQueryClient
from dataquery.models import ClientConfig


def test_dataframe_nested_list_and_metadata(monkeypatch):
    try:
        import pandas as pd  # noqa: F401
    except Exception:  # pragma: no cover
        pytest.skip("pandas not available")

    c = DataQueryClient(ClientConfig(base_url="https://api.example.com"))
    c.logger = SimpleNamespace(warning=lambda *a, **k: None)

    data = [
        {
            "_private": 1,
            "name": "n",
            "items": [{"k": 1}, {"k": 2}],
            "times": ["2024-01-01", "2024-02-01"],
            "metrics": [1, 2, 3],
        }
    ]
    df = c.to_dataframe(data, include_metadata=True, flatten_nested=True)
    assert "items_0_k" in df.columns and "items_1_k" in df.columns
