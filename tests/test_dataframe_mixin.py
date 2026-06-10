"""Coverage for DataFrameMixin (the pandas conversion surface).

Pure transformation logic — exercised with plain dicts/lists/objects so it
needs no HTTP client. Skipped when pandas is not installed.
"""

from datetime import datetime
from types import SimpleNamespace

import pytest

pd = pytest.importorskip("pandas")

import structlog  # noqa: E402

from dataquery.core import _mixins as mixins_mod  # noqa: E402
from dataquery.core._mixins import DataFrameMixin  # noqa: E402


class _DF(DataFrameMixin):
    def __init__(self):
        self.logger = structlog.get_logger("test")


@pytest.fixture
def df():
    return _DF()


# --------------------------------------------------------------------------- #
# to_dataframe — shapes
# --------------------------------------------------------------------------- #
def test_list_of_dicts(df):
    out = df.to_dataframe([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}])
    assert len(out) == 2
    assert "a" in out.columns and "b" in out.columns


def test_none_returns_empty(df):
    out = df.to_dataframe(None)
    assert out.empty


def test_primitive_wrapped_in_value_column(df):
    out = df.to_dataframe(42)
    assert list(out["value"]) == [42]


def test_nested_dict_is_flattened(df):
    out = df.to_dataframe([{"id": 1, "attributes": {"price": 10, "vol": 5}}])
    assert "attributes_price" in out.columns
    assert "attributes_vol" in out.columns


def test_nested_list_of_dicts_is_indexed(df):
    out = df.to_dataframe([{"id": 1, "items": [{"k": "a"}, {"k": "b"}]}])
    assert "items_0_k" in out.columns
    assert "items_1_k" in out.columns


def test_list_of_primitives_is_stringified(df):
    out = df.to_dataframe([{"id": 1, "tags": ["x", "y"]}])
    assert out.loc[0, "tags"] == str(["x", "y"])


def test_empty_list_returns_empty(df):
    assert df.to_dataframe([]).empty


# --------------------------------------------------------------------------- #
# metadata + column hints + custom transforms
# --------------------------------------------------------------------------- #
def test_metadata_columns_dropped_by_default(df):
    out = df.to_dataframe([{"_meta": 1, "a": 2}])
    assert "_meta" not in out.columns and "a" in out.columns


def test_metadata_columns_kept_when_requested(df):
    out = df.to_dataframe([{"_meta": 1, "a": 2}], include_metadata=True)
    assert "_meta" in out.columns


def test_date_columns_coerced(df):
    out = df.to_dataframe([{"date": "2024-01-15"}], date_columns=["date"])
    assert pd.api.types.is_datetime64_any_dtype(out["date"])


def test_numeric_columns_coerced(df):
    out = df.to_dataframe([{"val": "10"}, {"val": "20"}], numeric_columns=["val"])
    assert pd.api.types.is_numeric_dtype(out["val"])


def test_custom_transformation_applied(df):
    out = df.to_dataframe([{"a": 1}, {"a": 2}], custom_transformations={"a": lambda x: x * 10})
    assert list(out["a"]) == [10, 20]


def test_custom_transformation_failure_is_swallowed(df):
    def boom(_):
        raise ValueError("nope")

    # Should log + continue, not raise.
    out = df.to_dataframe([{"a": 1}], custom_transformations={"a": boom})
    assert "a" in out.columns


# --------------------------------------------------------------------------- #
# auto-conversion by column-name hint
# --------------------------------------------------------------------------- #
def test_auto_convert_date_hint(df):
    out = df.to_dataframe([{"created": "2024-01-15"}, {"created": "2024-01-16"}])
    assert pd.api.types.is_datetime64_any_dtype(out["created"])


def test_auto_convert_numeric_hint(df):
    out = df.to_dataframe([{"price": "10"}, {"price": "20"}, {"price": "30"}])
    assert pd.api.types.is_numeric_dtype(out["price"])


# --------------------------------------------------------------------------- #
# _convert_value
# --------------------------------------------------------------------------- #
def test_convert_value_datetime_isoformat(df):
    dt = datetime(2024, 1, 15, 10, 30)
    assert df._convert_value(dt) == dt.isoformat()


def test_convert_value_object_with_dict(df):
    obj = SimpleNamespace(x=1)
    assert "x" in df._convert_value(obj)


def test_convert_value_passthrough(df):
    assert df._convert_value(7) == 7
    assert df._convert_value(None) is None


# --------------------------------------------------------------------------- #
# typed convenience wrappers
# --------------------------------------------------------------------------- #
def test_groups_to_dataframe_unwraps_container(df):
    container = SimpleNamespace(groups=[{"group_id": "g1", "group_name": "One"}])
    out = df.groups_to_dataframe(container)
    assert len(out) == 1 and "group_id" in out.columns


def test_instruments_to_dataframe_unwraps_container(df):
    container = SimpleNamespace(instruments=[{"id": "AAPL", "name": "Apple"}])
    out = df.instruments_to_dataframe(container)
    assert len(out) == 1


def test_time_series_to_dataframe_reads_data_attr(df):
    ts = SimpleNamespace(data=[{"date": "2024-01-15", "value": "10"}])
    out = df.time_series_to_dataframe(ts)
    assert len(out) == 1
    assert pd.api.types.is_datetime64_any_dtype(out["date"])
    assert pd.api.types.is_numeric_dtype(out["value"])


def test_files_to_dataframe_unwraps_container(df):
    container = SimpleNamespace(file_group_ids=[{"file_group_id": "f1", "file_size": "100"}])
    out = df.files_to_dataframe(container)
    assert len(out) == 1
    assert pd.api.types.is_numeric_dtype(out["file_size"])


# --------------------------------------------------------------------------- #
# pandas-missing guard
# --------------------------------------------------------------------------- #
def test_to_dataframe_requires_pandas(df, monkeypatch):
    monkeypatch.setattr(mixins_mod, "HAS_PANDAS", False)
    with pytest.raises(ImportError, match="pandas is required"):
        df.to_dataframe([{"a": 1}])
