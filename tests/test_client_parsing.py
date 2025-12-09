from types import SimpleNamespace

from dataquery.utils import get_filename_from_response, parse_content_disposition


def test_parse_content_disposition_variants():
    assert parse_content_disposition('attachment; filename="file.csv"') == "file.csv"
    assert parse_content_disposition("attachment; filename=file.csv") == "file.csv"
    assert (
        parse_content_disposition("attachment; filename*=UTF-8''file%20name.csv")
        == "file name.csv"
    )
    assert parse_content_disposition("") is None
    assert parse_content_disposition(None) is None  # type: ignore[arg-type]


def test_get_filename_from_response_falls_back():
    resp = SimpleNamespace(headers={})
    assert get_filename_from_response(resp, "FG", "20240101").startswith("FG_20240101")


def test_get_filename_from_response_header():
    resp = SimpleNamespace(
        headers={"content-disposition": 'attachment; filename="data.bin"'}
    )
    assert get_filename_from_response(resp, "FG") == "data.bin"
