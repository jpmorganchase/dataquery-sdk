from dataquery.utils import format_file_size, format_duration


def test_format_file_size_edges():
    assert format_file_size(0) == "0 B"
    assert format_file_size(-1).endswith("B")
    assert format_file_size(1536).startswith("1.5")


def test_format_duration_edges():
    assert format_duration(0) == "0s"
    assert format_duration(-1).endswith("s")
    assert format_duration(120).startswith("2m")

