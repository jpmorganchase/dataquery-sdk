"""Tests for ``dataquery.sse_event_store``.

Covers the persistent SSE event-id store: atomic save, clear, missing file,
corrupted JSON, the storage-directory resolver, and the per-subscription
filename fingerprint.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from dataquery.models import ClientConfig
from dataquery.sse.event_store import (
    SSEEventIdStore,
    Subscription,
    _fingerprint_subscription,
    build_event_id_store,
    resolve_sse_state_dir,
)


def _config(tmp_path: Path, **overrides) -> ClientConfig:
    base = dict(
        base_url="https://api.example.com",
        oauth_enabled=False,
        bearer_token="T",
        download_dir=str(tmp_path),
    )
    base.update(overrides)
    return ClientConfig(**base)


# ---------------------------------------------------------------------------
# Fingerprint
# ---------------------------------------------------------------------------


def test_fingerprint_is_stable_for_identical_subscription():
    a = _fingerprint_subscription("G", "FG")
    b = _fingerprint_subscription("G", "FG")
    assert a == b


def test_fingerprint_changes_when_group_changes():
    assert _fingerprint_subscription("G1", None) != _fingerprint_subscription("G2", None)


def test_fingerprint_treats_list_order_as_irrelevant():
    """Sorted list serialisation means [A, B] and [B, A] map to the same file."""
    assert _fingerprint_subscription("G", ["A", "B"]) == _fingerprint_subscription("G", ["B", "A"])


def test_fingerprint_distinguishes_string_and_list_when_content_differs():
    # Sanity: changing the file-group-id at all changes the fingerprint.
    assert _fingerprint_subscription("G", "A") != _fingerprint_subscription("G", "B")


# ---------------------------------------------------------------------------
# resolve_sse_state_dir
# ---------------------------------------------------------------------------


def test_resolve_state_dir_uses_download_dir(tmp_path: Path):
    cfg = _config(tmp_path)
    state_dir = resolve_sse_state_dir(cfg)
    assert state_dir == tmp_path / ".sse_state"
    assert state_dir.exists()


def test_resolve_state_dir_prefers_token_storage_when_enabled(tmp_path: Path):
    custom = tmp_path / "tokens"
    custom.mkdir()
    cfg = _config(
        tmp_path,
        token_storage_enabled=True,
        token_storage_dir=str(custom),
    )
    state_dir = resolve_sse_state_dir(cfg)
    assert state_dir == custom / ".sse_state"


def test_resolve_state_dir_returns_none_when_unconfigured():
    cfg = ClientConfig(
        base_url="https://api.example.com",
        oauth_enabled=False,
        bearer_token="T",
        download_dir="",
    )
    # Empty download_dir → no anchor → None.
    assert resolve_sse_state_dir(cfg) is None


# ---------------------------------------------------------------------------
# build_event_id_store
# ---------------------------------------------------------------------------


def test_build_event_id_store_returns_store_with_per_subscription_path(tmp_path: Path):
    cfg = _config(tmp_path)
    s1 = build_event_id_store(cfg, Subscription.from_user("G"))
    s2 = build_event_id_store(cfg, Subscription.from_user("G", "FG1"))
    s3 = build_event_id_store(cfg, Subscription.from_user("G2"))
    assert s1 is not None and s2 is not None and s3 is not None
    paths = {s.file_path for s in (s1, s2, s3)}
    assert len(paths) == 3, "Each subscription must persist to its own file"
    for s in (s1, s2, s3):
        assert s.file_path.parent == tmp_path / ".sse_state"
        assert s.file_path.name.startswith("sse_") and s.file_path.suffix == ".json"


def test_build_event_id_store_returns_none_when_path_unavailable():
    cfg = ClientConfig(
        base_url="https://api.example.com",
        oauth_enabled=False,
        bearer_token="T",
        download_dir="",
    )
    assert build_event_id_store(cfg, Subscription.from_user("G")) is None


# ---------------------------------------------------------------------------
# Subscription dataclass
# ---------------------------------------------------------------------------


def test_subscription_from_user_normalises_none_to_empty_tuple():
    sub = Subscription.from_user("G", None)
    assert sub.file_group_ids == ()
    assert sub.file_group_csv is None


def test_subscription_from_user_wraps_single_string():
    sub = Subscription.from_user("G", "FG")
    assert sub.file_group_ids == ("FG",)
    assert sub.file_group_csv == "FG"


def test_subscription_from_user_sorts_iterable_for_determinism():
    a = Subscription.from_user("G", ["B", "A", "C"])
    b = Subscription.from_user("G", ["C", "B", "A"])
    assert a == b
    assert a.file_group_ids == ("A", "B", "C")


def test_subscription_query_params_omit_file_group_when_unrestricted():
    assert Subscription.from_user("G").query_params() == {"group-id": "G"}


def test_subscription_query_params_include_csv_when_restricted():
    params = Subscription.from_user("G", ["A", "B"]).query_params()
    assert params == {"group-id": "G", "file-group-id": "A,B"}


def test_subscription_label_round_trips_to_canonical_form():
    assert Subscription.from_user("G").label() == "group-id=G"
    assert Subscription.from_user("G", "FG").label() == "group-id=G&file-group-id=FG"
    assert Subscription.from_user("G", ["B", "A"]).label() == "group-id=G&file-group-id=A,B"


def test_subscription_fingerprint_matches_legacy_helper():
    """The wrapper exists precisely so persisted state files survive the refactor."""
    assert Subscription.from_user("G").fingerprint() == _fingerprint_subscription("G", None)
    assert Subscription.from_user("G", "FG").fingerprint() == _fingerprint_subscription("G", "FG")
    assert Subscription.from_user("G", ["A", "B"]).fingerprint() == _fingerprint_subscription("G", ["A", "B"])


def test_subscription_is_hashable_and_frozen():
    sub = Subscription.from_user("G", "FG")
    # Frozen → hashable, usable as dict key / set member.
    {sub: 1}
    with pytest.raises(Exception):
        sub.group_id = "X"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# SSEEventIdStore behaviour
# ---------------------------------------------------------------------------


def test_load_returns_none_when_file_missing(tmp_path: Path):
    store = SSEEventIdStore(tmp_path / "missing.json")
    assert store.load() is None


@pytest.mark.asyncio
async def test_save_then_load_round_trip(tmp_path: Path):
    store = SSEEventIdStore(tmp_path / "state.json", subscription="group-id=G")
    await store.save("evt-42")
    assert store.load() == "evt-42"


@pytest.mark.asyncio
async def test_save_overwrites_previous_value(tmp_path: Path):
    store = SSEEventIdStore(tmp_path / "state.json")
    await store.save("evt-1")
    await store.save("evt-2")
    assert store.load() == "evt-2"


@pytest.mark.asyncio
async def test_save_creates_parent_directory(tmp_path: Path):
    nested = tmp_path / "deeply" / "nested" / "state.json"
    store = SSEEventIdStore(nested)
    await store.save("evt-1")
    assert nested.exists()
    assert store.load() == "evt-1"


@pytest.mark.asyncio
async def test_save_empty_event_id_is_noop(tmp_path: Path):
    store = SSEEventIdStore(tmp_path / "state.json")
    await store.save("")
    assert not (tmp_path / "state.json").exists()


def test_load_returns_none_when_file_is_corrupt(tmp_path: Path):
    p = tmp_path / "state.json"
    p.write_text("{not valid json")
    store = SSEEventIdStore(p)
    assert store.load() is None


def test_load_returns_none_when_event_id_field_missing(tmp_path: Path):
    p = tmp_path / "state.json"
    p.write_text('{"some_other_field": "x"}')
    store = SSEEventIdStore(p)
    assert store.load() is None


@pytest.mark.asyncio
async def test_clear_removes_file(tmp_path: Path):
    store = SSEEventIdStore(tmp_path / "state.json")
    await store.save("evt-1")
    assert store.file_path.exists()
    store.clear()
    assert not store.file_path.exists()
    assert store.load() is None


def test_clear_when_file_missing_is_noop(tmp_path: Path):
    store = SSEEventIdStore(tmp_path / "missing.json")
    # Should not raise.
    store.clear()


@pytest.mark.asyncio
async def test_save_does_not_leave_temp_file_behind(tmp_path: Path):
    store = SSEEventIdStore(tmp_path / "state.json")
    await store.save("evt-1")
    leftovers = list(tmp_path.glob("*.tmp"))
    assert leftovers == []


# ---------------------------------------------------------------------------
# Save dedup + write-format optimisations
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_save_skips_disk_write_when_id_unchanged(tmp_path: Path):
    """Repeated saves of the same id must not trigger redundant disk writes.

    SSE servers commonly resend the boundary id on reconnect, so without
    dedup we'd hit the disk on every reconnect echo.
    """
    p = tmp_path / "state.json"
    store = SSEEventIdStore(p)
    await store.save("evt-1")
    mtime_first = p.stat().st_mtime_ns
    # Replay the same id many times — none should rewrite the file.
    for _ in range(5):
        await store.save("evt-1")
    assert p.stat().st_mtime_ns == mtime_first
    # A new id must still write.
    await store.save("evt-2")
    assert store.load() == "evt-2"


@pytest.mark.asyncio
async def test_save_after_load_dedups_against_persisted_value(tmp_path: Path):
    """Dedup must seed itself from the on-disk value at load() time so the
    very first save() in a fresh process doesn't rewrite the same id."""
    p = tmp_path / "state.json"
    SSEEventIdStore(p)  # write once via a sibling instance
    seed = SSEEventIdStore(p)
    await seed.save("evt-1")
    fresh = SSEEventIdStore(p)
    assert fresh.load() == "evt-1"
    mtime = p.stat().st_mtime_ns
    await fresh.save("evt-1")
    assert p.stat().st_mtime_ns == mtime


@pytest.mark.asyncio
async def test_save_writes_compact_json(tmp_path: Path):
    """Compact form (no indented whitespace) keeps per-event writes cheap."""
    p = tmp_path / "state.json"
    store = SSEEventIdStore(p, subscription="group-id=G")
    await store.save("evt-1")
    raw = p.read_text(encoding="utf-8")
    # No newlines or two-space indentation.
    assert "\n" not in raw
    assert ": " not in raw  # compact separator omits the space after colon


@pytest.mark.asyncio
async def test_concurrent_saves_are_serialised(tmp_path: Path):
    """Concurrent fire-and-forget saves must converge to a valid file."""
    p = tmp_path / "state.json"
    store = SSEEventIdStore(p)
    # Fire 20 distinct saves concurrently — the file must end up with one of
    # them, never half-written.
    await asyncio.gather(*(store.save(f"evt-{i}") for i in range(20)))
    final = store.load()
    assert final is not None
    assert final.startswith("evt-")


@pytest.mark.asyncio
async def test_clear_resets_dedup_cache(tmp_path: Path):
    """After clear(), the next save() of the same id must hit disk again."""
    p = tmp_path / "state.json"
    store = SSEEventIdStore(p)
    await store.save("evt-1")
    store.clear()
    assert not p.exists()
    await store.save("evt-1")
    assert p.exists() and store.load() == "evt-1"
