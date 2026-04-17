"""
Persistent storage for the last-seen SSE event ID.

Enables cross-process event replay: when a subscription reconnects, the stored
``last-event-id`` is sent to the server (as both a URL query parameter and the
``Last-Event-ID`` header) so the server can replay any events published while
the client was disconnected.

The on-disk format and atomic-write pattern mirrors the OAuth token persistence
in :mod:`dataquery.auth` (owner-only ``0o600`` temp file, then atomic rename).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional, Union

from .models import ClientConfig

logger = logging.getLogger(__name__)


def _fingerprint_subscription(group_id: str, file_group_id: Optional[Union[str, Iterable[str]]]) -> str:
    """Return a deterministic short hash identifying a subscription.

    The file name must be path-safe regardless of what characters appear in the
    group / file-group ids (they come from an external API), so we hash the
    canonical form rather than interpolating ids into the filename.
    """
    if file_group_id is None:
        fg_part = ""
    elif isinstance(file_group_id, str):
        fg_part = file_group_id
    else:
        fg_part = ",".join(sorted(str(x) for x in file_group_id))
    canonical = f"group-id={group_id}|file-group-id={fg_part}"
    return hashlib.sha1(canonical.encode("utf-8")).hexdigest()[:16]


def resolve_sse_state_dir(config: ClientConfig) -> Optional[Path]:
    """Resolve the directory where SSE event-id state files live.

    Mirrors the resolution order used by :class:`dataquery.auth.TokenManager`:

    1. ``token_storage_enabled`` + ``token_storage_dir`` → ``<dir>/.sse_state/``
    2. ``download_dir`` set → ``<download_dir>/.sse_state/``
    3. Otherwise ``None`` — persistence is silently disabled.
    """
    base_dir: Optional[Path] = None
    token_storage_enabled = bool(getattr(config, "token_storage_enabled", False))
    token_storage_dir = getattr(config, "token_storage_dir", None)
    if token_storage_enabled and token_storage_dir:
        base_dir = Path(token_storage_dir)
    elif getattr(config, "download_dir", None):
        try:
            if str(config.download_dir).strip():
                base_dir = Path(config.download_dir)
        except Exception:
            base_dir = None

    if not base_dir:
        return None
    state_dir = base_dir / ".sse_state"
    try:
        state_dir.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(state_dir, 0o700)
        except OSError:
            pass
    except OSError as exc:
        logger.warning("Could not create SSE state directory %s: %s", state_dir, exc)
        return None
    return state_dir


def build_event_id_store(
    config: ClientConfig,
    group_id: str,
    file_group_id: Optional[Union[str, Iterable[str]]] = None,
) -> Optional["SSEEventIdStore"]:
    """Return a configured :class:`SSEEventIdStore` or ``None`` if persistence
    is unavailable (no storage directory resolvable)."""
    state_dir = resolve_sse_state_dir(config)
    if state_dir is None:
        return None
    subscription_id = _fingerprint_subscription(group_id, file_group_id)
    file_path = state_dir / f"sse_{subscription_id}.json"
    subscription_label = f"group-id={group_id}"
    if file_group_id is not None:
        if isinstance(file_group_id, str):
            subscription_label += f"&file-group-id={file_group_id}"
        else:
            subscription_label += "&file-group-id=" + ",".join(sorted(str(x) for x in file_group_id))
    return SSEEventIdStore(file_path=file_path, subscription=subscription_label)


class SSEEventIdStore:
    """Persist the most recent SSE event id for a single subscription."""

    def __init__(self, file_path: Path, subscription: str = "") -> None:
        self.file_path = Path(file_path)
        self.subscription = subscription

    def load(self) -> Optional[str]:
        """Return the last persisted event id, or ``None`` if unavailable."""
        if not self.file_path.exists():
            return None
        try:
            with open(self.file_path, "r") as f:
                data = json.load(f)
            event_id = data.get("last_event_id")
            if isinstance(event_id, str) and event_id:
                return event_id
            return None
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            logger.warning("Failed to load SSE event id from %s: %s", self.file_path, exc)
            return None

    async def save(self, event_id: str) -> None:
        """Atomically persist ``event_id``. Failures are logged, not raised."""
        if not event_id:
            return
        payload = {
            "last_event_id": event_id,
            "updated_at": datetime.now().isoformat(),
            "subscription": self.subscription,
        }
        temp_file = self.file_path.with_suffix(".tmp")
        try:
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
            fd = os.open(temp_file, flags, 0o600)
            try:
                with os.fdopen(fd, "w") as f:
                    json.dump(payload, f, indent=2)
            except BaseException:
                try:
                    os.close(fd)
                except OSError:
                    pass
                raise
            temp_file.replace(self.file_path)
        except OSError as exc:
            logger.warning("Failed to save SSE event id to %s: %s", self.file_path, exc)
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except OSError:
                    pass

    def clear(self) -> None:
        """Delete the persisted event-id file, if it exists."""
        if self.file_path.exists():
            try:
                self.file_path.unlink()
                logger.info("SSE event id file removed: %s", self.file_path)
            except OSError as exc:
                logger.warning("Failed to remove SSE event id file %s: %s", self.file_path, exc)
