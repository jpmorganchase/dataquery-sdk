"""Persistent storage for the last-seen SSE event ID."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple, Union

from ..types.models import ClientConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Subscription:
    """Identifies a single SSE notification subscription."""

    group_id: str
    file_group_ids: Tuple[str, ...] = field(default_factory=tuple)

    @classmethod
    def from_user(
        cls,
        group_id: str,
        file_group_id: Optional[Union[str, Iterable[str]]] = None,
    ) -> "Subscription":
        if file_group_id is None:
            ids: Tuple[str, ...] = ()
        elif isinstance(file_group_id, str):
            ids = (file_group_id,)
        else:
            ids = tuple(sorted(str(x) for x in file_group_id))
        return cls(group_id=group_id, file_group_ids=ids)

    @property
    def file_group_csv(self) -> Optional[str]:
        return ",".join(self.file_group_ids) if self.file_group_ids else None

    def query_params(self) -> Dict[str, str]:
        """URL query parameters identifying this subscription."""
        params: Dict[str, str] = {"group-id": self.group_id}
        csv = self.file_group_csv
        if csv:
            params["file-group-id"] = csv
        return params

    def fingerprint(self) -> str:
        """Deterministic short hash; used as the on-disk state filename."""
        canonical = f"group-id={self.group_id}|file-group-id={','.join(self.file_group_ids)}"
        return hashlib.sha1(canonical.encode("utf-8")).hexdigest()[:16]

    def label(self) -> str:
        """Human-readable identifier; persisted in the store JSON metadata."""
        csv = self.file_group_csv
        return f"group-id={self.group_id}&file-group-id={csv}" if csv else f"group-id={self.group_id}"


def _fingerprint_subscription(group_id: str, file_group_id: Optional[Union[str, Iterable[str]]] = None) -> str:
    """Backward-compat wrapper. Prefer :meth:`Subscription.fingerprint`."""
    return Subscription.from_user(group_id, file_group_id).fingerprint()


def resolve_sse_state_dir(config: ClientConfig) -> Optional[Path]:
    """Resolve the directory where SSE event-id state files live."""
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
    subscription: Subscription,
) -> Optional["SSEEventIdStore"]:
    """Return a configured :class:`SSEEventIdStore` or ``None`` if persistence is unavailable."""
    state_dir = resolve_sse_state_dir(config)
    if state_dir is None:
        return None
    file_path = state_dir / f"sse_{subscription.fingerprint()}.json"
    return SSEEventIdStore(file_path=file_path, subscription=subscription.label())


class SSEEventIdStore:
    """Persist the most recent SSE event id for a single subscription."""

    def __init__(self, file_path: Path, subscription: str = "") -> None:
        self.file_path = Path(file_path)
        self.subscription = subscription
        self._last_saved_id: Optional[str] = None
        self._save_lock: Optional[asyncio.Lock] = None

    def load(self) -> Optional[str]:
        """Return the last persisted event id, or ``None`` if unavailable."""
        if not self.file_path.exists():
            return None
        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            event_id = data.get("last_event_id")
            if event_id is not None:
                event_id_str = str(event_id)
                self._last_saved_id = event_id_str
                return event_id_str
            return None
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            logger.warning("Failed to load SSE event id from %s: %s", self.file_path, exc)
            return None

    async def save(self, event_id: str) -> None:
        """Atomically persist ``event_id``. Failures are logged, not raised."""
        if not event_id or not event_id.isdigit():
            return
        if event_id == self._last_saved_id:
            return
        if self._save_lock is None:
            self._save_lock = asyncio.Lock()
        async with self._save_lock:
            if event_id == self._last_saved_id:
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
                    with os.fdopen(fd, "w", encoding="utf-8") as f:
                        json.dump(payload, f, separators=(",", ":"))
                except BaseException:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
                    raise
                temp_file.replace(self.file_path)
                self._last_saved_id = event_id
            except OSError as exc:
                logger.warning("Failed to save SSE event id to %s: %s", self.file_path, exc)
                if temp_file.exists():
                    try:
                        temp_file.unlink()
                    except OSError:
                        pass

    def clear(self) -> None:
        """Delete the persisted event-id file, if it exists."""
        self._last_saved_id = None
        if self.file_path.exists():
            try:
                self.file_path.unlink()
                logger.info("SSE event id file removed: %s", self.file_path)
            except OSError as exc:
                logger.warning("Failed to remove SSE event id file %s: %s", self.file_path, exc)
