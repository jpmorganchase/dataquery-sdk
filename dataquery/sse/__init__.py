"""Server-Sent Events (SSE) client, event-id persistence, and the
notification-driven download manager."""

from __future__ import annotations

from .client import SSEClient, SSEEvent
from .event_store import (
    SSEEventIdStore,
    Subscription,
    build_event_id_store,
    resolve_sse_state_dir,
)
from .subscriber import NotificationDownloadManager

__all__ = [
    "NotificationDownloadManager",
    "SSEClient",
    "SSEEvent",
    "SSEEventIdStore",
    "Subscription",
    "build_event_id_store",
    "resolve_sse_state_dir",
]
