"""SSE client tunables."""

from __future__ import annotations

# A connection that stays open at least this long is considered healthy for
# reconnect-backoff purposes — the next disconnect resets the exponential
# backoff so an expected periodic server-side recycle doesn't inflate the
# reconnect delay across cycles.
SSE_HEALTHY_CONNECTION_SECONDS = 30.0
