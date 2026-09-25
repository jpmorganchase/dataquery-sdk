"""Rate-limiter tunables."""

from __future__ import annotations

RATE_LIMIT_MIN_WAIT_SECONDS = 0.2

# Upper bound on a single sleep while waiting for a token. The lock is not held
# across the sleep, so waiters re-check the bucket at least this often.
RATE_LIMIT_POLL_INTERVAL_SECONDS = 0.1
