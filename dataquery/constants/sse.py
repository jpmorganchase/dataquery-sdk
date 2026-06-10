"""SSE client tunables."""

from __future__ import annotations

SSE_HEALTHY_CONNECTION_SECONDS = 30.0

SSE_MAX_AUTH_RETRIES = 2

# Generous per-line read buffer so a large frame can't trip aiohttp's LineTooLong.
SSE_READ_BUFSIZE = 1024 * 1024
