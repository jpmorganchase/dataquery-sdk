"""HTTP transport building blocks: OAuth/bearer auth, rate limiting, retry /
circuit breaker, and connection pool monitoring."""

from __future__ import annotations

from .auth import OAuthManager, TokenManager
from .connection_pool import (
    ConnectionPoolConfig,
    ConnectionPoolMonitor,
    ConnectionPoolStats,
    create_connection_pool_config,
)
from .rate_limiter import (
    EnhancedTokenBucketRateLimiter,
    QueuedRequest,
    QueuePriority,
    RateLimitConfig,
    RateLimitContext,
    RateLimitState,
    TokenBucketRateLimiter,
    create_rate_limiter,
)
from .retry import (
    CircuitBreaker,
    CircuitState,
    RetryConfig,
    RetryManager,
    RetryStats,
    RetryStrategy,
    create_retry_config,
    create_retry_manager,
)

__all__ = [
    # auth
    "OAuthManager",
    "TokenManager",
    # connection pool
    "ConnectionPoolConfig",
    "ConnectionPoolMonitor",
    "ConnectionPoolStats",
    "create_connection_pool_config",
    # rate limiter
    "EnhancedTokenBucketRateLimiter",
    "QueuedRequest",
    "QueuePriority",
    "RateLimitConfig",
    "RateLimitContext",
    "RateLimitState",
    "TokenBucketRateLimiter",
    "create_rate_limiter",
    # retry
    "CircuitBreaker",
    "CircuitState",
    "RetryConfig",
    "RetryManager",
    "RetryStats",
    "RetryStrategy",
    "create_retry_config",
    "create_retry_manager",
]
