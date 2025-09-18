"""
Enhanced rate limiting implementation for the DATAQUERY SDK.

Provides token bucket rate limiting with configurable burst capacity,
requests per minute limits, and a queuing mechanism to prevent breaches.
"""

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, Optional

import structlog

logger = structlog.get_logger(__name__)


class QueuePriority(Enum):
    """Priority levels for queued requests."""

    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4

    def __lt__(self, other):
        """Enable comparison of priorities."""
        if isinstance(other, QueuePriority):
            return self.value < other.value
        return NotImplemented

    def __le__(self, other):
        """Enable comparison of priorities."""
        if isinstance(other, QueuePriority):
            return self.value <= other.value
        return NotImplemented

    def __gt__(self, other):
        """Enable comparison of priorities."""
        if isinstance(other, QueuePriority):
            return self.value > other.value
        return NotImplemented

    def __ge__(self, other):
        """Enable comparison of priorities."""
        if isinstance(other, QueuePriority):
            return self.value >= other.value
        return NotImplemented


@dataclass
class QueuedRequest:
    """Represents a queued request."""

    priority: QueuePriority
    timestamp: float
    request_id: str
    operation: str
    future: asyncio.Future
    timeout: Optional[float] = None


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""

    requests_per_minute: int = 300  # Exactly 5 requests per second (200ms per request)
    burst_capacity: int = 1  # No burst allowed per specification
    window_size_seconds: int = 60
    retry_after_header: str = "Retry-After"
    enable_rate_limiting: bool = True

    # Queue configuration
    max_queue_size: int = 1000
    enable_queuing: bool = True
    queue_timeout: float = 600.0  # 10 minutes
    priority_enabled: bool = True

    # Adaptive rate limiting
    adaptive_rate_limiting: bool = True
    backoff_multiplier: float = 1.5
    max_backoff_seconds: float = 60.0

    def __post_init__(self):
        """Validate configuration."""
        if self.requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be positive")
        if self.burst_capacity <= 0:
            raise ValueError("burst_capacity must be positive")
        if self.window_size_seconds <= 0:
            raise ValueError("window_size_seconds must be positive")
        if self.burst_capacity > self.requests_per_minute:
            raise ValueError("burst_capacity cannot exceed requests_per_minute")
        if self.max_queue_size <= 0:
            raise ValueError("max_queue_size must be positive")
        if self.queue_timeout <= 0:
            raise ValueError("queue_timeout must be positive")


@dataclass
class RateLimitState:
    """Internal state for rate limiting."""

    tokens: float = field(default=0.0)
    last_refill: float = field(default_factory=time.time)
    request_count: int = 0
    window_start: float = field(default_factory=time.time)
    retry_after: Optional[float] = None

    # Queue state
    queue: deque = field(default_factory=deque)
    processing: bool = False
    last_request_time: float = field(default_factory=time.time)

    # Adaptive rate limiting
    consecutive_failures: int = 0
    current_backoff: float = 0.0
    last_failure_time: float = 0.0


class EnhancedTokenBucketRateLimiter:
    """Enhanced token bucket rate limiter with queuing mechanism."""

    def __init__(self, config: RateLimitConfig):
        self.config = config
        self.state = RateLimitState()
        self._lock: Optional[asyncio.Lock] = None
        self._queue_processor_task: Optional[asyncio.Task] = None
        self._shutdown_event: Optional[asyncio.Event] = None
        self._queue_processor_started = False

        # Initialize tokens to burst capacity
        self.state.tokens = float(config.burst_capacity)
        self.state.last_refill = time.time()

        logger.info(
            "Enhanced rate limiter initialized",
            requests_per_minute=config.requests_per_minute,
            burst_capacity=config.burst_capacity,
            queuing_enabled=config.enable_queuing,
            max_queue_size=config.max_queue_size,
        )

    def _get_lock(self) -> asyncio.Lock:
        """Get the asyncio lock, creating it if necessary."""
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _get_shutdown_event(self) -> asyncio.Event:
        """Get the shutdown event, creating it if necessary."""
        if self._shutdown_event is None:
            self._shutdown_event = asyncio.Event()
        return self._shutdown_event

    def _start_queue_processor(self):
        """Start the background queue processor."""
        if self._queue_processor_task is None or self._queue_processor_task.done():
            try:
                # Check if there's a running event loop
                loop = asyncio.get_running_loop()
                self._queue_processor_task = loop.create_task(self._process_queue())
                self._queue_processor_started = True
            except RuntimeError:
                # No running event loop, will start when acquire is called
                self._queue_processor_started = False

    async def _process_queue(self):
        """Background task to process queued requests."""
        while not self._get_shutdown_event().is_set():
            try:
                async with self._get_lock():
                    if not self.state.queue:
                        await asyncio.sleep(0.1)
                        continue

                    # Get next request from queue (priority-based)
                    request = self._get_next_request()
                    if request is None:
                        await asyncio.sleep(0.1)
                        continue

                    # Check if request has timed out
                    if (
                        request.timeout
                        and time.time() - request.timestamp > request.timeout
                    ):
                        if not request.future.done():
                            request.future.set_exception(
                                asyncio.TimeoutError("Request timed out in queue")
                            )
                        continue

                    # Try to acquire token
                    if await self._try_acquire_token():
                        # Remove from queue and resolve future
                        self.state.queue.remove(request)
                        if not request.future.done():
                            request.future.set_result(True)
                    else:
                        # Put back at front of queue and wait
                        await asyncio.sleep(self._calculate_wait_time())

            except Exception as e:
                logger.error("Error in queue processor", error=str(e))
                await asyncio.sleep(1.0)

    def _get_next_request(self) -> Optional[QueuedRequest]:
        """Get the next request from queue based on priority."""
        if not self.state.queue:
            return None

        if not self.config.priority_enabled:
            return self.state.queue[0]

        # Find highest priority request
        highest_priority = max(req.priority.value for req in self.state.queue)
        for request in self.state.queue:
            if request.priority.value == highest_priority:
                return request

        return None

    async def _try_acquire_token(self) -> bool:
        """Try to acquire a token without waiting."""
        self._refill_tokens()

        if self.state.tokens >= 1.0:
            self.state.tokens -= 1.0
            self.state.request_count += 1
            self.state.last_request_time = time.time()
            return True

        return False

    async def acquire(
        self,
        timeout: Optional[float] = None,
        priority: QueuePriority = QueuePriority.NORMAL,
        operation: str = "unknown",
    ) -> bool:
        """
        Acquire a token for making a request with optimized performance.

        Args:
            timeout: Maximum time to wait for a token (None = no timeout)
            priority: Priority level for queuing (ignored for performance)
            operation: Operation name for logging

        Returns:
            True if token acquired, False if timeout
        """
        if not self.config.enable_rate_limiting:
            return True

        # Use optimized simple acquisition for better performance
        return await self._optimized_acquire(timeout, operation)

    async def _optimized_acquire(
        self, timeout: Optional[float] = None, operation: str = "unknown"
    ) -> bool:
        """Optimized token acquisition with minimal overhead."""
        start_time = time.time()

        async with self._get_lock():
            while True:
                self._refill_tokens()

                if self.state.tokens >= 1.0:
                    self.state.tokens -= 1.0
                    self.state.request_count += 1
                    self.state.last_request_time = time.time()
                    return True

                # Calculate wait time more efficiently
                wait_time = self._calculate_wait_time()

                if timeout is not None:
                    elapsed = time.time() - start_time
                    if elapsed + wait_time > timeout:
                        return False

                # Use shorter sleep intervals for better responsiveness
                await asyncio.sleep(min(wait_time, 0.1))

    async def _simple_acquire(self, timeout: Optional[float] = None) -> bool:
        """Simple token bucket acquisition without queuing."""
        start_time = time.time()

        async with self._get_lock():
            while True:
                self._refill_tokens()

                if self.state.tokens >= 1.0:
                    self.state.tokens -= 1.0
                    self.state.request_count += 1
                    return True

                wait_time = self._calculate_wait_time()

                if timeout is not None:
                    elapsed = time.time() - start_time
                    if elapsed + wait_time > timeout:
                        return False

                await asyncio.sleep(wait_time)

    def _refill_tokens(self):
        """Refill tokens based on time elapsed."""
        now = time.time()
        time_elapsed = now - self.state.last_refill

        # Apply adaptive backoff if needed
        if self.config.adaptive_rate_limiting and self.state.current_backoff > 0:
            time_elapsed = min(time_elapsed, self.state.current_backoff)

        # Calculate tokens to add
        tokens_per_second = self.config.requests_per_minute / 60.0
        tokens_to_add = time_elapsed * tokens_per_second

        # Add tokens up to burst capacity
        self.state.tokens = min(
            self.config.burst_capacity, self.state.tokens + tokens_to_add
        )

        self.state.last_refill = now

        # Reset window if needed
        if now - self.state.window_start >= self.config.window_size_seconds:
            self.state.request_count = 0
            self.state.window_start = now

    def _calculate_wait_time(self) -> float:
        """Calculate time to wait for next token."""
        tokens_per_second = self.config.requests_per_minute / 60.0
        if tokens_per_second <= 0:
            return 1.0

        base_wait = 1.0 / tokens_per_second

        # Enforce minimum 200ms delay per DataQuery API specification
        min_wait = 0.2  # 200 milliseconds
        base_wait = max(base_wait, min_wait)

        # Apply adaptive backoff if needed
        if self.config.adaptive_rate_limiting and self.state.current_backoff > 0:
            return max(base_wait, self.state.current_backoff)

        return base_wait

    def handle_rate_limit_response(self, headers: Dict[str, str]) -> None:
        """
        Handle rate limit response from server with adaptive backoff.

        Args:
            headers: Response headers from the server
        """
        retry_after = headers.get(self.config.retry_after_header)
        if retry_after:
            try:
                self.state.retry_after = float(retry_after)
                self.state.consecutive_failures += 1
                self.state.last_failure_time = time.time()

                # Apply adaptive backoff
                if self.config.adaptive_rate_limiting:
                    self.state.current_backoff = min(
                        self.config.max_backoff_seconds,
                        self.state.current_backoff * self.config.backoff_multiplier,
                    )

                logger.warning(
                    "Server rate limit hit",
                    retry_after=retry_after,
                    consecutive_failures=self.state.consecutive_failures,
                    current_backoff=self.state.current_backoff,
                )
            except (ValueError, TypeError):
                logger.warning("Invalid Retry-After header", retry_after=retry_after)

    def handle_successful_request(self) -> None:
        """Handle successful request to reset adaptive backoff."""
        if self.config.adaptive_rate_limiting and self.state.consecutive_failures > 0:
            self.state.consecutive_failures = 0
            self.state.current_backoff = 0.0
            logger.debug("Rate limit backoff reset after successful request")

    def get_stats(self) -> Dict[str, Any]:
        """Get current rate limiting statistics."""
        return {
            "tokens_available": self.state.tokens,
            "burst_capacity": self.config.burst_capacity,
            "requests_per_minute": self.config.requests_per_minute,
            "request_count": self.state.request_count,
            "window_start": self.state.window_start,
            "retry_after": self.state.retry_after,
            "last_refill": self.state.last_refill,
            "queue_size": len(self.state.queue),
            "max_queue_size": self.config.max_queue_size,
            "consecutive_failures": self.state.consecutive_failures,
            "current_backoff": self.state.current_backoff,
            "last_request_time": self.state.last_request_time,
        }

    def reset(self) -> None:
        """Reset rate limiter state."""

        async def _reset():
            async with self._get_lock():
                self.state = RateLimitState()
                self.state.tokens = float(self.config.burst_capacity)
                self.state.last_refill = time.time()

                # Clear queue
                for request in self.state.queue:
                    if not request.future.done():
                        request.future.cancel()
                self.state.queue.clear()

        # Schedule reset
        asyncio.create_task(_reset())
        logger.info("Rate limiter reset")

    async def shutdown(self) -> None:
        """Shutdown the rate limiter and clear queue."""
        self._get_shutdown_event().set()

        if self._queue_processor_task:
            self._queue_processor_task.cancel()
            try:
                await self._queue_processor_task
            except asyncio.CancelledError:
                pass

        # Clear queue
        async with self._get_lock():
            for request in self.state.queue:
                if not request.future.done():
                    request.future.cancel()
            self.state.queue.clear()

        logger.info("Rate limiter shutdown complete")


class RateLimitContext:
    """Context manager for rate limiting."""

    def __init__(
        self,
        rate_limiter: EnhancedTokenBucketRateLimiter,
        timeout: Optional[float] = None,
        priority: QueuePriority = QueuePriority.NORMAL,
        operation: str = "unknown",
    ):
        self.rate_limiter = rate_limiter
        self.timeout = timeout
        self.priority = priority
        self.operation = operation
        self.acquired = False

    async def __aenter__(self):
        """Enter rate limit context."""
        self.acquired = await self.rate_limiter.acquire(
            self.timeout, self.priority, self.operation
        )
        if not self.acquired:
            raise TimeoutError(f"Rate limit timeout for operation: {self.operation}")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit rate limit context."""
        # Mark successful request if no exception occurred
        if exc_type is None:
            self.rate_limiter.handle_successful_request()


def create_rate_limiter(
    requests_per_minute: int = 300,  # Exactly 5 requests per second (200ms per request)
    burst_capacity: int = 1,  # No burst allowed per specification
    enable_rate_limiting: bool = True,
    enable_queuing: bool = True,
    max_queue_size: int = 1000,
    adaptive_rate_limiting: bool = True,
) -> EnhancedTokenBucketRateLimiter:
    """
    Create an enhanced rate limiter with the specified configuration.

    Args:
        requests_per_minute: Maximum requests per minute
        burst_capacity: Maximum burst capacity
        enable_rate_limiting: Whether to enable rate limiting
        enable_queuing: Whether to enable request queuing
        max_queue_size: Maximum number of queued requests
        adaptive_rate_limiting: Whether to enable adaptive backoff

    Returns:
        Configured enhanced rate limiter
    """
    config = RateLimitConfig(
        requests_per_minute=requests_per_minute,
        burst_capacity=burst_capacity,
        enable_rate_limiting=enable_rate_limiting,
        enable_queuing=enable_queuing,
        max_queue_size=max_queue_size,
        adaptive_rate_limiting=adaptive_rate_limiting,
    )

    return EnhancedTokenBucketRateLimiter(config)


# Backward compatibility
TokenBucketRateLimiter = EnhancedTokenBucketRateLimiter
