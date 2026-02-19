"""
Enhanced retry logic for the DATAQUERY SDK.

Provides exponential backoff, jitter, circuit breaker pattern,
and configurable retry strategies.
"""

import asyncio
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Type

import structlog

from .exceptions import NetworkError

logger = structlog.get_logger(__name__)


class RetryStrategy(str, Enum):
    """Retry strategies."""

    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    CONSTANT = "constant"


class CircuitState(str, Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class RetryConfig:
    """Configuration for retry logic."""

    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 300.0
    exponential_base: float = 2.0
    jitter: bool = True
    jitter_factor: float = 0.1
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    retryable_exceptions: List[Type[Exception]] = field(default_factory=list)
    non_retryable_exceptions: List[Type[Exception]] = field(default_factory=list)
    timeout: Optional[float] = None
    enable_circuit_breaker: bool = True
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: float = 300.0
    circuit_breaker_success_threshold: int = 2


@dataclass
class RetryStats:
    """Retry statistics."""

    total_attempts: int = 0
    successful_attempts: int = 0
    failed_attempts: int = 0
    retry_count: int = 0
    total_retry_time: float = 0.0
    last_retry_time: Optional[datetime] = None
    circuit_breaker_trips: int = 0
    circuit_breaker_resets: int = 0


class CircuitBreaker:
    """Circuit breaker implementation."""

    def __init__(self, config: RetryConfig):
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.success_count = 0
        self.last_state_change = datetime.now()

        logger.info(
            "Circuit breaker initialized",
            threshold=config.circuit_breaker_threshold,
            timeout=config.circuit_breaker_timeout,
        )

    def record_success(self) -> None:
        """Record a successful operation."""
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.circuit_breaker_success_threshold:
                self._close_circuit()
        else:
            self.failure_count = 0

    def record_failure(self) -> None:
        """Record a failed operation."""
        self.failure_count += 1
        self.last_failure_time = datetime.now()

        if self.state == CircuitState.CLOSED and self.failure_count >= self.config.circuit_breaker_threshold:
            self._open_circuit()
        elif self.state == CircuitState.HALF_OPEN:
            self._open_circuit()

    def can_execute(self) -> bool:
        """Check if operation can be executed."""
        if self.state == CircuitState.CLOSED:
            return True

        if self.state == CircuitState.OPEN:
            # Check if timeout has passed
            if self.last_failure_time and datetime.now() - self.last_failure_time >= timedelta(
                seconds=self.config.circuit_breaker_timeout
            ):
                self._half_open_circuit()
                return True
            return False

        # HALF_OPEN state
        return True

    def _open_circuit(self) -> None:
        """Open the circuit breaker."""
        if self.state != CircuitState.OPEN:
            self.state = CircuitState.OPEN
            self.last_state_change = datetime.now()
            self.success_count = 0
            logger.warning(
                "Circuit breaker opened",
                failure_count=self.failure_count,
                threshold=self.config.circuit_breaker_threshold,
            )

    def _half_open_circuit(self) -> None:
        """Set circuit breaker to half-open state."""
        if self.state != CircuitState.HALF_OPEN:
            self.state = CircuitState.HALF_OPEN
            self.last_state_change = datetime.now()
            self.success_count = 0
            logger.info("Circuit breaker half-open")

    def _close_circuit(self) -> None:
        """Close the circuit breaker."""
        if self.state != CircuitState.CLOSED:
            self.state = CircuitState.CLOSED
            self.last_state_change = datetime.now()
            self.failure_count = 0
            self.success_count = 0
            logger.info("Circuit breaker closed")

    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
        return {
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "last_failure_time": (self.last_failure_time.isoformat() if self.last_failure_time else None),
            "last_state_change": self.last_state_change.isoformat(),
            "threshold": self.config.circuit_breaker_threshold,
            "timeout": self.config.circuit_breaker_timeout,
        }


class RetryManager:
    """Enhanced retry manager with exponential backoff and circuit breaker."""

    def __init__(self, config: RetryConfig):
        self.config = config
        self.stats = RetryStats()
        self.circuit_breaker = CircuitBreaker(config) if config.enable_circuit_breaker else None

        # Default retryable exceptions
        if not config.retryable_exceptions:
            config.retryable_exceptions = [
                ConnectionError,
                TimeoutError,
                asyncio.TimeoutError,
                OSError,
            ]

        logger.info(
            "Retry manager initialized",
            max_retries=config.max_retries,
            strategy=config.strategy.value,
        )

    async def execute_with_retry(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function with retry logic.

        Args:
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            Function result

        Raises:
            Exception: If all retries fail
        """
        last_exception = None

        for attempt in range(self.config.max_retries + 1):
            self.stats.total_attempts += 1

            # Check circuit breaker
            if self.circuit_breaker and not self.circuit_breaker.can_execute():
                raise NetworkError("Circuit breaker is open - service temporarily unavailable")

            try:
                # Execute function
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)

                # Record success
                self.stats.successful_attempts += 1
                if self.circuit_breaker:
                    self.circuit_breaker.record_success()

                logger.debug("Operation successful", attempt=attempt + 1)
                return result

            except Exception as e:
                self.stats.failed_attempts += 1
                last_exception = e

                # Check if exception is retryable
                if not self._is_retryable_exception(e):
                    logger.warning(
                        "Non-retryable exception",
                        exception_type=type(e).__name__,
                        error=str(e),
                    )
                    raise e

                # Record failure in circuit breaker
                if self.circuit_breaker:
                    self.circuit_breaker.record_failure()

                # Check if this was the last attempt
                if attempt == self.config.max_retries:
                    logger.error(
                        "All retry attempts failed",
                        total_attempts=self.config.max_retries + 1,
                        last_error=str(e),
                    )
                    break

                # Calculate delay
                delay = self._calculate_delay(attempt)

                logger.warning(
                    "Operation failed, retrying",
                    attempt=attempt + 1,
                    max_attempts=self.config.max_retries + 1,
                    delay=delay,
                    error=str(e),
                )

                # Wait before retry
                start_time = time.time()
                await asyncio.sleep(delay)
                self.stats.total_retry_time += time.time() - start_time
                self.stats.retry_count += 1
                self.stats.last_retry_time = datetime.now()

        # All retries failed
        if last_exception is None:
            raise NetworkError("All retry attempts failed - operation could not be completed")
        raise last_exception

    def _is_retryable_exception(self, exception: Exception) -> bool:
        """Check if exception is retryable."""
        exception_type = type(exception)

        # Check non-retryable exceptions first
        for non_retryable in self.config.non_retryable_exceptions:
            if issubclass(exception_type, non_retryable):
                return False

        # Check retryable exceptions
        for retryable in self.config.retryable_exceptions:
            if issubclass(exception_type, retryable):
                return True

        # Default: retry on any exception if no specific lists provided
        return len(self.config.retryable_exceptions) == 0

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for retry attempt with optimized performance."""
        if self.config.strategy == RetryStrategy.EXPONENTIAL:
            # Use bit shifting for faster exponentiation when possible
            if self.config.exponential_base == 2.0:
                delay = self.config.base_delay * (1 << min(attempt, 10))  # Cap at 2^10
            else:
                delay = self.config.base_delay * (self.config.exponential_base**attempt)
        elif self.config.strategy == RetryStrategy.LINEAR:
            delay = self.config.base_delay * (attempt + 1)
        else:  # CONSTANT
            delay = self.config.base_delay

        # Apply jitter more efficiently
        if self.config.jitter and self.config.jitter_factor > 0:
            # Use faster random generation
            jitter = random.random() * self.config.jitter_factor * delay
            delay += jitter

        # Cap at maximum delay
        return min(delay, self.config.max_delay)

    def get_stats(self) -> Dict[str, Any]:
        """Get retry statistics."""
        stats = {
            "config": {
                "max_retries": self.config.max_retries,
                "strategy": self.config.strategy.value,
                "base_delay": self.config.base_delay,
                "max_delay": self.config.max_delay,
            },
            "stats": {
                "total_attempts": self.stats.total_attempts,
                "successful_attempts": self.stats.successful_attempts,
                "failed_attempts": self.stats.failed_attempts,
                "retry_count": self.stats.retry_count,
                "total_retry_time": self.stats.total_retry_time,
                "last_retry_time": (self.stats.last_retry_time.isoformat() if self.stats.last_retry_time else None),
            },
        }

        if self.circuit_breaker:
            stats["circuit_breaker"] = self.circuit_breaker.get_stats()

        return stats

    def reset_stats(self) -> None:
        """Reset retry statistics."""
        self.stats = RetryStats()
        if self.circuit_breaker:
            self.circuit_breaker = CircuitBreaker(self.config)
        logger.info("Retry statistics reset")


def create_retry_config(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 300.0,
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL,
    enable_circuit_breaker: bool = True,
) -> RetryConfig:
    """
    Create retry configuration.

    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Base delay between retries
        max_delay: Maximum delay between retries
        strategy: Retry strategy to use
        enable_circuit_breaker: Whether to enable circuit breaker

    Returns:
        Retry configuration
    """
    return RetryConfig(
        max_retries=max_retries,
        base_delay=base_delay,
        max_delay=max_delay,
        strategy=strategy,
        enable_circuit_breaker=enable_circuit_breaker,
    )


def create_retry_manager(config: RetryConfig) -> RetryManager:
    """
    Create a retry manager with the specified configuration.

    Args:
        config: Retry configuration

    Returns:
        Configured retry manager
    """
    return RetryManager(config)
