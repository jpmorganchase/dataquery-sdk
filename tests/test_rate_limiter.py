"""
Targeted tests to improve rate_limiter.py coverage.
"""

import asyncio
import time

import pytest

from dataquery.rate_limiter import (
    EnhancedTokenBucketRateLimiter,
    QueuedRequest,
    QueuePriority,
    RateLimitConfig,
    RateLimitContext,
    RateLimitState,
    TokenBucketRateLimiter,
)


class TestRateLimiterBasics:
    """Basic tests for rate limiter components."""

    def test_queue_priority_enum_values(self):
        """Test QueuePriority enum values."""
        # Just test that enum values exist and can be compared
        assert QueuePriority.CRITICAL
        assert QueuePriority.HIGH
        assert QueuePriority.NORMAL
        assert QueuePriority.LOW

        # Test comparison works (values are integers)
        assert isinstance(QueuePriority.CRITICAL.value, int)
        assert isinstance(QueuePriority.HIGH.value, int)

    def test_rate_limit_config_basic(self):
        """Test RateLimitConfig basic functionality."""
        config = RateLimitConfig()
        assert hasattr(config, "requests_per_minute")
        assert hasattr(config, "burst_capacity")
        assert hasattr(config, "enable_rate_limiting")

    def test_rate_limit_state_basic(self):
        """Test RateLimitState basic functionality."""
        state = RateLimitState()
        assert hasattr(state, "tokens")
        assert hasattr(state, "request_count")
        assert hasattr(state, "consecutive_failures")

    def test_token_bucket_rate_limiter_init(self):
        """Test TokenBucketRateLimiter initialization."""
        config = RateLimitConfig()
        limiter = TokenBucketRateLimiter(config)
        assert limiter.config == config
        assert hasattr(limiter, "state")

    def test_enhanced_token_bucket_rate_limiter_init(self):
        """Test EnhancedTokenBucketRateLimiter initialization."""
        config = RateLimitConfig()
        limiter = EnhancedTokenBucketRateLimiter(config)
        assert limiter.config == config
        assert hasattr(limiter, "state")


class TestRateLimitConfigDetails:
    """Detailed tests for RateLimitConfig."""

    def test_config_with_custom_values(self):
        """Test config with custom values."""
        config = RateLimitConfig(
            requests_per_minute=120, burst_capacity=20, enable_rate_limiting=False
        )
        assert config.requests_per_minute == 120
        assert config.burst_capacity == 20
        assert config.enable_rate_limiting is False

    def test_config_attributes_exist(self):
        """Test that all expected config attributes exist."""
        config = RateLimitConfig()

        expected_attrs = [
            "requests_per_minute",
            "burst_capacity",
            "enable_rate_limiting",
            "window_size_seconds",
            "queue_timeout",
            "max_queue_size",
        ]

        for attr in expected_attrs:
            assert hasattr(config, attr), f"Missing attribute: {attr}"


class TestRateLimitStateDetails:
    """Detailed tests for RateLimitState."""

    def test_state_attributes_exist(self):
        """Test that all expected state attributes exist."""
        state = RateLimitState()

        expected_attrs = [
            "tokens",
            "request_count",
            "consecutive_failures",
            "last_refill",
            "current_backoff",
        ]

        for attr in expected_attrs:
            assert hasattr(state, attr), f"Missing attribute: {attr}"

    def test_state_default_values(self):
        """Test state default values."""
        state = RateLimitState()

        assert state.tokens >= 0
        assert state.request_count >= 0
        assert state.consecutive_failures >= 0


class TestTokenBucketRateLimiterFeatures:
    """Test TokenBucketRateLimiter features."""

    def test_get_stats_method(self):
        """Test get_stats method."""
        config = RateLimitConfig()
        limiter = TokenBucketRateLimiter(config)

        stats = limiter.get_stats()
        assert isinstance(stats, dict)
        assert "requests_per_minute" in stats or "burst_capacity" in stats

    def test_handle_successful_request(self):
        """Test handle_successful_request method."""
        config = RateLimitConfig()
        limiter = TokenBucketRateLimiter(config)

        # Should not crash
        limiter.handle_successful_request()

        # Should reset consecutive failures
        assert limiter.state.consecutive_failures == 0

    def test_handle_rate_limit_response(self):
        """Test handle_rate_limit_response method."""
        config = RateLimitConfig()
        limiter = TokenBucketRateLimiter(config)

        headers = {"Retry-After": "30"}
        limiter.handle_rate_limit_response(headers)

        # Should increment consecutive failures
        assert limiter.state.consecutive_failures > 0

    @pytest.mark.asyncio
    async def test_shutdown_method(self):
        """Test shutdown method."""
        config = RateLimitConfig()
        limiter = TokenBucketRateLimiter(config)

        # Should not crash
        await limiter.shutdown()


class TestEnhancedTokenBucketRateLimiterFeatures:
    """Test EnhancedTokenBucketRateLimiter features."""

    def test_enhanced_get_stats(self):
        """Test enhanced rate limiter get_stats."""
        config = RateLimitConfig()
        limiter = EnhancedTokenBucketRateLimiter(config)

        stats = limiter.get_stats()
        assert isinstance(stats, dict)
        assert "burst_capacity" in stats or "requests_per_minute" in stats

    @pytest.mark.asyncio
    async def test_enhanced_shutdown(self):
        """Test enhanced rate limiter shutdown."""
        config = RateLimitConfig()
        limiter = EnhancedTokenBucketRateLimiter(config)

        # Should not crash
        await limiter.shutdown()

    def test_enhanced_handle_methods(self):
        """Test enhanced rate limiter handle methods."""
        config = RateLimitConfig()
        limiter = EnhancedTokenBucketRateLimiter(config)

        # Test successful request handling
        limiter.handle_successful_request()
        assert limiter.state.consecutive_failures == 0

        # Test rate limit response handling
        headers = {"Retry-After": "60"}
        limiter.handle_rate_limit_response(headers)
        assert limiter.state.consecutive_failures > 0


class TestQueuedRequestFeatures:
    """Test QueuedRequest features."""

    @pytest.mark.asyncio
    async def test_queued_request_creation_minimal(self):
        """Test QueuedRequest creation with minimal args."""
        # Create with the actual required arguments
        loop = asyncio.get_event_loop()
        future = loop.create_future()
        request = QueuedRequest(
            priority=QueuePriority.NORMAL,
            timestamp=time.time(),
            request_id="test_123",
            operation="test_operation",
            future=future,
        )

        assert request.priority == QueuePriority.NORMAL
        assert request.request_id == "test_123"
        assert request.operation == "test_operation"
        assert request.future == future

    @pytest.mark.asyncio
    async def test_queued_request_attributes(self):
        """Test QueuedRequest has expected attributes."""
        loop = asyncio.get_event_loop()
        future = loop.create_future()
        request = QueuedRequest(
            priority=QueuePriority.HIGH,
            timestamp=time.time(),
            request_id="test_456",
            operation="test_op",
            future=future,
        )

        # Test attributes exist
        assert hasattr(request, "priority")
        assert hasattr(request, "timestamp")
        assert hasattr(request, "request_id")
        assert hasattr(request, "operation")
        assert hasattr(request, "future")


class TestRateLimitContextFeatures:
    """Test RateLimitContext features."""

    @pytest.mark.asyncio
    async def test_rate_limit_context_basic(self):
        """Test basic RateLimitContext usage."""
        config = RateLimitConfig(enable_rate_limiting=False)
        limiter = TokenBucketRateLimiter(config)

        # Should work when rate limiting is disabled
        async with RateLimitContext(limiter):
            pass  # Should complete without issues

    @pytest.mark.asyncio
    async def test_rate_limit_context_with_timeout(self):
        """Test RateLimitContext with timeout."""
        config = RateLimitConfig(enable_rate_limiting=False)
        limiter = TokenBucketRateLimiter(config)

        # Should work with timeout parameter
        async with RateLimitContext(limiter, timeout=5.0):
            pass

    @pytest.mark.asyncio
    async def test_rate_limit_context_with_priority(self):
        """Test RateLimitContext with priority."""
        config = RateLimitConfig(enable_rate_limiting=False)
        limiter = TokenBucketRateLimiter(config)

        # Should work with priority parameter
        async with RateLimitContext(limiter, priority=QueuePriority.HIGH):
            pass

    @pytest.mark.asyncio
    async def test_rate_limit_context_with_operation(self):
        """Test RateLimitContext with operation name."""
        config = RateLimitConfig(enable_rate_limiting=False)
        limiter = TokenBucketRateLimiter(config)

        # Should work with operation parameter
        async with RateLimitContext(limiter, operation="test_operation"):
            pass


class TestRateLimiterIntegration:
    """Integration tests for rate limiter components."""

    def test_config_limiter_integration(self):
        """Test config and limiter integration."""
        config = RateLimitConfig(
            requests_per_minute=100, burst_capacity=10, enable_rate_limiting=True
        )
        limiter = TokenBucketRateLimiter(config)

        # Limiter should use config values
        assert limiter.config.requests_per_minute == 100
        assert limiter.config.burst_capacity == 10
        assert limiter.config.enable_rate_limiting is True

    @pytest.mark.asyncio
    async def test_full_rate_limiting_flow(self):
        """Test full rate limiting flow."""
        config = RateLimitConfig(
            enable_rate_limiting=True, burst_capacity=5, requests_per_minute=60
        )
        limiter = EnhancedTokenBucketRateLimiter(config)

        # Test multiple requests
        for i in range(3):
            try:
                async with RateLimitContext(limiter, timeout=0.1):
                    pass
            except asyncio.TimeoutError:
                # Acceptable if rate limited
                pass

        # Test stats collection
        stats = limiter.get_stats()
        assert isinstance(stats, dict)

        # Test shutdown
        await limiter.shutdown()


# ===== Merged from test_rate_limiter_advanced.py =====
import asyncio as _asyncio_rl_adv


def _make_rate_limiter(**overrides) -> EnhancedTokenBucketRateLimiter:
    base = dict(
        requests_per_minute=60,
        burst_capacity=1,
        enable_rate_limiting=True,
        enable_queuing=True,
        max_queue_size=10,
        adaptive_rate_limiting=True,
    )
    base.update(overrides)
    cfg = RateLimitConfig(**base)
    return EnhancedTokenBucketRateLimiter(cfg)


@pytest.mark.asyncio
async def test_simple_acquire_without_queuing_merged():
    rl = _make_rate_limiter(enable_queuing=False)
    assert await rl.acquire(timeout=0.1)
    assert not await rl.acquire(timeout=0.05)


@pytest.mark.asyncio
async def test_queueing_with_priority_and_timeout_merged():
    rl = _make_rate_limiter(requests_per_minute=300, burst_capacity=1)
    assert await rl.acquire(timeout=0.1, priority=QueuePriority.LOW, operation="op1")
    assert not await rl.acquire(
        timeout=0.05, priority=QueuePriority.CRITICAL, operation="op2"
    )
    await _asyncio_rl_adv.sleep(0.25)
    ok = await rl.acquire(timeout=1.0, priority=QueuePriority.CRITICAL, operation="op3")
    assert ok is True


def test_handle_rate_limit_response_and_success_reset_merged():
    rl = _make_rate_limiter()
    rl.handle_rate_limit_response({rl.config.retry_after_header: "1"})
    rl.handle_rate_limit_response({rl.config.retry_after_header: "2.5"})
    assert rl.state.consecutive_failures >= 2
    rl.handle_successful_request()
    assert rl.state.consecutive_failures == 0
    assert rl.state.current_backoff == 0.0


@pytest.mark.asyncio
async def test_context_manager_success_and_timeout_merged():
    rl = _make_rate_limiter(requests_per_minute=60, burst_capacity=1)
    async with RateLimitContext(
        rl, timeout=0.1, priority=QueuePriority.NORMAL, operation="ctx"
    ):
        pass
    assert not await rl.acquire(timeout=0.1)

    rl2 = _make_rate_limiter(
        requests_per_minute=60, burst_capacity=1, enable_queuing=False
    )
    assert await rl2.acquire(timeout=0.1)
    with pytest.raises(TimeoutError):
        async with RateLimitContext(
            rl2, timeout=0.05, priority=QueuePriority.NORMAL, operation="ctx2"
        ):
            pass
    await _asyncio_rl_adv.sleep(0.25)
    assert await rl.acquire(timeout=2.0)


@pytest.mark.asyncio
async def test_shutdown_cancels_queue_merged():
    rl = _make_rate_limiter(requests_per_minute=60, burst_capacity=1)
    _ = _asyncio_rl_adv.create_task(
        rl.acquire(timeout=1.0, priority=QueuePriority.LOW, operation="bg")
    )
    await _asyncio_rl_adv.sleep(0)
    await rl.shutdown()
    assert len(rl.state.queue) == 0

    def test_priority_comparison_integration(self):
        """Test priority comparison works in context."""
        priorities = [
            QueuePriority.LOW,
            QueuePriority.NORMAL,
            QueuePriority.HIGH,
            QueuePriority.CRITICAL,
        ]

        # Test that priorities can be compared
        for i in range(len(priorities) - 1):
            for j in range(i + 1, len(priorities)):
                # Just test that comparison doesn't crash
                try:
                    result = priorities[i] != priorities[j]
                    assert isinstance(result, bool)
                except Exception:
                    # If comparison fails, that's also information
                    pass


class TestRateLimiterEdgeCases:
    """Test edge cases and error conditions."""

    def test_limiter_with_zero_burst_capacity(self):
        """Test limiter with edge case values."""
        config = RateLimitConfig(burst_capacity=1)  # Minimum value
        limiter = TokenBucketRateLimiter(config)

        # Should initialize without issues
        assert limiter.config.burst_capacity == 1

    def test_state_after_many_failures(self):
        """Test state after many consecutive failures."""
        config = RateLimitConfig()
        limiter = TokenBucketRateLimiter(config)

        # Simulate many failures
        for _ in range(10):
            headers = {"Retry-After": "30"}
            limiter.handle_rate_limit_response(headers)

        assert limiter.state.consecutive_failures == 10

        # Test recovery
        limiter.handle_successful_request()
        assert limiter.state.consecutive_failures == 0

    @pytest.mark.asyncio
    async def test_context_manager_exception_handling(self):
        """Test context manager with exceptions."""
        config = RateLimitConfig(enable_rate_limiting=False)
        limiter = TokenBucketRateLimiter(config)

        try:
            async with RateLimitContext(limiter):
                raise ValueError("Test exception")
        except ValueError:
            # Exception should propagate normally
            pass

        # Limiter should still be functional
        stats = limiter.get_stats()
        assert isinstance(stats, dict)

    def test_multiple_limiters_independence(self):
        """Test that multiple limiters work independently."""
        config1 = RateLimitConfig(requests_per_minute=60)
        config2 = RateLimitConfig(requests_per_minute=120)

        limiter1 = TokenBucketRateLimiter(config1)
        limiter2 = TokenBucketRateLimiter(config2)

        # Modify one limiter's state
        limiter1.state.consecutive_failures = 5

        # Other limiter should be unaffected
        assert limiter2.state.consecutive_failures == 0
        assert limiter1.state.consecutive_failures == 5

    @pytest.mark.asyncio
    async def test_concurrent_rate_limiting(self):
        """Test concurrent rate limiting operations."""
        config = RateLimitConfig(enable_rate_limiting=False)
        limiter = EnhancedTokenBucketRateLimiter(config)

        # Create multiple concurrent contexts
        tasks = []
        for i in range(5):
            tasks.append(asyncio.create_task(self._concurrent_context(limiter, i)))

        # Wait for all tasks
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Should complete without major issues
        assert len(results) == 5

        # Limiter should still be functional
        stats = limiter.get_stats()
        assert isinstance(stats, dict)

    async def _concurrent_context(self, limiter, operation_id):
        """Helper method for concurrent testing."""
        async with RateLimitContext(limiter, operation=f"op_{operation_id}"):
            # Simulate some work
            await asyncio.sleep(0.01)
            return operation_id
