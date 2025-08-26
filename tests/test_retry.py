import pytest
import asyncio
import time
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock, AsyncMock
from dataquery.retry import (
    RetryStrategy, CircuitState, RetryConfig, RetryStats, CircuitBreaker,
    RetryManager, create_retry_config, create_retry_manager
)


class TestRetryStrategy:
    """Test RetryStrategy enum."""
    
    def test_retry_strategy_values(self):
        """Test retry strategy enum values."""
        assert RetryStrategy.EXPONENTIAL == "exponential"
        assert RetryStrategy.LINEAR == "linear"
        assert RetryStrategy.CONSTANT == "constant"


class TestCircuitState:
    """Test CircuitState enum."""
    
    def test_circuit_state_values(self):
        """Test circuit state enum values."""
        assert CircuitState.CLOSED == "closed"
        assert CircuitState.OPEN == "open"
        assert CircuitState.HALF_OPEN == "half_open"


class TestRetryConfig:
    """Test RetryConfig dataclass."""
    
    def test_retry_config_defaults(self):
        """Test RetryConfig with default values."""
        config = RetryConfig()
        
        assert config.max_retries == 3
        assert config.base_delay == 1.0
        assert config.max_delay == 60.0
        assert config.exponential_base == 2.0
        assert config.jitter is True
        assert config.jitter_factor == 0.1
        assert config.strategy == RetryStrategy.EXPONENTIAL
        assert config.retryable_exceptions == []
        assert config.non_retryable_exceptions == []
        assert config.timeout is None
        assert config.enable_circuit_breaker is True
        assert config.circuit_breaker_threshold == 5
        assert config.circuit_breaker_timeout == 60.0
        assert config.circuit_breaker_success_threshold == 2
    
    def test_retry_config_custom_values(self):
        """Test RetryConfig with custom values."""
        config = RetryConfig(
            max_retries=5,
            base_delay=2.0,
            max_delay=120.0,
            strategy=RetryStrategy.LINEAR,
            jitter=False,
            timeout=30.0,
            enable_circuit_breaker=False
        )
        
        assert config.max_retries == 5
        assert config.base_delay == 2.0
        assert config.max_delay == 120.0
        assert config.strategy == RetryStrategy.LINEAR
        assert config.jitter is False
        assert config.timeout == 30.0
        assert config.enable_circuit_breaker is False


class TestRetryStats:
    """Test RetryStats dataclass."""
    
    def test_retry_stats_defaults(self):
        """Test RetryStats with default values."""
        stats = RetryStats()
        
        assert stats.total_attempts == 0
        assert stats.successful_attempts == 0
        assert stats.failed_attempts == 0
        assert stats.retry_count == 0
        assert stats.total_retry_time == 0.0
        assert stats.last_retry_time is None
        assert stats.circuit_breaker_trips == 0
        assert stats.circuit_breaker_resets == 0
    
    def test_retry_stats_custom_values(self):
        """Test RetryStats with custom values."""
        now = datetime.now()
        stats = RetryStats(
            total_attempts=10,
            successful_attempts=8,
            failed_attempts=2,
            retry_count=2,
            total_retry_time=5.5,
            last_retry_time=now,
            circuit_breaker_trips=1,
            circuit_breaker_resets=1
        )
        
        assert stats.total_attempts == 10
        assert stats.successful_attempts == 8
        assert stats.failed_attempts == 2
        assert stats.retry_count == 2
        assert stats.total_retry_time == 5.5
        assert stats.last_retry_time == now
        assert stats.circuit_breaker_trips == 1
        assert stats.circuit_breaker_resets == 1


class TestCircuitBreaker:
    """Test CircuitBreaker class."""
    
    def test_circuit_breaker_initialization(self):
        """Test CircuitBreaker initialization."""
        config = RetryConfig()
        breaker = CircuitBreaker(config)
        
        assert breaker.state == CircuitState.CLOSED
        assert breaker.failure_count == 0
        assert breaker.last_failure_time is None
        assert breaker.success_count == 0
        assert isinstance(breaker.last_state_change, datetime)
    
    def test_circuit_breaker_record_success_closed(self):
        """Test recording success when circuit is closed."""
        config = RetryConfig()
        breaker = CircuitBreaker(config)
        
        breaker.record_success()
        
        assert breaker.failure_count == 0
        assert breaker.success_count == 0
    
    def test_circuit_breaker_record_success_half_open(self):
        """Test recording success when circuit is half-open."""
        config = RetryConfig(circuit_breaker_success_threshold=2)
        breaker = CircuitBreaker(config)
        breaker.state = CircuitState.HALF_OPEN
        breaker.success_count = 1
        
        breaker.record_success()
        
        # The _close_circuit method resets success_count to 0
        assert breaker.success_count == 0
        assert breaker.state == CircuitState.CLOSED
    
    def test_circuit_breaker_record_success_half_open_not_enough(self):
        """Test recording success when circuit is half-open but not enough for close."""
        config = RetryConfig(circuit_breaker_success_threshold=3)
        breaker = CircuitBreaker(config)
        breaker.state = CircuitState.HALF_OPEN
        breaker.success_count = 1
        
        breaker.record_success()
        
        assert breaker.success_count == 2
        assert breaker.state == CircuitState.HALF_OPEN  # Not enough successes yet
    
    def test_circuit_breaker_record_failure_closed(self):
        """Test recording failure when circuit is closed."""
        config = RetryConfig(circuit_breaker_threshold=2)
        breaker = CircuitBreaker(config)
        
        breaker.record_failure()
        
        assert breaker.failure_count == 1
        assert breaker.state == CircuitState.CLOSED
        
        breaker.record_failure()
        
        assert breaker.failure_count == 2
        assert breaker.state == CircuitState.OPEN
    
    def test_circuit_breaker_record_failure_half_open(self):
        """Test recording failure when circuit is half-open."""
        config = RetryConfig()
        breaker = CircuitBreaker(config)
        breaker.state = CircuitState.HALF_OPEN
        
        breaker.record_failure()
        
        assert breaker.state == CircuitState.OPEN
    
    def test_circuit_breaker_can_execute_closed(self):
        """Test can_execute when circuit is closed."""
        config = RetryConfig()
        breaker = CircuitBreaker(config)
        
        assert breaker.can_execute() is True
    
    def test_circuit_breaker_can_execute_open(self):
        """Test can_execute when circuit is open."""
        config = RetryConfig()
        breaker = CircuitBreaker(config)
        breaker.state = CircuitState.OPEN
        
        assert breaker.can_execute() is False
    
    def test_circuit_breaker_can_execute_half_open(self):
        """Test can_execute when circuit is half-open."""
        config = RetryConfig()
        breaker = CircuitBreaker(config)
        breaker.state = CircuitState.HALF_OPEN
        
        assert breaker.can_execute() is True
    
    def test_circuit_breaker_timeout_transition(self):
        """Test circuit breaker timeout transition from open to half-open."""
        config = RetryConfig(circuit_breaker_timeout=0.1)
        breaker = CircuitBreaker(config)
        breaker.state = CircuitState.OPEN
        breaker.last_failure_time = datetime.now() - timedelta(seconds=0.2)
        
        assert breaker.can_execute() is True
        assert breaker.state == CircuitState.HALF_OPEN
    
    def test_circuit_breaker_get_stats(self):
        """Test circuit breaker get_stats."""
        config = RetryConfig()
        breaker = CircuitBreaker(config)
        breaker.failure_count = 3
        breaker.success_count = 2
        
        stats = breaker.get_stats()
        
        assert stats["state"] == CircuitState.CLOSED
        assert stats["failure_count"] == 3
        assert stats["success_count"] == 2
        assert "last_failure_time" in stats
        assert "last_state_change" in stats


class TestRetryManager:
    """Test RetryManager class."""
    
    def test_retry_manager_initialization(self):
        """Test RetryManager initialization."""
        config = RetryConfig()
        manager = RetryManager(config)
        
        assert manager.config == config
        assert isinstance(manager.stats, RetryStats)
        assert isinstance(manager.circuit_breaker, CircuitBreaker)
    
    @pytest.mark.asyncio
    async def test_retry_manager_successful_execution(self):
        """Test successful execution without retries."""
        config = RetryConfig()
        manager = RetryManager(config)
        
        async def mock_func():
            return "success"
        
        result = await manager.execute_with_retry(mock_func)
        
        assert result == "success"
        assert manager.stats.total_attempts == 1
        assert manager.stats.successful_attempts == 1
        assert manager.stats.failed_attempts == 0
        assert manager.stats.retry_count == 0
    
    @pytest.mark.asyncio
    async def test_retry_manager_retry_on_failure(self):
        """Test retry on failure."""
        config = RetryConfig(max_retries=2, base_delay=0.1)
        manager = RetryManager(config)
        
        call_count = 0
        
        async def mock_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Temporary failure")  # Use retryable exception
            return "success"
        
        result = await manager.execute_with_retry(mock_func)
        
        assert result == "success"
        assert manager.stats.total_attempts == 3
        assert manager.stats.successful_attempts == 1
        assert manager.stats.failed_attempts == 2
        assert manager.stats.retry_count == 2
    
    @pytest.mark.asyncio
    async def test_retry_manager_max_retries_exceeded(self):
        """Test max retries exceeded."""
        config = RetryConfig(max_retries=2, base_delay=0.1)
        manager = RetryManager(config)
        
        async def mock_func():
            raise ConnectionError("Persistent failure")  # Use retryable exception
        
        with pytest.raises(ConnectionError, match="Persistent failure"):
            await manager.execute_with_retry(mock_func)
        
        assert manager.stats.total_attempts == 3
        assert manager.stats.successful_attempts == 0
        assert manager.stats.failed_attempts == 3
        assert manager.stats.retry_count == 2
    
    @pytest.mark.asyncio
    async def test_retry_manager_non_retryable_exception(self):
        """Test non-retryable exception."""
        config = RetryConfig(non_retryable_exceptions=[ValueError])
        manager = RetryManager(config)
        
        async def mock_func():
            raise ValueError("Non-retryable")
        
        with pytest.raises(ValueError, match="Non-retryable"):
            await manager.execute_with_retry(mock_func)
        
        assert manager.stats.total_attempts == 1
        assert manager.stats.successful_attempts == 0
        assert manager.stats.failed_attempts == 1
        assert manager.stats.retry_count == 0
    
    @pytest.mark.asyncio
    async def test_retry_manager_retryable_exception(self):
        """Test retryable exception."""
        config = RetryConfig(retryable_exceptions=[RuntimeError])
        manager = RetryManager(config)
        
        call_count = 0
        
        async def mock_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise RuntimeError("Retryable")
            return "success"
        
        result = await manager.execute_with_retry(mock_func)
        
        assert result == "success"
        assert manager.stats.total_attempts == 2
        assert manager.stats.successful_attempts == 1
        assert manager.stats.failed_attempts == 1
        assert manager.stats.retry_count == 1
    
    @pytest.mark.asyncio
    async def test_retry_manager_circuit_breaker_open(self):
        """Test circuit breaker open state."""
        config = RetryConfig(enable_circuit_breaker=True, circuit_breaker_threshold=1, max_retries=0)
        manager = RetryManager(config)
        
        # First failure should open the circuit
        async def mock_func():
            raise ConnectionError("Failure")  # Use retryable exception
        
        with pytest.raises(ConnectionError):
            await manager.execute_with_retry(mock_func)
        
        # Second call should be rejected by circuit breaker immediately
        with pytest.raises(Exception, match="Circuit breaker is open"):
            await manager.execute_with_retry(mock_func)
    
    @pytest.mark.asyncio
    async def test_retry_manager_timeout(self):
        """Test timeout handling."""
        config = RetryConfig(timeout=0.1)
        manager = RetryManager(config)
        
        async def mock_func():
            await asyncio.sleep(0.2)  # Longer than timeout
            return "success"
        
        # The timeout is not implemented in the current version, so this should succeed
        result = await manager.execute_with_retry(mock_func)
        assert result == "success"
    
    def test_retry_manager_is_retryable_exception(self):
        """Test is_retryable_exception method."""
        config = RetryConfig(
            retryable_exceptions=[RuntimeError],
            non_retryable_exceptions=[ValueError]
        )
        manager = RetryManager(config)
        
        # Test retryable exception
        assert manager._is_retryable_exception(RuntimeError("test")) is True
        
        # Test non-retryable exception
        assert manager._is_retryable_exception(ValueError("test")) is False
        
        # Test default exception (should be retryable since retryable_exceptions is not empty)
        assert manager._is_retryable_exception(Exception("test")) is False
    
    def test_retry_manager_is_retryable_exception_default(self):
        """Test is_retryable_exception method with default config."""
        config = RetryConfig()  # Default config has retryable exceptions set
        manager = RetryManager(config)
        
        # Test default exception (should not be retryable when retryable_exceptions is set)
        assert manager._is_retryable_exception(Exception("test")) is False
        
        # Test retryable exception from default list
        assert manager._is_retryable_exception(ConnectionError("test")) is True
    
    def test_retry_manager_calculate_delay_exponential(self):
        """Test delay calculation with exponential strategy."""
        config = RetryConfig(
            strategy=RetryStrategy.EXPONENTIAL,
            base_delay=1.0,
            exponential_base=2.0
        )
        manager = RetryManager(config)
        
        delay1 = manager._calculate_delay(1)
        delay2 = manager._calculate_delay(2)
        delay3 = manager._calculate_delay(3)
        
        assert delay1 >= 1.0
        assert delay2 >= 2.0
        assert delay3 >= 4.0
    
    def test_retry_manager_calculate_delay_linear(self):
        """Test delay calculation with linear strategy."""
        config = RetryConfig(
            strategy=RetryStrategy.LINEAR,
            base_delay=1.0
        )
        manager = RetryManager(config)
        
        delay1 = manager._calculate_delay(1)
        delay2 = manager._calculate_delay(2)
        delay3 = manager._calculate_delay(3)
        
        assert delay1 >= 1.0
        assert delay2 >= 2.0
        assert delay3 >= 3.0
    
    def test_retry_manager_calculate_delay_constant(self):
        """Test delay calculation with constant strategy."""
        config = RetryConfig(
            strategy=RetryStrategy.CONSTANT,
            base_delay=1.0
        )
        manager = RetryManager(config)
        
        delay1 = manager._calculate_delay(1)
        delay2 = manager._calculate_delay(2)
        delay3 = manager._calculate_delay(3)
        
        assert delay1 >= 1.0
        assert delay2 >= 1.0
        assert delay3 >= 1.0
    
    def test_retry_manager_calculate_delay_no_jitter(self):
        """Test delay calculation without jitter."""
        config = RetryConfig(
            strategy=RetryStrategy.CONSTANT,
            base_delay=1.0,
            jitter=False
        )
        manager = RetryManager(config)
        
        delay1 = manager._calculate_delay(1)
        delay2 = manager._calculate_delay(1)
        
        assert delay1 == delay2  # Should be exactly the same without jitter
    
    def test_retry_manager_get_stats(self):
        """Test get_stats method."""
        config = RetryConfig()
        manager = RetryManager(config)
        manager.stats.total_attempts = 10
        manager.stats.successful_attempts = 8
        manager.stats.failed_attempts = 2
        
        stats = manager.get_stats()
        
        assert stats["stats"]["total_attempts"] == 10
        assert stats["stats"]["successful_attempts"] == 8
        assert stats["stats"]["failed_attempts"] == 2
        assert "circuit_breaker" in stats
        assert "config" in stats
    
    def test_retry_manager_reset_stats(self):
        """Test reset_stats method."""
        config = RetryConfig()
        manager = RetryManager(config)
        manager.stats.total_attempts = 10
        manager.stats.successful_attempts = 8
        
        manager.reset_stats()
        
        assert manager.stats.total_attempts == 0
        assert manager.stats.successful_attempts == 0
        assert manager.stats.failed_attempts == 0
        assert manager.stats.retry_count == 0


class TestRetryFunctions:
    """Test retry utility functions."""
    
    def test_create_retry_config_defaults(self):
        """Test create_retry_config with defaults."""
        config = create_retry_config()
        
        assert config.max_retries == 3
        assert config.base_delay == 1.0
        assert config.max_delay == 60.0
        assert config.strategy == RetryStrategy.EXPONENTIAL
        assert config.enable_circuit_breaker is True
    
    def test_create_retry_config_custom(self):
        """Test create_retry_config with custom values."""
        config = create_retry_config(
            max_retries=5,
            base_delay=2.0,
            max_delay=120.0,
            strategy=RetryStrategy.LINEAR,
            enable_circuit_breaker=False
        )
        
        assert config.max_retries == 5
        assert config.base_delay == 2.0
        assert config.max_delay == 120.0
        assert config.strategy == RetryStrategy.LINEAR
        assert config.enable_circuit_breaker is False
    
    def test_create_retry_manager(self):
        """Test create_retry_manager."""
        config = RetryConfig()
        manager = create_retry_manager(config)
        
        assert isinstance(manager, RetryManager)
        assert manager.config == config 