"""
Focused tests to improve coverage on specific areas.
"""
import pytest
from datetime import datetime, timedelta
from dataquery.models import (
    ClientConfig, DownloadStatus, TokenStatus, DataType,
    Calendar, Frequency, Conversion, ErrorResponse,
    AuthenticationErrorResponse, Information, Available, Unavailable
)


class TestClientConfigCoverage:
    """Test ClientConfig methods to improve coverage."""
    
    def test_api_base_url_with_context_path(self):
        """Test api_base_url property with context path."""
        config = ClientConfig(
            base_url="https://api.example.com",
            context_path="/api/v2"
        )
        assert config.api_base_url == "https://api.example.com/api/v2"
        
    def test_api_base_url_without_context_path(self):
        """Test api_base_url property without context path."""
        config = ClientConfig(base_url="https://api.example.com")
        assert config.api_base_url == "https://api.example.com"


class TestEnumCoverage:
    """Test enum classes for coverage."""
    
    def test_download_status_all_values(self):
        """Test all DownloadStatus values."""
        assert DownloadStatus.PENDING == "pending"
        assert DownloadStatus.DOWNLOADING == "downloading" 
        assert DownloadStatus.COMPLETED == "completed"
        assert DownloadStatus.FAILED == "failed"
        assert DownloadStatus.CANCELLED == "cancelled"
        
    def test_token_status_all_values(self):
        """Test all TokenStatus values."""
        assert TokenStatus.VALID == "valid"
        assert TokenStatus.EXPIRED == "expired"
        assert TokenStatus.INVALID == "invalid"
        
    def test_data_type_all_values(self):
        """Test all DataType values."""
        assert DataType.REFERENCE_DATA == "REFERENCE_DATA"
        assert DataType.NO_REFERENCE_DATA == "NO_REFERENCE_DATA"
        assert DataType.ALL == "ALL"
        
    def test_calendar_values(self):
        """Test Calendar enum values."""
        assert Calendar.CAL_USBANK == "CAL_USBANK"
        assert Calendar.CAL_ALLDAYS == "CAL_ALLDAYS"
        assert Calendar.CAL_WEEKDAYS == "CAL_WEEKDAYS"
        
    def test_frequency_values(self):
        """Test Frequency enum values."""
        assert Frequency.FREQ_DAY == "FREQ_DAY"
        assert Frequency.FREQ_WEEK == "FREQ_WEEK"
        assert Frequency.FREQ_MONTH == "FREQ_MONTH"
        
    def test_conversion_values(self):
        """Test Conversion enum values."""
        assert Conversion.CONV_LASTBUS_ABS == "CONV_LASTBUS_ABS"
        assert Conversion.CONV_LASTBUS_REL == "CONV_LASTBUS_REL"


class TestErrorModelCoverage:
    """Test error models for coverage."""
    
    def test_error_response_creation(self):
        """Test ErrorResponse creation."""
        error = ErrorResponse(
            code=400,
            description="Bad Request",
            interaction_id="test-123"
        )
        assert error.code == 400
        assert error.description == "Bad Request"
        assert error.interaction_id == "test-123"
        
    def test_authentication_error_response(self):
        """Test AuthenticationErrorResponse."""
        error = AuthenticationErrorResponse(
            code=401,
            description="Authentication failed"
        )
        assert error.code == 401
        assert error.description == "Authentication failed"
        
    def test_information_model(self):
        """Test Information model."""
        info = Information(
            code="INFO_001", 
            description="Information message"
        )
        assert info.code == "INFO_001"
        assert info.description == "Information message"
        
    def test_available_model(self):
        """Test Available model."""
        available = Available(
            code="AVAILABLE",
            description="Services are functioning"
        )
        assert available.code == "AVAILABLE"
        assert available.description == "Services are functioning"
        
    def test_unavailable_model(self):
        """Test Unavailable model."""
        unavailable = Unavailable(
            code="UNAVAILABLE", 
            description="Services are not available"
        )
        assert unavailable.code == "UNAVAILABLE"
        assert unavailable.description == "Services are not available"


class TestModelSerialization:
    """Test model serialization for coverage."""
    
    def test_client_config_serialization(self):
        """Test ClientConfig serialization."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True,
            timeout=30
        )
        
        # Test dict serialization
        data = config.model_dump()
        assert isinstance(data, dict)
        assert data["base_url"] == "https://api.example.com"
        assert data["oauth_enabled"] is True
        assert data["timeout"] == 30
        
        # Test JSON serialization
        json_str = config.model_dump_json()
        assert isinstance(json_str, str)
        assert "api.example.com" in json_str
        
    def test_error_response_serialization(self):
        """Test ErrorResponse serialization."""
        error = ErrorResponse(
            code=500,
            description="Internal Server Error"
        )
        
        data = error.model_dump()
        assert data["code"] == 500
        assert data["description"] == "Internal Server Error"


class TestModelEdgeCases:
    """Test edge cases for better coverage."""
    
    def test_config_with_extra_fields(self):
        """Test models with extra fields."""
        config = ClientConfig(
            base_url="https://api.example.com",
            custom_field="custom_value"  # Extra field allowed
        )
        assert config.base_url == "https://api.example.com"
        # Extra field should be stored in __pydantic_extra__
        
    def test_error_with_various_types(self):
        """Test error response with different code types."""
        # String code
        error1 = ErrorResponse(code="ERR_001", description="String error code")
        assert error1.code == "ERR_001"
        
        # Integer code  
        error2 = ErrorResponse(code=404, description="Integer error code")
        assert error2.code == 404


class TestModelValidation:
    """Test model validation edge cases."""
    
    def test_required_fields(self):
        """Test required field validation."""
        # This should work fine
        config = ClientConfig(base_url="https://api.example.com")
        assert config.base_url == "https://api.example.com"
        
        # Error response requires code and description
        error = ErrorResponse(code=400, description="Bad Request")
        assert error.code == 400
        assert error.description == "Bad Request"
        
    def test_optional_fields(self):
        """Test optional field handling."""
        # Information model with minimal fields
        info = Information(code="INFO", description="Test info")
        assert info.code == "INFO"
        assert info.description == "Test info"
        
        # Error with interaction ID
        error = ErrorResponse(
            code=500,
            description="Server Error", 
            interaction_id="trace-123"
        )
        assert error.interaction_id == "trace-123"


class TestUtilityFunctions:
    """Test utility functions for coverage."""
    
    def test_model_copy(self):
        """Test model copying."""
        config = ClientConfig(
            base_url="https://api.example.com",
            oauth_enabled=True
        )
        
        # Test copy with updates
        config_copy = config.model_copy(update={"oauth_enabled": False})
        assert config_copy.base_url == "https://api.example.com"
        assert config_copy.oauth_enabled is False
        assert config.oauth_enabled is True  # Original unchanged
        
    def test_model_validation_context(self):
        """Test model validation with context."""
        # Test that models can be created and validated
        config = ClientConfig(base_url="https://api.example.com")
        
        # Test model validation passes
        validated = config.model_validate(config.model_dump())
        assert validated.base_url == config.base_url


class TestStringRepresentations:
    """Test string representations for coverage."""
    
    def test_config_str_repr(self):
        """Test ClientConfig string representation."""
        config = ClientConfig(base_url="https://api.example.com")
        str_repr = str(config)
        assert "api.example.com" in str_repr
        
    def test_error_str_repr(self):
        """Test ErrorResponse string representation.""" 
        error = ErrorResponse(code=404, description="Not Found")
        str_repr = str(error)
        assert "404" in str_repr


class TestRegressionCases:
    """Test specific regression cases for coverage."""
    
    def test_config_defaults(self):
        """Test configuration defaults."""
        config = ClientConfig(base_url="https://api.example.com")
        
        # Test default values are set correctly (timeout updated to 600)
        assert config.timeout == 600
        assert config.max_retries == 3
        assert config.retry_delay == 1
        assert config.oauth_enabled is True  # Current default
        
    def test_alias_handling(self):
        """Test field alias handling."""
        error = ErrorResponse(
            code=400,
            description="Bad Request",
            **{"x-dataquery-interaction-id": "test-123"}  # Using alias
        )
        # Test that alias is handled correctly
        assert error.interaction_id == "test-123"
