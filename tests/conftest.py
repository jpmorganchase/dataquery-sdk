"""
Pytest configuration and shared fixtures for DataQuery SDK tests.
"""

import pytest
import asyncio
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, AsyncMock, patch
from typing import Dict, Any, List

from dataquery.client import DataQueryClient
from dataquery.models import ClientConfig


@pytest.fixture(aud="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def temp_download_dir():
    """Create a temporary directory for download tests."""
    temp_dir = tempfile.mkdtemp(prefix="dataquery_test_")
    yield Path(temp_dir)
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def base_client_config():
    """Basic client configuration for testing."""
    return ClientConfig(
        base_url="https://api-developer.jpmorgan.com",
        context_path="/research/dataquery-authe/api/v2",
        client_id="test_client_id",
        client_secret="test_client_secret",
        oauth_enabled=True
    )


@pytest.fixture
def comprehensive_mock_data():
    """Comprehensive mock data for all API endpoints."""
    return {
        "groups_response": {
            "groups": [
                {
                    "id": "market_data",
                    "name": "Market Data",
                    "description": "Real-time and historical market data",
                    "last_updated": "2024-01-15T10:00:00Z",
                    "file_count": 250,
                    "total_size": 2048000,
                    "status": "active",
                    "data_types": ["equity", "fx", "rates"],
                    "regions": ["us", "eu", "apac"]
                },
                {
                    "id": "reference_data",
                    "name": "Reference Data", 
                    "description": "Static reference and master data",
                    "last_updated": "2024-01-14T18:00:00Z",
                    "file_count": 75,
                    "total_size": 512000,
                    "status": "active",
                    "data_types": ["reference"],
                    "regions": ["global"]
                },
                {
                    "id": "analytics",
                    "name": "Analytics Data",
                    "description": "Calculated analytics and risk metrics",
                    "last_updated": "2024-01-15T09:30:00Z", 
                    "file_count": 120,
                    "total_size": 1024000,
                    "status": "active",
                    "data_types": ["analytics", "risk"],
                    "regions": ["us", "eu"]
                }
            ],
            "pagination": {
                "page": 1,
                "per_page": 10,
                "total": 3,
                "total_pages": 1,
                "has_next": False,
                "has_previous": False
            }
        },
        "files_response": {
            "files": [
                {
                    "file_group_id": "mkt_data_20240115_001",
                    "filename": "market_data_20240115.csv",
                    "file_size": 1048576,  # 1MB
                    "last_modified": "2024-01-15T10:00:00Z",
                    "content_type": "text/csv",
                    "checksum": "sha256:abc123def456",
                    "compression": "gzip",
                    "format": "csv",
                    "schema_version": "v2.1",
                    "columns": ["symbol", "price", "volume", "timestamp"],
                    "row_count": 50000,
                    "data_date": "2024-01-15",
                    "tags": ["equity", "intraday"]
                },
                {
                    "file_group_id": "ref_data_20240115_001", 
                    "filename": "reference_data_20240115.json",
                    "file_size": 524288,  # 512KB
                    "last_modified": "2024-01-15T06:00:00Z",
                    "content_type": "application/json",
                    "checksum": "sha256:def456abc123",
                    "compression": None,
                    "format": "json",
                    "schema_version": "v1.0",
                    "record_count": 10000,
                    "data_date": "2024-01-15",
                    "tags": ["reference", "master"]
                },
                {
                    "file_group_id": "analytics_20240115_001", 
                    "filename": "risk_metrics_20240115.parquet",
                    "file_size": 2097152,  # 2MB
                    "last_modified": "2024-01-15T12:00:00Z",
                    "content_type": "application/octet-stream",
                    "checksum": "sha256:ghi789jkl012",
                    "compression": "snappy",
                    "format": "parquet",
                    "schema_version": "v3.0",
                    "row_count": 100000,
                    "data_date": "2024-01-15",
                    "tags": ["analytics", "risk", "var"]
                }
            ],
            "pagination": {
                "page": 1,
                "per_page": 50,
                "total": 3,
                "total_pages": 1
            }
        },
        "availability_response": {
            "available": True,
            "files": [
                {
                    "file_group_id": "mkt_data_20240115",
                    "file_datetime": "20240115T1000",
                    "available": True,
                    "file_size": 1048576,
                    "last_check": "2024-01-15T10:05:00Z",
                    "expiry": "2024-01-22T10:00:00Z",
                    "download_url": "https://api-developer.jpmorgan.com/download/mkt_data_20240115",
                    "checksum": "sha256:abc123def456"
                }
            ],
            "message": "All requested files are available for download",
            "check_timestamp": "2024-01-15T10:05:00Z",
            "cache_expires": "2024-01-15T10:10:00Z"
        },
        "instruments_response": {
            "instruments": [
                {
                    "id": "AAPL.O",
                    "name": "Apple Inc",
                    "type": "EQUITY",
                    "exchange": "NASDAQ",
                    "currency": "USD",
                    "country": "US",
                    "sector": "Technology",
                    "industry": "Consumer Electronics",
                    "market_cap": 3000000000000,
                    "active": True,
                    "identifiers": {
                        "ticker": "AAPL",
                        "isin": "US0378331005",
                        "cusip": "037833100",
                        "sedol": "2046251"
                    }
                },
                {
                    "id": "MSFT.O",
                    "name": "Microsoft Corporation", 
                    "type": "EQUITY",
                    "exchange": "NASDAQ",
                    "currency": "USD",
                    "country": "US",
                    "sector": "Technology",
                    "industry": "Software",
                    "market_cap": 2800000000000,
                    "active": True,
                    "identifiers": {
                        "ticker": "MSFT",
                        "isin": "US5949181045",
                        "cusip": "594918104",
                        "sedol": "2588173"
                    }
                },
                {
                    "id": "GOOGL.O",
                    "name": "Alphabet Inc Class A",
                    "type": "EQUITY", 
                    "exchange": "NASDAQ",
                    "currency": "USD",
                    "country": "US",
                    "sector": "Technology",
                    "industry": "Internet Software",
                    "market_cap": 1800000000000,
                    "active": True,
                    "identifiers": {
                        "ticker": "GOOGL",
                        "isin": "US02079K3059",
                        "cusip": "02079K305",
                        "sedol": "BYY88Y3"
                    }
                }
            ],
            "pagination": {
                "page": 1,
                "per_page": 100,
                "total": 3,
                "total_pages": 1
            }
        },
        "time_series_response": {
            "data": [
                {
                    "date": "2024-01-15",
                    "instrument": "AAPL.O",
                    "attributes": {
                        "price": 185.64,
                        "volume": 52428800,
                        "open": 184.35,
                        "high": 186.40,
                        "low": 184.11,
                        "close": 185.64,
                        "vwap": 185.22,
                        "market_cap": 2879516800000
                    }
                },
                {
                    "date": "2024-01-15",
                    "instrument": "MSFT.O",
                    "attributes": {
                        "price": 388.47,
                        "volume": 18547200,
                        "open": 387.30,
                        "high": 390.25,
                        "low": 386.95,
                        "close": 388.47,
                        "vwap": 388.85,
                        "market_cap": 2889547200000
                    }
                },
                {
                    "date": "2024-01-15",
                    "instrument": "GOOGL.O",
                    "attributes": {
                        "price": 152.38,
                        "volume": 25847600,
                        "open": 151.90,
                        "high": 153.45,
                        "low": 151.55,
                        "close": 152.38,
                        "vwap": 152.67,
                        "market_cap": 1903844800000
                    }
                }
            ],
            "metadata": {
                "start_date": "2024-01-15",
                "end_date": "2024-01-15",
                "frequency": "FREQ_DAY",
                "calendar": "CAL_USBANK",
                "conversion": "CONV_LASTBUS_ABS",
                "data_type": "REFERENCE_DATA",
                "instruments": ["AAPL.O", "MSFT.O", "GOOGL.O"],
                "attributes": ["price", "volume", "open", "high", "low", "close", "vwap", "market_cap"],
                "total_records": 3,
                "currency": "USD",
                "timezone": "America/New_York"
            }
        },
        "grid_data_response": {
            "data": {
                "rows": [
                    ["AAPL.O", "Apple Inc", "Technology", 185.64, 52428800, 2879516800000],
                    ["MSFT.O", "Microsoft Corporation", "Technology", 388.47, 18547200, 2889547200000],
                    ["GOOGL.O", "Alphabet Inc Class A", "Technology", 152.38, 25847600, 1903844800000]
                ],
                "columns": [
                    {"name": "symbol", "type": "string", "description": "Instrument symbol"},
                    {"name": "name", "type": "string", "description": "Company name"},
                    {"name": "sector", "type": "string", "description": "Business sector"},
                    {"name": "price", "type": "numeric", "description": "Last traded price"},
                    {"name": "volume", "type": "numeric", "description": "Trading volume"},
                    {"name": "market_cap", "type": "numeric", "description": "Market capitalization"}
                ]
            },
            "metadata": {
                "total_rows": 3,
                "total_columns": 6,
                "query": "SELECT symbol, name, sector, price, volume, market_cap FROM market_data WHERE date = '2024-01-15' AND sector = 'Technology'",
                "execution_time_ms": 145,
                "data_source": "market_data",
                "generated_at": "2024-01-15T10:05:30Z",
                "cache_ttl": 300
            }
        },
        "filters_response": {
            "filters": [
                {
                    "name": "exchange",
                    "type": "string",
                    "description": "Trading exchange",
                    "values": ["NYSE", "NASDAQ", "LSE", "TSE", "HKEX"],
                    "default": "NYSE",
                    "required": False
                },
                {
                    "name": "currency",
                    "type": "string",
                    "description": "Trading currency",
                    "values": ["USD", "EUR", "GBP", "JPY", "HKD"],
                    "default": "USD",
                    "required": False
                },
                {
                    "name": "sector",
                    "type": "string",
                    "description": "Business sector",
                    "values": ["Technology", "Healthcare", "Financial", "Energy", "Consumer"],
                    "default": None,
                    "required": False
                },
                {
                    "name": "market_cap_min",
                    "type": "numeric",
                    "description": "Minimum market capitalization",
                    "min_value": 0,
                    "max_value": 10000000000000,
                    "default": 0,
                    "required": False
                },
                {
                    "name": "active_only",
                    "type": "boolean",
                    "description": "Include only actively traded instruments",
                    "default": True,
                    "required": False
                }
            ],
            "filter_combinations": [
                ["exchange", "currency"],
                ["sector", "market_cap_min"],
                ["active_only"]
            ]
        },
        "attributes_response": {
            "attributes": [
                {
                    "id": "price",
                    "name": "Price",
                    "type": "numeric",
                    "description": "Last traded price",
                    "unit": "currency",
                    "precision": 4,
                    "nullable": False,
                    "category": "pricing"
                },
                {
                    "id": "volume",
                    "name": "Volume",
                    "type": "numeric",
                    "description": "Trading volume",
                    "unit": "shares",
                    "precision": 0,
                    "nullable": False,
                    "category": "volume"
                },
                {
                    "id": "market_cap",
                    "name": "Market Capitalization",
                    "type": "numeric",
                    "description": "Total market value",
                    "unit": "currency",
                    "precision": 0,
                    "nullable": True,
                    "category": "fundamental"
                },
                {
                    "id": "pe_ratio",
                    "name": "P/E Ratio",
                    "type": "numeric",
                    "description": "Price to earnings ratio",
                    "unit": "ratio",
                    "precision": 2,
                    "nullable": True,
                    "category": "fundamental"
                },
                {
                    "id": "beta",
                    "name": "Beta",
                    "type": "numeric",
                    "description": "Market beta coefficient",
                    "unit": "coefficient",
                    "precision": 3,
                    "nullable": True,
                    "category": "risk"
                }
            ],
            "categories": [
                {"name": "pricing", "description": "Price-related attributes"},
                {"name": "volume", "description": "Volume-related attributes"},
                {"name": "fundamental", "description": "Fundamental analysis attributes"},
                {"name": "risk", "description": "Risk measurement attributes"}
            ]
        },
        "error_responses": {
            "400_validation": {
                "code": "VALIDATION_ERROR",
                "description": "Request validation failed: invalid date format",
                "details": {
                    "field": "start_date",
                    "provided": "2024-1-15",
                    "expected": "YYYYMMDD format (e.g., 20240115)"
                },
                "x-dataquery-interaction-id": "error-validation-123"
            },
            "401_unauthorized": {
                "code": "UNAUTHORIZED",
                "description": "Authentication failed: invalid or expired token",
                "details": {
                    "reason": "token_expired",
                    "expires_at": "2024-01-15T09:00:00Z"
                },
                "x-dataquery-interaction-id": "error-auth-456"
            },
            "403_forbidden": {
                "code": "FORBIDDEN",
                "description": "Access denied: insufficient permissions for requested resource",
                "details": {
                    "required_permission": "data.market.read",
                    "user_permissions": ["data.reference.read"]
                },
                "x-dataquery-interaction-id": "error-forbidden-789"
            },
            "404_not_found": {
                "code": "NOT_FOUND",
                "description": "Resource not found: group does not exist",
                "details": {
                    "resource_type": "group",
                    "resource_id": "invalid_group_123"
                },
                "x-dataquery-interaction-id": "error-notfound-012"
            },
            "429_rate_limit": {
                "code": "RATE_LIMIT_EXCEEDED",
                "description": "Rate limit exceeded: too many requests",
                "details": {
                    "limit": 300,
                    "window": "60s",
                    "retry_after": 45
                },
                "x-dataquery-interaction-id": "error-ratelimit-345"
            },
            "500_server_error": {
                "code": "INTERNAL_ERROR",
                "description": "Internal server error: temporary service disruption",
                "details": {
                    "error_id": "srv-err-678",
                    "retry_recommended": True
                },
                "x-dataquery-interaction-id": "error-server-678"
            }
        }
    }


@pytest.fixture
def mock_download_content():
    """Mock file content for download testing."""
    return {
        "csv_content": b"""symbol,price,volume,timestamp
AAPL,185.64,52428800,2024-01-15T16:00:00Z
MSFT,388.47,18547200,2024-01-15T16:00:00Z
GOOGL,152.38,25847600,2024-01-15T16:00:00Z""",
        "json_content": b"""{
    "instruments": [
        {"symbol": "AAPL", "name": "Apple Inc", "sector": "Technology"},
        {"symbol": "MSFT", "name": "Microsoft Corporation", "sector": "Technology"},
        {"symbol": "GOOGL", "name": "Alphabet Inc", "sector": "Technology"}
    ],
    "metadata": {
        "generated_at": "2024-01-15T10:00:00Z",
        "record_count": 3
    }
}""",
        "binary_content": b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x10\x00\x00\x00\x10\x08\x06\x00\x00\x00\x1f\xf3\xffa"  # Sample PNG header
    }


@pytest.fixture
def mock_client_factory():
    """Factory for creating mocked DataQuery clients."""
    
    def create_mocked_client(config: ClientConfig = None) -> DataQueryClient:
        if config is None:
            config = ClientConfig(
                base_url="https://api-developer.jpmorgan.com",
                context_path="/research/dataquery-authe/api/v2"
            )
        
        with patch.object(DataQueryClient, '_setup_enhanced_components'):
            client = DataQueryClient(config)
            
            # Setup all required mocks
            client.rate_limiter = AsyncMock()
            client.rate_limiter.acquire = AsyncMock()
            client.rate_limiter.release = AsyncMock()
            client.rate_limiter.shutdown = AsyncMock()
            client.rate_limiter.get_stats = Mock(return_value={"rate_limiting": "stats"})
            client.rate_limiter.handle_rate_limit_response = Mock()
            
            client.retry_manager = AsyncMock()
            client.retry_manager.get_stats = Mock(return_value={"retry": "stats"})
            
            client.pool_monitor = Mock()
            client.pool_monitor.start_monitoring = Mock()
            client.pool_monitor.stop_monitoring = Mock()
            client.pool_monitor.get_pool_summary = Mock(return_value={"pool": "stats"})
            
            client.logging_manager = Mock()
            client.logging_manager.get_stats = Mock(return_value={"logging": "stats"})
            client.logging_manager.log_operation_start = Mock()
            client.logging_manager.log_operation_end = Mock()
            client.logging_manager.log_operation_error = Mock()
            
            client.logger = Mock()
            
            client.auth_manager = AsyncMock()
            client.auth_manager.is_authenticated = Mock(return_value=True)
            client.auth_manager.get_headers = AsyncMock(return_value={"Authorization": "Bearer test_token"})
            client.auth_manager.get_stats = Mock(return_value={"auth": "stats"})
            client.auth_manager.get_auth_info = Mock(return_value={"authenticated": True})
            
            return client
    
    return create_mocked_client


@pytest.fixture
def async_response_factory():
    """Factory for creating async HTTP response mocks."""
    
    def create_response(status: int = 200, headers: Dict[str, str] = None, json_data: Any = None, content: bytes = None):
        from tests.test_client_advanced import AsyncContextManagerMock
        
        response = AsyncMock()
        response.status = status
        response.headers = headers or {}
        response.url = "https://api-developer.jpmorgan.com/research/dataquery-authe/api/v2/test"
        
        if json_data is not None:
            response.json = AsyncMock(return_value=json_data)
        
        if content is not None:
            response.content.iter_chunked = AsyncMock(return_value=iter([content]))
        
        return AsyncContextManagerMock(response)
    
    return create_response


@pytest.fixture
def async_session_factory():
    """Factory for creating async session mocks."""
    
    def create_session():
        import aiohttp
        session = AsyncMock(spec=aiohttp.ClientSession)
        session.close = AsyncMock()
        session.closed = False
        return session
    
    return create_session
