# Configuration

DataQuery SDK can be configured through environment variables, configuration files, or programmatically. The SDK comes with pre-configured defaults for JPMorgan's DataQuery API, so you only need to set your credentials to get started.

## Environment Variables

### Required Configuration

- **`DATAQUERY_CLIENT_ID`**: OAuth client ID for API authentication
- **`DATAQUERY_CLIENT_SECRET`**: OAuth client secret for API authentication

> **Note**: The SDK is pre-configured with JPMorgan's DataQuery API endpoints. All other configuration options have sensible defaults and are optional.

### Host Configuration (Pre-configured for JPMorgan)

The SDK comes pre-configured with JPMorgan's DataQuery API endpoints:

**Default Configuration (automatically set):**
```bash
# Main API endpoints (pre-configured)
DATAQUERY_BASE_URL=https://api-developer.jpmorgan.com
DATAQUERY_CONTEXT_PATH=/research/dataquery-authe/api/v2

# Files API endpoints (pre-configured)
DATAQUERY_FILES_BASE_URL=https://api-strm-gw01.jpmchase.com
DATAQUERY_FILES_CONTEXT_PATH=/research/dataquery-authe/api/v2

# OAuth endpoint (pre-configured)
DATAQUERY_OAUTH_TOKEN_URL=https://authe.jpmorgan.com/as/token.oauth2
```

**Override Only If Needed:**
You only need to set these environment variables if you want to override the defaults for a different environment:

```bash
# Only set these if you need to override the defaults
export DATAQUERY_BASE_URL="https://your-custom-api.com"
export DATAQUERY_CONTEXT_PATH="/your/custom/path"
export DATAQUERY_FILES_BASE_URL="https://your-custom-files-api.com"
export DATAQUERY_FILES_CONTEXT_PATH="/your/custom/files/path"
export DATAQUERY_OAUTH_TOKEN_URL="https://your-custom-auth.com/token"
```

### Authentication Configuration

#### OAuth 2.0 (Recommended)
- **`DATAQUERY_CLIENT_ID`**: OAuth client ID (**required**)
- **`DATAQUERY_CLIENT_SECRET`**: OAuth client secret (**required**)
- **`DATAQUERY_OAUTH_ENABLED`**: Enable OAuth authentication (default: `true`)
- **`DATAQUERY_OAUTH_TOKEN_URL`**: OAuth token endpoint URL (default: `https://authe.jpmorgan.com/as/token.oauth2`)
- **`DATAQUERY_OAUTH_AUD`**: OAuth audience (default: `JPMC:URI:RS-06785-DataQueryExternalApi-PROD`)
- **`DATAQUERY_GRANT_TYPE`**: OAuth grant type (default: `client_credentials`)

#### Bearer Token (Alternative)
- **`DATAQUERY_BEARER_TOKEN`**: Bearer token for direct API access
- **`DATAQUERY_TOKEN_REFRESH_THRESHOLD`**: Token refresh threshold in seconds (default: `300`)

### Connection Configuration
- **`DATAQUERY_TIMEOUT`**: Request timeout in seconds (default: `600.0`)
- **`DATAQUERY_MAX_RETRIES`**: Maximum number of retries (default: `3`)
- **`DATAQUERY_RETRY_DELAY`**: Delay between retries in seconds (default: `1.0`)
- **`DATAQUERY_POOL_CONNECTIONS`**: Connection pool size (default: `10`)
- **`DATAQUERY_POOL_MAXSIZE`**: Maximum connections in pool (default: `20`)

### Rate Limiting Configuration
- **`DATAQUERY_REQUESTS_PER_MINUTE`**: Rate limit for requests per minute (default: `300`)
- **`DATAQUERY_BURST_CAPACITY`**: Burst capacity for rate limiting (default: `20`)

### Logging Configuration
- **`DATAQUERY_LOG_LEVEL`**: Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`)
- **`DATAQUERY_ENABLE_DEBUG_LOGGING`**: Enable debug logging (`true`/`false`)
- **`DATAQUERY_LOG_REQUESTS`**: Log HTTP requests (`true`/`false`)

### Download Configuration
- **`DATAQUERY_DOWNLOAD_DIR`**: Default download directory (default: `./downloads`)
- **`DATAQUERY_CREATE_DIRECTORIES`**: Create directories if they don't exist (`true`/`false`)
- **`DATAQUERY_OVERWRITE_EXISTING`**: Overwrite existing files (`true`/`false`)
- **`DATAQUERY_WORKFLOW_DIR`**: Workflow files subdirectory (default: `workflow`)
- **`DATAQUERY_GROUPS_DIR`**: Groups files subdirectory (default: `groups`)
- **`DATAQUERY_AVAILABILITY_DIR`**: Availability files subdirectory (default: `availability`)
- **`DATAQUERY_DEFAULT_DIR`**: Default files subdirectory (default: `files`)

### Advanced Configuration
- **`DATAQUERY_USER_AGENT`**: User agent string for requests
- **`DATAQUERY_ENABLE_HTTP2`**: Enable HTTP/2 support (`true`/`false`)
- **`DATAQUERY_KEEPALIVE_TIMEOUT`**: Keep-alive timeout in seconds (default: `30`)
- **`DATAQUERY_ENABLE_CONNECTION_POOLING`**: Enable connection pooling (`true`/`false`)
- **`DATAQUERY_DEVELOPMENT_MODE`**: Enable development mode (`true`/`false`)
- **`DATAQUERY_DEV_BASE_URL`**: Development API base URL

## Default Values

The SDK comes with the following sensible defaults, so you only need to configure what you want to change:

| Configuration | Default Value | Description |
|---------------|---------------|-------------|
| **API Configuration** | | |
| `DATAQUERY_BASE_URL` | `https://api-developer.jpmorgan.com` | Main API base URL |
| `DATAQUERY_API_VERSION` | `2.0.0` | API version |
| `DATAQUERY_CONTEXT_PATH` | `/research/dataquery-authe/api/v2` | API context path |
| `DATAQUERY_FILES_BASE_URL` | `https://api-strm-gw01.jpmchase.com` | Files API base URL |
| `DATAQUERY_FILES_CONTEXT_PATH` | `/research/dataquery-authe/api/v2` | Files API context path |
| **Authentication** | | |
| `DATAQUERY_OAUTH_ENABLED` | `true` | OAuth authentication enabled by default |
| `DATAQUERY_OAUTH_TOKEN_URL` | `https://authe.jpmorgan.com/as/token.oauth2` | OAuth token endpoint |
| `DATAQUERY_OAUTH_AUD` | `JPMC:URI:RS-06785-DataQueryExternalApi-PROD` | OAuth audience |
| `DATAQUERY_GRANT_TYPE` | `client_credentials` | OAuth grant type |
| `DATAQUERY_TOKEN_REFRESH_THRESHOLD` | `300` | Token refresh threshold (5 minutes) |
| **Connection** | | |
| `DATAQUERY_TIMEOUT` | `6000.0` | Request timeout (100 minutes) |
| `DATAQUERY_MAX_RETRIES` | `3` | Maximum retry attempts |
| `DATAQUERY_RETRY_DELAY` | `1.0` | Delay between retries (1 second) |
| `DATAQUERY_POOL_CONNECTIONS` | `10` | Connection pool size |
| `DATAQUERY_POOL_MAXSIZE` | `20` | Maximum connections per pool |
| **Rate Limiting** | | |
| `DATAQUERY_REQUESTS_PER_MINUTE` | `300` | Rate limit (requests per minute) |
| `DATAQUERY_BURST_CAPACITY` | `20` | Burst capacity for rate limiting |
| **Logging** | | |
| `DATAQUERY_LOG_LEVEL` | `INFO` | Logging level |
| `DATAQUERY_ENABLE_DEBUG_LOGGING` | `false` | Debug logging disabled |
| **Download** | | |
| `DATAQUERY_DOWNLOAD_DIR` | `./downloads` | Default download directory |
| `DATAQUERY_CREATE_DIRECTORIES` | `true` | Create directories automatically |
| `DATAQUERY_OVERWRITE_EXISTING` | `false` | Don't overwrite existing files |
| `DATAQUERY_CHUNK_SIZE` | `1048576` | Download chunk size (1MB) |
| `DATAQUERY_ENABLE_RANGE_REQUESTS` | `true` | Enable HTTP range requests |
| `DATAQUERY_SHOW_PROGRESS` | `true` | Show download progress |
| **Batch Downloads** | | |
| `DATAQUERY_MAX_CONCURRENT_DOWNLOADS` | `5` | Maximum concurrent downloads |
| `DATAQUERY_BATCH_SIZE` | `10` | Batch size for operations |
| `DATAQUERY_RETRY_FAILED` | `true` | Retry failed downloads |
| `DATAQUERY_MAX_RETRY_ATTEMPTS` | `2` | Maximum retry attempts for failed downloads |
| `DATAQUERY_CREATE_DATE_FOLDERS` | `true` | Create date-based folders |
| `DATAQUERY_PRESERVE_PATH_STRUCTURE` | `true` | Preserve original path structure |
| `DATAQUERY_FLATTEN_STRUCTURE` | `false` | Flatten directory structure |
| `DATAQUERY_SHOW_BATCH_PROGRESS` | `true` | Show batch progress |
| `DATAQUERY_SHOW_INDIVIDUAL_PROGRESS` | `true` | Show individual file progress |
| `DATAQUERY_CONTINUE_ON_ERROR` | `true` | Continue processing on errors |
| `DATAQUERY_LOG_ERRORS` | `true` | Log errors |
| `DATAQUERY_SAVE_ERROR_LOG` | `true` | Save error log to file |
| `DATAQUERY_USE_ASYNC_DOWNLOADS` | `true` | Use async downloads |
| **Workflow** | | |
| `DATAQUERY_WORKFLOW_DIR` | `workflow` | Workflow files subdirectory |
| `DATAQUERY_GROUPS_DIR` | `groups` | Groups files subdirectory |
| `DATAQUERY_AVAILABILITY_DIR` | `availability` | Availability files subdirectory |
| `DATAQUERY_DEFAULT_DIR` | `files` | Default files subdirectory |
| **Security** | | |
| `DATAQUERY_MASK_SECRETS` | `true` | Mask secrets in logs |
| `DATAQUERY_TOKEN_STORAGE_ENABLED` | `false` | Enable token storage |
| `DATAQUERY_TOKEN_STORAGE_DIR` | `.tokens` | Token storage directory |

## Configuration File

Create a `.env` file in your project root:

```bash
# .env
# Required - OAuth credentials
DATAQUERY_CLIENT_ID=your_client_id_here
DATAQUERY_CLIENT_SECRET=your_client_secret_here

# Optional - Override defaults only if needed
# DATAQUERY_BASE_URL=https://api-developer.jpmorgan.com  # Pre-configured
# DATAQUERY_CONTEXT_PATH=/research/dataquery-authe/api/v2  # Pre-configured
# DATAQUERY_OAUTH_TOKEN_URL=https://authe.jpmorgan.com/as/token.oauth2  # Pre-configured
# DATAQUERY_OAUTH_AUD=JPMC:URI:RS-06785-DataQueryExternalApi-PROD  # Pre-configured

# Optional Configuration
DATAQUERY_DOWNLOAD_DIR=./downloads
DATAQUERY_LOG_LEVEL=INFO
DATAQUERY_ENABLE_DEBUG_LOGGING=false
DATAQUERY_TIMEOUT=600.0
DATAQUERY_MAX_RETRIES=3
```

## Programmatic Configuration

Configure the SDK programmatically:

```python
from dataquery import DataQuery
from dataquery.models import ClientConfig

# Minimal configuration - only credentials required, everything else uses defaults
config = ClientConfig(
    client_id="your_client_id",
    client_secret="your_client_secret"
)

# Use with DataQuery
async with DataQuery(config=config) as dq:
    # Your code here
    pass

# Or override specific defaults
config = ClientConfig(
    client_id="your_client_id",
    client_secret="your_client_secret",
    timeout=300.0,  # Override default timeout
    log_level="DEBUG",  # Override default log level
    base_url="https://your-custom-api.com",  # Override default base URL
    context_path="/your/custom/path"  # Override default context path
)
```

## Advanced Configuration

### Connection Settings

```python
from dataquery.models import ClientConfig

config = ClientConfig(
    api_key="your-api-key",
    # Connection settings
    timeout=30.0,
    max_retries=3,
    retry_delay=1.0,
    # Rate limiting
    requests_per_minute=1000,
    burst_capacity=10,
    # Connection pooling
    pool_connections=10,
    pool_maxsize=10,
    # SSL settings
    verify_ssl=True,
    ssl_cert_path=None,
    ssl_key_path=None
)
```

### Download Settings

```python
from dataquery.models import DownloadOptions

options = DownloadOptions(
    # Destination
    destination_path="./downloads",
    create_directories=True,
    overwrite_existing=False,
    
    # Download behavior
    chunk_size=1048576,
    max_retries=3,
    retry_delay=1.0,
    timeout=600.0,
    
    # Parallel downloads
    enable_range_requests=True,
    num_parts=5,
    
    # Progress
    show_progress=True,
    progress_callback=None
)
```

## Logging Configuration

### Basic Logging

```python
import logging
from dataquery import DataQuery

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async with DataQuery() as dq:
    # Logs will be output according to your configuration
    pass
```

### Advanced Logging

```python
import logging
from dataquery import DataQuery
from dataquery.models import DataQueryConfig

# Create custom logger
logger = logging.getLogger('dataquery')
logger.setLevel(logging.DEBUG)

# Create handler
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)

# Create formatter
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
handler.setFormatter(formatter)

# Add handler to logger
logger.addHandler(handler)

# Use with DataQuery
config = DataQueryConfig(
    api_key="your-api-key",
    enable_debug_logging=True
)

async with DataQuery(config=config) as dq:
    # Debug logs will be output
    pass
```

## Proxy Configuration

### HTTP Proxy

```python
from dataquery.models import ClientConfig

config = ClientConfig(
    api_key="your-api-key",
    proxy_url="http://proxy.company.com:8080"
)

async with DataQuery(config=config) as dq:
    # All requests will go through the proxy
    pass
```

### SOCKS Proxy

```python
config = DataQueryConfig(
    api_key="your-api-key",
    proxy_url="socks5://proxy.company.com:1080"
)
```

### Authenticated Proxy

```python
config = DataQueryConfig(
    api_key="your-api-key",
    proxy_url="http://username:password@proxy.company.com:8080"
)
```

## SSL/TLS Configuration

### Custom SSL Certificates

```python
config = DataQueryConfig(
    api_key="your-api-key",
    ssl_cert_path="/path/to/client.crt",
    ssl_key_path="/path/to/client.key",
    verify_ssl=True
)
```

### Disable SSL Verification (Not Recommended)

```python
config = DataQueryConfig(
    api_key="your-api-key",
    verify_ssl=False  # Only for development/testing
)
```

## Environment-Specific Configuration

### Development

```python
# development.py
from dataquery.models import DataQueryConfig

DEV_CONFIG = DataQueryConfig(
    api_key="dev-api-key",
    api_url="https://dev-api.dataquery.com",
    log_level="DEBUG",
    enable_debug_logging=True,
    timeout=60.0
)
```

### Production

```python
# production.py
from dataquery.models import DataQueryConfig

PROD_CONFIG = DataQueryConfig(
    api_key="prod-api-key",
    api_url="https://api.dataquery.com",
    log_level="WARNING",
    enable_debug_logging=False,
    timeout=30.0,
    max_retries=5
)
```

### Testing

```python
# test_config.py
from dataquery.models import DataQueryConfig

TEST_CONFIG = DataQueryConfig(
    api_key="test-api-key",
    api_url="https://test-api.dataquery.com",
    log_level="ERROR",
    timeout=10.0,
    max_retries=1
)
```

## Configuration Validation

The SDK validates configuration on startup:

```python
from dataquery.models import DataQueryConfig
from dataquery.exceptions import ConfigurationError

try:
    config = DataQueryConfig(
        api_key="",  # Invalid: empty API key
        api_url="invalid-url"  # Invalid: malformed URL
    )
except ConfigurationError as e:
    print(f"Configuration error: {e}")
```

## Best Practices

1. **Use environment variables** for sensitive data like API keys
2. **Use configuration files** for complex setups
3. **Validate configuration** before using in production
4. **Use different configs** for different environments
5. **Monitor logs** to ensure proper configuration
6. **Test configuration** in a safe environment first
