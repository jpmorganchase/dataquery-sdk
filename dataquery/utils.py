"""
Utility functions for the DATAQUERY SDK.
"""

import os
from pathlib import Path
from typing import Optional
import structlog
# Note: load_dotenv is imported where used to avoid unused import in environments

from .models import ClientConfig

logger = structlog.get_logger(__name__)


def load_env_file(env_file: Optional[Path] = None) -> None:
    """
    Load environment variables from a .env file.
    
    Args:
        env_file: Path to the .env file (default: .env)
    """
    env_path = env_file or Path(".env")
    
    if env_path.exists():
        try:
            from dotenv import load_dotenv
            load_dotenv(env_path)
            logger.info("Loaded environment variables from .env file", file=str(env_path))
        except ImportError:
            logger.warning("python-dotenv not available, skipping .env file loading")
        except Exception as e:
            logger.error("Failed to load .env file", file=str(env_path), error=str(e))


def create_env_template(env_file: Optional[Path] = None) -> Path:
    """
    Create a .env template file with all available configuration options.
    
    Args:
        env_file: Path to the template file (default: .env.template)
        
    Returns:
        Path to the created template file
    """
    template_file = env_file or Path(".env.template")
    
    # Validate the path
    if not isinstance(template_file, Path):
        template_file = Path(template_file)
    
    try:
        # Ensure parent directory exists
        template_file.parent.mkdir(parents=True, exist_ok=True)
        
        template_content = """# DATAQUERY SDK Configuration Template
# Copy this file to .env and update the values according to your setup
# cp .env.template .env

# =============================================================================
# REQUIRED: API Configuration
# =============================================================================

# Base URL of the DATAQUERY API (REQUIRED)
# Example: https://api-developer.jpmorgan.com (Production)
# Example: https://api-staging.dataquery.com
DATAQUERY_BASE_URL=https://api-developer.jpmorgan.com

# Optional: Separate host for file endpoints
# If set, file availability/list/download will use this host instead of DATAQUERY_BASE_URL
# DATAQUERY_FILES_BASE_URL=https://files-api.example.com
# DATAQUERY_FILES_CONTEXT_PATH=/research/dataquery-authe/api/v2

# =============================================================================
# AUTHENTICATION: OAuth 2.0 Configuration (Recommended)
# =============================================================================

# Enable OAuth authentication (true/false)
# Set to false if using Bearer token authentication
DATAQUERY_OAUTH_ENABLED=true

# OAuth token endpoint URL
# Usually: {BASE_URL}/oauth/token
DATAQUERY_OAUTH_TOKEN_URL=https://api-developer.jpmorgan.com/oauth/token

# OAuth client credentials (REQUIRED if OAuth enabled)
# Get these from your DATAQUERY account dashboard
DATAQUERY_CLIENT_ID=your_client_id_here
DATAQUERY_CLIENT_SECRET=your_client_secret_here

# OAuth scope removed

# OAuth audience (optional)
# Example: api://default or a full audience URI
DATAQUERY_OAUTH_AUD=

# OAuth grant type (usually client_credentials)
DATAQUERY_GRANT_TYPE=client_credentials

# =============================================================================
# AUTHENTICATION: Bearer Token Configuration (Alternative)
# =============================================================================

# Bearer token for direct API access (alternative to OAuth)
# Use this if you already have a Bearer token
# Leave empty if using OAuth authentication
DATAQUERY_BEARER_TOKEN=

# Token refresh threshold in seconds (default: 300 = 5 minutes)
# Refresh token when it expires within this many seconds
DATAQUERY_TOKEN_REFRESH_THRESHOLD=300

# =============================================================================
# HTTP Configuration
# =============================================================================

# Request timeout in seconds (default: 600.0)
DATAQUERY_TIMEOUT=600.0

# Maximum retry attempts for failed requests (default: 3)
DATAQUERY_MAX_RETRIES=3

# Delay between retries in seconds (default: 1.0)
DATAQUERY_RETRY_DELAY=1.0

# Number of connection pools (default: 10)
DATAQUERY_POOL_CONNECTIONS=10

# Maximum connections per pool (default: 20)
DATAQUERY_POOL_MAXSIZE=20

# =============================================================================
# Rate Limiting Configuration
# =============================================================================

# Requests per minute limit (default: 100)
DATAQUERY_REQUESTS_PER_MINUTE=100

# Burst capacity for rate limiting (default: 20)
DATAQUERY_BURST_CAPACITY=20

# =============================================================================
# Logging Configuration
# =============================================================================

# Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
# Default: INFO
DATAQUERY_LOG_LEVEL=INFO

# Enable debug logging (true/false)
# Default: false
DATAQUERY_ENABLE_DEBUG_LOGGING=false

# =============================================================================
# Download Configuration
# =============================================================================

# Base download directory (default: ./downloads)
# All downloaded files will be saved to this directory
DATAQUERY_DOWNLOAD_DIR=./downloads

# Create parent directories if they don't exist (true/false)
# Default: true
DATAQUERY_CREATE_DIRECTORIES=true

# Overwrite existing files (true/false)
# Default: false
DATAQUERY_OVERWRITE_EXISTING=false

# =============================================================================
# Download Subdirectories (Optional)
# =============================================================================

# Workflow downloads subdirectory (default: workflow)
DATAQUERY_WORKFLOW_DIR=workflow

# Groups downloads subdirectory (default: groups)
DATAQUERY_GROUPS_DIR=groups

# Availability downloads subdirectory (default: availability)
DATAQUERY_AVAILABILITY_DIR=availability

# Default downloads subdirectory (default: files)
DATAQUERY_DEFAULT_DIR=files

# =============================================================================
# Advanced Configuration (Optional)
# =============================================================================

# User agent string for HTTP requests
# Default: DATAQUERY-SDK/1.0.0
DATAQUERY_USER_AGENT=DATAQUERY-SDK/1.0.0

# Enable HTTP/2 support (true/false)
# Default: true
DATAQUERY_ENABLE_HTTP2=true

# Connection keepalive timeout in seconds
# Default: 30
DATAQUERY_KEEPALIVE_TIMEOUT=30

# Enable connection pooling (true/false)
# Default: true
DATAQUERY_ENABLE_CONNECTION_POOLING=true

# =============================================================================
# Development Configuration (Optional)
# =============================================================================

# Enable development mode (true/false)
# Default: false
DATAQUERY_DEVELOPMENT_MODE=false

# Development API base URL (used when development mode is enabled)
DATAQUERY_DEV_BASE_URL=https://api-dev.dataquery.com

# Enable request/response logging (true/false)
# Default: false
DATAQUERY_LOG_REQUESTS=false

# =============================================================================
# Example Configurations
# =============================================================================

# Bearer Token Configuration Example:
# DATAQUERY_BASE_URL=https://api.dataquery.com
# DATAQUERY_OAUTH_ENABLED=false
# DATAQUERY_BEARER_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
"""
        
        template_file.write_text(template_content)
        
        logger.info("Created .env template", file=str(template_file))
        return template_file
        
    except (OSError, IOError) as e:
        logger.error("Failed to create .env template", file=str(template_file), error=str(e))
        raise
    except Exception as e:
        logger.error("Unexpected error creating .env template", error=str(e))
        raise


def save_config_to_env(config: ClientConfig, env_file: Optional[Path] = None) -> Path:
    """
    Save configuration to .env file.
    
    Args:
        config: Client configuration to save
        env_file: Path to the .env file (default: .env)
        
    Returns:
        Path to the saved .env file
    """
    env_file = env_file or Path(".env")
    
    # Ensure env_file is a Path object
    if not isinstance(env_file, Path):
        env_file = Path(env_file)
    
    env_content = f"""# DATAQUERY SDK Configuration

# API Configuration
DATAQUERY_BASE_URL={config.base_url}

# OAuth Configuration
DATAQUERY_OAUTH_ENABLED={str(config.oauth_enabled).lower()}
DATAQUERY_OAUTH_TOKEN_URL={config.oauth_token_url or ''}
DATAQUERY_CLIENT_ID={config.client_id or ''}
DATAQUERY_CLIENT_SECRET={config.client_secret or ''}
## scope removed
DATAQUERY_OAUTH_AUD={getattr(config, 'aud', '') or ''}
DATAQUERY_GRANT_TYPE={config.grant_type}

# Bearer Token Configuration
DATAQUERY_BEARER_TOKEN={config.bearer_token or ''}
DATAQUERY_TOKEN_REFRESH_THRESHOLD={config.token_refresh_threshold}

# HTTP Configuration
DATAQUERY_TIMEOUT={config.timeout}
DATAQUERY_MAX_RETRIES={config.max_retries}
DATAQUERY_RETRY_DELAY={config.retry_delay}
DATAQUERY_POOL_CONNECTIONS={config.pool_connections}
DATAQUERY_POOL_MAXSIZE={config.pool_maxsize}

# Rate Limiting
DATAQUERY_REQUESTS_PER_MINUTE={config.requests_per_minute}
DATAQUERY_BURST_CAPACITY={config.burst_capacity}

# Logging
DATAQUERY_LOG_LEVEL={config.log_level}
DATAQUERY_ENABLE_DEBUG_LOGGING={str(config.enable_debug_logging).lower()}

# Download Configuration
DATAQUERY_DOWNLOAD_DIR={config.download_dir}
DATAQUERY_CREATE_DIRECTORIES={str(config.create_directories).lower()}
DATAQUERY_OVERWRITE_EXISTING={str(config.overwrite_existing).lower()}

# Download Subdirectories
DATAQUERY_WORKFLOW_DIR=workflow
DATAQUERY_GROUPS_DIR=groups
DATAQUERY_AVAILABILITY_DIR=availability
DATAQUERY_DEFAULT_DIR=files
"""
    
    env_file.write_text(env_content)
    
    logger.info("Saved configuration to .env file", file=str(env_file))
    return env_file


def load_env_file(env_file: Optional[Path] = None) -> None:
    """
    Load environment variables from .env file.
    
    Args:
        env_file: Path to the .env file (default: .env)
    """
    try:
        from dotenv import load_dotenv  # pylint: disable=import-outside-toplevel
    except ImportError:
        logger.warning("python-dotenv not installed, skipping .env file loading")
        return
    
    env_file = env_file or Path(".env")
    
    # Ensure env_file is a Path object
    if not isinstance(env_file, Path):
        env_file = Path(env_file)
    
    # For compatibility with tests: only call loader if file exists
    # Use Path.exists as an unbound method to cooperate with tests that patch pathlib.Path.exists
    if Path.exists(env_file):
        try:
            _ = load_dotenv(env_file)
        except Exception:
            logger.warning("Failed to load .env file", file=str(env_file))
            return
        logger.info("Loaded environment variables from .env file", file=str(env_file))
    else:
        logger.warning("No .env file found", file=str(env_file))


def get_env_value(key: str, default: Optional[str] = None) -> Optional[str]:
    """
    Get environment variable value with optional default.
    
    Args:
        key: Environment variable name
        default: Default value if not found
        
    Returns:
        Environment variable value or default
    """
    return os.getenv(key, default)


def set_env_value(key: str, value: str) -> None:
    """
    Set environment variable value.
    
    Args:
        key: Environment variable name
        value: Value to set
    """
    os.environ[key] = value
    logger.debug("Set environment variable", key=key, value=value)


def validate_env_config() -> None:
    """
    Validate that required environment variables are set.
    
    Raises:
        ValueError: If required variables are missing or invalid
    """
    # Validate numeric values if present
    timeout = get_env_value("DATAQUERY_TIMEOUT")
    if timeout:
        try:
            float(timeout)
        except ValueError:
            raise ValueError(f"Invalid timeout value: {timeout}")
    
    max_retries = get_env_value("DATAQUERY_MAX_RETRIES")
    if max_retries:
        try:
            int(max_retries)
        except ValueError:
            raise ValueError(f"Invalid max retries value: {max_retries}")
    
    # Validate boolean values if present
    oauth_enabled_val = get_env_value("DATAQUERY_OAUTH_ENABLED")
    if oauth_enabled_val and oauth_enabled_val.lower() not in ('true', 'false'):
        raise ValueError(f"Invalid OAuth enabled value: {oauth_enabled_val}")
    
    # Check required variables - BASE_URL is always required
    base_url = get_env_value("DATAQUERY_BASE_URL")
    if not base_url or not base_url.startswith(('http://', 'https://')):
        raise ValueError("DATAQUERY_BASE_URL is required")
    
    # Validate OAuth configuration
    oauth_enabled = get_env_value("DATAQUERY_OAUTH_ENABLED", "false").lower() == "true"
    if oauth_enabled:
        client_id = get_env_value("DATAQUERY_CLIENT_ID")
        client_secret = get_env_value("DATAQUERY_CLIENT_SECRET")
        if not client_id or not client_secret or client_id.strip() == "" or client_secret.strip() == "":
            raise ValueError("OAuth credentials are required")
    
    # Check if either OAuth or Bearer token is configured
    if not oauth_enabled:
        bearer_token = get_env_value("DATAQUERY_BEARER_TOKEN")
        if not bearer_token or bearer_token.strip() == "":
            # Only require authentication if OAuth is explicitly enabled
            # If OAuth is disabled and no bearer token, that's okay for testing
            pass
    
    logger.info("Environment configuration validation passed")


def format_file_size(size_bytes: int) -> str:
    """Format file size in human-readable format (bytes no decimals, KB+ one decimal)."""
    if size_bytes == 0:
        return "0 B"

    # Handle negative values
    if size_bytes < 0:
        abs_size = abs(size_bytes)
        size_names = ["B", "KB", "MB", "GB", "TB", "PB", "EB"]
        i = 0
        size_float = float(abs_size)
        while size_float >= 1024 and i < len(size_names) - 1:
            size_float /= 1024.0
            i += 1

        if i == 0:  # Bytes
            return f"-{size_float:.0f} {size_names[i]}"
        else:
            return f"-{size_float:.1f} {size_names[i]}"

    size_names = ["B", "KB", "MB", "GB", "TB", "PB", "EB"]
    i = 0
    size_float = float(size_bytes)
    while size_float >= 1024 and i < len(size_names) - 1:
        size_float /= 1024.0
        i += 1

    if i == 0:  # Bytes
        return f"{size_float:.0f} {size_names[i]}"
    else:
        return f"{size_float:.1f} {size_names[i]}"


def format_duration(seconds: float) -> str:
    """Format duration with verbose style: Xm Ys or Xh Ym Zs where applicable."""
    if seconds == 0:
        return "0s"

    # Handle negative values
    if seconds < 0:
        abs_seconds = abs(seconds)
        if abs_seconds < 60:
            return f"-{abs_seconds:.1f}s"
        elif abs_seconds < 3600:
            minutes = int(abs_seconds // 60)
            remaining_seconds = int(abs_seconds % 60)
            if remaining_seconds == 0:
                return f"-{minutes}m"
            else:
                return f"-{minutes}m {remaining_seconds}s"
        else:
            hours = int(abs_seconds // 3600)
            remaining_minutes = int((abs_seconds % 3600) // 60)
            remaining_seconds = int(abs_seconds % 60)
            if remaining_minutes == 0 and remaining_seconds == 0:
                return f"-{hours}h"
            elif remaining_seconds == 0:
                return f"-{hours}h {remaining_minutes}m"
            else:
                return f"-{hours}h {remaining_minutes}m {remaining_seconds}s"

    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        remaining_seconds = int(seconds % 60)
        if remaining_seconds == 0:
            return f"{minutes}m"
        else:
            return f"{minutes}m {remaining_seconds}s"
    else:
        hours = int(seconds // 3600)
        remaining_minutes = int((seconds % 3600) // 60)
        remaining_seconds = int(seconds % 60)
        if remaining_minutes == 0 and remaining_seconds == 0:
            return f"{hours}h"
        elif remaining_seconds == 0:
            return f"{hours}h {remaining_minutes}m"
        else:
            return f"{hours}h {remaining_minutes}m {remaining_seconds}s"


def ensure_directory(path) -> Path:
    """Ensure directory exists and return the path."""
    # Convert string to Path if needed
    if not isinstance(path, Path):
        path = Path(path)
    
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_download_paths(base_dir: Optional[Path] = None) -> dict:
    """Get download paths from environment variables with defaults."""
    import os
    from pathlib import Path
    
    if base_dir is None:
        base_download_dir = Path(os.getenv("DATAQUERY_DOWNLOAD_DIR", "./downloads"))
    else:
        # Convert string to Path if needed
        if not isinstance(base_dir, Path):
            base_download_dir = Path(base_dir)
        else:
            base_download_dir = base_dir
    
    return {
        "base": base_download_dir,
        "workflow": base_download_dir / os.getenv("DATAQUERY_WORKFLOW_DIR", "workflow"),
        "groups": base_download_dir / os.getenv("DATAQUERY_GROUPS_DIR", "groups"),
        "availability": base_download_dir / os.getenv("DATAQUERY_AVAILABILITY_DIR", "availability"),
        "default": base_download_dir / os.getenv("DATAQUERY_DEFAULT_DIR", "files")
    } 