"""
Environment-based configuration for the DATAQUERY SDK.

This module provides a comprehensive configuration system that loads all settings
from environment variables with proper defaults, validation, and type conversion.
"""

import os
from pathlib import Path
from typing import Optional, Dict, Any
from dotenv import load_dotenv

from .models import ClientConfig
from .exceptions import ConfigurationError


class EnvConfig:
    """Environment-based configuration loader for DATAQUERY SDK."""
    
    # Environment variable prefix
    PREFIX = "DATAQUERY_"
    
    # Default values for configuration
    DEFAULTS = {
        # API Configuration
        'BASE_URL': None,
        'CONTEXT_PATH': '',
        'API_VERSION': '2.0.0',
            # Optional separate files host
            'FILES_BASE_URL': None,
            'FILES_CONTEXT_PATH': '',
        
        # OAuth Configuration
        'OAUTH_ENABLED': 'false',
        'OAUTH_TOKEN_URL': None,
        'CLIENT_ID': None,
        'CLIENT_SECRET': None,
        # scope removed
        'AUDIENCE': '',
        'GRANT_TYPE': 'client_credentials',
        
        # Bearer Token Configuration
        'BEARER_TOKEN': None,
        'TOKEN_REFRESH_THRESHOLD': '300',
        
        # HTTP Configuration
        'TIMEOUT': '6000.0',
        'MAX_RETRIES': '3',
        'RETRY_DELAY': '1.0',
        
        # Connection Pooling
        'POOL_CONNECTIONS': '10',
        'POOL_MAXSIZE': '20',
        
        # Rate Limiting
        'REQUESTS_PER_MINUTE': '100',
        'BURST_CAPACITY': '20',
        
        # Proxy Configuration
        'PROXY_ENABLED': 'false',
        'PROXY_URL': '',
        'PROXY_USERNAME': '',
        'PROXY_PASSWORD': '',
        'PROXY_VERIFY_SSL': 'true',
        
        # Logging
        'LOG_LEVEL': 'INFO',
        'ENABLE_DEBUG_LOGGING': 'false',
        
        # Download Configuration
        'DOWNLOAD_DIR': './downloads',
        'CREATE_DIRECTORIES': 'true',
        'OVERWRITE_EXISTING': 'false',
        
        # Batch Download Configuration
        'MAX_CONCURRENT_DOWNLOADS': '3',
        'BATCH_SIZE': '10',
        'RETRY_FAILED': 'true',
        'MAX_RETRY_ATTEMPTS': '2',
        'CREATE_DATE_FOLDERS': 'true',
        'PRESERVE_PATH_STRUCTURE': 'true',
        'FLATTEN_STRUCTURE': 'false',
        'SHOW_BATCH_PROGRESS': 'true',
        'SHOW_INDIVIDUAL_PROGRESS': 'true',
        'CONTINUE_ON_ERROR': 'true',
        'LOG_ERRORS': 'true',
        'SAVE_ERROR_LOG': 'true',
        'USE_ASYNC_DOWNLOADS': 'true',
        'CHUNK_SIZE': '8192',
        
        # Download Options
        'ENABLE_RANGE_REQUESTS': 'true',
        'SHOW_PROGRESS': 'true',
        
        # Workflow Configuration
        'WORKFLOW_DIR': 'workflow',
        'GROUPS_DIR': 'groups',
        'AVAILABILITY_DIR': 'availability',
        'DEFAULT_DIR': 'files',
        
        # Security
        'MASK_SECRETS': 'true',
        'TOKEN_STORAGE_ENABLED': 'false',
        'TOKEN_STORAGE_DIR': '.tokens',
    }
    
    # Environment variable names
    ENV_VARS = {
        # API Configuration
        'base_url': 'DATAQUERY_BASE_URL',
        'context_path': 'DATAQUERY_CONTEXT_PATH',
        'api_version': 'DATAQUERY_API_VERSION',
            # Optional separate files host
            'files_base_url': 'DATAQUERY_FILES_BASE_URL',
            'files_context_path': 'DATAQUERY_FILES_CONTEXT_PATH',
        
        # OAuth Configuration
        'oauth_enabled': 'DATAQUERY_OAUTH_ENABLED',
        'oauth_token_url': 'DATAQUERY_OAUTH_TOKEN_URL',
        'client_id': 'DATAQUERY_CLIENT_ID',
        'client_secret': 'DATAQUERY_CLIENT_SECRET',
        # scope removed
        'aud': 'DATAQUERY_OAUTH_AUD',
        'grant_type': 'DATAQUERY_OAUTH_GRANT_TYPE',
        
        # Bearer Token Configuration
        'bearer_token': 'DATAQUERY_BEARER_TOKEN',
        'token_refresh_threshold': 'DATAQUERY_TOKEN_REFRESH_THRESHOLD',
        
        # HTTP Configuration
        'timeout': 'DATAQUERY_TIMEOUT',
        'max_retries': 'DATAQUERY_MAX_RETRIES',
        'retry_delay': 'DATAQUERY_RETRY_DELAY',
        
        # Connection Pooling
        'pool_connections': 'DATAQUERY_POOL_CONNECTIONS',
        'pool_maxsize': 'DATAQUERY_POOL_MAXSIZE',
        
        # Rate Limiting
        'requests_per_minute': 'DATAQUERY_REQUESTS_PER_MINUTE',
        'burst_capacity': 'DATAQUERY_BURST_CAPACITY',
        
        # Proxy Configuration
        'proxy_enabled': 'DATAQUERY_PROXY_ENABLED',
        'proxy_url': 'DATAQUERY_PROXY_URL',
        'proxy_username': 'DATAQUERY_PROXY_USERNAME',
        'proxy_password': 'DATAQUERY_PROXY_PASSWORD',
        'proxy_verify_ssl': 'DATAQUERY_PROXY_VERIFY_SSL',
        
        # Logging
        'log_level': 'DATAQUERY_LOG_LEVEL',
        'enable_debug_logging': 'DATAQUERY_ENABLE_DEBUG_LOGGING',
        
        # Download Configuration
        'download_dir': 'DATAQUERY_DOWNLOAD_DIR',
        'create_directories': 'DATAQUERY_CREATE_DIRECTORIES',
        'overwrite_existing': 'DATAQUERY_OVERWRITE_EXISTING',
    }
    


    @classmethod
    def load_env_file(cls, env_file: Optional[Path] = None) -> None:
        """Load environment variables from .env file."""
        if env_file is None:
            env_file = Path(".env")
        
        if env_file.exists():
            load_dotenv(env_file)
    
    @classmethod
    def get_env_var(cls, key: str, default: Optional[str] = None) -> Optional[str]:
        """Get environment variable with prefix."""
        env_key = f"{cls.PREFIX}{key}"
        value = os.getenv(env_key, default)
        
        # Handle empty strings as None
        if value == "":
            return None
        
        return value
    
    @classmethod
    def get_bool(cls, key: str, default: str = "false") -> bool:
        """Get boolean environment variable."""
        value = cls.get_env_var(key, default)
        if value is None:
            return False
        return value.lower() in ("true", "1", "yes", "on")
    
    @classmethod
    def get_int(cls, key: str, default: str = "0") -> int:
        """Get integer environment variable."""
        value = cls.get_env_var(key, default)
        if value is None:
            return 0
        try:
            return int(value)
        except ValueError:
            raise ConfigurationError(f"Invalid integer value for {cls.PREFIX}{key}: {value}")
    
    @classmethod
    def get_float(cls, key: str, default: str = "0.0") -> float:
        """Get float environment variable."""
        value = cls.get_env_var(key, default)
        if value is None:
            return 0.0
        try:
            return float(value)
        except ValueError:
            raise ConfigurationError(f"Invalid float value for {cls.PREFIX}{key}: {value}")
    
    @classmethod
    def get_path(cls, key: str, default: str = ".") -> Path:
        """Get path environment variable."""
        value = cls.get_env_var(key, default)
        if value is None:
            return Path(".")
        return Path(value)
    
    @classmethod
    def create_client_config(cls, config_data: Optional[Dict[str, Any]] = None, env_file: Optional[Path] = None) -> ClientConfig:
        """
        Create ClientConfig from environment variables or provided config data.
        
        Args:
            config_data: Optional dictionary with configuration data
            env_file: Optional path to .env file
            
        Returns:
            ClientConfig instance
            
        Raises:
            ConfigurationError: If required configuration is missing or invalid
        """
        if config_data is None:
            # Load .env file if provided
            if env_file is not None:
                cls.load_env_file(env_file)
            
            # Get base URL (required)
            base_url = cls.get_env_var("BASE_URL")
            if not base_url:
                raise ConfigurationError(f"{cls.PREFIX}BASE_URL environment variable is required")
            
            # Auto-generate OAuth token URL if not provided
            oauth_token_url = cls.get_env_var("OAUTH_TOKEN_URL")
            if not oauth_token_url and cls.get_bool("OAUTH_ENABLED"):
                oauth_token_url = f"{base_url}/oauth/token"
            
            return ClientConfig(
                # API configuration
                base_url=base_url,
                context_path=cls.get_env_var("CONTEXT_PATH"),
                files_base_url=cls.get_env_var("FILES_BASE_URL"),
                files_context_path=cls.get_env_var("FILES_CONTEXT_PATH"),
                
                # OAuth configuration
                oauth_enabled=cls.get_bool("OAUTH_ENABLED"),
                oauth_token_url=oauth_token_url,
                client_id=cls.get_env_var("CLIENT_ID"),
                client_secret=cls.get_env_var("CLIENT_SECRET"),
                # scope removed
                aud=cls.get_env_var("OAUTH_AUD"),
                grant_type=cls.get_env_var("GRANT_TYPE") or "client_credentials",
                
                # Bearer token configuration
                bearer_token=cls.get_env_var("BEARER_TOKEN"),
                token_refresh_threshold=cls.get_int("TOKEN_REFRESH_THRESHOLD", "300"),
                
                # HTTP configuration
                timeout=cls.get_float("TIMEOUT", "600.0"),
                max_retries=cls.get_int("MAX_RETRIES", "3"),
                retry_delay=cls.get_float("RETRY_DELAY", "1.0"),
                
                # Connection pooling
                pool_connections=cls.get_int("POOL_CONNECTIONS", "10"),
                pool_maxsize=cls.get_int("POOL_MAXSIZE", "20"),
                
                # Rate limiting
                requests_per_minute=cls.get_int("REQUESTS_PER_MINUTE", "100"),
                burst_capacity=cls.get_int("BURST_CAPACITY", "20"),
                
                # Proxy configuration
                proxy_enabled=cls.get_bool("PROXY_ENABLED"),
                proxy_url=cls.get_env_var("PROXY_URL"),
                proxy_username=cls.get_env_var("PROXY_USERNAME"),
                proxy_password=cls.get_env_var("PROXY_PASSWORD"),
                proxy_verify_ssl=cls.get_bool("PROXY_VERIFY_SSL"),
                
                # Logging
                log_level=cls.get_env_var("LOG_LEVEL") or "INFO",
                enable_debug_logging=cls.get_bool("ENABLE_DEBUG_LOGGING"),
                
                # Download configuration
                download_dir=cls.get_env_var("DOWNLOAD_DIR") or "./downloads",
                create_directories=cls.get_bool("CREATE_DIRECTORIES"),
                overwrite_existing=cls.get_bool("OVERWRITE_EXISTING"),
            )
        else:
            # Use provided config data
            return ClientConfig(**config_data)
    
    @classmethod
    def get_download_options(cls) -> Dict[str, Any]:
        """Get download options from environment variables."""
        return {
            "chunk_size": cls.get_int("CHUNK_SIZE", "8192"),
            "max_retries": cls.get_int("MAX_RETRIES", "3"),
            "retry_delay": cls.get_float("RETRY_DELAY", "1.0"),
            "timeout": cls.get_float("TIMEOUT", "600.0"),
            "enable_range_requests": cls.get_bool("ENABLE_RANGE_REQUESTS"),
            "show_progress": cls.get_bool("SHOW_PROGRESS"),
            "create_directories": cls.get_bool("CREATE_DIRECTORIES"),
            "overwrite_existing": cls.get_bool("OVERWRITE_EXISTING"),
        }
    
    @classmethod
    def get_batch_download_options(cls) -> Dict[str, Any]:
        """Get batch download options from environment variables."""
        return {
            "max_concurrent_downloads": cls.get_int("MAX_CONCURRENT_DOWNLOADS", "3"),
            "batch_size": cls.get_int("BATCH_SIZE", "10"),
            "retry_failed": cls.get_bool("RETRY_FAILED"),
            "max_retry_attempts": cls.get_int("MAX_RETRY_ATTEMPTS", "2"),
            "create_date_folders": cls.get_bool("CREATE_DATE_FOLDERS"),
            "preserve_path_structure": cls.get_bool("PRESERVE_PATH_STRUCTURE"),
            "flatten_structure": cls.get_bool("FLATTEN_STRUCTURE"),
            "show_batch_progress": cls.get_bool("SHOW_BATCH_PROGRESS"),
            "show_individual_progress": cls.get_bool("SHOW_INDIVIDUAL_PROGRESS"),
            "continue_on_error": cls.get_bool("CONTINUE_ON_ERROR"),
            "log_errors": cls.get_bool("LOG_ERRORS"),
            "save_error_log": cls.get_bool("SAVE_ERROR_LOG"),
            "use_async_downloads": cls.get_bool("USE_ASYNC_DOWNLOADS"),
            "chunk_size": cls.get_int("CHUNK_SIZE", "8192"),
        }
    
    @classmethod
    def get_workflow_paths(cls) -> Dict[str, Path]:
        """Get workflow directory paths from environment variables."""
        base_download_dir = cls.get_path("DOWNLOAD_DIR", "./downloads")
        
        return {
            "base": base_download_dir,
            "workflow": base_download_dir / (cls.get_env_var("WORKFLOW_DIR") or "workflow"),
            "groups": base_download_dir / (cls.get_env_var("GROUPS_DIR") or "groups"),
            "availability": base_download_dir / (cls.get_env_var("AVAILABILITY_DIR") or "availability"),
            "default": base_download_dir / (cls.get_env_var("DEFAULT_DIR") or "files"),
        }
    
    @classmethod
    def get_token_storage_config(cls) -> Dict[str, Any]:
        """Get token storage configuration from environment variables."""
        return {
            "enabled": cls.get_bool("TOKEN_STORAGE_ENABLED"),
            "directory": cls.get_env_var("TOKEN_STORAGE_DIR", ".tokens"),
        }
    
    @classmethod
    def validate_config(cls, config: ClientConfig) -> None:
        """Validate configuration and raise ConfigurationError if invalid."""
        errors = []
        
        # Check required fields
        if not config.base_url:
            errors.append("BASE_URL is required")
        
        # Check OAuth configuration
        if config.oauth_enabled:
            if not config.client_id:
                errors.append("CLIENT_ID is required when OAuth is enabled")
            if not config.client_secret:
                errors.append("CLIENT_SECRET is required when OAuth is enabled")
            if not config.oauth_token_url:
                errors.append("OAUTH_TOKEN_URL is required when OAuth is enabled")
        
        # Check Bearer token configuration
        if not config.has_oauth_credentials and not config.has_bearer_token:
            errors.append("Either OAuth credentials or Bearer token must be configured")
        
        # Check numeric values
        if config.timeout <= 0:
            errors.append("TIMEOUT must be positive")
        if config.max_retries < 0:
            errors.append("MAX_RETRIES must be non-negative")
        if config.retry_delay < 0:
            errors.append("RETRY_DELAY must be non-negative")
        if config.pool_connections <= 0:
            errors.append("POOL_CONNECTIONS must be positive")
        if config.pool_maxsize <= 0:
            errors.append("POOL_MAXSIZE must be positive")
        if config.requests_per_minute <= 0:
            errors.append("REQUESTS_PER_MINUTE must be positive")
        if config.burst_capacity <= 0:
            errors.append("BURST_CAPACITY must be positive")
        
        if errors:
            raise ConfigurationError(f"Configuration validation failed: {'; '.join(errors)}")
    
    @classmethod
    def create_env_template(cls, output_path: Optional[Path] = None) -> Path:
        """
        Create a .env template file with all available configuration options.
        
        Args:
            output_path: Optional output path for the template
            
        Returns:
            Path to the created template file
        """
        if output_path is None:
            output_path = Path(".env.template")
        
        # Ensure output_path is a Path object
        if not isinstance(output_path, Path):
            output_path = Path(output_path)
        
        template_content = f"""# DATAQUERY SDK Environment Configuration Template
# Copy this file to .env and update the values as needed

# API Configuration
{cls.PREFIX}BASE_URL=https://api.dataquery.com
{cls.PREFIX}CONTEXT_PATH=

# OAuth Configuration
{cls.PREFIX}OAUTH_ENABLED=true
{cls.PREFIX}OAUTH_TOKEN_URL=https://api.dataquery.com/oauth/token
{cls.PREFIX}CLIENT_ID=your_client_id_here
{cls.PREFIX}CLIENT_SECRET=your_client_secret_here
{cls.PREFIX}SCOPE=data.read
{cls.PREFIX}AUDIENCE=
{cls.PREFIX}GRANT_TYPE=client_credentials

# Bearer Token Configuration (alternative to OAuth)
{cls.PREFIX}BEARER_TOKEN=your_bearer_token_here
{cls.PREFIX}TOKEN_REFRESH_THRESHOLD=300

# HTTP Configuration
{cls.PREFIX}TIMEOUT=6000.0
{cls.PREFIX}MAX_RETRIES=3
{cls.PREFIX}RETRY_DELAY=1.0

# Connection Pooling
{cls.PREFIX}POOL_CONNECTIONS=10
{cls.PREFIX}POOL_MAXSIZE=20

# Rate Limiting
{cls.PREFIX}REQUESTS_PER_MINUTE=100
{cls.PREFIX}BURST_CAPACITY=20

# Proxy Configuration
{cls.PREFIX}PROXY_ENABLED=false
{cls.PREFIX}PROXY_URL=
{cls.PREFIX}PROXY_USERNAME=
{cls.PREFIX}PROXY_PASSWORD=
{cls.PREFIX}PROXY_VERIFY_SSL=true

# Logging
{cls.PREFIX}LOG_LEVEL=INFO
{cls.PREFIX}ENABLE_DEBUG_LOGGING=false

# Download Configuration
{cls.PREFIX}DOWNLOAD_DIR=./downloads
{cls.PREFIX}CREATE_DIRECTORIES=true
{cls.PREFIX}OVERWRITE_EXISTING=false

# Batch Download Configuration
{cls.PREFIX}MAX_CONCURRENT_DOWNLOADS=3
{cls.PREFIX}BATCH_SIZE=10
{cls.PREFIX}RETRY_FAILED=true
{cls.PREFIX}MAX_RETRY_ATTEMPTS=2
{cls.PREFIX}CREATE_DATE_FOLDERS=true
{cls.PREFIX}PRESERVE_PATH_STRUCTURE=true
{cls.PREFIX}FLATTEN_STRUCTURE=false
{cls.PREFIX}SHOW_BATCH_PROGRESS=true
{cls.PREFIX}SHOW_INDIVIDUAL_PROGRESS=true
{cls.PREFIX}CONTINUE_ON_ERROR=true
{cls.PREFIX}LOG_ERRORS=true
{cls.PREFIX}SAVE_ERROR_LOG=true
{cls.PREFIX}USE_ASYNC_DOWNLOADS=true
{cls.PREFIX}CHUNK_SIZE=8192

# Download Options
{cls.PREFIX}ENABLE_RANGE_REQUESTS=true
{cls.PREFIX}SHOW_PROGRESS=true

# Workflow Configuration
{cls.PREFIX}WORKFLOW_DIR=workflow
{cls.PREFIX}GROUPS_DIR=groups
{cls.PREFIX}AVAILABILITY_DIR=availability
{cls.PREFIX}DEFAULT_DIR=files

# Security
{cls.PREFIX}MASK_SECRETS=true
{cls.PREFIX}TOKEN_STORAGE_ENABLED=true
{cls.PREFIX}TOKEN_STORAGE_DIR=.tokens
"""
        
        output_path.write_text(template_content)
        return output_path
    
    @classmethod
    def get_all_env_vars(cls) -> Dict[str, str]:
        """Get all DATAQUERY environment variables."""
        env_vars = {}
        for key in cls.DEFAULTS.keys():
            value = cls.get_env_var(key)
            if value is not None:
                env_vars[key] = value
        return env_vars
    
    @classmethod
    def mask_secrets(cls, config_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Mask sensitive configuration values."""
        sensitive_keys = {
            "client_id", "client_secret", "bearer_token", 
            "oauth_token_url", "aud"
        }
        
        masked_config = config_dict.copy()
        for key in sensitive_keys:
            if key in masked_config and masked_config[key]:
                masked_config[key] = "***"
        
        return masked_config 



 