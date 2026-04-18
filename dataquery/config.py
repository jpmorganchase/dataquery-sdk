"""
Environment-based configuration for the DATAQUERY SDK.

Loads settings from environment variables (or a ``.env`` file) into a
:class:`ClientConfig` Pydantic model. Field declarations live on
``ClientConfig`` — this module only handles the env-var → field projection,
type coercion, and cross-field validation.

Single source of truth: :data:`ClientConfig.model_fields`. The
``DEFAULTS`` table here is derived from the model at import time so the two
cannot drift.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional, Union, get_args, get_origin

from dotenv import load_dotenv
from pydantic_core import PydanticUndefined

from .exceptions import ConfigurationError
from .models import ClientConfig

# ---------------------------------------------------------------------------
# Per-field metadata that the model itself does not encode.
# ---------------------------------------------------------------------------

# Field-name → env-key overrides for fields whose env name does not follow
# the field-name-uppercased convention. Keep this list short — every entry
# is a deliberate deviation, not a typo.
_ENV_NAME_OVERRIDES: Dict[str, str] = {
    "aud": "OAUTH_AUD",
}

# Env keys whose default differs from the model default. Currently only
# ``TOKEN_STORAGE_DIR``: the model declares ``None`` (the directory is
# resolved from ``download_dir`` at use-time) but historically the env
# fallback has been ``.tokens``.
_DEFAULT_OVERRIDES: Dict[str, str] = {
    "TOKEN_STORAGE_DIR": ".tokens",
}

# Field names whose values must be redacted from log output / serialised
# config dumps.
_SENSITIVE_FIELDS = frozenset(
    {
        "client_id",
        "client_secret",
        "bearer_token",
        "oauth_token_url",
        "aud",
    }
)


def _env_name_for(field_name: str) -> str:
    """Project a Pydantic field name to its env-var key (no PREFIX)."""
    return _ENV_NAME_OVERRIDES.get(field_name, field_name.upper())


def _unwrap_optional(annotation: Any) -> Any:
    """``Optional[X]`` → ``X``; pass-through for everything else."""
    if get_origin(annotation) is Union:
        args = [a for a in get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return annotation


def _build_defaults() -> Dict[str, Optional[str]]:
    """Compute the ``EnvConfig.DEFAULTS`` table from the model.

    Booleans become ``"true"``/``"false"`` (lowercase, matching the
    tokens :meth:`EnvConfig.get_bool` accepts). Numeric / string defaults
    are stringified directly. ``None`` defaults stay ``None``.
    """
    defaults: Dict[str, Optional[str]] = {}
    for field_name, field in ClientConfig.model_fields.items():
        env_key = _env_name_for(field_name)
        if field.default is PydanticUndefined or field.default is None:
            defaults[env_key] = None
            continue
        value = field.default
        if isinstance(value, bool):
            defaults[env_key] = "true" if value else "false"
        else:
            defaults[env_key] = str(value)
    defaults.update(_DEFAULT_OVERRIDES)
    return defaults


class EnvConfig:
    """Environment-based configuration loader for the DataQuery SDK.

    All methods are class methods — this class is a namespace, never
    instantiated. Field definitions, types, and defaults all originate from
    :class:`dataquery.models.ClientConfig`.
    """

    PREFIX = "DATAQUERY_"
    DEFAULTS: Dict[str, Optional[str]] = _build_defaults()

    # ------------------------------------------------------------------
    # .env file loading
    # ------------------------------------------------------------------

    @classmethod
    def load_env_file(cls, env_file: Optional[Path] = None) -> None:
        """Load environment variables from a ``.env`` file (no-op if missing)."""
        if env_file is None:
            env_file = Path(".env")
        if env_file.exists():
            load_dotenv(env_file)

    # ------------------------------------------------------------------
    # Typed env-var getters
    #
    # Empty strings are treated as "unset" — the legacy contract preserved
    # so callers that read ``EnvConfig.get_int("MAX_RETRIES")`` with the
    # var explicitly set to "" still get ``0``, not the model default.
    # ------------------------------------------------------------------

    @classmethod
    def get_env_var(cls, key: str, default: Optional[str] = None) -> Optional[str]:
        """Read ``$DATAQUERY_<key>`` with the model default as the fallback."""
        if default is None:
            default = cls.DEFAULTS.get(key)
        value = os.getenv(f"{cls.PREFIX}{key}", default)
        return None if value == "" else value

    @classmethod
    def get_bool(cls, key: str, default: Optional[str] = None) -> bool:
        value = cls.get_env_var(key, default if default is not None else cls.DEFAULTS.get(key, "false"))
        return value.lower() in ("true", "1", "yes", "on") if value else False

    @classmethod
    def get_int(cls, key: str, default: Optional[str] = None) -> int:
        value = cls.get_env_var(key, default if default is not None else cls.DEFAULTS.get(key, "0"))
        if value is None:
            return 0
        try:
            return int(value)
        except ValueError as exc:
            raise ConfigurationError(f"Invalid integer value for {cls.PREFIX}{key}: {value}") from exc

    @classmethod
    def get_float(cls, key: str, default: Optional[str] = None) -> float:
        value = cls.get_env_var(key, default if default is not None else cls.DEFAULTS.get(key, "0.0"))
        if value is None:
            return 0.0
        try:
            return float(value)
        except ValueError as exc:
            raise ConfigurationError(f"Invalid float value for {cls.PREFIX}{key}: {value}") from exc

    @classmethod
    def get_path(cls, key: str, default: str = ".") -> Path:
        value = cls.get_env_var(key, default)
        return Path(value) if value else Path(".")

    # ------------------------------------------------------------------
    # ClientConfig factory
    # ------------------------------------------------------------------

    @classmethod
    def create_client_config_with_defaults(cls, base_url: str) -> ClientConfig:
        """Build a :class:`ClientConfig` with only ``base_url`` overridden."""
        return ClientConfig(base_url=base_url)

    @classmethod
    def create_client_config(
        cls,
        config_data: Optional[Dict[str, Any]] = None,
        env_file: Optional[Path] = None,
    ) -> ClientConfig:
        """Build a :class:`ClientConfig` from env vars or an explicit dict.

        When ``config_data`` is provided the env-var path is skipped entirely
        — the dict is passed straight to :class:`ClientConfig`.
        """
        if config_data is not None:
            return ClientConfig(**config_data)

        if env_file is not None:
            cls.load_env_file(env_file)

        if not cls.get_env_var("BASE_URL"):
            raise ConfigurationError(f"{cls.PREFIX}BASE_URL environment variable is required")

        kwargs: Dict[str, Any] = {}
        for field_name, field in ClientConfig.model_fields.items():
            kwargs[field_name] = cls._read_field(field_name, field)

        # Auto-derive an OAuth token URL when OAuth is enabled but the URL
        # is missing — preserves legacy behaviour for users who only set
        # base_url + credentials.
        if kwargs.get("oauth_enabled") and not kwargs.get("oauth_token_url"):
            kwargs["oauth_token_url"] = f"{kwargs['base_url']}/oauth/token"

        return ClientConfig(**kwargs)

    @classmethod
    def _read_field(cls, field_name: str, field: Any) -> Any:
        """Read & coerce a single env var to the field's declared type."""
        env_key = _env_name_for(field_name)
        ann = _unwrap_optional(field.annotation)
        if ann is bool:
            return cls.get_bool(env_key)
        if ann is int:
            return cls.get_int(env_key)
        if ann is float:
            return cls.get_float(env_key)
        # Strings, paths, anything else — return raw string or None.
        return cls.get_env_var(env_key)

    # ------------------------------------------------------------------
    # Subset views (used by the CLI / examples)
    # ------------------------------------------------------------------

    @classmethod
    def get_download_options(cls) -> Dict[str, Any]:
        """Subset of config relevant to a single-file download."""
        return {
            "chunk_size": cls.get_int("CHUNK_SIZE"),
            "max_retries": cls.get_int("MAX_RETRIES"),
            "retry_delay": cls.get_float("RETRY_DELAY"),
            "timeout": cls.get_float("TIMEOUT"),
            "enable_range_requests": cls.get_bool("ENABLE_RANGE_REQUESTS"),
            "show_progress": cls.get_bool("SHOW_PROGRESS"),
            "create_directories": cls.get_bool("CREATE_DIRECTORIES"),
            "overwrite_existing": cls.get_bool("OVERWRITE_EXISTING"),
        }

    @classmethod
    def get_batch_download_options(cls) -> Dict[str, Any]:
        """Subset of config relevant to batch downloads."""
        return {
            "max_concurrent_downloads": cls.get_int("MAX_CONCURRENT_DOWNLOADS"),
            "batch_size": cls.get_int("BATCH_SIZE"),
            "retry_failed": cls.get_bool("RETRY_FAILED"),
            "max_retry_attempts": cls.get_int("MAX_RETRY_ATTEMPTS"),
            "create_date_folders": cls.get_bool("CREATE_DATE_FOLDERS"),
            "preserve_path_structure": cls.get_bool("PRESERVE_PATH_STRUCTURE"),
            "flatten_structure": cls.get_bool("FLATTEN_STRUCTURE"),
            "show_batch_progress": cls.get_bool("SHOW_BATCH_PROGRESS"),
            "show_individual_progress": cls.get_bool("SHOW_INDIVIDUAL_PROGRESS"),
            "continue_on_error": cls.get_bool("CONTINUE_ON_ERROR"),
            "log_errors": cls.get_bool("LOG_ERRORS"),
            "save_error_log": cls.get_bool("SAVE_ERROR_LOG"),
            "use_async_downloads": cls.get_bool("USE_ASYNC_DOWNLOADS"),
            "chunk_size": cls.get_int("CHUNK_SIZE"),
        }

    @classmethod
    def get_workflow_paths(cls) -> Dict[str, Path]:
        """Resolve the workflow subdirectory layout under ``DOWNLOAD_DIR``."""
        base = cls.get_path("DOWNLOAD_DIR", "./downloads")
        return {
            "base": base,
            "workflow": base / (cls.get_env_var("WORKFLOW_DIR") or "workflow"),
            "groups": base / (cls.get_env_var("GROUPS_DIR") or "groups"),
            "availability": base / (cls.get_env_var("AVAILABILITY_DIR") or "availability"),
            "default": base / (cls.get_env_var("DEFAULT_DIR") or "files"),
        }

    @classmethod
    def get_token_storage_config(cls) -> Dict[str, Any]:
        """OAuth token storage settings."""
        return {
            "enabled": cls.get_bool("TOKEN_STORAGE_ENABLED"),
            "directory": cls.get_env_var("TOKEN_STORAGE_DIR", ".tokens"),
        }

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @classmethod
    def validate_config(cls, config: ClientConfig) -> None:
        """Raise :class:`ConfigurationError` if the cross-field invariants fail."""
        errors = []

        if not config.base_url:
            errors.append("BASE_URL is required")

        if config.oauth_enabled:
            if not config.client_id:
                errors.append("CLIENT_ID is required when OAuth is enabled")
            if not config.client_secret:
                errors.append("CLIENT_SECRET is required when OAuth is enabled")
            if not config.oauth_token_url:
                errors.append("OAUTH_TOKEN_URL is required when OAuth is enabled")

        if not config.has_oauth_credentials and not config.has_bearer_token:
            errors.append("Either OAuth credentials or Bearer token must be configured")

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

    # ------------------------------------------------------------------
    # Template & introspection helpers
    # ------------------------------------------------------------------

    @classmethod
    def create_env_template(cls, output_path: Optional[Path] = None) -> Path:
        """Write a ``.env`` template listing every supported variable.

        Lines are auto-generated from :data:`ClientConfig.model_fields` so a
        new field shows up here automatically (no separate maintenance).
        """
        if output_path is None:
            output_path = Path(".env.template")
        if not isinstance(output_path, Path):
            output_path = Path(output_path)

        lines = [
            "# DATAQUERY SDK Environment Configuration Template",
            "# Copy this file to .env and update the values as needed.",
            "# Defaults shown are the values used when the variable is unset.",
            "",
        ]
        for field_name, field in ClientConfig.model_fields.items():
            env_key = _env_name_for(field_name)
            default = cls.DEFAULTS.get(env_key)
            description = (field.description or "").strip()
            if description:
                lines.append(f"# {description}")
            lines.append(f"{cls.PREFIX}{env_key}={default if default is not None else ''}")
            lines.append("")

        output_path.write_text("\n".join(lines))
        return output_path

    @classmethod
    def get_all_env_vars(cls) -> Dict[str, str]:
        """Return only ``DATAQUERY_*`` vars actually set in the process env."""
        prefix_len = len(cls.PREFIX)
        return {k[prefix_len:]: v for k, v in os.environ.items() if k.startswith(cls.PREFIX)}

    @classmethod
    def mask_secrets(cls, config_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Return a copy of ``config_dict`` with sensitive values redacted."""
        masked = config_dict.copy()
        for key in _SENSITIVE_FIELDS:
            if masked.get(key):
                masked[key] = "***"
        return masked
