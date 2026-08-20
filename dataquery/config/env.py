"""Environment-based configuration for the DATAQUERY SDK."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Union, get_args, get_origin

from dotenv import load_dotenv
from pydantic_core import PydanticUndefined

from ..types.exceptions import ConfigurationError
from ..types.models import ClientConfig

_ENV_NAME_OVERRIDES: Dict[str, str] = {
    "aud": "OAUTH_AUD",
}

_DEFAULT_OVERRIDES: Dict[str, str] = {
    "TOKEN_STORAGE_DIR": ".tokens",
}

_SENSITIVE_FIELDS = frozenset(
    {
        "client_id",
        "client_secret",
        "bearer_token",
        "oauth_token_url",
        "aud",
    }
)

# User-level config: credentials saved once (e.g. by `dataquery mcp-connect
# --save-credentials`) and reused by every later SDK call in any process.
_USER_CONFIG_DIR_ENV = "DATAQUERY_CONFIG_DIR"
_USER_CONFIG_DIR_DEFAULT = "~/.dataquery"
_USER_ENV_FILENAME = ".env"

_USER_ENV_HEADER = [
    "# DataQuery SDK credentials.",
    "# Written by `dataquery mcp-connect --save-credentials`; loaded as a",
    "# fallback only, so shell env vars and a local .env still win.",
    "# Values are read literally: ${VAR} is not expanded here.",
    "",
]

_ENV_LINE_RE = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=")

# Characters common in ids, secrets and URLs that need no .env quoting.
_UNQUOTED_EXTRA_CHARS = "._-:/@+~"


def _format_env_value(value: str) -> str:
    """Render a value for a ``.env`` line, quoting only when it needs it."""
    if value and all(ch.isalnum() or ch in _UNQUOTED_EXTRA_CHARS for ch in value):
        return value
    if "'" not in value and "\n" not in value and "\r" not in value:
        return f"'{value}'"
    # Escapes keep the entry on a single physical line, which the line-based
    # merge in ``save_user_env`` depends on.
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r")
    return f'"{escaped}"'


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
    """Compute the ``EnvConfig.DEFAULTS`` table from the model."""
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
    """Environment-based configuration loader for the DataQuery SDK."""

    PREFIX = "DATAQUERY_"
    DEFAULTS: Dict[str, Optional[str]] = _build_defaults()

    @classmethod
    def load_env_file(cls, env_file: Optional[Path] = None) -> None:
        """Load environment variables from a ``.env`` file (no-op if missing)."""
        if env_file is None:
            env_file = Path(".env")
        if env_file.exists():
            load_dotenv(env_file)

    @classmethod
    def user_config_dir(cls) -> Path:
        """Directory holding user-level SDK config (``$DATAQUERY_CONFIG_DIR`` or ``~/.dataquery``)."""
        raw = os.getenv(_USER_CONFIG_DIR_ENV) or _USER_CONFIG_DIR_DEFAULT
        return Path(raw).expanduser()

    @classmethod
    def user_env_file(cls) -> Path:
        """Path of the user-level ``.env`` holding saved credentials."""
        return cls.user_config_dir() / _USER_ENV_FILENAME

    @classmethod
    def load_user_env_file(cls) -> bool:
        """Load the user-level ``.env`` as a fallback; never overrides what is already set."""
        env_file = cls.user_env_file()
        if not env_file.exists():
            return False
        # interpolate=False: values here are literal secrets, and dotenv's
        # ${VAR} expansion runs after unquoting, so no quoting style escapes it.
        load_dotenv(env_file, override=False, interpolate=False)
        return True

    @classmethod
    def save_user_env(cls, values: Mapping[str, Optional[str]]) -> Path:
        """Merge ``DATAQUERY_<key>=<value>`` pairs into the user-level ``.env``.

        Keys are unprefixed (``CLIENT_ID``) and empty values are skipped. Lines
        for a key being saved are rewritten in place, anything else already in
        the file is preserved, and the file is (re)written owner-readable only.
        """
        updates: Dict[str, str] = {}
        for key, value in values.items():
            if value:
                updates[f"{cls.PREFIX}{key}"] = value

        env_file = cls.user_env_file()
        env_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            env_file.parent.chmod(0o700)
        except OSError:  # pragma: no cover - non-POSIX filesystem
            pass

        lines: List[str] = []
        written: set[str] = set()
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                match = _ENV_LINE_RE.match(line)
                line_key = match.group(1) if match else None
                if line_key is None or line_key not in updates:
                    lines.append(line)
                elif line_key not in written:
                    lines.append(f"{line_key}={_format_env_value(updates[line_key])}")
                    written.add(line_key)
                # else: drop a stale duplicate of a key just rewritten above
        else:
            lines.extend(_USER_ENV_HEADER)

        for key, value in updates.items():
            if key not in written:
                lines.append(f"{key}={_format_env_value(value)}")

        # O_CREAT mode only applies to new files, so chmod covers the rest.
        fd = os.open(env_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as handle:
            handle.write("\n".join(lines) + "\n")
        try:
            env_file.chmod(0o600)
        except OSError:  # pragma: no cover - non-POSIX filesystem
            pass
        return env_file

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
        """Build a :class:`ClientConfig` from env vars or an explicit dict."""
        if config_data is not None:
            return ClientConfig(**config_data)

        if env_file is not None:
            cls.load_env_file(env_file)

        # Lowest precedence, loaded last so it fills only what is still unset:
        # credentials saved by `mcp-connect --save-credentials`, which let a
        # later DataQuery() authenticate with no environment of its own.
        cls.load_user_env_file()

        if not cls.get_env_var("BASE_URL"):
            raise ConfigurationError(f"{cls.PREFIX}BASE_URL environment variable is required")

        kwargs: Dict[str, Any] = {}
        for field_name, field in ClientConfig.model_fields.items():
            kwargs[field_name] = cls._read_field(field_name, field)

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
        return cls.get_env_var(env_key)

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

    @classmethod
    def validate_config(cls, config: ClientConfig) -> None:
        """Raise :class:`ConfigurationError` if the cross-field invariants fail."""
        errors = []

        if not config.base_url:
            errors.append("BASE_URL is required")

        if config.oauth_enabled:
            if not config.client_id:
                errors.append("CLIENT_ID is required when OAuth is enabled")
            if not config.get_client_secret():
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

    @classmethod
    def create_env_template(cls, output_path: Optional[Path] = None) -> Path:
        """Write a ``.env`` template listing every supported variable."""
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
