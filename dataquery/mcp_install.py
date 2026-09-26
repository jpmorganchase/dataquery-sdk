"""One-time setup: register ``dataquery mcp-connect`` with an MCP client app."""

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .types.exceptions import ConfigurationError

# Server names this installer writes: safe as a JSON key, a TOML bare key and a CLI argument.
SERVER_NAME_RE = re.compile(r"[A-Za-z0-9_-]+")

_CLAUDE_CLI_TIMEOUT = 60.0


def dataquery_command() -> List[str]:
    """The absolute command that runs this environment's ``dataquery`` CLI.

    MCP apps are often GUI apps that don't inherit your shell's PATH, so the
    config gets a full path rather than a bare ``dataquery``.
    """
    script = Path(sys.executable).with_name("dataquery.exe" if os.name == "nt" else "dataquery")
    if script.is_file():
        return [str(script)]
    # e.g. a --user install, whose scripts live apart from the interpreter
    return [sys.executable, "-m", "dataquery.cli"]


def server_entry(url: Optional[str] = None) -> Dict[str, Any]:
    """The server entry: this environment's ``mcp-connect``, with no secrets in it."""
    command, *args = dataquery_command()
    args.append("mcp-connect")
    if url:
        args += ["--url", url]
    return {"command": command, "args": args}


def _app_data_dir() -> Path:
    """The per-user application data directory for this OS."""
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support"
    if sys.platform == "win32":
        return Path(os.environ.get("APPDATA") or Path.home() / "AppData" / "Roaming")
    return Path(os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config")


@dataclass(frozen=True)
class JsonApp:
    """An MCP app that keeps its user-level servers in a JSON file."""

    label: str
    parts: Tuple[str, ...]  # the file, relative to the app-data or home directory
    in_app_data: bool
    servers_key: str = "mcpServers"
    stdio_type: bool = False  # the app's schema names the transport: "type": "stdio"

    def config_path(self) -> Path:
        return (_app_data_dir() if self.in_app_data else Path.home()).joinpath(*self.parts)

    def add(self, name: str, entry: Dict[str, Any]) -> Tuple[Path, bool]:
        """Add or replace the server in this app's config; returns ``(path, replaced)``."""
        if self.stdio_type:
            entry = {"type": "stdio", **entry}
        path = self.config_path()
        return path, add_to_config_file(path, name, entry, self.servers_key)


JSON_APPS: Dict[str, JsonApp] = {
    "claude-desktop": JsonApp("Claude Desktop", ("Claude", "claude_desktop_config.json"), in_app_data=True),
    "cursor": JsonApp("Cursor", (".cursor", "mcp.json"), in_app_data=False, stdio_type=True),
    "vscode": JsonApp(
        "VS Code", ("Code", "User", "mcp.json"), in_app_data=True, servers_key="servers", stdio_type=True
    ),
}


def codex_config_path() -> Path:
    """The Codex config, shared by the ChatGPT desktop app, the Codex CLI and its IDE extension."""
    return Path.home() / ".codex" / "config.toml"


def _write_atomically(path: Path, text: str) -> None:
    """Replace ``path`` with ``text`` in one step, keeping the file's permissions."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")  # created 0600
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
        if path.exists():
            # Keep the file's own permissions: it may hold other servers' secrets.
            shutil.copymode(path, tmp)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def add_to_config_file(path: Path, name: str, entry: Dict[str, Any], servers_key: str = "mcpServers") -> bool:
    """Add or replace ``<servers_key>[name]`` in an MCP app's JSON config.

    Everything else in the file is kept, and a file that isn't valid JSON is
    left untouched. Returns ``True`` if an existing entry was replaced.
    """
    path = path.expanduser().resolve()  # through a symlink, so a dotfiles link survives
    config: Any = {}
    if path.exists():
        text = path.read_text(encoding="utf-8")
        if text.strip():
            try:
                config = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ConfigurationError(f"{path} is not valid JSON ({exc}); fix it and re-run") from exc
    servers = config.setdefault(servers_key, {}) if isinstance(config, dict) else None
    if not isinstance(servers, dict):
        raise ConfigurationError(f"{path} has no usable '{servers_key}' object; fix it and re-run")
    replaced = name in servers
    servers[name] = entry
    _write_atomically(path, json.dumps(config, indent=2) + "\n")
    return replaced


def add_to_codex(name: str, entry: Dict[str, Any]) -> Tuple[Path, bool]:
    """Add or replace the server in the Codex config; returns ``(path, replaced)``.

    The table goes between marker comments, so a re-run replaces exactly what an
    earlier run wrote and the rest of the file stays byte for byte. A server of
    the same name added some other way is left alone, and nothing is written
    unless the result parses back to this entry.
    """
    path = codex_config_path().resolve()
    text = path.read_text(encoding="utf-8") if path.exists() else ""
    try:
        servers = tomllib.loads(text).get("mcp_servers", {})
    except tomllib.TOMLDecodeError as exc:
        raise ConfigurationError(f"{path} is not valid TOML ({exc}); fix it and re-run") from exc

    begin, end = f"# >>> dataquery mcp-install: {name} >>>", f"# <<< dataquery mcp-install: {name} <<<"
    # A JSON string (non-ASCII left as is) is also a valid TOML basic string.
    args = ", ".join(json.dumps(arg, ensure_ascii=False) for arg in entry["args"])
    block = (
        f"{begin}\n[mcp_servers.{name}]\ncommand = {json.dumps(entry['command'], ensure_ascii=False)}\n"
        f"args = [{args}]\n{end}\n"
    )
    ours = re.compile(rf"^{re.escape(begin)}$.*?^{re.escape(end)}$\n?", re.MULTILINE | re.DOTALL)
    replaced = ours.search(text) is not None
    if replaced:
        new_text = ours.sub(lambda _: block, text, count=1)
    elif isinstance(servers, dict) and name in servers:
        raise ConfigurationError(
            f"{path} already has a '{name}' server that mcp-install didn't add; remove its "
            f"[mcp_servers.{name}] table and re-run, or pick another --name"
        )
    else:
        separator = "" if not text.strip() else ("\n" if text.endswith("\n") else "\n\n")
        new_text = text + separator + block

    try:
        written = tomllib.loads(new_text).get("mcp_servers", {}).get(name)
    except (tomllib.TOMLDecodeError, AttributeError):
        written = None
    if written != entry:
        raise ConfigurationError(f"Couldn't add '{name}' to {path} cleanly; add this to it by hand:\n{block}")
    _write_atomically(path, new_text)
    return path, replaced


def add_to_claude_code(name: str, entry: Dict[str, Any]) -> None:
    """Register the server in Claude Code's user scope through its own CLI.

    A user-scope server of the same name is replaced, so re-running updates it.
    """
    spec = json.dumps({"type": "stdio", **entry})
    claude = shutil.which("claude")
    if claude is None:
        raise ConfigurationError(
            f"Claude Code CLI ('claude') not found on PATH. Add the server yourself with:\n"
            f"  claude mcp add-json -s user {name} '{spec}'"
        )

    def claude_mcp(*args: str) -> "subprocess.CompletedProcess[str]":
        try:
            return subprocess.run(
                [claude, "mcp", *args], capture_output=True, text=True, timeout=_CLAUDE_CLI_TIMEOUT, check=False
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ConfigurationError(f"'claude mcp {args[0]}' failed: {exc}") from exc

    claude_mcp("remove", "-s", "user", name)  # fails harmlessly when there is none
    result = claude_mcp("add-json", "-s", "user", name, spec)
    if result.returncode != 0:
        raise ConfigurationError(f"'claude mcp add-json' failed: {(result.stderr or result.stdout).strip()}")
