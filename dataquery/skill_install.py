"""Install the bundled ``dataquery`` agent skill into coding-agent apps.

Every supported app reads the same Agent Skills format (a folder holding a
``SKILL.md``); they differ only in where they look. See
https://agentskills.io/specification.
"""

import shutil
import tempfile
from dataclasses import dataclass
from importlib import resources
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import Dict, Optional, Tuple

from . import __version__
from .types.exceptions import ConfigurationError

SKILL_NAME = "dataquery"

# Written into every installed copy, so a re-run knows the folder is ours to replace.
MARKER_FILE = ".installed-by-dataquery-sdk"


@dataclass(frozen=True)
class SkillApp:
    """Where one app looks for skills, relative to the home and project directories."""

    label: str
    user_parts: Tuple[str, ...]
    project_parts: Tuple[str, ...]

    def skills_root(self, scope: str, project_dir: Optional[Path] = None) -> Path:
        if scope == "user":
            return Path.home().joinpath(*self.user_parts)
        return (project_dir or Path.cwd()).joinpath(*self.project_parts)


SKILL_APPS: Dict[str, SkillApp] = {
    "claude-code": SkillApp("Claude Code", (".claude", "skills"), (".claude", "skills")),
    "codex": SkillApp("Codex", (".agents", "skills"), (".agents", "skills")),
    "vscode": SkillApp("VS Code (GitHub Copilot)", (".copilot", "skills"), (".github", "skills")),
    "cursor": SkillApp("Cursor", (".cursor", "skills"), (".cursor", "skills")),
}


def bundled_skill() -> Traversable:
    """The skill folder shipped inside this package."""
    skill = resources.files("dataquery").joinpath("skills", SKILL_NAME)
    if not skill.joinpath("SKILL.md").is_file():
        raise ConfigurationError(f"The bundled skill is missing from this installation: {skill}")
    return skill


def target_dir(app: str, scope: str, project_dir: Optional[Path] = None) -> Path:
    return SKILL_APPS[app].skills_root(scope, project_dir) / SKILL_NAME


def _is_ours(path: Path) -> bool:
    """True for a real folder this installer wrote, or an earlier copy of this skill."""
    if path.is_symlink() or not path.is_dir():
        return False
    if (path / MARKER_FILE).is_file():
        return True
    skill_md = path / "SKILL.md"
    if not skill_md.is_file():
        return False
    try:
        frontmatter = skill_md.read_text(encoding="utf-8").split("---")[1]
    except (IndexError, OSError, UnicodeDecodeError):
        return False
    return any(line.strip() == f"name: {SKILL_NAME}" for line in frontmatter.splitlines())


def _copy_tree(src: Traversable, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    for item in src.iterdir():
        if item.name.startswith((".", "__")):
            continue
        if item.is_dir():
            _copy_tree(item, dest / item.name)
        else:
            (dest / item.name).write_bytes(item.read_bytes())


def install(
    app: str, scope: str = "user", project_dir: Optional[Path] = None, force: bool = False
) -> Tuple[Path, bool]:
    """Copy the bundled skill into ``app``'s skills folder; returns ``(path, replaced)``.

    An existing folder is replaced only if it is a copy of this skill, unless
    ``force``. A symlink is never followed or replaced without ``force``, since
    it usually points at someone's working copy.
    """
    dest = target_dir(app, scope, project_dir)
    exists = dest.exists() or dest.is_symlink()
    if exists and not force and not _is_ours(dest):
        kind = "a symlink" if dest.is_symlink() else "not a copy of this skill"
        raise ConfigurationError(f"{dest} already exists and is {kind}; pass --force to replace it")

    dest.parent.mkdir(parents=True, exist_ok=True)
    # Build the new copy beside the target, then swap, so a failure never leaves half a skill.
    staging = Path(tempfile.mkdtemp(prefix=f".{SKILL_NAME}-", dir=dest.parent))
    try:
        new_copy = staging / SKILL_NAME
        _copy_tree(bundled_skill(), new_copy)
        (new_copy / MARKER_FILE).write_text(f"dataquery-sdk {__version__}\n", encoding="utf-8")
        if exists:
            _remove(dest)
        new_copy.rename(dest)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return dest, exists


def uninstall(app: str, scope: str = "user", project_dir: Optional[Path] = None, force: bool = False) -> Optional[Path]:
    """Remove the installed skill; returns its path, or ``None`` if it was not installed."""
    dest = target_dir(app, scope, project_dir)
    if not (dest.exists() or dest.is_symlink()):
        return None
    if not force and not _is_ours(dest):
        raise ConfigurationError(f"{dest} is not a copy of this skill; pass --force to remove it")
    _remove(dest)
    return dest


def _remove(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink()
    else:
        shutil.rmtree(path)
