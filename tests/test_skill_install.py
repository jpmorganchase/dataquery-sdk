import re
from pathlib import Path

import pytest

from dataquery import cli, skill_install
from dataquery.types.exceptions import ConfigurationError


@pytest.fixture
def home(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))
    return home


def _frontmatter(skill_md: Path) -> str:
    return skill_md.read_text(encoding="utf-8").split("---")[1]


def test_bundled_skill_meets_the_agent_skills_spec():
    """Codex, VS Code and Cursor enforce agentskills.io limits that Claude Code does not."""
    skill = skill_install.bundled_skill()
    fm = _frontmatter(Path(str(skill.joinpath("SKILL.md"))))
    name = re.search(r"^name:\s*(\S+)\s*$", fm, re.M).group(1)
    assert name == skill_install.SKILL_NAME == skill.name
    assert re.fullmatch(r"[a-z0-9]+(-[a-z0-9]+)*", name) and len(name) <= 64
    block = re.search(r"^description: >-\n((?:  .*\n?)+)", fm, re.M).group(1)
    description = " ".join(line.strip() for line in block.splitlines())
    assert 0 < len(description) <= 1024
    # Only spec fields: skills-ref (the spec's validator) rejects app-specific keys
    # such as Claude Code's disable-model-invocation.
    keys = set(re.findall(r"^([a-z-]+):", fm, re.M))
    assert keys <= {"name", "description", "license", "compatibility", "metadata", "allowed-tools"}
    for ref in ("endpoints.md", "functions.md", "group-ids.md", "parameters.md"):
        assert skill.joinpath("references", ref).is_file()


@pytest.mark.parametrize(
    "app,parts",
    [
        ("claude-code", (".claude", "skills")),
        ("codex", (".agents", "skills")),
        ("vscode", (".copilot", "skills")),
        ("cursor", (".cursor", "skills")),
    ],
)
def test_user_install_goes_where_each_app_looks(home, app, parts):
    path, replaced = skill_install.install(app)
    assert path == home.joinpath(*parts, "dataquery")
    assert not replaced
    assert (path / "SKILL.md").is_file()
    assert (path / "references" / "functions.md").is_file()
    assert (path / skill_install.MARKER_FILE).is_file()


@pytest.mark.parametrize(
    "app,parts",
    [
        ("claude-code", (".claude", "skills")),
        ("codex", (".agents", "skills")),
        ("vscode", (".github", "skills")),
        ("cursor", (".cursor", "skills")),
    ],
)
def test_project_install(tmp_path, app, parts):
    path, _ = skill_install.install(app, "project", tmp_path)
    assert path == tmp_path.joinpath(*parts, "dataquery")
    assert (path / "SKILL.md").is_file()


def test_reinstall_replaces_our_copy_and_drops_stale_files(home):
    path, _ = skill_install.install("codex")
    (path / "stale.md").write_text("old")
    path, replaced = skill_install.install("codex")
    assert replaced
    assert not (path / "stale.md").exists()
    assert not [p for p in path.parent.iterdir() if p.name.startswith(".dataquery-")]  # no staging left


def test_an_unmarked_older_copy_of_this_skill_counts_as_ours(home):
    dest = skill_install.target_dir("cursor", "user")
    dest.mkdir(parents=True)
    (dest / "SKILL.md").write_text("---\nname: dataquery\ndescription: old\n---\nbody\n")
    _, replaced = skill_install.install("cursor")
    assert replaced


def test_a_foreign_folder_is_not_replaced_without_force(home):
    dest = skill_install.target_dir("claude-code", "user")
    dest.mkdir(parents=True)
    (dest / "SKILL.md").write_text("---\nname: something-else\n---\n")
    with pytest.raises(ConfigurationError, match="not a copy of this skill"):
        skill_install.install("claude-code")
    assert "something-else" in (dest / "SKILL.md").read_text()

    skill_install.install("claude-code", force=True)
    assert "name: dataquery" in (dest / "SKILL.md").read_text()


def test_a_symlinked_skill_is_left_alone_without_force(home, tmp_path):
    working_copy = tmp_path / "checkout"
    working_copy.mkdir()
    (working_copy / "SKILL.md").write_text("---\nname: dataquery\n---\n")
    dest = skill_install.target_dir("claude-code", "user")
    dest.parent.mkdir(parents=True)
    dest.symlink_to(working_copy)

    with pytest.raises(ConfigurationError, match="symlink"):
        skill_install.install("claude-code")
    assert dest.is_symlink()
    assert (working_copy / "SKILL.md").exists()


def test_uninstall(home):
    assert skill_install.uninstall("vscode") is None
    path, _ = skill_install.install("vscode")
    assert skill_install.uninstall("vscode") == path
    assert not path.exists()

    path.mkdir(parents=True)
    (path / "notes.txt").write_text("mine")
    with pytest.raises(ConfigurationError):
        skill_install.uninstall("vscode")
    assert (path / "notes.txt").exists()


def _run(*argv):
    args = cli.create_parser().parse_args(["skill-install", *argv])
    return cli.cmd_skill_install(args)


def test_cli_installs_into_every_app(home, capsys):
    assert _run("--app", "all") == 0
    out = capsys.readouterr().out
    for app in skill_install.SKILL_APPS:
        assert skill_install.target_dir(app, "user").joinpath("SKILL.md").is_file()
    assert "Codex: installed" in out and "Cursor: installed" in out


def test_cli_project_scope(tmp_path, home, capsys):
    assert _run("--app", "vscode", "cursor", "--scope", "project", "--project-dir", str(tmp_path)) == 0
    assert (tmp_path / ".github" / "skills" / "dataquery" / "SKILL.md").is_file()
    assert (tmp_path / ".cursor" / "skills" / "dataquery" / "SKILL.md").is_file()


def test_cli_rejects_project_dir_without_project_scope(tmp_path, capsys):
    assert _run("--project-dir", str(tmp_path)) == 1
    assert "--project-dir needs --scope project" in capsys.readouterr().err


def test_cli_one_failure_does_not_stop_the_other_apps(home, capsys):
    blocked = skill_install.target_dir("codex", "user")
    blocked.mkdir(parents=True)
    (blocked / "README").write_text("someone else's")

    assert _run("--app", "codex", "cursor") == 1
    captured = capsys.readouterr()
    assert "Codex:" in captured.err and "--force" in captured.err
    assert skill_install.target_dir("cursor", "user").joinpath("SKILL.md").is_file()


def test_cli_uninstall(home, capsys):
    _run("--app", "claude-code")
    assert _run("--app", "claude-code", "--uninstall") == 0
    assert "Claude Code: removed" in capsys.readouterr().out
    assert not skill_install.target_dir("claude-code", "user").exists()
