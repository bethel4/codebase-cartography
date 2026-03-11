import os
import subprocess
from pathlib import Path

import networkx as nx
import pytest

from agents import surveyor
from models import ModuleGraphSummary, ModuleNode


@pytest.fixture
def fake_repo(tmp_path):
    """Create a tiny repo with two Python files and a git history."""
    repo = tmp_path / "repo"
    repo.mkdir()
    pkg = repo / "package"
    pkg.mkdir()

    file_a = pkg / "a.py"
    file_a.write_text("import package.b\n")

    file_b = pkg / "b.py"
    file_b.write_text("print('hello')\n")

    subprocess.run(["git", "-C", str(repo), "init"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "test@example.com"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "tester"], check=True)
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-m", "initial"], check=True)
    return repo


def test_build_module_graph(monkeypatch, tmp_path, fake_repo):
    """Surveyor should create nodes for each .py file and a matching edge."""
    parsed_cache: dict[str, surveyor.ParsedPythonFile] = {}

    def fake_parse(path, language):
        """Return deterministic data for each file."""
        name = Path(path).name
        if name == "a.py":
            return surveyor.ParsedPythonFile(
                imports=["package.b"],
                functions=["foo"],
                classes=["A"],
                complexity_score=1,
            )
        return surveyor.ParsedPythonFile(
            imports=[],
            functions=["bar"],
            classes=["B"],
            complexity_score=0,
        )

    monkeypatch.setattr(surveyor, "extract_imports_and_defs", fake_parse)

    nodes, graph, summary = surveyor.build_module_graph(fake_repo)

    assert isinstance(graph, nx.DiGraph)
    assert len(nodes) == 2
    assert any(node.path.endswith("a.py") for node in nodes)
    assert summary.module_count == 2
    assert summary.edge_count == 1
    assert graph.has_edge(str(fake_repo / "package" / "a.py"), str(fake_repo / "package" / "b.py"))


def test_extract_git_velocity_success(monkeypatch):
    """extract_git_velocity should count commits returned by git."""
    class Result:
        returncode = 0

        def __init__(self):
            self.stdout = "aaa\nbbb\n"

    monkeypatch.setattr(surveyor, "_run_git", lambda repo, args: Result())

    repo = Path(".")
    count = surveyor.extract_git_velocity(repo, repo / "foo.py", days=10)
    assert count == 2


def test_extract_git_velocity_failure(monkeypatch):
    """Failures should gracefully return zero."""
    def fail_run(repo, args):
        raise RuntimeError("git fail")

    monkeypatch.setattr(surveyor, "_run_git", fail_run)
    count = surveyor.extract_git_velocity(Path("."), Path("foo.py"))
    assert count == 0
