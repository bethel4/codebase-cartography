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

    # Cycle metadata should always exist (empty for this graph)
    assert "cycles" in graph.graph
    assert graph.graph["cycles"] == []


def test_build_module_graph_records_cycles(monkeypatch, fake_repo):
    """Surveyor should record cycles in graph metadata without failing."""
    def fake_parse(path, language):
        name = Path(path).name
        if name == "a.py":
            return surveyor.ParsedPythonFile(imports=["package.b"], functions=[], classes=[], complexity_score=0)
        if name == "b.py":
            return surveyor.ParsedPythonFile(imports=["package.a"], functions=[], classes=[], complexity_score=0)
        return surveyor.ParsedPythonFile(imports=[], functions=[], classes=[], complexity_score=0)

    monkeypatch.setattr(surveyor, "extract_imports_and_defs", fake_parse)

    _, graph, summary = surveyor.build_module_graph(fake_repo)
    a_path = str(fake_repo / "package" / "a.py")
    b_path = str(fake_repo / "package" / "b.py")

    assert summary.cycle_count >= 1
    assert any(set(cycle) == {a_path, b_path} for cycle in graph.graph.get("cycles", []))
    assert graph.nodes[a_path]["in_cycle"] is True
    assert graph.nodes[b_path]["in_cycle"] is True


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


def test_detect_dead_exports(tmp_path):
    """detect_dead_exports should flag public exports unused elsewhere."""
    repo = tmp_path / "repo"
    repo.mkdir()

    (repo / "a.py").write_text(
        "\n".join(
            [
                "def used():",
                "    return 1",
                "",
                "def unused():",
                "    return 2",
                "",
                "class AlsoUnused:",
                "    pass",
            ]
        )
        + "\n"
    )

    (repo / "b.py").write_text("from a import used\nprint(used())\n")

    dead = surveyor.detect_dead_exports(repo)
    assert dead[str(repo / "a.py")] == ["AlsoUnused", "unused"]
    assert dead[str(repo / "b.py")] == []


def test_detect_unused_exports_classifies_entrypoints(tmp_path):
    """detect_unused_exports should move CLI-like files out of dead_exports."""
    repo = tmp_path / "repo"
    (repo / "bin").mkdir(parents=True)

    script = repo / "bin" / "tool.py"
    script.write_text(
        "\n".join(
            [
                "def main():",
                "    return 123",
                "",
                "if __name__ == \"__main__\":",
                "    raise SystemExit(main())",
            ]
        )
        + "\n"
    )

    dead, entrypoints, frameworks = surveyor.detect_unused_exports(repo)
    assert dead[str(script)] == []
    assert entrypoints[str(script)] == ["main"]
    assert frameworks[str(script)] == []


def test_detect_dead_exports_does_not_flag_internal_helpers(tmp_path):
    """Internal helpers referenced in the same file should not be flagged as dead."""
    repo = tmp_path / "repo"
    repo.mkdir()

    mod = repo / "m.py"
    mod.write_text(
        "\n".join(
            [
                "def helper():",
                "    return 1",
                "",
                "def public():",
                "    return helper()",
            ]
        )
        + "\n"
    )

    dead = surveyor.detect_dead_exports(repo)
    # helper() is referenced by public(), so it should not be flagged.
    # public() itself is not referenced anywhere, so it remains a candidate.
    assert dead[str(mod)] == ["public"]
