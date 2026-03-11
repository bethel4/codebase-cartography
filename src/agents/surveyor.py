from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import networkx as nx
from tree_sitter import Language

from analyzers.tree_sitter_analyzer import LanguageRouter, make_parser
from models import ModuleGraphSummary, ModuleNode


@dataclass
class ParsedPythonFile:
    imports: list[str]
    functions: list[str]
    classes: list[str]
    complexity_score: int


PY_COMPLEXITY_NODES = {
    "if_statement",
    "for_statement",
    "while_statement",
    "try_statement",
    "with_statement",
    "except_clause",
    "match_statement",
}


def _node_text(code: bytes, node) -> str:
    return code[node.start_byte : node.end_byte].decode("utf-8", errors="replace")


def _collect_dotted_name(node, code: bytes) -> str | None:
    if node is None:
        return None
    if node.type == "dotted_name":
        return _node_text(code, node)
    for child in node.children:
        if child.type == "dotted_name":
            return _node_text(code, child)
    return None


def _parse_python(code: str, language: Language) -> ParsedPythonFile:
    parser = make_parser(language)
    code_bytes = code.encode("utf-8")
    tree = parser.parse(code_bytes)
    root = tree.root_node

    imports: list[str] = []
    functions: list[str] = []
    classes: list[str] = []
    complexity_score = 0

    stack = [root]
    while stack:
        node = stack.pop()
        if node.type in PY_COMPLEXITY_NODES:
            complexity_score += 1

        if node.type == "import_statement":
            for child in node.children:
                if child.type == "dotted_name":
                    imports.append(_node_text(code_bytes, child))
                elif child.type == "aliased_import":
                    dotted = _collect_dotted_name(child, code_bytes)
                    if dotted:
                        imports.append(dotted)

        elif node.type == "import_from_statement":
            module = None
            for child in node.children:
                if child.type in {"dotted_name", "relative_import"}:
                    module = _node_text(code_bytes, child)
                    break
            if module:
                for child in node.children:
                    if child.type in {"dotted_name", "aliased_import"}:
                        name = _collect_dotted_name(child, code_bytes)
                        if name:
                            imports.append(f"{module}.{name}")

        elif node.type == "function_definition":
            name_node = next((c for c in node.children if c.type == "identifier"), None)
            if name_node:
                functions.append(_node_text(code_bytes, name_node))

        elif node.type == "class_definition":
            name_node = next((c for c in node.children if c.type == "identifier"), None)
            if name_node:
                classes.append(_node_text(code_bytes, name_node))

        stack.extend(reversed(node.children))

    return ParsedPythonFile(
        imports=sorted(set(imports)),
        functions=sorted(set(functions)),
        classes=sorted(set(classes)),
        complexity_score=complexity_score,
    )


def extract_imports_and_defs(file_path: str | Path, language: Language) -> ParsedPythonFile:
    """Parse a Python file and extract imports, functions, and classes."""
    file_path = Path(file_path)
    code = file_path.read_text(encoding="utf-8", errors="replace")
    return _parse_python(code, language)


def _run_git(repo_path: Path, args: Iterable[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(repo_path), *args],
        capture_output=True,
        text=True,
        check=True,
    )


def extract_git_velocity(repo_path: Path, file_path: Path, days: int = 30) -> int:
    """Count commits touching a file within the last N days."""
    try:
        rel_path = os.path.relpath(file_path, repo_path)
        result = _run_git(
            repo_path,
            ["log", f"--since={days} days ago", "--pretty=format:%h", "--", rel_path],
        )
        commits = [line for line in result.stdout.splitlines() if line.strip()]
        return len(commits)
    except Exception:
        return 0


def extract_last_modified(repo_path: Path, file_path: Path) -> str:
    """Return ISO timestamp of the last git commit or filesystem mtime."""
    try:
        rel_path = os.path.relpath(file_path, repo_path)
        result = _run_git(repo_path, ["log", "-1", "--format=%cI", "--", rel_path])
        value = result.stdout.strip()
        if value:
            return value
    except Exception:
        pass

    timestamp = file_path.stat().st_mtime
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def _resolve_import(import_name: str, file_path: Path, repo_path: Path) -> str:
    """Resolve an import name to a repo file path when possible."""
    if import_name.startswith("."):
        parts = import_name.split(".")
        dots = 0
        for part in parts:
            if part == "":
                dots += 1
            else:
                break
        remainder = ".".join(parts[dots:])
        base = file_path.parent
        for _ in range(max(dots - 1, 0)):
            base = base.parent
        if remainder:
            candidate = base / Path(remainder.replace(".", "/"))
        else:
            candidate = base
    else:
        candidate = repo_path / Path(import_name.replace(".", "/"))

    py_path = candidate.with_suffix(".py")
    if py_path.exists():
        return str(py_path)
    init_path = candidate / "__init__.py"
    if init_path.exists():
        return str(init_path)
    return import_name


def build_module_graph(repo_path: str | Path = "target_repo") -> tuple[list[ModuleNode], nx.DiGraph, ModuleGraphSummary]:
    """Analyze Python modules in a repository and build a dependency graph."""
    repo_path = Path(repo_path)
    router = LanguageRouter(languages=["python"])
    python_language = router.language_for_path("file.py")
    if python_language is None:
        raise RuntimeError("Python language not available in tree-sitter library.")

    graph = nx.DiGraph()
    modules: list[ModuleNode] = []

    for root, _, files in os.walk(repo_path):
        for filename in files:
            path = Path(root) / filename
            if path.suffix != ".py":
                continue

            parsed = extract_imports_and_defs(path, python_language)
            velocity = extract_git_velocity(repo_path, path)
            last_modified = extract_last_modified(repo_path, path)

            node = ModuleNode(
                path=str(path),
                imports=parsed.imports,
                functions=parsed.functions,
                classes=parsed.classes,
                complexity_score=parsed.complexity_score,
                change_velocity_30d=velocity,
                last_modified=last_modified,
                is_dead_code_candidate=(velocity == 0 and parsed.complexity_score == 0),
                language="python",
            )
            modules.append(node)

            graph.add_node(str(path), **node.model_dump())
            for imp in parsed.imports:
                resolved = _resolve_import(imp, path, repo_path)
                graph.add_edge(str(path), resolved)

    if graph.number_of_nodes() > 0:
        pagerank = nx.pagerank(graph)
        for node_id, score in pagerank.items():
            graph.nodes[node_id]["pagerank"] = float(score)

    cycles = list(nx.simple_cycles(graph))
    cycle_nodes = {node for cycle in cycles for node in cycle}
    for node_id in graph.nodes:
        graph.nodes[node_id]["in_cycle"] = node_id in cycle_nodes

    summary = ModuleGraphSummary(
        module_count=len(modules),
        edge_count=graph.number_of_edges(),
        cycle_count=len(cycles),
    )

    return modules, graph, summary
