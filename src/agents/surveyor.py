from __future__ import annotations

import ast
import json
import logging
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

LOGGER = logging.getLogger(__name__)


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


def _collect_exports_and_uses(repo_path: Path) -> tuple[dict[str, list[str]], dict[str, set[str]]]:
    """
    Collect exported symbols and referenced symbol "uses" per file.

    This is intentionally best-effort: it relies on Python's `ast` module and does not
    attempt to resolve dynamic references (e.g., importlib, getattr, plugin registries).
    """

    exports_by_file: dict[str, list[str]] = {}
    uses_by_file: dict[str, set[str]] = {}

    for path in repo_path.rglob("*.py"):
        try:
            code = path.read_text(encoding="utf-8", errors="replace")
            tree = ast.parse(code)
        except (OSError, UnicodeDecodeError, SyntaxError):
            continue

        exports: list[str] = []
        dunder_all: list[str] | None = None

        for node in tree.body:
            if (
                isinstance(node, ast.Assign)
                and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id == "__all__"
            ):
                if isinstance(node.value, (ast.List, ast.Tuple)):
                    items: list[str] = []
                    for elt in node.value.elts:
                        if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                            items.append(elt.value)
                    if items:
                        dunder_all = items
                continue

        if dunder_all is not None:
            exports = list(dunder_all)
        else:
            for node in tree.body:
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    if not node.name.startswith("_"):
                        exports.append(node.name)

        # Collect symbol "uses" within the file. Note: `ast` does not represent
        # `def foo(...):` / `class Bar:` names as `ast.Name` nodes, so counting all
        # `ast.Name` occurrences does not incorrectly treat definition sites as uses.
        uses: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    # "from x import y" references y; "from x import y as z" still references y.
                    uses.add(alias.name.split(".")[0])
            elif isinstance(node, ast.Attribute):
                uses.add(node.attr)
            elif isinstance(node, ast.Name):
                uses.add(node.id)

        exports_by_file[str(path)] = sorted(set(exports))
        uses_by_file[str(path)] = uses

    return exports_by_file, uses_by_file

def detect_dead_exports(repo_path: Path) -> dict[str, list[str]]:
    """
    Detect exported Python symbols that appear unused within the repository.

    Returns only exports that are not referenced anywhere (including within their own file).
    Treat as "dead code candidates" to triage, not proof it's safe to delete.
    """
    exports_by_file, uses_by_file = _collect_exports_and_uses(repo_path)
    global_uses: set[str] = set()
    for uses in uses_by_file.values():
        global_uses |= uses

    dead_by_file: dict[str, list[str]] = {}
    for file_path, exports in exports_by_file.items():
        dead_by_file[file_path] = [symbol for symbol in exports if symbol not in global_uses]

    return dead_by_file


def _classify_export_context(file_path: str, code: str) -> str:
    """
    Classify a file into a context bucket for unused-export triage.

    Buckets:
    - "entrypoint": CLI / executable modules where exports are invoked via framework discovery
    - "framework": framework-discovered modules (Dagster-like definitions/sensors/resources/ops)
    - "library": regular modules where unused exports are more likely real
    """
    normalized = file_path.replace("\\", "/")

    # CLI scripts: most commonly located under bin/, but also detectable via __main__ guards.
    if "/bin/" in normalized or normalized.endswith("/bin") or 'if __name__ == "__main__"' in code:
        return "entrypoint"

    # Framework-discovered code: common patterns for Dagster-like repos.
    if normalized.endswith("definitions.py") or any(
        part in normalized for part in ("/sensors/", "/resources/", "/ops/")
    ):
        return "framework"

    if "import dagster" in code or "from dagster" in code:
        return "framework"

    return "library"


def detect_unused_exports(repo_path: Path) -> tuple[dict[str, list[str]], dict[str, list[str]], dict[str, list[str]]]:
    """
    Detect unused exports and classify them to reduce false positives.

    Returns (dead_exports, entrypoint_exports, framework_exports).
    """
    exports_by_file, uses_by_file = _collect_exports_and_uses(repo_path)
    global_uses: set[str] = set()
    for uses in uses_by_file.values():
        global_uses |= uses

    dead_by_file: dict[str, list[str]] = {}
    entrypoint_by_file: dict[str, list[str]] = {}
    framework_by_file: dict[str, list[str]] = {}

    for path, exports in exports_by_file.items():
        entrypoint_by_file[path] = []
        framework_by_file[path] = []

        try:
            code = Path(path).read_text(encoding="utf-8", errors="replace")
        except OSError:
            dead_by_file[path] = []
            continue

        bucket = _classify_export_context(path, code)
        if bucket == "entrypoint":
            entrypoint_by_file[path] = exports
            dead_by_file[path] = []
        elif bucket == "framework":
            framework_by_file[path] = exports
            dead_by_file[path] = []
        else:
            dead_by_file[path] = [symbol for symbol in exports if symbol not in global_uses]

    return dead_by_file, entrypoint_by_file, framework_by_file


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
    """
    Analyze modules in a repository and build a dependency graph.

    Coverage:
    - Python: imports + defs via tree-sitter
    - dbt (best-effort): if `target/manifest.json` exists, add SQL/YAML files as nodes and
      connect them using dbt's dependency graph (`depends_on.nodes`).

    Note: For dbt, we model *file-level* dependencies (model SQL depends on upstream model SQL / source YAML).
    """
    repo_path = Path(repo_path)
    router = LanguageRouter(languages=["python"])
    python_language = router.language_for_path("file.py")
    if python_language is None:
        raise RuntimeError("Python language not available in tree-sitter library.")

    graph = nx.DiGraph()
    modules: list[ModuleNode] = []
    dead_exports, entrypoint_exports, framework_exports = detect_unused_exports(repo_path)

    def _add_generic_file_node(path: Path, *, language: str) -> None:
        velocity = extract_git_velocity(repo_path, path)
        last_modified = extract_last_modified(repo_path, path)
        node = ModuleNode(
            path=str(path),
            imports=[],
            functions=[],
            classes=[],
            dead_exports=[],
            entrypoint_exports=[],
            framework_exports=[],
            complexity_score=0,
            change_velocity_30d=velocity,
            last_modified=last_modified,
            is_dead_code_candidate=(velocity == 0),
            language=language,
        )
        modules.append(node)
        graph.add_node(str(path), **node.model_dump())

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
                dead_exports=dead_exports.get(str(path), []),
                entrypoint_exports=entrypoint_exports.get(str(path), []),
                framework_exports=framework_exports.get(str(path), []),
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

    # dbt: add SQL/YAML file nodes + edges using manifest.json (if present).
    manifest_path = repo_path / "target" / "manifest.json"
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest_nodes: dict[str, dict] = {}
            manifest_nodes.update(manifest.get("nodes") or {})
            manifest_nodes.update(manifest.get("sources") or {})

            def _file_for(unique_id: str) -> Path | None:
                node = manifest_nodes.get(unique_id) or {}
                rel = node.get("original_file_path")
                if not isinstance(rel, str) or not rel:
                    return None
                return repo_path / rel

            # Add nodes for models and sources.
            for unique_id, node in manifest_nodes.items():
                rtype = node.get("resource_type")
                if rtype == "model":
                    f = _file_for(unique_id)
                    if f and f.exists() and f.suffix == ".sql" and str(f) not in graph:
                        _add_generic_file_node(f, language="sql")
                elif rtype == "source":
                    f = _file_for(unique_id)
                    if f and f.exists() and f.suffix in {".yml", ".yaml"} and str(f) not in graph:
                        _add_generic_file_node(f, language="yaml")

            # Add edges: model file -> upstream file (model/source).
            for unique_id, node in manifest_nodes.items():
                if node.get("resource_type") != "model":
                    continue
                model_file = _file_for(unique_id)
                if not model_file or not model_file.exists():
                    continue
                model_id = str(model_file)
                if model_id not in graph:
                    _add_generic_file_node(model_file, language="sql")
                depends_on = node.get("depends_on") or {}
                upstream_ids = depends_on.get("nodes") or []
                if not isinstance(upstream_ids, list):
                    continue
                for upstream_uid in upstream_ids:
                    if not isinstance(upstream_uid, str):
                        continue
                    upstream_file = _file_for(upstream_uid)
                    if not upstream_file or not upstream_file.exists():
                        continue
                    upstream_id = str(upstream_file)
                    if upstream_id not in graph:
                        _add_generic_file_node(
                            upstream_file,
                            language=("yaml" if upstream_file.suffix in {".yml", ".yaml"} else "sql"),
                        )
                    graph.add_edge(model_id, upstream_id)
        except Exception:
            pass

    # Mark repo modules that are not imported by any other repo module (all languages).
    try:
        for node_id in list(graph.nodes):
            attrs = graph.nodes[node_id]
            if not isinstance(node_id, str):
                continue
            if not node_id.startswith(str(repo_path)):
                continue
            if not any(node_id.endswith(ext) for ext in (".py", ".sql", ".yml", ".yaml")):
                continue
            indeg = graph.in_degree(node_id)
            outdeg = graph.out_degree(node_id)
            attrs["in_degree"] = int(indeg)
            attrs["out_degree"] = int(outdeg)
            attrs["unimported"] = bool(indeg == 0)
            attrs["is_orphan"] = bool(indeg == 0 and outdeg == 0)
    except Exception:  # pragma: no cover
        pass

    if graph.number_of_nodes() > 0:
        pagerank = nx.pagerank(graph)
        for node_id, score in pagerank.items():
            graph.nodes[node_id]["pagerank"] = float(score)

    cycles = list(nx.simple_cycles(graph))
    graph.graph["cycles"] = cycles
    if cycles:
        LOGGER.warning("Detected %s module import cycle(s)", len(cycles))
    cycle_nodes = {node for cycle in cycles for node in cycle}
    for node_id in graph.nodes:
        graph.nodes[node_id]["in_cycle"] = node_id in cycle_nodes

    summary = ModuleGraphSummary(
        module_count=len(modules),
        edge_count=graph.number_of_edges(),
        cycle_count=len(cycles),
    )

    return modules, graph, summary
