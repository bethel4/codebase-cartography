from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path

import networkx as nx

from analyzers.dag_config_parser import DAGConfigAnalyzer
from analyzers.dbt_manifest_lineage import DbtManifestLineageAnalyzer
from analyzers.sql_lineage import SQLDependency, SQLLineageAnalyzer
from analyzers.tree_sitter_analyzer import PythonDataFlowAnalyzer
from graph.lineage_graph import DataLineageGraph
from models import DataNode, LineageGraphSummary

LOGGER = logging.getLogger(__name__)


def _ensure_node(nodes: dict[str, DataNode], graph: DataLineageGraph, name: str, kind: str, source_file: str) -> DataNode:
    node = nodes.get(name)
    if node is None:
        node = DataNode(name=name, kind=kind, source_files=[source_file])
    else:
        if source_file not in node.source_files:
            node.source_files.append(source_file)
    nodes[name] = node
    graph.add_node(node)
    return node


def _apply_sql_dependencies(
    dependencies: list[SQLDependency],
    nodes: dict[str, DataNode],
    graph: DataLineageGraph,
    seen: set[tuple[str, str, str]],
    kind: str,
    transformation_type: str,
) -> None:
    for dependency in dependencies:
        target_node = _ensure_node(nodes, graph, dependency.target, kind="table", source_file=str(dependency.source_file))
        for source in dependency.sources:
            node_key = (source, target_node.name, str(dependency.source_file))
            if node_key in seen:
                continue
            _ensure_node(nodes, graph, source, kind=kind, source_file=str(dependency.source_file))
            graph.add_edge(source, target_node.name, source_file=str(dependency.source_file), transformation_type=transformation_type)
            seen.add(node_key)


def _maybe_run_dbt_compile(repo_path: Path, *, profiles_dir: Path | None = None) -> bool:
    """
    Best-effort `dbt compile` for dbt projects.

    This is intentionally non-fatal: if dbt isn't installed or compilation fails,
    we keep going with heuristic SQL parsing.
    """
    if not (repo_path / "dbt_project.yml").exists():
        return False

    env = os.environ.copy()
    if profiles_dir and profiles_dir.exists():
        env.setdefault("DBT_PROFILES_DIR", str(profiles_dir))

    profile_name = None
    try:
        project_file = repo_path / "dbt_project.yml"
        for raw_line in project_file.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw_line.split("#", 1)[0].strip()
            if not line:
                continue
            if line.startswith("profile:"):
                profile_name = line.split(":", 1)[1].strip().strip('"').strip("'") or None
                break
    except OSError:
        profile_name = None

    def _run(cmd: list[str]) -> subprocess.CompletedProcess[str] | None:
        try:
            return subprocess.run(cmd, cwd=str(repo_path), capture_output=True, text=True, env=env)
        except FileNotFoundError:
            LOGGER.info("dbt not installed; skipping dbt-based lineage enrichment.")
            return None

    # If this repo has packages configured, try to resolve them first so `compile`
    # can succeed (common for dbt_utils, etc.).
    if (repo_path / "packages.yml").exists() and not (repo_path / "dbt_packages").exists():
        deps_cmd = ["dbt", "--no-use-colors"]
        if profile_name:
            deps_cmd += ["--profile", profile_name]
        deps_cmd += ["deps"]
        res = _run(deps_cmd)
        if res is None:
            return False
        if res.returncode != 0:
            LOGGER.info("dbt deps failed (exit=%s). stderr:\n%s", res.returncode, res.stderr[-2000:])
            return False

    cmd = ["dbt", "--no-use-colors"]
    if profile_name:
        cmd += ["--profile", profile_name]
    cmd += ["compile"]
    try:
        res = subprocess.run(cmd, cwd=str(repo_path), capture_output=True, text=True, env=env)
    except Exception as exc:  # pragma: no cover
        LOGGER.info("dbt compile failed unexpectedly: %s", exc)
        return False

    if res.returncode != 0:
        LOGGER.info("dbt compile failed (exit=%s). stderr:\n%s", res.returncode, res.stderr[-2000:])
        return False
    return True


def build_lineage_graph(
    repo_path: str | Path = "target_repo",
    *,
    dbt_compile: bool = True,
    dbt_profiles_dir: str | Path | None = None,
) -> tuple[list[DataNode], DataLineageGraph, LineageGraphSummary]:
    repo_path = Path(repo_path)
    graph = DataLineageGraph()
    nodes: dict[str, DataNode] = {}

    sql_analyzer = SQLLineageAnalyzer()
    dbt_manifest_analyzer = DbtManifestLineageAnalyzer()
    python_analyzer = PythonDataFlowAnalyzer()
    dag_analyzer = DAGConfigAnalyzer()
    seen_edges: set[tuple[str, str, str]] = set()

    # ---- dbt manifest / compilation (optional) ----
    manifest_path = repo_path / "target" / "manifest.json"
    compiled_root = repo_path / "target" / "compiled"
    if dbt_compile and (repo_path / "dbt_project.yml").exists() and (not manifest_path.exists() or not compiled_root.exists()):
        profiles_dir = Path(dbt_profiles_dir) if dbt_profiles_dir else (Path(__file__).resolve().parents[2] / "dbt")
        compiled = _maybe_run_dbt_compile(repo_path, profiles_dir=profiles_dir)
        # Optional: trace logging (do not break analysis if it fails).
        try:
            from datetime import datetime, timezone

            from cartography_trace import log_cartography_trace

            log_cartography_trace(
                {
                    "agent": "Hydrologist",
                    "action": "dbt_compile",
                    "evidence_source": str(repo_path / "dbt_project.yml"),
                    "line_range": None,
                    "method": "static_analysis",
                    "confidence": 1.0 if compiled else 0.6,
                    "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                }
            )
        except Exception:
            pass

    manifest_dependencies = dbt_manifest_analyzer.analyze_manifest(manifest_path)
    # Ensure all dbt models/sources appear as nodes even if they have no edges.
    for dataset_id, resource_type in dbt_manifest_analyzer.list_datasets(manifest_path):
        node = _ensure_node(nodes, graph, dataset_id, kind="table", source_file=str(manifest_path))
        graph.graph.nodes[node.name]["dbt_resource_type"] = resource_type

    if manifest_dependencies:
        _apply_sql_dependencies(
            [
                SQLDependency(target=d.target, sources=d.sources, source_file=d.source_file)
                for d in manifest_dependencies
            ],
            nodes,
            graph,
            seen_edges,
            kind="table",
            transformation_type="dbt_manifest",
        )

    # Keep compiled-SQL parsing as an optional enrichment: it can add non-manifest
    # edges (e.g., ad-hoc SQL in hooks). When a manifest exists, the manifest is
    # the authoritative model dependency graph.
    if compiled_root.exists() and not manifest_dependencies:
        compiled_dependencies = sql_analyzer.analyze_directory(compiled_root)
        _apply_sql_dependencies(
            compiled_dependencies,
            nodes,
            graph,
            seen_edges,
            kind="table",
            transformation_type="dbt_compiled",
        )

    for path in repo_path.rglob("*.sql"):
        # Skip vendor/dbt dependencies to keep lineage focused and avoid noisy parsing.
        if "dbt_packages" in path.parts:
            continue
        try:
            path.relative_to(compiled_root)
            continue
        except ValueError:
            pass
        # Skip dbt build artifacts; manifest/compiled outputs are handled separately.
        if "target" in path.parts:
            continue
        for dependency in sql_analyzer.analyze_file(path):
            _apply_sql_dependencies(
                [dependency],
                nodes,
                graph,
                seen_edges,
                kind="table",
                transformation_type="sql",
            )

    for path in repo_path.rglob("*.py"):
        script_node = _ensure_node(nodes, graph, f"python:{path}", kind="script", source_file=str(path))
        for access in python_analyzer.analyze_file(path):
            for dataset in access.datasets:
                if dataset == PythonDataFlowAnalyzer.DYNAMIC_REFERENCE:
                    continue
                dataset_node = _ensure_node(nodes, graph, dataset, kind="dataset", source_file=access.source_file)
                if access.direction == "read":
                    graph.add_edge(
                        dataset_node.name,
                        script_node.name,
                        source_file=access.source_file,
                        transformation_type="python_read",
                    )
                else:
                    graph.add_edge(
                        script_node.name,
                        dataset_node.name,
                        source_file=access.source_file,
                        transformation_type="python_write",
                    )

    for edge in dag_analyzer.analyze_repo(repo_path):
        _ensure_node(nodes, graph, edge.source, kind="dag_task", source_file=edge.source_file)
        _ensure_node(nodes, graph, edge.target, kind="dag_task", source_file=edge.source_file)
        graph.add_edge(edge.source, edge.target, source_file=edge.source_file, transformation_type=edge.transformation_type)

    # Cycle detection (best-effort): lineage ideally forms a DAG, but real-world graphs
    # can contain cycles due to feedback loops or modeling choices.
    try:
        cycles = list(nx.simple_cycles(graph.graph))
    except Exception as exc:  # pragma: no cover
        LOGGER.warning("Cycle detection failed for lineage graph: %s", exc)
        cycles = []

    graph.graph.graph["data_cycles"] = cycles
    if cycles:
        LOGGER.warning("Detected %s data lineage cycle(s)", len(cycles))
        cycle_nodes = {node for cycle in cycles for node in cycle}
        for node_id in graph.graph.nodes:
            graph.graph.nodes[node_id]["in_cycle"] = node_id in cycle_nodes

    summary = LineageGraphSummary(
        node_count=graph.graph.number_of_nodes(),
        edge_count=graph.graph.number_of_edges(),
    )

    # Flag orphan/unconnected datasets (useful for spotting "dead" models).
    try:
        for node_id in graph.graph.nodes:
            indeg = graph.graph.in_degree(node_id)
            outdeg = graph.graph.out_degree(node_id)
            graph.graph.nodes[node_id]["in_degree"] = int(indeg)
            graph.graph.nodes[node_id]["out_degree"] = int(outdeg)
            graph.graph.nodes[node_id]["is_orphan"] = bool(indeg == 0 and outdeg == 0)
            graph.graph.nodes[node_id]["is_source"] = bool(indeg == 0)
            graph.graph.nodes[node_id]["is_sink"] = bool(outdeg == 0)
    except Exception:  # pragma: no cover
        pass

    return list(nodes.values()), graph, summary
