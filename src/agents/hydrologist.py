from __future__ import annotations

from pathlib import Path

from analyzers.dag_config_parser import DAGConfigAnalyzer
from analyzers.sql_lineage import SQLDependency, SQLLineageAnalyzer
from analyzers.tree_sitter_analyzer import PythonDataFlowAnalyzer
from graph.lineage_graph import DataLineageGraph
from models import DataNode, LineageGraphSummary


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


def build_lineage_graph(repo_path: str | Path = "target_repo") -> tuple[list[DataNode], DataLineageGraph, LineageGraphSummary]:
    repo_path = Path(repo_path)
    graph = DataLineageGraph()
    nodes: dict[str, DataNode] = {}

    sql_analyzer = SQLLineageAnalyzer()
    python_analyzer = PythonDataFlowAnalyzer()
    dag_analyzer = DAGConfigAnalyzer()
    seen_edges: set[tuple[str, str, str]] = set()

    compiled_root = repo_path / "target" / "compiled"
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
        try:
            path.relative_to(compiled_root)
            continue
        except ValueError:
            pass
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

    summary = LineageGraphSummary(
        node_count=graph.graph.number_of_nodes(),
        edge_count=graph.graph.number_of_edges(),
    )

    return list(nodes.values()), graph, summary
