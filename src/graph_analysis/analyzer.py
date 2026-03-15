from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import networkx as nx


def _safe_pagerank(graph: nx.DiGraph) -> dict[str, float]:
    """
    Compute PageRank best-effort.

    PageRank can fail to converge on some graphs; in that case return an empty mapping.
    """
    if graph.number_of_nodes() == 0:
        return {}
    try:
        pr = nx.pagerank(graph)
    except Exception:
        return {}
    return {str(k): float(v) for k, v in pr.items()}


def _safe_density(graph: nx.DiGraph) -> float:
    try:
        return float(nx.density(graph))
    except Exception:
        return 0.0


def _roots(graph: nx.DiGraph) -> list[str]:
    return sorted([str(n) for n, deg in graph.in_degree() if deg == 0])


def _leaves(graph: nx.DiGraph) -> list[str]:
    return sorted([str(n) for n, deg in graph.out_degree() if deg == 0])


def _cycles(graph: nx.DiGraph, limit: int = 50) -> list[list[str]]:
    cycles: list[list[str]] = []
    try:
        for idx, cycle in enumerate(nx.simple_cycles(graph)):
            if idx >= limit:
                break
            cycles.append([str(x) for x in cycle])
    except Exception:
        return []
    return cycles


def _longest_path(graph: nx.DiGraph) -> list[str]:
    if not nx.is_directed_acyclic_graph(graph):
        return []
    try:
        return [str(x) for x in nx.dag_longest_path(graph)]
    except Exception:
        return []


def _graph_depth(graph: nx.DiGraph) -> int:
    """
    Depth heuristic for DAGs: maximum shortest-path distance from any root.
    Returns 0 if not a DAG or graph is empty.
    """
    if graph.number_of_nodes() == 0 or not nx.is_directed_acyclic_graph(graph):
        return 0
    roots = _roots(graph)
    if not roots:
        return 0
    max_depth = 0
    for r in roots:
        if r not in graph:
            continue
        lengths = nx.single_source_shortest_path_length(graph, r)
        if lengths:
            max_depth = max(max_depth, max(lengths.values()))
    return int(max_depth)


def _centrality(graph: nx.DiGraph) -> tuple[dict[str, float], dict[str, float]]:
    """
    Return (betweenness, degree) centrality maps.

    Betweenness is expensive; for large graphs we compute an approximation using a sample.
    """
    n = graph.number_of_nodes()
    degree = {str(k): float(v) for k, v in nx.degree_centrality(graph).items()} if n else {}

    if n == 0:
        return {}, degree

    if n <= 800:
        between = nx.betweenness_centrality(graph, normalized=True)
    else:
        # Approximate: sample up to 100 nodes.
        k = min(100, n)
        between = nx.betweenness_centrality(graph, k=k, normalized=True, seed=7)
    between = {str(k): float(v) for k, v in between.items()}
    return between, degree


def _top_k(mapping: dict[str, float], k: int = 15) -> list[dict[str, Any]]:
    return [{"id": node, "score": float(score)} for node, score in sorted(mapping.items(), key=lambda x: x[1], reverse=True)[:k]]


def upstream_nodes(graph: nx.DiGraph, node: str) -> list[str]:
    """Immediate upstream dependencies (predecessors)."""
    if node not in graph:
        return []
    return sorted([str(n) for n in graph.predecessors(node)])


def downstream_nodes(graph: nx.DiGraph, node: str) -> list[str]:
    """Immediate downstream dependents (successors)."""
    if node not in graph:
        return []
    return sorted([str(n) for n in graph.successors(node)])


def impact_descendants(graph: nx.DiGraph, node: str) -> list[str]:
    """Transitive downstream impact (descendants)."""
    if node not in graph:
        return []
    return sorted([str(n) for n in nx.descendants(graph, node)])


@dataclass(frozen=True)
class GraphAnalysis:
    graph_stats: dict[str, Any]
    roots: list[str]
    leaves: list[str]
    cycles: list[list[str]]
    pagerank_top: list[dict[str, Any]]
    critical_nodes: list[dict[str, Any]]
    longest_path: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "graph_stats": self.graph_stats,
            "roots": self.roots,
            "leaves": self.leaves,
            "cycles": self.cycles,
            "pagerank_top": self.pagerank_top,
            "critical_nodes": self.critical_nodes,
            "longest_path": self.longest_path,
        }


def analyze_graph(graph: nx.DiGraph, critical_k: int = 15, cycle_limit: int = 50) -> GraphAnalysis:
    pagerank = _safe_pagerank(graph)
    pagerank_top = _top_k(pagerank, k=critical_k) if pagerank else []

    between, degree = _centrality(graph)
    critical = _top_k(between, k=critical_k)
    if not critical:
        critical = _top_k(degree, k=critical_k)

    stats = {
        "number_of_nodes": graph.number_of_nodes(),
        "number_of_edges": graph.number_of_edges(),
        "graph_density": _safe_density(graph),
        "depth": _graph_depth(graph),
    }
    if nx.is_directed_acyclic_graph(graph):
        stats["is_dag"] = True
    else:
        stats["is_dag"] = False

    return GraphAnalysis(
        graph_stats=stats,
        roots=_roots(graph),
        leaves=_leaves(graph),
        cycles=_cycles(graph, limit=cycle_limit),
        pagerank_top=pagerank_top,
        critical_nodes=critical,
        longest_path=_longest_path(graph),
    )
