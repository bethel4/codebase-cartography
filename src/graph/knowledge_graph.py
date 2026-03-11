from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import networkx as nx
from networkx.readwrite import json_graph


class KnowledgeGraph:
    """
    Small wrapper around a directed NetworkX graph.

    This layer is responsible for:
    - bridging typed node/edge records into a NetworkX `DiGraph`
    - serialization/deserialization to/from node-link JSON
    """

    def __init__(self, graph: nx.DiGraph | None = None) -> None:
        self.graph: nx.DiGraph = graph or nx.DiGraph()

    def add_node(self, node_id: str, **attrs: Any) -> None:
        self.graph.add_node(node_id, **attrs)

    def add_edge(self, source: str, target: str, **attrs: Any) -> None:
        self.graph.add_edge(source, target, **attrs)

    def to_json_data(self) -> dict[str, Any]:
        # Explicit edges key keeps forward-compat with NetworkX 3.6+ warnings.
        return json_graph.node_link_data(self.graph, edges="links")

    @classmethod
    def from_json_data(cls, data: dict[str, Any]) -> "KnowledgeGraph":
        graph = json_graph.node_link_graph(data, directed=True, edges="links")
        if not isinstance(graph, nx.DiGraph):
            graph = nx.DiGraph(graph)
        return cls(graph)

    def write_json(self, path: str | Path) -> None:
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(self.to_json_data(), indent=2), encoding="utf-8")

    @classmethod
    def read_json(cls, path: str | Path) -> "KnowledgeGraph":
        input_path = Path(path)
        data = json.loads(input_path.read_text(encoding="utf-8"))
        return cls.from_json_data(data)


def graph_to_json_data(graph: nx.DiGraph) -> dict[str, Any]:
    return json_graph.node_link_data(graph, edges="links")


def graph_from_json_data(data: dict[str, Any]) -> nx.DiGraph:
    graph = json_graph.node_link_graph(data, directed=True, edges="links")
    if isinstance(graph, nx.DiGraph):
        return graph
    return nx.DiGraph(graph)


def write_graph_json(graph: nx.DiGraph, path: str | Path) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    data = graph_to_json_data(graph)
    output_path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def read_graph_json(path: str | Path) -> nx.DiGraph:
    input_path = Path(path)
    data = json.loads(input_path.read_text(encoding="utf-8"))
    return graph_from_json_data(data)
