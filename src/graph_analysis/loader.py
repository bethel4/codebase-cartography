from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import networkx as nx
from networkx.readwrite import json_graph


def load_node_link_json(path: str | Path) -> dict[str, Any]:
    """Load a NetworkX node-link JSON artifact (supports `links` or legacy `edges`)."""
    path = Path(path)
    data = json.loads(path.read_text(encoding="utf-8"))

    # Normalize legacy key for NetworkX node_link_graph.
    if "links" not in data and "edges" in data:
        data = dict(data)
        data["links"] = data.get("edges")
    return data


def load_digraph(path: str | Path) -> nx.DiGraph:
    """Load a directed graph from node-link JSON."""
    data = load_node_link_json(path)
    graph = json_graph.node_link_graph(data, directed=True, edges="links")
    if isinstance(graph, nx.DiGraph):
        return graph
    return nx.DiGraph(graph)


def write_json(data: dict[str, Any], path: str | Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return path

