from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

import networkx as nx

from models import DataNode


@dataclass
class DataLineageGraph:
    """Wrapper around a NetworkX graph for data lineage."""

    graph: nx.DiGraph = field(default_factory=nx.DiGraph)

    def add_node(self, node: DataNode) -> None:
        self.graph.add_node(node.name, **node.model_dump())

    def add_edge(
        self,
        source: str,
        target: str,
        source_file: str,
        transformation_type: str = "unknown",
        line_range: str = "",
    ) -> None:
        self.graph.add_edge(
            source,
            target,
            source_file=source_file,
            transformation_type=transformation_type,
            line_range=line_range,
        )

    def blast_radius(self, node_name: str) -> list[str]:
        if node_name not in self.graph:
            return []
        visited: set[str] = set()
        queue = deque([node_name])
        while queue:
            current = queue.popleft()
            for _, neighbor in self.graph.out_edges(current):
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append(neighbor)
        return sorted(visited)

    def find_sources(self) -> list[str]:
        return sorted([node for node, degree in self.graph.in_degree() if degree == 0])

    def find_sinks(self) -> list[str]:
        return sorted([node for node, degree in self.graph.out_degree() if degree == 0])
