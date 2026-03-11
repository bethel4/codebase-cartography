from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from collections import deque
from pathlib import Path
from typing import Any, Iterable

import networkx as nx


def _load_node_link_json(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Load NetworkX node-link JSON produced by `networkx.readwrite.json_graph`."""
    data = json.loads(path.read_text(encoding="utf-8"))
    nodes = data.get("nodes") or []
    links = data.get("links") or data.get("edges") or []
    if not isinstance(nodes, list) or not isinstance(links, list):
        raise ValueError(f"Unexpected node-link JSON structure in {path}")
    return nodes, links


def load_graph(path: Path) -> nx.DiGraph:
    """Load a DiGraph from node-link JSON (supports both `links` and `edges`)."""
    nodes, links = _load_node_link_json(path)
    graph = nx.DiGraph()

    for node in nodes:
        node_id = node.get("id") or node.get("name")
        if not node_id:
            continue
        attrs = {k: v for k, v in node.items() if k not in {"id"}}
        graph.add_node(str(node_id), **attrs)

    for link in links:
        source = link.get("source")
        target = link.get("target")
        if source is None or target is None:
            continue
        attrs = {k: v for k, v in link.items() if k not in {"source", "target"}}
        graph.add_edge(str(source), str(target), **attrs)

    return graph


def _escape_dot(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def node_label(node_id: str, attrs: dict[str, Any]) -> str:
    kind = attrs.get("kind") or attrs.get("language")
    if kind:
        return f"{node_id}\\n[{kind}]"
    return node_id


def to_dot(graph: nx.DiGraph) -> str:
    """Emit a Graphviz DOT representation without extra dependencies."""
    lines: list[str] = []
    lines.append("digraph G {")
    lines.append('  graph [rankdir="LR"];')
    lines.append('  node [shape="box", fontsize=10];')
    lines.append('  edge [fontsize=9];')

    for node_id, attrs in graph.nodes(data=True):
        label = _escape_dot(node_label(str(node_id), attrs))
        lines.append(f'  "{_escape_dot(str(node_id))}" [label="{label}"];')

    for source, target, attrs in graph.edges(data=True):
        transformation = attrs.get("transformation_type")
        edge_label = f' [label="{_escape_dot(str(transformation))}"]' if transformation else ""
        lines.append(f'  "{_escape_dot(str(source))}" -> "{_escape_dot(str(target))}"{edge_label};')

    lines.append("}")
    return "\n".join(lines) + "\n"


def focus_subgraph(graph: nx.DiGraph, center: str, depth: int) -> nx.DiGraph:
    """Return a subgraph around `center` including in/out neighbors up to `depth`."""
    if center not in graph:
        return nx.DiGraph()

    visited: set[str] = {center}
    queue: deque[tuple[str, int]] = deque([(center, 0)])
    while queue:
        current, dist = queue.popleft()
        if dist >= depth:
            continue
        neighbors: Iterable[str] = set(graph.predecessors(current)) | set(graph.successors(current))
        for neighbor in neighbors:
            if neighbor in visited:
                continue
            visited.add(neighbor)
            queue.append((neighbor, dist + 1))

    return graph.subgraph(sorted(visited)).copy()


def limit_nodes_by_degree(graph: nx.DiGraph, max_nodes: int) -> nx.DiGraph:
    """Keep only the highest-degree nodes (plus any edges between them)."""
    if max_nodes <= 0 or graph.number_of_nodes() <= max_nodes:
        return graph
    ranked = sorted(graph.degree(), key=lambda pair: pair[1], reverse=True)
    keep = {node for node, _ in ranked[:max_nodes]}
    return graph.subgraph(sorted(keep)).copy()


def write_dot(graph: nx.DiGraph, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(to_dot(graph), encoding="utf-8")


def try_render_png(dot_path: Path, png_path: Path) -> bool:
    """Render DOT to PNG if Graphviz `dot` is available."""
    dot_bin = shutil.which("dot")
    if not dot_bin:
        return False
    png_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        subprocess.run([dot_bin, "-Tpng", str(dot_path), "-o", str(png_path)], check=False)
    except OSError:
        return False
    return png_path.exists()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Visualize a cartography graph JSON as DOT/PNG.")
    parser.add_argument("--graph", required=True, help="Path to a node-link JSON graph (.cartography/*.json).")
    parser.add_argument("--out-dot", default=None, help="Output DOT path (default: build/<name>.dot).")
    parser.add_argument("--render-png", action="store_true", help="Also render PNG (requires Graphviz dot).")
    parser.add_argument("--out-png", default=None, help="PNG output path (default: build/<name>.png).")
    parser.add_argument("--max-nodes", type=int, default=200, help="Limit nodes for readability (default: 200).")
    parser.add_argument("--focus", default=None, help="Center node id/name to focus on.")
    parser.add_argument("--depth", type=int, default=2, help="Focus depth for --focus (default: 2).")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    graph_path = Path(args.graph)
    if not graph_path.exists():
        raise FileNotFoundError(graph_path)

    graph = load_graph(graph_path)
    if args.focus:
        graph = focus_subgraph(graph, args.focus, args.depth)
    graph = limit_nodes_by_degree(graph, args.max_nodes)

    out_dot = Path(args.out_dot) if args.out_dot else Path("build") / f"{graph_path.stem}.dot"
    write_dot(graph, out_dot)

    if args.render_png:
        out_png = Path(args.out_png) if args.out_png else Path("build") / f"{graph_path.stem}.png"
        rendered = try_render_png(out_dot, out_png)
        if rendered:
            print(f"Wrote PNG: {out_png}")
        else:
            print("Graphviz `dot` not available; wrote DOT only:", out_dot)

    print(f"Wrote DOT: {out_dot} (nodes={graph.number_of_nodes()}, edges={graph.number_of_edges()})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
