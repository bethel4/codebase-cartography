import json
from pathlib import Path

import networkx as nx
from networkx.readwrite import json_graph

from graph_analysis.analyzer import analyze_graph, impact_descendants
from graph_analysis.loader import load_digraph, write_json
from graph_analysis.visualization import render_pyvis


def _write_node_link(graph: nx.DiGraph, path: Path) -> None:
    data = json_graph.node_link_data(graph, edges="links")
    path.write_text(json.dumps(data), encoding="utf-8")


def test_load_and_analyze(tmp_path: Path) -> None:
    g = nx.DiGraph()
    g.add_edge("a", "b")
    g.add_edge("b", "c")
    g.graph["cycles"] = []

    path = tmp_path / "g.json"
    _write_node_link(g, path)

    loaded = load_digraph(path)
    analysis = analyze_graph(loaded, critical_k=5, cycle_limit=10).to_dict()

    assert analysis["graph_stats"]["number_of_nodes"] == 3
    assert analysis["graph_stats"]["number_of_edges"] == 2
    assert analysis["roots"] == ["a"]
    assert analysis["leaves"] == ["c"]
    assert impact_descendants(loaded, "a") == ["b", "c"]

    out = write_json({"ok": True}, tmp_path / "report.json")
    assert out.exists()


def test_render_pyvis_missing_dependency(tmp_path: Path) -> None:
    g = nx.DiGraph()
    g.add_edge("a", "b")
    out = tmp_path / "graph.html"

    try:
        import pyvis  # noqa: F401
    except Exception:
        # If pyvis isn't installed, render_pyvis should raise ImportError.
        try:
            render_pyvis(g, out)
        except ImportError:
            return
        raise AssertionError("Expected ImportError when pyvis is missing")

