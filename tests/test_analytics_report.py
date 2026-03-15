import json
from pathlib import Path

import networkx as nx
from networkx.readwrite import json_graph

from reports.analytics_report import write_analytics_report


def _write_node_link(graph: nx.DiGraph, path: Path) -> None:
    data = json_graph.node_link_data(graph, edges="links")
    path.write_text(json.dumps(data), encoding="utf-8")


def test_write_analytics_report(tmp_path: Path) -> None:
    module_graph = nx.DiGraph()
    module_graph.add_node(
        "a.py",
        path="a.py",
        pagerank=0.7,
        change_velocity_30d=3,
        dead_exports=[],
        entrypoint_exports=[],
        framework_exports=[],
        is_dead_code_candidate=False,
    )
    module_graph.add_node(
        "b.py",
        path="b.py",
        pagerank=0.3,
        change_velocity_30d=0,
        dead_exports=["old_fn"],
        entrypoint_exports=[],
        framework_exports=[],
        is_dead_code_candidate=True,
    )
    module_graph.add_edge("a.py", "b.py")
    module_graph.graph["cycles"] = []

    lineage_graph = nx.DiGraph()
    lineage_graph.add_node("raw.users", kind="table")
    lineage_graph.add_node("analytics.users", kind="table")
    lineage_graph.add_edge("raw.users", "analytics.users", transformation_type="sql", source_file="model.sql")
    lineage_graph.add_node("SELECT * FROM something\n", kind="table")
    lineage_graph.graph["data_cycles"] = []

    module_path = tmp_path / "module_graph.json"
    lineage_path = tmp_path / "lineage_graph.json"
    out_path = tmp_path / "report.md"

    _write_node_link(module_graph, module_path)
    _write_node_link(lineage_graph, lineage_path)

    written = write_analytics_report(module_path, lineage_path, out_path, top_n=5)
    text = written.read_text(encoding="utf-8")

    assert written.exists()
    assert "## Module Graph (Surveyor)" in text
    assert "## Lineage Graph (Hydrologist)" in text
    assert "`b.py`: old_fn" in text
    # noisy identifiers should be suppressed from the listing
    assert "SELECT * FROM something" not in text
