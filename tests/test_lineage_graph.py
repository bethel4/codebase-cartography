import pytest

from graph.lineage_graph import DataLineageGraph
from models import DataNode


@pytest.fixture
def sample_graph():
    graph = DataLineageGraph()
    graph.add_node(DataNode(name="raw.users"))
    graph.add_node(DataNode(name="analytics.users"))
    graph.add_node(DataNode(name="analytics.orders"))
    graph.add_edge("raw.users", "analytics.users", source_file="users.sql")
    graph.add_edge("raw.users", "analytics.orders", source_file="orders.sql")
    graph.add_edge("analytics.users", "analytics.orders", source_file="join.sql")
    return graph


def test_find_sources(sample_graph):
    """Sources should be nodes with no incoming edges."""
    assert sample_graph.find_sources() == ["raw.users"]


def test_find_sinks(sample_graph):
    """Sinks should be nodes without outgoing edges."""
    assert sample_graph.find_sinks() == ["analytics.orders"]


def test_blast_radius(sample_graph):
    """Blast radius returns downstream dependents."""
    radius = sample_graph.blast_radius("raw.users")
    assert radius == ["analytics.orders", "analytics.users"]
    # nonexistent node returns empty list
    assert sample_graph.blast_radius("missing") == []


def test_hydrologist_records_data_cycles(tmp_path):
    """Hydrologist should record lineage cycles in graph metadata without crashing."""
    pytest.importorskip("sqlglot")
    from agents.hydrologist import build_lineage_graph

    repo = tmp_path / "repo"
    repo.mkdir()

    (repo / "a.sql").write_text("CREATE TABLE a AS SELECT * FROM b;")
    (repo / "b.sql").write_text("CREATE TABLE b AS SELECT * FROM a;")

    _, graph, summary = build_lineage_graph(repo)
    assert summary.edge_count == 2
    assert "data_cycles" in graph.graph.graph
    assert any(set(cycle) == {"a", "b"} for cycle in graph.graph.graph["data_cycles"])
