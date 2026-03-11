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
