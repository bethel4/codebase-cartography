from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import networkx as nx


def _node_title(attrs: dict[str, Any]) -> str:
    try:
        return json.dumps(attrs, indent=2, ensure_ascii=False)
    except Exception:
        return str(attrs)


def render_pyvis(graph: nx.DiGraph, out_html: str | Path, height: str = "800px") -> Path:
    """
    Render a directed graph to an interactive HTML file using PyVis.

    Requires `pyvis` to be installed (e.g., `uv add pyvis`).
    """
    try:
        from pyvis.network import Network  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise ImportError("pyvis is required for this visualization. Install it with: uv add pyvis") from exc

    try:
        import jinja2  # noqa: F401
    except Exception as exc:  # pragma: no cover
        raise ImportError("pyvis requires jinja2. Install it with: uv add jinja2") from exc

    out_html = Path(out_html)
    out_html.parent.mkdir(parents=True, exist_ok=True)

    net = Network(height=height, width="100%", directed=True, bgcolor="#0b1020", font_color="#e7e9f3")
    net.toggle_physics(True)

    # Add nodes with tooltips.
    for node_id, attrs in graph.nodes(data=True):
        net.add_node(str(node_id), title=_node_title(dict(attrs)))

    for source, target, attrs in graph.edges(data=True):
        net.add_edge(str(source), str(target), title=_node_title(dict(attrs)))

    # `write_html` is more reliable than `show` in non-notebook environments.
    net.write_html(str(out_html), open_browser=False, notebook=False)
    return out_html
