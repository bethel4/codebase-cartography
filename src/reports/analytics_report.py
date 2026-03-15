from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import networkx as nx
from networkx.readwrite import json_graph


def _load_node_link_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_graph(path: Path) -> tuple[dict[str, Any], nx.DiGraph]:
    data = _load_node_link_json(path)
    graph = json_graph.node_link_graph(data, directed=True, edges="links")
    if not isinstance(graph, nx.DiGraph):
        graph = nx.DiGraph(graph)
    return data, graph


def _shorten_path(value: str, max_len: int = 90) -> str:
    if len(value) <= max_len:
        return value
    return "…" + value[-(max_len - 1) :]


def _format_list(values: list[str], limit: int) -> list[str]:
    shown = values[:limit]
    if len(values) > limit:
        shown.append(f"(+{len(values) - limit} more)")
    return shown


@dataclass(frozen=True)
class ModuleGraphStats:
    internal_modules: int
    total_nodes: int
    total_edges: int
    cycle_count: int
    dead_file_candidates: int
    dead_export_modules: int
    dead_export_symbols: int
    top_pagerank: list[tuple[str, float]]
    top_velocity: list[tuple[str, int]]
    top_dead_exports: list[tuple[str, list[str]]]
    cycles_preview: list[list[str]]


@dataclass(frozen=True)
class LineageGraphStats:
    total_nodes: int
    total_edges: int
    cycle_count: int
    sources: list[str]
    sinks: list[str]
    clean_sources: list[str]
    clean_sinks: list[str]
    suppressed_sources: int
    suppressed_sinks: int
    transformation_types: list[tuple[str, int]]
    top_blast_radius: list[tuple[str, int]]
    cycles_preview: list[list[str]]


def analyze_module_graph(module_graph_json: Path, top_n: int = 10) -> ModuleGraphStats:
    data, graph = _load_graph(module_graph_json)
    nodes = data.get("nodes") or []

    internal_nodes = [n for n in nodes if isinstance(n, dict) and n.get("path")]
    internal_modules = len(internal_nodes)

    cycles: list[list[str]] = []
    graph_meta = data.get("graph") or {}
    if isinstance(graph_meta, dict):
        cycles = graph_meta.get("cycles") or []
    if not isinstance(cycles, list):
        cycles = []

    dead_files = [n for n in internal_nodes if n.get("is_dead_code_candidate") is True]

    dead_export_modules: list[tuple[str, list[str]]] = []
    dead_export_symbols = 0
    for n in internal_nodes:
        exports = n.get("dead_exports") or []
        if isinstance(exports, list) and exports:
            dead_export_modules.append((str(n.get("path")), [str(x) for x in exports]))
            dead_export_symbols += len(exports)

    def _score_float(node: dict[str, Any], key: str) -> float:
        try:
            return float(node.get(key) or 0.0)
        except Exception:
            return 0.0

    def _score_int(node: dict[str, Any], key: str) -> int:
        try:
            return int(node.get(key) or 0)
        except Exception:
            return 0

    top_pagerank = sorted(
        ((str(n["path"]), _score_float(n, "pagerank")) for n in internal_nodes),
        key=lambda pair: pair[1],
        reverse=True,
    )[:top_n]

    top_velocity = sorted(
        ((str(n["path"]), _score_int(n, "change_velocity_30d")) for n in internal_nodes),
        key=lambda pair: pair[1],
        reverse=True,
    )[:top_n]

    top_dead_exports = sorted(dead_export_modules, key=lambda pair: len(pair[1]), reverse=True)[:top_n]
    cycles_preview = [[_shorten_path(x) for x in cycle] for cycle in cycles[: min(len(cycles), 5)]]

    return ModuleGraphStats(
        internal_modules=internal_modules,
        total_nodes=graph.number_of_nodes(),
        total_edges=graph.number_of_edges(),
        cycle_count=len(cycles),
        dead_file_candidates=len(dead_files),
        dead_export_modules=len(dead_export_modules),
        dead_export_symbols=dead_export_symbols,
        top_pagerank=top_pagerank,
        top_velocity=top_velocity,
        top_dead_exports=top_dead_exports,
        cycles_preview=cycles_preview,
    )


def analyze_lineage_graph(lineage_graph_json: Path, top_n: int = 10) -> LineageGraphStats:
    data, graph = _load_graph(lineage_graph_json)

    graph_meta = data.get("graph") or {}
    cycles: list[list[str]] = []
    if isinstance(graph_meta, dict):
        cycles = graph_meta.get("data_cycles") or []
    if not isinstance(cycles, list):
        cycles = []

    sources = sorted([node for node, degree in graph.in_degree() if degree == 0])
    sinks = sorted([node for node, degree in graph.out_degree() if degree == 0])

    def _is_clean_asset_id(value: str) -> bool:
        if not value:
            return False
        if value.startswith("python:"):
            return True
        # suppress obvious non-identifiers (multiline SQL, html, f-strings, etc.)
        if any(ch in value for ch in ("\n", "\r", "\t")):
            return False
        if len(value) > 120:
            return False
        if value.startswith(("f\"", "f'", "b\"", "b'")):
            return False
        if " " in value:
            return False
        return True

    clean_sources = [str(x) for x in sources if _is_clean_asset_id(str(x))]
    clean_sinks = [str(x) for x in sinks if _is_clean_asset_id(str(x))]
    suppressed_sources = len(sources) - len(clean_sources)
    suppressed_sinks = len(sinks) - len(clean_sinks)

    transformation_counter: Counter[str] = Counter()
    for _, _, attrs in graph.edges(data=True):
        value = attrs.get("transformation_type") or "unknown"
        transformation_counter[str(value)] += 1
    transformation_types = transformation_counter.most_common(top_n)

    # Blast radius: compute descendants count for the highest out-degree nodes first,
    # then sort by true descendant count.
    degree_ranked = sorted(graph.out_degree(), key=lambda pair: pair[1], reverse=True)
    candidates = [node for node, _ in degree_ranked if _is_clean_asset_id(str(node))][: max(50, top_n * 5)]
    blast: list[tuple[str, int]] = []
    for node in candidates:
        try:
            blast.append((str(node), len(nx.descendants(graph, node))))
        except Exception:
            continue
    top_blast = sorted(blast, key=lambda pair: pair[1], reverse=True)[:top_n]

    cycles_preview = [[_shorten_path(str(x), max_len=60) for x in cycle] for cycle in cycles[: min(len(cycles), 5)]]

    return LineageGraphStats(
        total_nodes=graph.number_of_nodes(),
        total_edges=graph.number_of_edges(),
        cycle_count=len(cycles),
        sources=sources,
        sinks=sinks,
        clean_sources=clean_sources,
        clean_sinks=clean_sinks,
        suppressed_sources=suppressed_sources,
        suppressed_sinks=suppressed_sinks,
        transformation_types=transformation_types,
        top_blast_radius=top_blast,
        cycles_preview=cycles_preview,
    )


def render_markdown(module_stats: ModuleGraphStats, lineage_stats: LineageGraphStats) -> str:
    now = datetime.now(tz=timezone.utc).isoformat()
    lines: list[str] = []
    lines.append("# Cartography Analytics Report")
    lines.append("")
    lines.append(f"_Generated: {now}_")
    lines.append("")

    lines.append("## Module Graph (Surveyor)")
    lines.append("")
    lines.append(f"- Internal Python modules: **{module_stats.internal_modules}**")
    lines.append(f"- Graph nodes/edges: **{module_stats.total_nodes}** / **{module_stats.total_edges}**")
    lines.append(f"- Import cycles detected: **{module_stats.cycle_count}**")
    lines.append(f"- File-level dead-code candidates: **{module_stats.dead_file_candidates}**")
    lines.append(f"- Unused exported symbols (dead_exports): **{module_stats.dead_export_symbols}** across **{module_stats.dead_export_modules}** files")
    lines.append("")

    if module_stats.cycles_preview:
        lines.append("**Cycle preview (first 5):**")
        for cycle in module_stats.cycles_preview:
            lines.append(f"- {' -> '.join(cycle)}")
        lines.append("")

    lines.append("**Top modules by PageRank:**")
    for path, score in module_stats.top_pagerank:
        lines.append(f"- `{_shorten_path(path)}`: {score:.6f}")
    lines.append("")

    lines.append("**Top files by change velocity (30d):**")
    for path, velocity in module_stats.top_velocity:
        lines.append(f"- `{_shorten_path(path)}`: {velocity}")
    lines.append("")

    if module_stats.top_dead_exports:
        lines.append("**Top dead_exports candidates (by symbol count):**")
        for path, exports in module_stats.top_dead_exports:
            shown = ", ".join(_format_list(exports, 12))
            lines.append(f"- `{_shorten_path(path)}`: {shown}")
        lines.append("")

    lines.append("## Lineage Graph (Hydrologist)")
    lines.append("")
    lines.append(f"- Graph nodes/edges: **{lineage_stats.total_nodes}** / **{lineage_stats.total_edges}**")
    lines.append(f"- Data cycles detected: **{lineage_stats.cycle_count}**")
    lines.append(f"- Sources (in-degree=0): **{len(lineage_stats.sources)}**")
    lines.append(f"- Sinks (out-degree=0): **{len(lineage_stats.sinks)}**")
    if lineage_stats.suppressed_sources or lineage_stats.suppressed_sinks:
        lines.append(
            f"- Suppressed noisy source/sink ids: **{lineage_stats.suppressed_sources}** / **{lineage_stats.suppressed_sinks}**"
        )
    lines.append("")

    if lineage_stats.cycles_preview:
        lines.append("**Cycle preview (first 5):**")
        for cycle in lineage_stats.cycles_preview:
            lines.append(f"- {' -> '.join(cycle)}")
        lines.append("")

    lines.append("**Top sources:**")
    for node in _format_list(lineage_stats.clean_sources, 10):
        lines.append(f"- `{node}`")
    lines.append("")

    lines.append("**Top sinks:**")
    for node in _format_list(lineage_stats.clean_sinks, 10):
        lines.append(f"- `{node}`")
    lines.append("")

    lines.append("**Transformation types (edge counts):**")
    for name, count in lineage_stats.transformation_types:
        lines.append(f"- `{name}`: {count}")
    lines.append("")

    if lineage_stats.top_blast_radius:
        lines.append("**Largest blast radius (descendants count):**")
        for node, count in lineage_stats.top_blast_radius:
            lines.append(f"- `{node}`: {count}")
        lines.append("")

    lines.append("## Notes")
    lines.append("")
    lines.append("- `dead_exports` is conservative: it only lists exported symbols not referenced anywhere in the repo.")
    lines.append("- `entrypoint_exports` and `framework_exports` are expected false positives for “unused by import”.")
    lines.append("- Cycles are recorded for visibility; the pipeline does not fail if cycles exist.")
    lines.append("")
    return "\n".join(lines)


def write_analytics_report(
    module_graph_json: str | Path = ".cartography/module_graph.json",
    lineage_graph_json: str | Path = ".cartography/lineage_graph.json",
    out_path: str | Path = "build/analytics_report.md",
    top_n: int = 10,
) -> Path:
    module_stats = analyze_module_graph(Path(module_graph_json), top_n=top_n)
    lineage_stats = analyze_lineage_graph(Path(lineage_graph_json), top_n=top_n)
    report = render_markdown(module_stats, lineage_stats)

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report, encoding="utf-8")
    return out_path
