from __future__ import annotations

"""
Phase 4: Archivist + Navigator foundations.

This module implements:
1) `generate_CODEBASE_md()` - a "living context" Markdown file for coding agents.
2) Incremental update mode for module/lineage graphs based on git commits.

Evidence and logging:
- All Phase 4 operations must call `log_cartography_trace()` with the Week 1 audit
  format. Each logged action includes the evidence source artifact(s), the method,
  and a confidence score.
"""

import json
import os
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import networkx as nx

from cartography_trace import log_cartography_trace
from graph.knowledge_graph import graph_from_json_data, graph_to_json_data, read_graph_json, write_graph_json


STATE_PATH = Path(".cartography/phase4_state.json")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _run_git(repo_path: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(repo_path), *args],
        capture_output=True,
        text=True,
        check=True,
    )


def get_head_commit(repo_path: Path) -> str | None:
    try:
        return _run_git(repo_path, ["rev-parse", "HEAD"]).stdout.strip() or None
    except Exception:
        return None


def load_phase4_state(path: Path = STATE_PATH) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_phase4_state(state: dict[str, Any], path: Path = STATE_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def get_changed_files(repo_path: Path, since_commit: str) -> list[str]:
    """
    Return workspace-relative file paths changed since `since_commit` (exclusive).
    """
    try:
        res = _run_git(repo_path, ["diff", "--name-only", f"{since_commit}..HEAD"])
        return [line.strip() for line in res.stdout.splitlines() if line.strip()]
    except Exception:
        return []


def _is_meaningful_dataset_id(node_id: str) -> bool:
    if not node_id:
        return False
    if len(node_id) > 140:
        return False
    if "\n" in node_id or "\r" in node_id or "\t" in node_id:
        return False
    if node_id.startswith(("f\"", "f'", "b'", "b\"", "#")):
        return False
    if " " in node_id:
        return False
    if any(x in node_id for x in ("{", "}", "(", ")", ";", "\\", "\"", "'")):
        return False
    if not re.match(r"^[A-Za-z0-9_.:-]+$", node_id):
        return False
    sqlish = {
        "vacuum",
        "checkpoint",
        "install",
        "load",
        "call",
        "drop",
        "create",
        "alter",
        "set",
        "select",
        "insert",
        "update",
        "delete",
    }
    if node_id.lower() in sqlish:
        return False
    if node_id.isupper() and "." not in node_id and "_" not in node_id and len(node_id) <= 30:
        return False
    return True


@dataclass(frozen=True)
class CodebaseSection:
    title: str
    body: str


def generate_CODEBASE_md(
    *,
    repo_root: Path = Path("."),
    target_repo: Path = Path("target_repo"),
    module_graph_path: Path = Path(".cartography/module_graph.json"),
    lineage_graph_path: Path = Path(".cartography/lineage_graph.json"),
    semanticist_report_path: Path = Path(".cartography/semanticist_report.json"),
    out_path: Path = Path("CODEBASE.md"),
) -> Path:
    """
    Generate a "living context" CODEBASE.md file.

    Evidence citations:
    - Bullet points include evidence of the form `(evidence: <path>:<start>-<end>, method=<method>)`
      where line ranges come from Semanticist `evidence_symbols` when available.
    - Summaries are produced via deterministic static analysis over `.cartography/*.json`.

    Logging:
    - Logs one trace entry per major section generation step.
    """
    repo_root = Path(repo_root)
    target_repo = Path(target_repo)

    module_graph = read_graph_json(module_graph_path)
    lineage_graph = read_graph_json(lineage_graph_path)
    semantic = json.loads(semanticist_report_path.read_text(encoding="utf-8")) if semanticist_report_path.exists() else {}

    records = {m.get("module_name"): m for m in (semantic.get("modules") or []) if isinstance(m, dict) and m.get("module_name")}

    def line_range_for(module_name: str) -> list[int] | None:
        rec = records.get(module_name) or {}
        ev = rec.get("evidence_symbols") or []
        starts = [s.get("lineno") for s in ev if isinstance(s, dict) and isinstance(s.get("lineno"), int)]
        ends = [s.get("end_lineno") for s in ev if isinstance(s, dict) and isinstance(s.get("end_lineno"), int)]
        if starts and ends:
            return [min(starts), max(ends)]
        return None

    # ---- Architecture Overview (deterministic) ----
    domain_map = semantic.get("domains") or {}
    if isinstance(domain_map, dict) and domain_map:
        domain_counts = sorted(((k, len(v)) for k, v in domain_map.items()), key=lambda x: x[1], reverse=True)
        top_domains = ", ".join(f"{k} ({n})" for k, n in domain_counts[:6])
    else:
        top_domains = "unknown (Semanticist domains missing)"

    arch_overview = (
        "This repository implements a multi-agent codebase intelligence pipeline: "
        "Surveyor builds a module dependency graph (imports/defs + PageRank/velocity), "
        "Hydrologist builds a data lineage graph (SQL/dbt + Python dataset reads/writes + DAG configs), "
        "and Semanticist enriches modules with purpose, domain tags, and doc-drift signals. "
        f"Primary semantic domains observed: {top_domains}."
    )
    log_cartography_trace(
        {
            "agent": "Navigator",
            "action": "architecture_overview",
            "evidence_source": str(semanticist_report_path),
            "line_range": None,
            "method": "static_analysis",
            "confidence": 0.8,
            "timestamp": _utc_now_iso(),
        }
    )

    # ---- Critical Path (top 5 by PageRank) ----
    pr_items: list[tuple[float, str]] = []
    for node_id, attrs in module_graph.nodes(data=True):
        path = str(attrs.get("path") or node_id)
        if not path.endswith(".py"):
            continue
        pr_items.append((float(attrs.get("pagerank") or 0.0), path))
    pr_items.sort(reverse=True, key=lambda x: x[0])
    top5 = pr_items[:5]
    critical_lines = []
    for score, p in top5:
        module_name = os.path.relpath(p, repo_root) if Path(p).is_absolute() else p
        lr = line_range_for(module_name)
        lr_str = f"{lr[0]}-{lr[1]}" if lr else "?"
        critical_lines.append(f"- `{module_name}` (PageRank={score:.4f}) (evidence: `{module_graph_path}`; method=graph_traversal; file_evidence: `{module_name}:{lr_str}`)")
    log_cartography_trace(
        {
            "agent": "Navigator",
            "action": "critical_path_pagerank_top5",
            "evidence_source": str(module_graph_path),
            "line_range": None,
            "method": "graph_traversal",
            "confidence": 1.0,
            "timestamp": _utc_now_iso(),
        }
    )

    # ---- Data Sources & Sinks (from Hydrologist) ----
    in_deg: dict[str, int] = {}
    out_deg: dict[str, int] = {}
    for s, t, attrs in lineage_graph.edges(data=True):
        out_deg[str(s)] = out_deg.get(str(s), 0) + 1
        in_deg[str(t)] = in_deg.get(str(t), 0) + 1

    sources: list[str] = []
    sinks: list[str] = []
    for nid, attrs in lineage_graph.nodes(data=True):
        kind = attrs.get("kind")
        node_id = str(attrs.get("id") or attrs.get("name") or nid)
        if kind not in {"dataset", "table"}:
            continue
        if not _is_meaningful_dataset_id(node_id):
            continue
        if in_deg.get(str(nid), 0) == 0:
            sources.append(node_id)
        if out_deg.get(str(nid), 0) == 0:
            sinks.append(node_id)

    sources = sorted(set(sources))[:15]
    sinks = sorted(set(sinks))[:15]
    ds_lines = ["- Sources:"] + [f"  - `{x}` (evidence: `{lineage_graph_path}`; method=graph_traversal)" for x in sources]
    ds_lines += ["- Sinks:"] + [f"  - `{x}` (evidence: `{lineage_graph_path}`; method=graph_traversal)" for x in sinks]

    log_cartography_trace(
        {
            "agent": "Navigator",
            "action": "data_sources_and_sinks",
            "evidence_source": str(lineage_graph_path),
            "line_range": None,
            "method": "graph_traversal",
            "confidence": 0.7,
            "timestamp": _utc_now_iso(),
        }
    )

    # ---- Known Debt (cycles + doc drift) ----
    cycles = module_graph.graph.get("cycles") if isinstance(module_graph.graph, dict) else None
    if not isinstance(cycles, list):
        try:
            cycles = list(nx.simple_cycles(module_graph))
        except Exception:
            cycles = []
    # Only report cycles that look like real file cycles.
    filtered_cycles = [c for c in cycles if isinstance(c, list) and c and all(str(x).endswith(".py") for x in c)]
    cycles = filtered_cycles
    cycle_count = len(cycles)
    sample_cycle = cycles[0] if cycles else []

    drifted = []
    for m in (semantic.get("modules") or []):
        if not isinstance(m, dict):
            continue
        if m.get("doc_drift") is True:
            drifted.append(str(m.get("module_name")))
    drifted = sorted(set(drifted))[:20]

    debt_lines = [
        f"- Import cycles detected: `{cycle_count}` (evidence: `{module_graph_path}`; method=graph_traversal)",
        f"- Sample cycle (first): `{sample_cycle}` (evidence: `{module_graph_path}`; method=graph_traversal)",
        "- Doc drift flagged modules (top 20):",
        *[f"  - `{m}` (evidence: `{semanticist_report_path}`; method=static_analysis)" for m in drifted],
    ]
    log_cartography_trace(
        {
            "agent": "Navigator",
            "action": "known_debt_cycles_and_doc_drift",
            "evidence_source": f"{module_graph_path};{semanticist_report_path}",
            "line_range": None,
            "method": "static_analysis",
            "confidence": 0.85,
            "timestamp": _utc_now_iso(),
        }
    )

    # ---- High-Velocity Files ----
    velocity_items: list[tuple[float, str]] = []
    for node_id, attrs in module_graph.nodes(data=True):
        path = str(attrs.get("path") or node_id)
        if not path.endswith(".py"):
            continue
        velocity_items.append((float(attrs.get("change_velocity_30d") or 0.0), path))
    velocity_items.sort(reverse=True, key=lambda x: x[0])
    top_velocity = velocity_items[:10]
    velocity_lines = []
    for velocity, p in top_velocity:
        module_name = os.path.relpath(p, repo_root) if Path(p).is_absolute() else p
        velocity_lines.append(f"- `{module_name}` (change_velocity_30d={int(velocity)}) (evidence: `{module_graph_path}`; method=static_analysis)")
    log_cartography_trace(
        {
            "agent": "Navigator",
            "action": "high_velocity_files_top10",
            "evidence_source": str(module_graph_path),
            "line_range": None,
            "method": "static_analysis",
            "confidence": 1.0,
            "timestamp": _utc_now_iso(),
        }
    )

    sections = [
        CodebaseSection("Architecture Overview", arch_overview),
        CodebaseSection("Critical Path", "\n".join(critical_lines) if critical_lines else "- (no modules found)"),
        CodebaseSection("Data Sources & Sinks", "\n".join(ds_lines)),
        CodebaseSection("Known Debt", "\n".join(debt_lines)),
        CodebaseSection("High-Velocity Files", "\n".join(velocity_lines) if velocity_lines else "- (no velocity data found)"),
    ]

    md = [
        "# CODEBASE.md",
        "",
        f"_Generated: `{_utc_now_iso()}`_",
        "",
    ]
    for s in sections:
        md += [f"## {s.title}", "", s.body.strip(), ""]

    out_path = Path(out_path)
    out_path.write_text("\n".join(md).rstrip() + "\n", encoding="utf-8")
    log_cartography_trace(
        {
            "agent": "Navigator",
            "action": "generate_CODEBASE_md",
            "evidence_source": str(out_path),
            "line_range": None,
            "method": "static_analysis",
            "confidence": 1.0,
            "timestamp": _utc_now_iso(),
        }
    )
    return out_path


def update_module_graph_incremental(
    *,
    target_repo: Path = Path("target_repo"),
    module_graph_path: Path = Path(".cartography/module_graph.json"),
    changed_files: list[str],
) -> None:
    """
    Incrementally update the module graph for changed `.py` files.

    Strategy:
    - Load existing module graph (node-link JSON).
    - For each changed python file:
      - remove its outgoing edges
      - re-parse imports/defs/complexity via Surveyor helpers
      - update velocity/last_modified
      - re-add edges for resolved imports
    - Recompute PageRank and import cycles.
    """
    py_files = [f for f in changed_files if f.endswith(".py")]
    if not py_files:
        return

    from agents.surveyor import _resolve_import, extract_imports_and_defs, extract_git_velocity, extract_last_modified
    from analyzers.tree_sitter_analyzer import LanguageRouter

    g = read_graph_json(module_graph_path) if module_graph_path.exists() else nx.DiGraph()
    router = LanguageRouter(languages=["python"])
    python_language = router.language_for_path("file.py")
    if python_language is None:
        raise RuntimeError("Python language not available in tree-sitter library.")

    for rel_path in py_files:
        abs_path = target_repo / rel_path
        node_id = str(abs_path)
        if not abs_path.exists():
            if g.has_node(node_id):
                g.remove_node(node_id)
            continue

        parsed = extract_imports_and_defs(abs_path, python_language)
        velocity = extract_git_velocity(target_repo, abs_path)
        last_modified = extract_last_modified(target_repo, abs_path)

        existing = dict(g.nodes[node_id]) if g.has_node(node_id) else {}
        existing.update(
            {
                "id": node_id,
                "path": node_id,
                "imports": parsed.imports,
                "functions": parsed.functions,
                "classes": parsed.classes,
                "complexity_score": parsed.complexity_score,
                "change_velocity_30d": velocity,
                "last_modified": last_modified,
                "language": "python",
            }
        )
        g.add_node(node_id, **existing)

        # Remove old outgoing edges then re-add.
        for _, tgt in list(g.out_edges(node_id)):
            g.remove_edge(node_id, tgt)
        for imp in parsed.imports:
            resolved = _resolve_import(imp, abs_path, target_repo)
            g.add_edge(node_id, resolved)

    if g.number_of_nodes() > 0:
        pr = nx.pagerank(g)
        for nid, score in pr.items():
            g.nodes[nid]["pagerank"] = float(score)
    cycles = list(nx.simple_cycles(g))
    g.graph["cycles"] = cycles
    cycle_nodes = {n for cyc in cycles for n in cyc}
    for nid in g.nodes:
        g.nodes[nid]["in_cycle"] = nid in cycle_nodes

    write_graph_json(g, module_graph_path)
    log_cartography_trace(
        {
            "agent": "Navigator",
            "action": "update_module_graph_incremental",
            "evidence_source": str(module_graph_path),
            "line_range": None,
            "method": "static_analysis",
            "confidence": 0.9,
            "timestamp": _utc_now_iso(),
        }
    )


def update_lineage_graph_incremental(
    *,
    target_repo: Path = Path("target_repo"),
    lineage_graph_path: Path = Path(".cartography/lineage_graph.json"),
    changed_files: list[str],
) -> None:
    """
    Incrementally update the lineage graph for changed `.py` / `.sql` / `schema*.yml` files.

    This is intentionally conservative:
    - Remove edges whose `source_file` matches a changed file.
    - Remove that file from node `source_files`; drop nodes that have no remaining `source_files`.
    - Recompute edges for those files using the same analyzers Hydrologist uses.
    """
    relevant = [f for f in changed_files if f.endswith((".py", ".sql", ".yml", ".yaml"))]
    if not relevant:
        return

    from analyzers.dag_config_parser import DAGConfigAnalyzer
    from analyzers.sql_lineage import SQLLineageAnalyzer
    from analyzers.tree_sitter_analyzer import PythonDataFlowAnalyzer

    g = read_graph_json(lineage_graph_path) if lineage_graph_path.exists() else nx.DiGraph()

    changed_abs = {str(target_repo / f) for f in relevant}

    # Remove edges tied to changed files.
    for s, t, attrs in list(g.edges(data=True)):
        sf = attrs.get("source_file")
        if isinstance(sf, str) and sf in changed_abs:
            g.remove_edge(s, t)

    # Update node source_files / drop nodes with no remaining provenance.
    for nid, attrs in list(g.nodes(data=True)):
        sfiles = attrs.get("source_files")
        if isinstance(sfiles, list):
            new = [x for x in sfiles if x not in changed_abs]
            g.nodes[nid]["source_files"] = new
            if not new:
                # Drop fully-unprovenanced nodes; safe because we already removed edges.
                if g.degree(nid) == 0:
                    g.remove_node(nid)

    sql_analyzer = SQLLineageAnalyzer()
    python_analyzer = PythonDataFlowAnalyzer()
    dag_analyzer = DAGConfigAnalyzer()

    def ensure_node(name: str, kind: str, source_file: str) -> None:
        if not g.has_node(name):
            g.add_node(name, id=name, name=name, kind=kind, source_files=[source_file])
            return
        # Merge provenance.
        sf = g.nodes[name].get("source_files")
        if not isinstance(sf, list):
            sf = []
        if source_file not in sf:
            sf.append(source_file)
        g.nodes[name]["source_files"] = sf
        if not g.nodes[name].get("kind"):
            g.nodes[name]["kind"] = kind

    # Re-add edges for changed files.
    for rel in relevant:
        abs_path = target_repo / rel
        abs_s = str(abs_path)
        if not abs_path.exists():
            continue

        if abs_path.suffix == ".sql":
            for dep in sql_analyzer.analyze_file(abs_path):
                ensure_node(dep.target, kind="table", source_file=abs_s)
                for src in dep.sources:
                    ensure_node(src, kind="table", source_file=abs_s)
                    g.add_edge(src, dep.target, source_file=abs_s, transformation_type="sql")

        if abs_path.suffix == ".py":
            script_node = f"python:{abs_path}"
            ensure_node(script_node, kind="script", source_file=abs_s)
            for access in python_analyzer.analyze_file(abs_path):
                for dataset in access.datasets:
                    if dataset == PythonDataFlowAnalyzer.DYNAMIC_REFERENCE:
                        continue
                    ensure_node(dataset, kind="dataset", source_file=access.source_file)
                    if access.direction == "read":
                        g.add_edge(dataset, script_node, source_file=access.source_file, transformation_type="python_read")
                    else:
                        g.add_edge(script_node, dataset, source_file=access.source_file, transformation_type="python_write")

            # DAG config edges are also extracted from python files.
            text = abs_path.read_text(encoding="utf-8", errors="ignore")
            for edge in dag_analyzer._parse_python(text, abs_path):
                ensure_node(edge.source, kind="dag_task", source_file=edge.source_file)
                ensure_node(edge.target, kind="dag_task", source_file=edge.source_file)
                g.add_edge(edge.source, edge.target, source_file=edge.source_file, transformation_type=edge.transformation_type)

        if abs_path.name.startswith("schema") and abs_path.suffix in {".yml", ".yaml"}:
            for edge in dag_analyzer._parse_schema(abs_path):
                ensure_node(edge.source, kind="dag_task", source_file=edge.source_file)
                ensure_node(edge.target, kind="dag_task", source_file=edge.source_file)
                g.add_edge(edge.source, edge.target, source_file=edge.source_file, transformation_type=edge.transformation_type)

    write_graph_json(g, lineage_graph_path)
    log_cartography_trace(
        {
            "agent": "Navigator",
            "action": "update_lineage_graph_incremental",
            "evidence_source": str(lineage_graph_path),
            "line_range": None,
            "method": "static_analysis",
            "confidence": 0.8,
            "timestamp": _utc_now_iso(),
        }
    )


def phase4_incremental_run(
    *,
    target_repo: Path = Path("target_repo"),
    module_graph_path: Path = Path(".cartography/module_graph.json"),
    lineage_graph_path: Path = Path(".cartography/lineage_graph.json"),
    semanticist_report_path: Path = Path(".cartography/semanticist_report.json"),
    codebase_md_path: Path = Path("CODEBASE.md"),
) -> Path:
    """
    Phase 4 entrypoint:
    - Detects git changes since last Phase 4 run.
    - Incrementally updates Surveyor/Hydrologist artifacts for changed files.
    - Regenerates CODEBASE.md.
    - Updates Phase 4 state with the current HEAD commit.
    """
    state = load_phase4_state()
    head = get_head_commit(target_repo)
    last = state.get("last_commit")

    # Ensure required artifacts exist; if not, run full analyses once.
    if not module_graph_path.exists():
        from agents.surveyor import build_module_graph
        from graph.knowledge_graph import write_graph_json

        _, graph, _ = build_module_graph(target_repo)
        write_graph_json(graph, module_graph_path)
    log_cartography_trace(
        {
            "agent": "Navigator",
            "action": "build_module_graph_full",
            "evidence_source": str(module_graph_path),
            "line_range": None,
            "method": "static_analysis",
            "confidence": 0.9,
                "timestamp": _utc_now_iso(),
            }
        )

    if not lineage_graph_path.exists():
        from agents.hydrologist import build_lineage_graph
        from graph.knowledge_graph import write_graph_json

        _, graph, _ = build_lineage_graph(target_repo)
        write_graph_json(graph.graph, lineage_graph_path)
    log_cartography_trace(
        {
            "agent": "Navigator",
            "action": "build_lineage_graph_full",
            "evidence_source": str(lineage_graph_path),
            "line_range": None,
            "method": "static_analysis",
            "confidence": 0.85,
                "timestamp": _utc_now_iso(),
            }
        )

    changed: list[str] = []
    if head and last and head != last:
        changed = get_changed_files(target_repo, last)
    elif head and not last:
        # First run: treat as full context generation; graphs may already exist.
        changed = []

    log_cartography_trace(
        {
            "agent": "Navigator",
            "action": "detect_git_changes",
            "evidence_source": str(target_repo),
            "line_range": None,
            "method": "git_diff",
            "confidence": 1.0 if head else 0.4,
            "timestamp": _utc_now_iso(),
        }
    )

    if changed:
        update_module_graph_incremental(target_repo=target_repo, module_graph_path=module_graph_path, changed_files=changed)
        update_lineage_graph_incremental(target_repo=target_repo, lineage_graph_path=lineage_graph_path, changed_files=changed)

    out = generate_CODEBASE_md(
        target_repo=target_repo,
        module_graph_path=module_graph_path,
        lineage_graph_path=lineage_graph_path,
        semanticist_report_path=semanticist_report_path,
        out_path=codebase_md_path,
    )

    if head:
        state["last_commit"] = head
        state["last_run_at"] = _utc_now_iso()
        save_phase4_state(state)

    return out
