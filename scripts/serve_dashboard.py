from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def _load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _short_json(obj: object, max_chars: int = 4000) -> str:
    try:
        raw = json.dumps(obj, ensure_ascii=False, indent=2)
    except Exception:
        raw = str(obj)
    if len(raw) <= max_chars:
        return raw
    return raw[: max_chars - 50] + "\n...(truncated)\n"


def _build_context(build_dir: Path) -> str:
    """
    Build a compact context string from existing artifacts.
    Used as a system message for chat to keep answers grounded.
    """
    parts: list[str] = []

    analysis = _load_json(build_dir / "graph_analysis" / "analysis_report.json")
    if analysis:
        mg = analysis.get("module_graph", {}).get("graph_stats", {})
        lg = analysis.get("lineage_graph", {}).get("graph_stats", {})
        parts.append("Graph analysis summary:")
        if mg:
            parts.append(f"- module_graph nodes={mg.get('number_of_nodes')} edges={mg.get('number_of_edges')} dag={mg.get('is_dag')}")
        if lg:
            parts.append(f"- lineage_graph nodes={lg.get('number_of_nodes')} edges={lg.get('number_of_edges')} dag={lg.get('is_dag')}")

    if not parts:
        parts.append("Graph context: module_graph.json and lineage_graph.json are available; answer using node ids and paths.")

    parts.append("Guidelines: Be concise. Prefer referencing concrete node ids, file paths, and edges.")
    return "\n".join(parts)

def _refresh_embedded_graph_html(build_dir: Path, *, max_nodes: int = 800) -> None:
    """
    Regenerate `build/module_graph.html` and `build/lineage_graph.html` from the latest
    `.cartography/*.json` artifacts.

    The dashboard embeds these HTML viewers in iframes; each viewer contains embedded
    node/link JSON, so the HTML must be regenerated when the underlying graphs change.
    """
    root = Path(__file__).resolve().parents[1]
    exporter = root / "scripts" / "export_graph_html.py"
    if not exporter.exists():
        return

    build_dir.mkdir(parents=True, exist_ok=True)
    exporter_mtime = None
    try:
        exporter_mtime = exporter.stat().st_mtime
    except OSError:
        exporter_mtime = None

    graphs = [
        (Path(".cartography") / "module_graph.json", build_dir / "module_graph.html"),
        (Path(".cartography") / "lineage_graph.json", build_dir / "lineage_graph.html"),
    ]
    for graph_path, out_path in graphs:
        if not graph_path.exists():
            continue
        try:
            out_mtime = out_path.stat().st_mtime if out_path.exists() else 0
            in_mtime = graph_path.stat().st_mtime
            # Regenerate if the graph changed OR if the exporter template changed.
            if out_path.exists() and out_mtime >= in_mtime and (exporter_mtime is None or out_mtime >= exporter_mtime):
                continue
        except OSError:
            pass
        try:
            subprocess.run(
                [
                    sys.executable,
                    str(exporter),
                    "--graph",
                    str(graph_path),
                    "--out",
                    str(out_path),
                    "--max-nodes",
                    str(max_nodes),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        except Exception:
            # Non-fatal: the server can still run; UI will show last generated HTML.
            pass


def _refresh_graph_analysis(build_dir: Path) -> None:
    """
    Generate `build/graph_analysis/analysis_report.json` from the latest `.cartography/*.json`.

    The dashboard uses this report for quick stats and grounding context. If the report
    is missing or stale, the UI can look "empty" even when graphs exist.
    """
    module_path = Path(".cartography") / "module_graph.json"
    lineage_path = Path(".cartography") / "lineage_graph.json"
    if not module_path.exists() or not lineage_path.exists():
        return

    out_dir = build_dir / "graph_analysis"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "analysis_report.json"
    try:
        if out_path.exists():
            newest_in = max(module_path.stat().st_mtime, lineage_path.stat().st_mtime)
            if out_path.stat().st_mtime >= newest_in:
                return
    except OSError:
        pass

    try:
        repo_root = Path(__file__).resolve().parents[1]
        sys.path.insert(0, str(repo_root / "src"))
        from graph_analysis.analyzer import analyze_graph  # type: ignore
        from graph_analysis.loader import load_digraph, write_json  # type: ignore

        module_graph = load_digraph(str(module_path))
        lineage_graph = load_digraph(str(lineage_path))
        report = {
            "module_graph": analyze_graph(module_graph, critical_k=15, cycle_limit=50).to_dict(),
            "lineage_graph": analyze_graph(lineage_graph, critical_k=15, cycle_limit=50).to_dict(),
        }
        write_json(report, out_path)
    except Exception:
        # Non-fatal: dashboard can still run without the analysis report.
        return


def _is_repo_module_node(node: dict) -> bool:
    node_id = node.get("id")
    if not isinstance(node_id, str):
        return False
    # Prefer "real" code modules/files over imported symbols like `json` or `pathlib.Path`.
    if not node_id.endswith(".py"):
        return False
    return node_id.startswith(("target_repo/", "src/"))


def _answer_top_pagerank_modules() -> str | None:
    """
    Deterministic answer (no LLM) for: "top central modules by PageRank".

    This avoids ungrounded LLM responses and uses the generated artifact directly.
    """
    graph_path = Path(".cartography") / "module_graph.json"
    if not graph_path.exists():
        return None
    data = _load_json(graph_path)
    nodes = [n for n in (data.get("nodes") or []) if isinstance(n, dict) and _is_repo_module_node(n)]
    links = data.get("links") or data.get("edges") or []
    if not isinstance(links, list):
        links = []

    in_deg: dict[str, int] = {}
    out_deg: dict[str, int] = {}
    for e in links:
        if not isinstance(e, dict):
            continue
        s = e.get("source")
        t = e.get("target")
        if not isinstance(s, str) or not isinstance(t, str):
            continue
        out_deg[s] = out_deg.get(s, 0) + 1
        in_deg[t] = in_deg.get(t, 0) + 1

    ranked = sorted(
        ((float(n.get("pagerank") or 0.0), n) for n in nodes),
        key=lambda x: x[0],
        reverse=True,
    )
    top = ranked[:10]
    if not top:
        return "No module nodes with PageRank found in `.cartography/module_graph.json`."

    lines = []
    lines.append("Top 10 most central *repo modules* by PageRank (from `.cartography/module_graph.json`):")
    for i, (pr, n) in enumerate(top, start=1):
        node_id = n.get("id")
        indeg = in_deg.get(node_id, 0)
        outdeg = out_deg.get(node_id, 0)
        vel = n.get("change_velocity_30d", 0)
        cyc = bool(n.get("in_cycle"))
        lines.append(
            f"{i}. {node_id} — pagerank={pr:.6g}, in={indeg}, out={outdeg}, velocity_30d={vel}, in_cycle={cyc}"
        )

    lines.append("")
    lines.append(
        "Why these rank high (intuition): PageRank is high when many important modules depend on a module (high fan-in), "
        "and/or it sits on many dependency paths (a hub/utility/entrypoint). Use the in/out degrees above to sanity-check "
        "that centrality."
    )
    lines.append("If you want, tell me which one to inspect and I’ll summarize its upstream/downstream neighbors.")
    return "\n".join(lines)


def _answer_languages_used() -> str | None:
    graph_path = Path(".cartography") / "module_graph.json"
    if not graph_path.exists():
        return None
    data = _load_json(graph_path)
    nodes = [n for n in (data.get("nodes") or []) if isinstance(n, dict)]

    # Count language only for real file nodes (those with a path/id that looks like a file).
    counts: dict[str, int] = {}
    total = 0
    for n in nodes:
        node_id = n.get("id") or n.get("path")
        lang = n.get("language")
        if not isinstance(node_id, str) or not isinstance(lang, str):
            continue
        if not (node_id.startswith(("target_repo/", "src/")) and (node_id.endswith(".py") or node_id.endswith(".sql"))):
            continue
        counts[lang] = counts.get(lang, 0) + 1
        total += 1

    if not counts:
        return "No `language` fields found for repo file nodes in `.cartography/module_graph.json`."

    lines = []
    lines.append("Languages detected in the module graph (repo file nodes only):")
    for lang, cnt in sorted(counts.items(), key=lambda x: (-x[1], x[0])):
        pct = (cnt / total) * 100 if total else 0
        lines.append(f"- {lang}: {cnt} files ({pct:.1f}%)")
    lines.append("")
    lines.append("Evidence source: `.cartography/module_graph.json` node attribute `language`.")
    return "\n".join(lines)


def _answer_git_velocity() -> str | None:
    graph_path = Path(".cartography") / "module_graph.json"
    if not graph_path.exists():
        return None
    data = _load_json(graph_path)
    nodes = [n for n in (data.get("nodes") or []) if isinstance(n, dict) and _is_repo_module_node(n)]
    if not nodes:
        return "No repo module nodes found in `.cartography/module_graph.json`."

    # change_velocity_30d is computed by the Surveyor agent using git history.
    rows = []
    touched = 0
    for n in nodes:
        v = n.get("change_velocity_30d")
        if isinstance(v, (int, float)):
            if v > 0:
                touched += 1
            rows.append((float(v), str(n.get("id")), n.get("last_modified")))
    rows.sort(reverse=True)
    top = rows[:10]

    lines = []
    lines.append("Git velocity (30d) = number of commits touching a file in last 30 days.")
    lines.append(f"Repo python files with any changes in last 30d: {touched}")
    lines.append("")
    lines.append("Top 10 highest-velocity repo modules (from `.cartography/module_graph.json`):")
    for i, (v, node_id, last_mod) in enumerate(top, start=1):
        lines.append(f"{i}. {node_id} — change_velocity_30d={int(v)} last_modified={last_mod}")
    lines.append("")
    lines.append("Evidence source: Surveyor node attributes `change_velocity_30d` and `last_modified`.")
    return "\n".join(lines)


class ArtifactIndex:
    """
    Loads and indexes generated artifacts so chat answers can be grounded in evidence.

    Artifacts used:
    - `.cartography/module_graph.json`
    - `.cartography/lineage_graph.json`
    - `.cartography/semanticist_report.json` (optional)
    """

    def __init__(self, build_dir: Path) -> None:
        self.build_dir = build_dir
        self.module_graph = _load_json(Path(".cartography") / "module_graph.json")
        self.lineage_graph = _load_json(Path(".cartography") / "lineage_graph.json")

        semantic_path = Path(".cartography") / "semanticist_report.json"
        self.semanticist_report = _load_json(semantic_path) if semantic_path.exists() else {}

        self.module_nodes = [n for n in (self.module_graph.get("nodes") or []) if isinstance(n, dict)]
        self.module_edges = [
            e for e in (self.module_graph.get("links") or self.module_graph.get("edges") or []) if isinstance(e, dict)
        ]
        self.module_by_id = {str(n.get("id")): n for n in self.module_nodes if isinstance(n.get("id"), str)}
        self.module_in, self.module_out = self._adjacency(self.module_edges)

        self.lineage_nodes = [n for n in (self.lineage_graph.get("nodes") or []) if isinstance(n, dict)]
        self.lineage_edges = [
            e for e in (self.lineage_graph.get("links") or self.lineage_graph.get("edges") or []) if isinstance(e, dict)
        ]
        self.lineage_by_id = {
            str(n.get("id") or n.get("name")): n for n in self.lineage_nodes if (n.get("id") or n.get("name"))
        }
        self.lineage_in, self.lineage_out = self._adjacency(self.lineage_edges)
        self.lineage_out_edges = self._edge_index(self.lineage_edges)

        self.sem_by_path: dict[str, dict] = {}
        self.sem_by_name: dict[str, dict] = {}
        for rec in (self.semanticist_report.get("modules") or []):
            if not isinstance(rec, dict):
                continue
            if isinstance(rec.get("path"), str):
                self.sem_by_path[rec["path"]] = rec
            if isinstance(rec.get("module_name"), str):
                self.sem_by_name[rec["module_name"]] = rec

        self._repo_module_pagerank_top = self._top_repo_modules_by("pagerank", limit=10)
        self._repo_module_velocity_top = self._top_repo_modules_by("change_velocity_30d", limit=10)
        self._lineage_sources_top = self._top_lineage_nodes(kind_allow={"dataset", "table"}, degree="in", degree_value=0, limit=10)
        self._lineage_sinks_top = self._top_lineage_nodes(kind_allow={"dataset", "table"}, degree="out", degree_value=0, limit=10)
        self._ingestion_domain_samples = self._ingestion_modules_from_semantics(limit=12)

    @staticmethod
    def _adjacency(edges: list[dict]) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
        incoming: dict[str, set[str]] = {}
        outgoing: dict[str, set[str]] = {}
        for e in edges:
            s = e.get("source")
            t = e.get("target")
            if not isinstance(s, str) or not isinstance(t, str):
                continue
            outgoing.setdefault(s, set()).add(t)
            incoming.setdefault(t, set()).add(s)
        return incoming, outgoing

    @staticmethod
    def _edge_index(edges: list[dict]) -> dict[str, list[dict]]:
        out_edges: dict[str, list[dict]] = {}
        for e in edges:
            s = e.get("source")
            t = e.get("target")
            if not isinstance(s, str) or not isinstance(t, str):
                continue
            out_edges.setdefault(s, []).append(e)
        return out_edges

    def _top_repo_modules_by(self, field: str, limit: int = 10) -> list[dict]:
        nodes = [n for n in self.module_nodes if _is_repo_module_node(n) and isinstance(n.get(field), (int, float))]
        nodes.sort(key=lambda n: float(n.get(field) or 0.0), reverse=True)
        out = []
        for n in nodes[:limit]:
            out.append(
                {
                    "id": n.get("id"),
                    field: n.get(field),
                    "last_modified": n.get("last_modified"),
                    "in_cycle": n.get("in_cycle"),
                }
            )
        return out

    def _top_lineage_nodes(
        self,
        kind_allow: set[str] | None,
        degree: str,
        degree_value: int,
        limit: int = 10,
    ) -> list[dict]:
        out: list[dict] = []
        for node_id, n in self.lineage_by_id.items():
            kind = n.get("kind")
            if kind_allow and kind not in kind_allow:
                continue
            deg = len(self.lineage_in.get(node_id, set())) if degree == "in" else len(self.lineage_out.get(node_id, set()))
            if deg != degree_value:
                continue
            out.append({"id": node_id, "kind": kind, "source_files": n.get("source_files")})
        out.sort(key=lambda x: str(x.get("id")))
        return out[:limit]

    def _ingestion_modules_from_semantics(self, limit: int = 12) -> list[dict]:
        mods = []
        for rec in (self.semanticist_report.get("modules") or []):
            if not isinstance(rec, dict):
                continue
            domain = str(rec.get("domain") or "").lower()
            if "ingest" not in domain:
                continue
            mods.append({"path": rec.get("path"), "module_name": rec.get("module_name"), "domain": rec.get("domain"), "purpose": rec.get("purpose")})
        return mods[:limit]

    def _descendant_count(self, start: str, max_visits: int = 2000) -> int:
        seen = set()
        stack = [start]
        while stack and len(seen) < max_visits:
            cur = stack.pop()
            for nxt in self.lineage_out.get(cur, set()):
                if nxt in seen:
                    continue
                seen.add(nxt)
                stack.append(nxt)
        return len(seen)

    def primary_ingestion_path(self, max_sources: int = 3) -> str:
        """
        Best-effort ingestion path summary from the lineage graph.

        Heuristic: treat dataset/table nodes with in_degree==0 as "sources", then show their immediate
        downstream edges and rank by descendant reach.
        """
        candidates = []
        for src in self._lineage_sources_top:
            src_id = src.get("id")
            if not isinstance(src_id, str):
                continue
            reach = self._descendant_count(src_id)
            candidates.append((reach, src))
        candidates.sort(key=lambda x: x[0], reverse=True)
        top = candidates[:max_sources]

        lines = []
        lines.append("Primary ingestion path (best-effort from `.cartography/lineage_graph.json`):")
        if not top:
            lines.append("- No dataset/table sources (in_degree==0) found in the lineage graph.")
        for reach, src in top:
            src_id = src["id"]
            lines.append(f"- Source: `{src_id}` (kind={src.get('kind')}) downstream_reach≈{reach}")
            out_edges = self.lineage_out_edges.get(src_id, [])[:6]
            if not out_edges:
                # Still show first-level neighbors if present.
                neigh = sorted(self.lineage_out.get(src_id, set()))[:6]
                if neigh:
                    lines.append(f"  -> {', '.join(f'`{n}`' for n in neigh)}")
                continue
            for e in out_edges:
                tgt = e.get('target')
                tf = e.get('transformation_type')
                sf = e.get('source_file')
                if isinstance(tgt, str):
                    lines.append(f"  -> `{tgt}` via `{tf}` (source_file={sf})")

        if self._ingestion_domain_samples:
            lines.append("")
            lines.append("Semanticist ingestion-domain samples (from `.cartography/semanticist_report.json`):")
            for rec in self._ingestion_domain_samples[:6]:
                name = rec.get("module_name") or rec.get("path")
                lines.append(f"- `{name}`: {rec.get('purpose')}")

        lines.append("")
        lines.append("Tip: click a node in the graph first, then ask 'show upstream/downstream' to drill in.")
        return "\n".join(lines)
    def _semantic_for_module_node(self, node: dict) -> dict | None:
        path = node.get("path")
        node_id = node.get("id")
        if isinstance(path, str) and path in self.sem_by_path:
            return self.sem_by_path[path]
        if isinstance(node_id, str) and node_id in self.sem_by_name:
            return self.sem_by_name[node_id]
        # semanticist stores absolute paths; try suffix match
        if isinstance(node_id, str):
            for p, rec in self.sem_by_path.items():
                if p.endswith(node_id) or node_id.endswith(p):
                    return rec
        return None

    def _tokenize(self, text: str) -> list[str]:
        return [t for t in re.split(r"[^a-zA-Z0-9_./:-]+", (text or "").lower()) if t]

    def search_module_nodes(self, query: str, limit: int = 6) -> list[dict]:
        tokens = self._tokenize(query)
        if not tokens:
            return []
        scored: list[tuple[int, dict]] = []
        for n in self.module_nodes:
            node_id = str(n.get("id") or "").lower()
            path = str(n.get("path") or "").lower()
            blob = node_id + " " + path
            score = sum(2 for t in tokens if t in blob)
            if score:
                scored.append((score, n))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [n for _, n in scored[:limit]]

    def search_lineage_nodes(self, query: str, limit: int = 6) -> list[dict]:
        tokens = self._tokenize(query)
        if not tokens:
            return []
        scored: list[tuple[int, dict]] = []
        for n in self.lineage_nodes:
            node_id = str(n.get("id") or n.get("name") or "").lower()
            kind = str(n.get("kind") or "").lower()
            blob = node_id + " " + kind
            score = sum(2 for t in tokens if t in blob)
            if score:
                scored.append((score, n))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [n for _, n in scored[:limit]]

    def evidence_for(self, question: str, focus: dict | None) -> str:
        parts: list[str] = []

        parts.append(
            f"MODULE_GRAPH: nodes={len(self.module_nodes)} edges={len(self.module_edges)} cycles={len(self.module_graph.get('cycles') or []) if isinstance(self.module_graph.get('cycles'), list) else 0}"
        )
        parts.append(
            f"LINEAGE_GRAPH: nodes={len(self.lineage_nodes)} edges={len(self.lineage_edges)} cycles={len(self.lineage_graph.get('data_cycles') or []) if isinstance(self.lineage_graph.get('data_cycles'), list) else 0}"
        )

        if self.semanticist_report:
            parts.append(f"SEMANTICIST: modules={len(self.semanticist_report.get('modules') or [])} domains={len(self.semanticist_report.get('domains') or {})}")
        else:
            parts.append("SEMANTICIST: missing (run `python3 src/cli.py semantic` to generate `.cartography/semanticist_report.json`).")

        # Always include a few anchor facts so broad questions (e.g., "primary ingestion path")
        # have concrete evidence even without a node-specific search hit.
        if self._repo_module_pagerank_top:
            parts.append("")
            parts.append("TOP_REPO_MODULES_PAGERANK:")
            parts.append(_short_json(self._repo_module_pagerank_top, 1800))
        if self._repo_module_velocity_top:
            parts.append("")
            parts.append("TOP_REPO_MODULES_VELOCITY_30D:")
            parts.append(_short_json(self._repo_module_velocity_top, 1800))
        if self._lineage_sources_top:
            parts.append("")
            parts.append("LINEAGE_SOURCES (dataset/table with in_degree==0):")
            parts.append(_short_json(self._lineage_sources_top, 1800))
        if self._lineage_sinks_top:
            parts.append("")
            parts.append("LINEAGE_SINKS (dataset/table with out_degree==0):")
            parts.append(_short_json(self._lineage_sinks_top, 1800))
        if self._ingestion_domain_samples:
            parts.append("")
            parts.append("SEMANTICIST_INGESTION_DOMAIN_SAMPLES:")
            parts.append(_short_json(self._ingestion_domain_samples, 1800))

        # Focus context from UI click.
        if isinstance(focus, dict):
            node_id = focus.get("node_id")
            if isinstance(node_id, str) and node_id in self.module_by_id:
                n = self.module_by_id[node_id]
                parts.append("")
                parts.append("FOCUS_MODULE:")
                parts.append(
                    _short_json(
                        {
                            "id": n.get("id"),
                            "path": n.get("path"),
                            "pagerank": n.get("pagerank"),
                            "change_velocity_30d": n.get("change_velocity_30d"),
                            "in_cycle": n.get("in_cycle"),
                            "dead_exports": n.get("dead_exports"),
                            "entrypoint_exports": n.get("entrypoint_exports"),
                            "framework_exports": n.get("framework_exports"),
                        },
                        2500,
                    )
                )
                parts.append(
                    _short_json(
                        {
                            "upstream": sorted(self.module_in.get(node_id, set()))[:40],
                            "downstream": sorted(self.module_out.get(node_id, set()))[:40],
                        },
                        2500,
                    )
                )
                sem = self._semantic_for_module_node(n)
                if sem:
                    parts.append("FOCUS_SEMANTIC:")
                    parts.append(_short_json(sem, 3000))
            elif isinstance(node_id, str) and node_id in self.lineage_by_id:
                n = self.lineage_by_id[node_id]
                parts.append("")
                parts.append("FOCUS_LINEAGE:")
                parts.append(_short_json({"id": node_id, "kind": n.get("kind"), "source_files": n.get("source_files")}, 2500))
                parts.append(
                    _short_json(
                        {
                            "upstream": sorted(self.lineage_in.get(node_id, set()))[:40],
                            "downstream": sorted(self.lineage_out.get(node_id, set()))[:40],
                        },
                        2500,
                    )
                )

        # Search results for the question.
        rel_mod = self.search_module_nodes(question, limit=6)
        if rel_mod:
            parts.append("")
            parts.append("RELATED_MODULES:")
            for n in rel_mod:
                parts.append(_short_json({"id": n.get("id"), "path": n.get("path"), "pagerank": n.get("pagerank"), "in_cycle": n.get("in_cycle")}, 900))
                sem = self._semantic_for_module_node(n)
                if sem:
                    parts.append(_short_json({"purpose": sem.get("purpose"), "domain": sem.get("domain"), "docstring_flag": sem.get("docstring_flag")}, 900))

        rel_lin = self.search_lineage_nodes(question, limit=6)
        if rel_lin:
            parts.append("")
            parts.append("RELATED_LINEAGE:")
            for n in rel_lin:
                parts.append(_short_json({"id": n.get("id") or n.get("name"), "kind": n.get("kind")}, 700))

        return "\n".join(parts)


def _load_dotenv(dotenv_path: Path) -> None:
    """
    Minimal .env loader (no external dependency).

    Loads KEY=VALUE pairs into os.environ if not already set.
    """
    if not dotenv_path.exists():
        return
    try:
        for raw_line in dotenv_path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
    except Exception:
        return


def _ollama_chat(ollama_host: str, model: str, messages: list[dict[str, str]], context: str) -> str:
    payload = {
        "model": model,
        "stream": False,
        "messages": [{"role": "system", "content": context}, *messages],
        "options": {"temperature": 0.2},
    }
    url = ollama_host.rstrip("/") + "/api/chat"
    raw = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    api_key = os.environ.get("OLLAMA_API_KEY", "")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    req = Request(url, data=raw, headers=headers, method="POST")
    try:
        with urlopen(req, timeout=180) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        raise RuntimeError(f"Ollama error {exc.code}: {exc.read().decode('utf-8', errors='replace')}") from exc
    except URLError as exc:
        raise RuntimeError(f"Could not reach Ollama at {ollama_host}. Is it running?") from exc

    message = (data.get("message") or {}).get("content")
    if not isinstance(message, str):
        raise RuntimeError("Unexpected Ollama response format.")
    return message


def _openrouter_chat(
    openrouter_host: str,
    api_key: str,
    model: str,
    messages: list[dict[str, str]],
    context: str,
    *,
    max_tokens: int = 800,
) -> str:
    """
    OpenRouter-compatible Chat Completions API.
    """
    url = openrouter_host.rstrip("/") + "/api/v1/chat/completions"
    payload = {
        "model": model,
        "messages": [{"role": "system", "content": context}, *messages],
        "temperature": 0.2,
        # Keep this modest to avoid OpenRouter 402 errors on low-credit accounts.
        "max_tokens": max_tokens,
    }
    raw = json.dumps(payload).encode("utf-8")
    req = Request(
        url,
        data=raw,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
            # These are recommended by OpenRouter; harmless if ignored.
            "HTTP-Referer": "http://127.0.0.1",
            "X-Title": "codebase-cartography",
        },
        method="POST",
    )
    try:
        with urlopen(req, timeout=180) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        raise RuntimeError(f"OpenRouter error {exc.code}: {exc.read().decode('utf-8', errors='replace')}") from exc
    except URLError as exc:
        raise RuntimeError(f"Could not reach OpenRouter at {openrouter_host}.") from exc

    try:
        return data["choices"][0]["message"]["content"]
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("Unexpected OpenRouter response format.") from exc


class Handler(SimpleHTTPRequestHandler):
    def __init__(
        self,
        *args,
        directory: str | None = None,
        provider: str = "ollama",
        ollama_host: str = "",
        openrouter_host: str = "",
        openrouter_api_key: str = "",
        openrouter_max_tokens: int = 800,
        artifacts: ArtifactIndex | None = None,
        navigator: object | None = None,
        context: str = "",
        **kwargs,
    ):
        self.provider = provider
        self.ollama_host = ollama_host
        self.openrouter_host = openrouter_host
        self.openrouter_api_key = openrouter_api_key
        self.openrouter_max_tokens = openrouter_max_tokens
        self.artifacts = artifacts
        self.navigator = navigator
        self.context = context
        super().__init__(*args, directory=directory, **kwargs)

    def _json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def end_headers(self) -> None:
        # Avoid stale UI when graphs are regenerated but the browser caches HTML/JS.
        self.send_header("Cache-Control", "no-store")
        return super().end_headers()

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/api/version":
            # Debug endpoint: confirm which dashboard HTML is being served.
            try:
                index_path = Path(self.directory or "build") / "index.html"
                raw = index_path.read_text(encoding="utf-8", errors="ignore") if index_path.exists() else ""
                version = ""
                m = re.search(r"CARTOGRAPHY_DASHBOARD_VERSION\\s*=\\s*\\\"(?P<v>[^\\\"]+)\\\"", raw)
                if m:
                    version = m.group("v")
                self._json(
                    200,
                    {
                        "ok": True,
                        "served_directory": str(self.directory or ""),
                        "index_exists": index_path.exists(),
                        "index_mtime": index_path.stat().st_mtime if index_path.exists() else None,
                        "dashboard_version": version or None,
                    },
                )
            except Exception as exc:
                self._json(500, {"ok": False, "error": str(exc)})
            return
        if self.path == "/api/ping":
            art = self.artifacts
            self._json(
                200,
                {
                    "ok": True,
                    "provider": self.provider,
                    "artifacts": {
                        "module_nodes": len(art.module_nodes) if art else 0,
                        "module_edges": len(art.module_edges) if art else 0,
                        "lineage_nodes": len(art.lineage_nodes) if art else 0,
                        "lineage_edges": len(art.lineage_edges) if art else 0,
                        "has_semanticist_report": bool(art.semanticist_report) if art else False,
                    },
                },
            )
            return
        # If the embedded graph HTML is requested, refresh it if artifacts changed.
        # This makes the dashboard update even if the server wasn't restarted.
        if self.path.split("?", 1)[0].endswith(("module_graph.html", "lineage_graph.html")):
            try:
                _refresh_embedded_graph_html(Path(self.directory or "build"))
            except Exception:
                pass
        return super().do_GET()

    def do_POST(self) -> None:  # noqa: N802
        if self.path == "/api/navigate":
            try:
                length = int(self.headers.get("Content-Length") or "0")
                raw = self.rfile.read(length)
                req = json.loads(raw.decode("utf-8"))
                # Accept either:
                # - { "query": "..." }
                # - { "tool": "...", "args": {...} }
                query = str(req.get("query") or "").strip()
                tool = str(req.get("tool") or "").strip()
                args = req.get("args") or {}
                if args and not isinstance(args, dict):
                    raise ValueError("args must be a JSON object")

                nav = self.navigator
                if nav is None:
                    # Import Navigator from src (same pattern as export scripts).
                    repo_root = Path(__file__).resolve().parents[1]
                    sys.path.insert(0, str(repo_root / "src"))
                    from agents.navigator import Navigator  # type: ignore

                    nav = Navigator()
                # Keep a long-lived Navigator in sync with on-disk artifacts.
                if hasattr(nav, "refresh_artifacts"):
                    try:
                        nav.refresh_artifacts()  # type: ignore[attr-defined]
                    except Exception:
                        pass
                if tool:
                    out = nav.run_tool(tool, args if isinstance(args, dict) else {})  # type: ignore[attr-defined]
                else:
                    if not query:
                        raise ValueError("Missing query (or tool).")
                    out = nav.answer(query)  # type: ignore[attr-defined]
                self._json(200, out)
            except Exception as exc:
                self._json(500, {"error": str(exc)})
            return

        if self.path != "/api/chat":
            self._json(404, {"error": "not found"})
            return

        try:
            length = int(self.headers.get("Content-Length") or "0")
            raw = self.rfile.read(length)
            req = json.loads(raw.decode("utf-8"))
            model = str(req.get("model") or ("mistral:latest" if self.provider == "ollama" else "openai/gpt-4o-mini"))
            messages = req.get("messages") or []
            if not isinstance(messages, list):
                raise ValueError("messages must be a list")
            messages = [{"role": m.get("role", "user"), "content": m.get("content", "")} for m in messages if isinstance(m, dict)]
            focus = req.get("focus") if isinstance(req, dict) else None

            # Deterministic, graph-grounded answers for common questions.
            last_user = next((m.get("content", "") for m in reversed(messages) if m.get("role") == "user"), "")
            if isinstance(last_user, str):
                q = last_user.lower()
                if "ingestion" in q and ("primary" in q or "path" in q):
                    if self.artifacts:
                        self._json(200, {"content": self.artifacts.primary_ingestion_path()})
                        return
                if ("pagerank" in q or "page rank" in q) and ("top 10" in q or "top10" in q) and ("module" in q):
                    answer = _answer_top_pagerank_modules()
                    if answer:
                        self._json(200, {"content": answer})
                        return
                if "language" in q and ("used" in q or "use" in q or "type" in q):
                    answer = _answer_languages_used()
                    if answer:
                        self._json(200, {"content": answer})
                        return
                if "git velocity" in q or ("change velocity" in q) or ("velocity" in q and "30" in q):
                    answer = _answer_git_velocity()
                    if answer:
                        self._json(200, {"content": answer})
                        return

            # General grounding: attach evidence from artifacts for *every* question.
            if self.artifacts and isinstance(last_user, str) and last_user.strip():
                evidence = self.artifacts.evidence_for(last_user, focus if isinstance(focus, dict) else None)
                grounded_system = (
                    self.context
                    + "\n\nYou must answer using ONLY the EVIDENCE below. "
                    + "If the evidence is insufficient, say exactly what artifact is missing and how to generate it.\n\n"
                    + "When naming modules/datasets, use exact node ids/paths from EVIDENCE.\n\n"
                    + "EVIDENCE:\n"
                    + evidence
                )
                # Replace any existing system message with our grounded one.
                filtered = [m for m in messages if m.get("role") != "system"]
                messages = [{"role": "system", "content": grounded_system}, *filtered]

            if self.provider == "openrouter":
                if not self.openrouter_api_key:
                    raise RuntimeError("Missing OpenRouter API key. Set it in `.env` as `openRoute=...` (or pass --openrouter-key-env).")
                if "/" not in model:
                    raise RuntimeError(
                        f"OpenRouter provider requires an OpenRouter model id (e.g. 'openai/gpt-4o-mini'), got: {model!r}. "
                        "Pick an 'OpenRouter: ...' option in the dropdown."
                    )
                content = _openrouter_chat(
                    self.openrouter_host,
                    self.openrouter_api_key,
                    model,
                    messages,
                    context=self.context,
                    max_tokens=self.openrouter_max_tokens,
                )
            else:
                content = _ollama_chat(self.ollama_host, model, messages, context=self.context)
            self._json(200, {"content": content})
        except Exception as exc:
            self._json(500, {"error": str(exc)})


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serve the Cartography dashboard + Ollama-backed chat.")
    parser.add_argument("--build-dir", default="build", help="Directory containing index.html and graph viewers")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument(
        "--refresh-graphs",
        action="store_true",
        default=True,
        help="Regenerate build/*_graph.html from latest `.cartography/*.json` on startup.",
    )
    parser.add_argument(
        "--no-refresh-graphs",
        dest="refresh_graphs",
        action="store_false",
        help="Do not regenerate build/*_graph.html on startup.",
    )
    parser.add_argument(
        "--max-nodes",
        type=int,
        default=800,
        help="Max nodes when regenerating embedded graph HTML (default: 800).",
    )
    parser.add_argument("--provider", choices=["ollama", "openrouter"], default="openrouter", help="Chat and Navigator LLM provider (default: openrouter)")
    # Prefer 127.0.0.1 over localhost to avoid IPv6 ::1 resolution issues when Ollama
    # is bound to 127.0.0.1 only (common on Linux).
    parser.add_argument("--ollama-host", default="http://127.0.0.1:11434")
    parser.add_argument("--openrouter-host", default="https://openrouter.ai")
    parser.add_argument("--openrouter-key-env", default="openRoute", help="Env var name holding your OpenRouter API key (default: openRoute)")
    parser.add_argument("--openrouter-max-tokens", type=int, default=800, help="Max tokens to request from OpenRouter (default: 800)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    build_dir = Path(args.build_dir)
    if args.refresh_graphs:
        _refresh_embedded_graph_html(build_dir, max_nodes=args.max_nodes)
    _refresh_graph_analysis(build_dir)
    context = _build_context(build_dir)
    artifacts = ArtifactIndex(build_dir)

    _load_dotenv(Path(".env"))
    openrouter_key = os.environ.get(args.openrouter_key_env, "")

    # Preload Navigator once to keep /api/navigate snappy.
    try:
        repo_root = Path(__file__).resolve().parents[1]
        sys.path.insert(0, str(repo_root / "src"))
        from agents.navigator import Navigator  # type: ignore
        from agents.semanticist import OpenRouterHttpClient  # type: ignore

        llm_client = None
        if openrouter_key:
            llm_client = OpenRouterHttpClient(api_key=openrouter_key, base_url=args.openrouter_host, timeout_s=120)
        navigator = Navigator(llm_client=llm_client, llm_model=os.environ.get("NAVIGATOR_OPENROUTER_MODEL", "openai/gpt-4o-mini"))
    except Exception:
        navigator = None

    def handler(*h_args, **h_kwargs):
        return Handler(
            *h_args,
            directory=str(build_dir),
            provider=args.provider,
            ollama_host=args.ollama_host,
            openrouter_host=args.openrouter_host,
            openrouter_api_key=openrouter_key,
            context=context,
            openrouter_max_tokens=args.openrouter_max_tokens,
            artifacts=artifacts,
            navigator=navigator,
            **h_kwargs,
        )

    httpd = ThreadingHTTPServer((args.host, args.port), handler)
    print(f"Serving dashboard on http://{args.host}:{args.port}/")
    if args.provider == "openrouter":
        print(f"Chat proxy: /api/chat -> OpenRouter ({args.openrouter_host})")
        print(f"OpenRouter key env: {args.openrouter_key_env} ({'set' if openrouter_key else 'missing'})")
    else:
        print(f"Chat proxy: /api/chat -> {args.ollama_host}")
    httpd.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
