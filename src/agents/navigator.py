from __future__ import annotations

"""
Navigator (Phase 4): query-time agent with evidence citations.

This module provides:
- A lightweight, dependency-free "LangGraph-style" agent that can answer questions
  using existing Cartographer artifacts (.cartography/*.json + CODEBASE.md).
- Every answer returns explicit evidence blocks: file path, line range, method, confidence.
- Every query/answer step is logged via `cartography_trace.jsonl`.

If `langgraph` is installed, `build_langgraph_agent()` will create a graph-backed
runner. If it is not installed, the Navigator still works via `Navigator.answer()`.
"""

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cartography_trace import log_cartography_trace
from graph.knowledge_graph import read_graph_json


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class Evidence:
    evidence_source: str
    line_range: list[int] | None
    method: str
    confidence: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "evidence_source": self.evidence_source,
            "line_range": self.line_range,
            "method": self.method,
            "confidence": self.confidence,
        }


class Navigator:
    """
    Artifact-backed Q&A agent.

    The Navigator does *not* assume access to the raw repo at query time; it answers
    from artifacts and cites them. For any claim about a module, it includes:
    - module path (from module graph / semantic report)
    - line range (from Semanticist `evidence_symbols` when available)
    - method: `static_analysis` or `graph_traversal`
    """

    def __init__(
        self,
        *,
        repo_root: Path = Path("."),
        module_graph_path: Path = Path(".cartography/module_graph.json"),
        lineage_graph_path: Path = Path(".cartography/lineage_graph.json"),
        semanticist_report_path: Path = Path(".cartography/semanticist_report.json"),
        codebase_md_path: Path = Path("CODEBASE.md"),
        llm_client: Any | None = None,
        llm_model: str = "openai/gpt-4o-mini",
    ) -> None:
        self.repo_root = Path(repo_root)
        self.module_graph_path = Path(module_graph_path)
        self.lineage_graph_path = Path(lineage_graph_path)
        self.semanticist_report_path = Path(semanticist_report_path)
        self.codebase_md_path = Path(codebase_md_path)

        self.module_graph = read_graph_json(self.module_graph_path) if self.module_graph_path.exists() else None
        self.lineage_graph = read_graph_json(self.lineage_graph_path) if self.lineage_graph_path.exists() else None
        self.semantic = json.loads(self.semanticist_report_path.read_text(encoding="utf-8")) if self.semanticist_report_path.exists() else {}
        self._artifact_mtime: dict[str, float] = {}
        self._semantic_search_blob: dict[str, str] = {}
        self._semanticist_size_bytes: int | None = None

        # Optional LLM client (e.g., OpenRouterHttpClient). Used only for
        # generative rephrasing when requested; Navigator always returns evidence.
        self.llm_client = llm_client
        self.llm_model = llm_model

        self.records = {
            m.get("module_name"): m
            for m in (self.semantic.get("modules") or [])
            if isinstance(m, dict) and isinstance(m.get("module_name"), str)
        }
        self._rebuild_semantic_index()

        # Build adjacency once for tool queries.
        self._module_in: dict[str, list[str]] = {}
        self._module_out: dict[str, list[str]] = {}
        if self.module_graph is not None:
            for s, t in self.module_graph.edges():
                self._module_out.setdefault(str(s), []).append(str(t))
                self._module_in.setdefault(str(t), []).append(str(s))

        self._lineage_in: dict[str, list[dict[str, Any]]] = {}
        self._lineage_out: dict[str, list[dict[str, Any]]] = {}
        if self.lineage_graph is not None:
            for s, t, attrs in self.lineage_graph.edges(data=True):
                rec = {"source": str(s), "target": str(t), **(attrs or {})}
                self._lineage_out.setdefault(str(s), []).append(rec)
                self._lineage_in.setdefault(str(t), []).append(rec)

    def _resolve_lineage_node(self, dataset: str) -> tuple[str | None, list[str]]:
        """
        Resolve a user-provided dataset name to an actual node id in the lineage graph.

        Tries exact match, then suffix match (e.g. "orders" matches "raw.raw_orders"
        or "schema.orders"). Returns (resolved_id, known_table_nodes) so callers can
        suggest valid names when the dataset is not found.
        """
        dataset = dataset.strip()
        if not dataset or self.lineage_graph is None:
            return None, []

        # Collect table/dataset node ids for hints.
        known: list[str] = []
        candidates: list[str] = []
        needle = dataset.lower()
        for nid in self.lineage_graph.nodes():
            node_id = str(nid)
            attrs = self.lineage_graph.nodes.get(nid) or {}
            kind = attrs.get("kind")
            if kind in ("table", "dataset"):
                known.append(node_id)
            # Exact match.
            if node_id.lower() == needle:
                return node_id, known
            # Suffix or substring match (e.g. "daily_active_users" -> "analytics.daily_active_users").
            nid_l = node_id.lower()
            if needle in nid_l or nid_l.endswith(needle) or (needle.endswith(nid_l) and nid_l):
                candidates.append(node_id)
        if candidates:
            # Prefer exact suffix match, then shortest (most specific) id.
            candidates.sort(key=lambda x: (0 if x.lower().endswith(needle) else 1, len(x)))
            return candidates[0], known
        return None, known

    def _rebuild_semantic_index(self) -> None:
        """
        Build a lowercase searchable blob per module for fast keyword search.

        This avoids recomputing concatenated strings on every query.
        """
        blobs: dict[str, str] = {}
        for module_name, rec in self.records.items():
            try:
                domain = str(rec.get("domain") or "")
                purpose = str(rec.get("purpose") or "")
                blobs[module_name] = f"{domain} {purpose} {module_name}".lower()
            except Exception:
                blobs[module_name] = module_name.lower()
        self._semantic_search_blob = blobs

    def _resolve_module_record(self, path: str) -> tuple[str | None, dict[str, Any] | None]:
        """
        Best-effort lookup of a Semanticist record for a given module path.

        Handles common mismatches between absolute vs. repo-relative paths and
        suffix-only paths (e.g., asking about `src/foo.py` when the report
        stored `/abs/path/to/src/foo.py`).
        """
        path = path.strip()
        if not path:
            return None, None

        # 1) Direct hit by module_name.
        rec = self.records.get(path)
        if rec:
            return path, rec

        # 2) Try absolute/relative normalization against repo_root.
        try:
            p = Path(path)
            if p.is_absolute():
                rel = os.path.relpath(p, self.repo_root)
                rec = self.records.get(rel)
                if rec:
                    return rel, rec
            else:
                abs_p = (self.repo_root / p).resolve()
                abs_s = str(abs_p)
                rec = self.records.get(abs_s)
                if rec:
                    return abs_s, rec
        except Exception:
            pass

        # 3) Fallback: suffix match on module_name.
        for module_name, rec in self.records.items():
            try:
                if module_name.endswith(path) or path.endswith(module_name):
                    return module_name, rec
            except Exception:
                continue

        return None, None

    def _canonical_module_id(self, module_path: str) -> str:
        """
        Map a user-supplied module path to the canonical node id used in the
        module graph, handling absolute/relative/suffix-only mismatches.
        """
        module_path = module_path.strip()
        if not self.module_graph or not module_path:
            return module_path

        # Fast path: exact match on a node path attribute.
        for node_id, attrs in self.module_graph.nodes(data=True):
            path = str(attrs.get("path") or node_id)
            if path == module_path:
                return path

        # Try to resolve via repo_root.
        try:
            p = Path(module_path)
            candidates: list[str] = []
            for node_id, attrs in self.module_graph.nodes(data=True):
                path = str(attrs.get("path") or node_id)
                try:
                    np = Path(path)
                    if np.is_absolute() and p.is_absolute():
                        if np.resolve() == p.resolve():
                            return path
                    # Suffix-based heuristic (handles repo-relative vs. absolute).
                    if path.endswith(module_path) or module_path.endswith(path):
                        candidates.append(path)
                except Exception:
                    continue
            if candidates:
                # Deterministic choice: shortest path string first.
                candidates.sort(key=len)
                return candidates[0]
        except Exception:
            pass

        return module_path

    def refresh_artifacts(self) -> None:
        """
        Reload on-disk artifacts.

        The dashboard server may keep a long-lived Navigator instance. When you rerun
        Surveyor/Hydrologist/Semanticist, the underlying `.cartography/*.json` files
        change; this method re-reads them so answers reflect the latest state.
        """
        def _maybe_reload_graph(path: Path, current):
            key = str(path)
            if not path.exists():
                self._artifact_mtime.pop(key, None)
                return None
            try:
                mtime = path.stat().st_mtime
            except OSError:
                return current
            if self._artifact_mtime.get(key) == mtime and current is not None:
                return current
            self._artifact_mtime[key] = mtime
            return read_graph_json(path)

        self.module_graph = _maybe_reload_graph(self.module_graph_path, self.module_graph)
        self.lineage_graph = _maybe_reload_graph(self.lineage_graph_path, self.lineage_graph)

        # Semanticist report can be large; only reload if it changed.
        key = str(self.semanticist_report_path)
        if not self.semanticist_report_path.exists():
            self.semantic = {}
            self.records = {}
            self._semantic_search_blob = {}
            self._artifact_mtime.pop(key, None)
            self._semanticist_size_bytes = None
        else:
            try:
                mtime = self.semanticist_report_path.stat().st_mtime
                size = self.semanticist_report_path.stat().st_size
            except OSError:
                mtime = None
                size = None
            if mtime is None or self._artifact_mtime.get(key) != mtime:
                self.semantic = json.loads(self.semanticist_report_path.read_text(encoding="utf-8"))
                self._artifact_mtime[key] = float(mtime or 0.0)
                self._semanticist_size_bytes = int(size) if isinstance(size, int) else None

        self.records = {
            m.get("module_name"): m
            for m in (self.semantic.get("modules") or [])
            if isinstance(m, dict) and isinstance(m.get("module_name"), str)
        }
        self._rebuild_semantic_index()

        self._module_in = {}
        self._module_out = {}
        if self.module_graph is not None:
            for s, t in self.module_graph.edges():
                self._module_out.setdefault(str(s), []).append(str(t))
                self._module_in.setdefault(str(t), []).append(str(s))

        self._lineage_in = {}
        self._lineage_out = {}
        if self.lineage_graph is not None:
            for s, t, attrs in self.lineage_graph.edges(data=True):
                rec = {"source": str(s), "target": str(t), **(attrs or {})}
                self._lineage_out.setdefault(str(s), []).append(rec)
                self._lineage_in.setdefault(str(t), []).append(rec)

    def _line_range_for(self, module_name: str) -> list[int] | None:
        rec = self.records.get(module_name) or {}
        ev = rec.get("evidence_symbols") or []
        starts = [s.get("lineno") for s in ev if isinstance(s, dict) and isinstance(s.get("lineno"), int)]
        ends = [s.get("end_lineno") for s in ev if isinstance(s, dict) and isinstance(s.get("end_lineno"), int)]
        if starts and ends:
            return [min(starts), max(ends)]
        return None

    def _top_pagerank_modules(self, k: int = 5) -> list[tuple[str, float]]:
        if self.module_graph is None:
            return []
        items: list[tuple[str, float]] = []
        for node_id, attrs in self.module_graph.nodes(data=True):
            path = str(attrs.get("path") or node_id)
            if not path.endswith(".py"):
                continue
            rel = os.path.relpath(path, self.repo_root) if Path(path).is_absolute() else path
            items.append((rel, float(attrs.get("pagerank") or 0.0)))
        items.sort(key=lambda x: x[1], reverse=True)
        return items[:k]

    def _search_semantics(self, needle: str, limit: int = 8) -> list[str]:
        needle_l = needle.lower().strip()
        if not needle_l:
            return []

        # Token-based scoring is more robust than strict substring checks
        # (e.g., "dbt model staging logic" should match "dbt staging model ...").
        tokens = [t for t in re.findall(r"[a-z0-9_]+", needle_l) if len(t) >= 3]
        stop = {"where", "what", "main", "code", "logic", "implemented", "implementation"}
        tokens = [t for t in tokens if t not in stop]

        scored: list[tuple[int, str]] = []
        for module_name, blob in self._semantic_search_blob.items():
            if needle_l in blob:
                scored.append((100, module_name))
                continue
            if not tokens:
                continue
            score = sum(1 for t in tokens if t in blob)
            if score > 0:
                scored.append((score, module_name))

        scored.sort(key=lambda x: (-x[0], x[1]))
        return [m for _, m in scored[:limit]]

    # -----------------------
    # Tooling (Phase 4)
    # -----------------------

    def find_implementation(self, concept: str, *, limit: int = 8) -> dict[str, Any]:
        """
        Tool: semantic search for an implementation concept.

        Evidence source:
        - `.cartography/semanticist_report.json` (module_name + purpose + domain + evidence_symbols)
        """
        concept = concept.strip()
        hits = self._search_semantics(concept, limit=limit)
        evidence: list[Evidence] = [Evidence(evidence_source=str(self.semanticist_report_path), line_range=None, method="static_analysis", confidence=1.0)]
        items: list[dict[str, Any]] = []
        for module_name in hits:
            rec = self.records.get(module_name) or {}
            lr = self._line_range_for(module_name)
            evidence.append(Evidence(evidence_source=module_name, line_range=lr, method="static_analysis", confidence=0.85))
            items.append({"path": module_name, "domain": rec.get("domain"), "purpose": rec.get("purpose")})

        # Fallback for dbt-heavy repos: search lineage nodes (tables/datasets) by id substring.
        lineage_items: list[dict[str, Any]] = []
        if not items and self.lineage_graph is not None:
            needle = concept.lower()
            for nid, attrs in self.lineage_graph.nodes(data=True):
                node_id = str(attrs.get("id") or attrs.get("name") or nid)
                if needle not in node_id.lower():
                    continue
                kind = attrs.get("kind")
                if kind not in {"dataset", "table"}:
                    continue
                lineage_items.append(
                    {
                        "dataset_id": node_id,
                        "kind": kind,
                        "source_files": attrs.get("source_files") if isinstance(attrs.get("source_files"), list) else [],
                    }
                )
                if len(lineage_items) >= limit:
                    break
            if lineage_items:
                evidence.append(Evidence(evidence_source=str(self.lineage_graph_path), line_range=None, method="graph_traversal", confidence=0.9))

        log_cartography_trace(
            {
                "agent": "Navigator",
                "action": "find_implementation",
                "evidence_source": str(self.semanticist_report_path),
                "line_range": None,
                "method": "static_analysis",
                "confidence": 0.85 if items else 0.6,
                "timestamp": _utc_now_iso(),
            }
        )
        return {"concept": concept, "results": items, "lineage_matches": lineage_items, "evidence": [e.to_dict() for e in evidence]}

    def trace_lineage(self, dataset: str, direction: str, *, depth: int = 2, limit: int = 30) -> dict[str, Any]:
        """
        Tool: trace data lineage upstream/downstream in the lineage graph.

        direction:
        - "upstream": what produces this dataset
        - "downstream": what this dataset affects

        Evidence source:
        - `.cartography/lineage_graph.json` (nodes/edges + `source_file` attributes)
        """
        if self.lineage_graph is None:
            out = {"error": "Missing lineage graph artifact.", "evidence": [Evidence(str(self.lineage_graph_path), None, "static_analysis", 1.0).to_dict()]}
            return out

        direction = direction.strip().lower()
        if direction not in {"upstream", "downstream"}:
            raise ValueError("direction must be 'upstream' or 'downstream'")

        start = dataset.strip()
        resolved_node, known_nodes = self._resolve_lineage_node(start)
        if resolved_node is None:
            evidence = [Evidence(evidence_source=str(self.lineage_graph_path), line_range=None, method="graph_traversal", confidence=1.0)]
            log_cartography_trace(
                {
                    "agent": "Navigator",
                    "action": "trace_lineage",
                    "evidence_source": str(self.lineage_graph_path),
                    "line_range": None,
                    "method": "graph_traversal",
                    "confidence": 0.5,
                    "timestamp": _utc_now_iso(),
                }
            )
            return {
                "dataset": start,
                "direction": direction,
                "edges": [],
                "resolved_node": None,
                "node_not_found": True,
                "hint": f"No lineage node named '{start}' found. Use an exact id from the graph.",
                "known_table_nodes": sorted(known_nodes)[:30],
                "evidence": [e.to_dict() for e in evidence],
            }

        seen = {resolved_node}
        frontier = [resolved_node]
        edges_out: list[dict[str, Any]] = []

        for _ in range(max(1, depth)):
            nxt: list[str] = []
            for node in frontier:
                if direction == "upstream":
                    for e in self._lineage_in.get(node, [])[:limit]:
                        edges_out.append(e)
                        src = str(e.get("source"))
                        if src not in seen:
                            seen.add(src)
                            nxt.append(src)
                else:
                    for e in self._lineage_out.get(node, [])[:limit]:
                        edges_out.append(e)
                        tgt = str(e.get("target"))
                        if tgt not in seen:
                            seen.add(tgt)
                            nxt.append(tgt)
            frontier = nxt
            if not frontier:
                break

        evidence = [Evidence(evidence_source=str(self.lineage_graph_path), line_range=None, method="graph_traversal", confidence=1.0)]
        log_cartography_trace(
            {
                "agent": "Navigator",
                "action": "trace_lineage",
                "evidence_source": str(self.lineage_graph_path),
                "line_range": None,
                "method": "graph_traversal",
                "confidence": 0.9,
                "timestamp": _utc_now_iso(),
            }
        )
        return {
            "dataset": start,
            "direction": direction,
            "edges": edges_out[:limit],
            "resolved_node": resolved_node,
            "evidence": [e.to_dict() for e in evidence],
        }

    def blast_radius(self, module_path: str, *, limit: int = 30) -> dict[str, Any]:
        """
        Tool: estimate what changes might impact if you modify a module.

        - Uses module graph for direct importers/imports.
        - Uses lineage graph edges tagged with `source_file` equal to the module path (best-effort).

        Evidence sources:
        - `.cartography/module_graph.json`
        - `.cartography/lineage_graph.json`
        """
        module_path = module_path.strip()
        evidence: list[Evidence] = [
            Evidence(evidence_source=str(self.module_graph_path), line_range=None, method="graph_traversal", confidence=1.0),
            Evidence(evidence_source=str(self.lineage_graph_path), line_range=None, method="graph_traversal", confidence=0.8),
        ]

        node_id = self._canonical_module_id(module_path)

        importers = sorted(set(self._module_in.get(node_id, [])))[:limit]
        imports = sorted(set(self._module_out.get(node_id, [])))[:limit]

        lineage_impacts: list[dict[str, Any]] = []
        if self.lineage_graph is not None:
            for s, t, attrs in self.lineage_graph.edges(data=True):
                sf = attrs.get("source_file")
                if not isinstance(sf, str):
                    continue
                if sf == module_path or sf == node_id or sf.endswith(module_path) or sf.endswith(node_id):
                    lineage_impacts.append(
                        {
                            "source": str(s),
                            "target": str(t),
                            "transformation_type": attrs.get("transformation_type"),
                        }
                    )
        lr = self._line_range_for(node_id) or self._line_range_for(module_path)
        evidence.append(Evidence(evidence_source=node_id or module_path, line_range=lr, method="static_analysis", confidence=0.8))

        log_cartography_trace(
            {
                "agent": "Navigator",
                "action": "blast_radius",
                "evidence_source": str(self.module_graph_path),
                "line_range": None,
                "method": "graph_traversal",
                "confidence": 0.85,
                "timestamp": _utc_now_iso(),
            }
        )
        return {
            "module": module_path,
            "importers": importers,
            "imports": imports,
            "lineage_edges_from_source_file": lineage_impacts[:limit],
            "evidence": [e.to_dict() for e in evidence],
        }

    def explain_module(self, path: str) -> dict[str, Any]:
        """
        Tool: explain what a module does.

        Implementation:
        - Uses Semanticist purpose/domain/doc drift as the explanation baseline.
        - If `llm_client` is configured, optionally rephrases the baseline explanation
          (still grounded in the same evidence) for readability.

        Evidence source:
        - `.cartography/semanticist_report.json`
        """
        path = path.strip()
        key, rec = self._resolve_module_record(path)
        evidence: list[Evidence] = [Evidence(evidence_source=str(self.semanticist_report_path), line_range=None, method="static_analysis", confidence=1.0)]
        if not rec:
            log_cartography_trace(
                {
                    "agent": "Navigator",
                    "action": "explain_module",
                    "evidence_source": str(self.semanticist_report_path),
                    "line_range": None,
                    "method": "static_analysis",
                    "confidence": 0.6,
                    "timestamp": _utc_now_iso(),
                }
            )
            return {"path": path, "error": "Module not found in semanticist report.", "evidence": [e.to_dict() for e in evidence]}

        canonical = key or path
        lr = self._line_range_for(canonical)
        evidence.append(Evidence(evidence_source=canonical, line_range=lr, method="static_analysis", confidence=0.85))

        baseline = {
            "path": canonical,
            "domain": rec.get("domain"),
            "purpose": rec.get("purpose"),
            "docstring_flag": rec.get("docstring_flag"),
            "doc_drift": rec.get("doc_drift"),
        }

        explanation: str | None = None
        if self.llm_client is not None:
            try:
                prompt = (
                    "Explain the module concisely for an engineer.\n"
                    "Rules:\n"
                    "- Use ONLY the facts in BASELINE.\n"
                    "- Do not invent modules, systems, or dependencies.\n"
                    "- 4-8 sentences max.\n\n"
                    f"BASELINE_JSON: {json.dumps(baseline)}\n"
                )
                explanation = str(self.llm_client.chat(self.llm_model, [{"role": "user", "content": prompt}], temperature=0.2)).strip()
                log_cartography_trace(
                    {
                        "agent": "Navigator",
                        "action": "explain_module_llm",
                        "evidence_source": str(self.semanticist_report_path),
                        "line_range": None,
                        "method": "llm_inference",
                        "confidence": 0.6,
                        "timestamp": _utc_now_iso(),
                    }
                )
                evidence.append(Evidence(evidence_source="openrouter", line_range=None, method="llm_inference", confidence=0.6))
            except Exception:
                explanation = None

        out = {
            **baseline,
            "explanation": explanation,
            "evidence": [e.to_dict() for e in evidence],
        }
        log_cartography_trace(
            {
                "agent": "Navigator",
                "action": "explain_module",
                "evidence_source": str(self.semanticist_report_path),
                "line_range": None,
                "method": "static_analysis",
                "confidence": 0.85,
                "timestamp": _utc_now_iso(),
            }
        )
        return out

    def run_tool(self, tool_name: str, args: dict[str, Any]) -> dict[str, Any]:
        """
        Execute a structured tool call.

        Supported tools:
        - find_implementation(concept)
        - trace_lineage(dataset, direction)
        - blast_radius(module_path)
        - explain_module(path)
        """
        tool_name = tool_name.strip()
        if tool_name == "find_implementation":
            return self.find_implementation(str(args.get("concept") or ""))
        if tool_name == "trace_lineage":
            return self.trace_lineage(str(args.get("dataset") or ""), str(args.get("direction") or ""))
        if tool_name == "blast_radius":
            return self.blast_radius(str(args.get("module_path") or ""))
        if tool_name == "explain_module":
            return self.explain_module(str(args.get("path") or ""))
        raise ValueError(f"Unknown tool: {tool_name!r}")

    def answer(self, query: str) -> dict[str, Any]:
        """
        Answer a query using artifacts, returning evidence-cited results.
        """
        # Always answer against the latest artifacts (important for long-running UI servers).
        try:
            self.refresh_artifacts()
        except Exception:
            pass
        log_cartography_trace(
            {
                "agent": "Navigator",
                "action": "query",
                "evidence_source": str(self.codebase_md_path),
                "line_range": None,
                "method": "static_analysis",
                "confidence": 1.0,
                "timestamp": _utc_now_iso(),
            }
        )

        q = query.strip().lower()
        evidence: list[Evidence] = []

        if any(k in q for k in ("critical path", "pagerank", "most important module", "top modules")):
            top = self._top_pagerank_modules(5)
            lines = ["Critical path (top 5 modules by PageRank):"]
            for module_name, score in top:
                lr = self._line_range_for(module_name)
                evidence.append(Evidence(evidence_source=module_name, line_range=lr, method="static_analysis", confidence=0.9))
                lines.append(f"- {module_name} (PageRank={score:.4f})")
            evidence.append(Evidence(evidence_source=str(self.module_graph_path), line_range=None, method="graph_traversal", confidence=1.0))
            out = {"answer": "\n".join(lines), "evidence": [e.to_dict() for e in evidence]}
            log_cartography_trace(
                {
                    "agent": "Navigator",
                    "action": "answer",
                    "evidence_source": str(self.module_graph_path),
                    "line_range": None,
                    "method": "graph_traversal",
                    "confidence": 0.9,
                    "timestamp": _utc_now_iso(),
                }
            )
            return out

        # Heuristic routing for common question shapes (so "every question" doesn't hit the default help text).
        path_match = re.search(r"(?P<path>[\\w./-]+\\.(?:py|sql|yml|yaml))", q)
        if "explain" in q and path_match:
            return self.explain_module(path_match.group("path"))

        if any(k in q for k in ("what breaks", "blast radius", "impact if", "what would break")) and path_match:
            return self.blast_radius(path_match.group("path"))

        # Try to trace lineage when the question sounds like "what produces/depends on/affects".
        if any(k in q for k in ("what produces", "upstream", "downstream", "lineage", "depends on", "affects")) and self.lineage_graph is not None:
            direction = "upstream" if any(k in q for k in ("produce", "upstream", "depends on")) else "downstream"
            # Best-effort: pick a node id mentioned in the query.
            candidates: list[str] = []
            for nid, attrs in self.lineage_graph.nodes(data=True):
                node_id = str(attrs.get("id") or attrs.get("name") or nid)
                nid_l = node_id.lower()
                if not nid_l:
                    continue
                # Exact id substring match.
                if nid_l in q:
                    candidates.append(node_id)
                    continue
                # Fallback: match on the last segment (e.g., "daily_active_users"
                # from "analytics.daily_active_users") to better handle natural
                # language questions like "What produces the daily_active_users table?".
                simple = nid_l.split(".")[-1]
                if simple and simple in q:
                    candidates.append(node_id)
            if candidates:
                # Deduplicate while preserving order.
                seen_ids: set[str] = set()
                ordered: list[str] = []
                for c in candidates:
                    if c in seen_ids:
                        continue
                    seen_ids.add(c)
                    ordered.append(c)
                return self.trace_lineage(ordered[0], direction)

        # Semantic search: broaden beyond "where is" phrasing by defaulting to find_implementation.
        m = re.search(r"(where is|find|locate)\s+(.+)", q)
        needle = (m.group(2) if m else query).strip().strip("?")
        impl = self.find_implementation(needle)
        # Present a compact answer string for query mode (even when empty, so users
        # don't get the same generic fallback for every question).
        lines = [f"Best matches for {needle!r}:"]
        results = (impl.get("results") or [])[:8]
        lineage_matches = (impl.get("lineage_matches") or [])[:8]
        if not results and not lineage_matches:
            lines.append("- (no semantic matches; try Tool mode → `trace_lineage` or include an exact file/dataset id)")
        for item in results:
            lines.append(f"- {item.get('path')}: {item.get('purpose') or item.get('domain') or ''}".rstrip())
        for item in lineage_matches:
            lines.append(f"- dataset `{item.get('dataset_id')}` (kind={item.get('kind')})")
        return {"answer": "\n".join(lines), "evidence": impl.get("evidence") or []}

        # Default: offer high-signal pointers.
        top = self._top_pagerank_modules(5)
        lines = [
            "I can answer from Cartographer artifacts (with citations).",
            "Try asking about: critical path, data sources/sinks, doc drift, or a keyword search.",
            "",
            "Top modules (PageRank):",
            *[f"- {m} (PageRank={s:.4f})" for m, s in top],
        ]
        evidence.append(Evidence(evidence_source=str(self.module_graph_path), line_range=None, method="graph_traversal", confidence=1.0))
        out = {"answer": "\n".join(lines), "evidence": [e.to_dict() for e in evidence]}
        log_cartography_trace(
            {
                "agent": "Navigator",
                "action": "answer",
                "evidence_source": str(self.module_graph_path),
                "line_range": None,
                "method": "graph_traversal",
                "confidence": 0.75,
                "timestamp": _utc_now_iso(),
            }
        )
        return out


def build_langgraph_agent() -> Any | None:
    """
    Optional LangGraph integration.

    Returns a compiled LangGraph app if `langgraph` is installed; otherwise `None`.
    """
    try:
        from langgraph.graph import StateGraph  # type: ignore
    except Exception:
        return None

    # Minimal placeholder graph that delegates to Navigator.answer().
    class NavState(dict):
        pass

    nav = Navigator()

    def _answer(state: NavState) -> NavState:
        q = str(state.get("query") or "")
        out = nav.answer(q)
        state["answer"] = out
        return state

    g = StateGraph(NavState)
    g.add_node("answer", _answer)
    g.set_entry_point("answer")
    g.set_finish_point("answer")
    return g.compile()
