from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class PipelineEdge:
    source: str
    target: str
    source_file: str
    transformation_type: str


class DAGConfigAnalyzer:
    _set_downstream = re.compile(r"([A-Za-z_][\w\.]*?)\.set_downstream\(\s*([A-Za-z_][\w\.]*)\s*\)")
    _set_upstream = re.compile(r"([A-Za-z_][\w\.]*?)\.set_upstream\(\s*([A-Za-z_][\w\.]*)\s*\)")
    _arrow = re.compile(r"([A-Za-z_][\w\.]*?)\s*(>>|<<)\s*([A-Za-z_][\w\.]*)")

    def analyze_repo(self, repo_path: Path) -> list[PipelineEdge]:
        edges: list[PipelineEdge] = []
        for path in repo_path.rglob("*.py"):
            text = path.read_text(encoding="utf-8", errors="ignore")
            edges.extend(self._parse_python(text, path))

        for path in repo_path.rglob("**/schema*.yml"):
            edges.extend(self._parse_schema(path))
        return edges

    def _parse_python(self, text: str, path: Path) -> list[PipelineEdge]:
        edges: list[PipelineEdge] = []
        for match in self._set_downstream.finditer(text):
            edges.append(PipelineEdge(source=match.group(1), target=match.group(2), source_file=str(path), transformation_type="airflow"))
        for match in self._set_upstream.finditer(text):
            edges.append(PipelineEdge(source=match.group(2), target=match.group(1), source_file=str(path), transformation_type="airflow"))
        for match in self._arrow.finditer(text):
            left, arrow, right = match.groups()
            if arrow == ">>":
                edges.append(PipelineEdge(source=left, target=right, source_file=str(path), transformation_type="airflow"))
            else:
                edges.append(PipelineEdge(source=right, target=left, source_file=str(path), transformation_type="airflow"))
        return edges

    def _parse_schema(self, path: Path) -> list[PipelineEdge]:
        edges: list[PipelineEdge] = []
        current_model: str | None = None
        depends_indent: int | None = None
        source_prefix: str | None = None
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            stripped = line.lstrip()
            indent = len(line) - len(stripped)
            if stripped.startswith("- name:"):
                current_model = stripped.split(":", 1)[1].strip()
                depends_indent = None
                source_prefix = None
                continue
            if not current_model:
                continue
            if stripped.startswith("depends_on:"):
                depends_indent = indent
                source_prefix = None
                continue
            if depends_indent is not None and indent > depends_indent:
                if stripped.startswith("- ref:"):
                    ref = stripped.split(":", 1)[1].strip().strip('"\'')
                    edges.append(PipelineEdge(source=ref, target=current_model, source_file=str(path), transformation_type="dbt_schema"))
                elif stripped.startswith("- source:"):
                    source_prefix = stripped.split(":", 1)[1].strip()
                elif stripped.startswith("table:") and source_prefix:
                    table_name = stripped.split(":", 1)[1].strip()
                    source = f"{source_prefix}.{table_name}"
                    edges.append(PipelineEdge(source=source, target=current_model, source_file=str(path), transformation_type="dbt_schema"))
            else:
                depends_indent = None
                source_prefix = None
        return edges
