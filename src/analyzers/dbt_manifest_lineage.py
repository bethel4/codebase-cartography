from __future__ import annotations

"""
dbt manifest lineage extraction.

Why this exists
---------------
dbt model SQL is often heavily templated (Jinja + macros). Even after best-effort
macro stripping, SQL parsers can fail. dbt itself materializes a dependency graph
in `target/manifest.json` after `dbt compile` (or `dbt run`).

This analyzer consumes `manifest.json` to produce best-effort lineage edges:
- model -> upstream models/sources (via `depends_on.nodes`)
- source nodes become dataset identifiers like `<schema>.<table>`

Evidence / confidence
---------------------
The `manifest.json` is a primary artifact produced by dbt, so edges derived from
it are treated as `method="static_analysis"` with high confidence (typically 1.0).
"""

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class DbtManifestDependency:
    """A dependency edge set derived from a dbt manifest."""

    target: str
    sources: list[str]
    source_file: Path


class DbtManifestLineageAnalyzer:
    """Extract model/source dependencies from a dbt `target/manifest.json` file."""

    _quote_cleanup = re.compile(r"[`\"']")

    def analyze_manifest(self, manifest_path: Path) -> list[DbtManifestDependency]:
        if not manifest_path.exists():
            return []
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
        nodes: dict[str, dict[str, Any]] = {}
        nodes.update(data.get("nodes") or {})
        nodes.update(data.get("sources") or {})

        unique_id_to_dataset: dict[str, str] = {}
        for unique_id, node in nodes.items():
            dataset = self._dataset_id(node)
            if dataset:
                unique_id_to_dataset[unique_id] = dataset

        dependencies: list[DbtManifestDependency] = []
        for unique_id, node in nodes.items():
            if node.get("resource_type") != "model":
                continue
            target = unique_id_to_dataset.get(unique_id) or self._dataset_id(node)
            if not target:
                continue
            depends_on = node.get("depends_on") or {}
            upstream_nodes = depends_on.get("nodes") or []
            sources: list[str] = []
            for upstream_id in upstream_nodes:
                dataset = unique_id_to_dataset.get(upstream_id)
                if dataset:
                    sources.append(dataset)
            sources = sorted(set(sources))
            if sources:
                dependencies.append(
                    DbtManifestDependency(
                        target=target,
                        sources=sources,
                        source_file=manifest_path,
                    )
                )
        return dependencies

    def list_datasets(self, manifest_path: Path) -> list[tuple[str, str]]:
        """
        Return all dataset identifiers found in a manifest.

        Returns a list of `(dataset_id, resource_type)` where resource_type is one of
        {"model", "source"} when known.
        """
        if not manifest_path.exists():
            return []
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
        nodes: dict[str, dict[str, Any]] = {}
        nodes.update(data.get("nodes") or {})
        nodes.update(data.get("sources") or {})

        out: list[tuple[str, str]] = []
        for _, node in nodes.items():
            resource_type = str(node.get("resource_type") or "")
            if resource_type not in {"model", "source"}:
                continue
            dataset = self._dataset_id(node)
            if dataset:
                out.append((dataset, resource_type))
        # Stable ordering, de-duped.
        seen: set[tuple[str, str]] = set()
        result: list[tuple[str, str]] = []
        for item in sorted(out):
            if item in seen:
                continue
            seen.add(item)
            result.append(item)
        return result

    def _dataset_id(self, node: dict[str, Any]) -> str | None:
        """
        Build a stable dataset identifier.

        Preference order:
        1) For models: `alias` or `name` (keeps compatibility with dbt model filenames).
        2) For sources: `<schema>.<identifier>` (schema from source definition).
        3) Fallback: `name`.
        """
        resource_type = node.get("resource_type")
        if resource_type == "model":
            return (node.get("alias") or node.get("name") or "").strip() or None

        if resource_type == "source":
            schema = (node.get("schema") or "").strip()
            identifier = (node.get("identifier") or node.get("name") or "").strip()
            schema = self._normalize(schema)
            identifier = self._normalize(identifier)
            if schema and identifier:
                return f"{schema}.{identifier}"
            return identifier or None

        return (node.get("name") or "").strip() or None

    def _normalize(self, value: str) -> str:
        value = self._quote_cleanup.sub("", value).strip()
        return value
