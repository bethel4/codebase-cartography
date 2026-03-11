from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


NodeKind = Literal["module", "import", "table", "dataset", "script", "dag_task", "unknown"]
EdgeKind = Literal["imports", "lineage", "dag_depends", "unknown"]


class KnowledgeNode(BaseModel):
    """Typed node record for the central knowledge graph."""

    id: str
    kind: NodeKind = "unknown"
    attrs: dict[str, Any] = Field(default_factory=dict)


class KnowledgeEdge(BaseModel):
    """Typed edge record for the central knowledge graph."""

    source: str
    target: str
    kind: EdgeKind = "unknown"
    attrs: dict[str, Any] = Field(default_factory=dict)

