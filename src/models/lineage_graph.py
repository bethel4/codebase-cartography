from __future__ import annotations

from pydantic import BaseModel, Field


class DataNode(BaseModel):
    """Schema for a data lineage node (table/view/file)."""

    name: str
    kind: str = "table"
    source_files: list[str] = Field(default_factory=list)


class LineageGraphSummary(BaseModel):
    """Summary metadata for a data lineage graph."""

    node_count: int = Field(0, ge=0)
    edge_count: int = Field(0, ge=0)
