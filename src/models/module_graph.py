from __future__ import annotations

from pydantic import BaseModel, Field


class ModuleNode(BaseModel):
    """Schema for a module node in the dependency graph."""

    path: str
    imports: list[str]
    functions: list[str]
    classes: list[str]
    dead_exports: list[str] = Field(default_factory=list)
    entrypoint_exports: list[str] = Field(default_factory=list)
    framework_exports: list[str] = Field(default_factory=list)
    complexity_score: int = 0
    change_velocity_30d: int = 0
    is_dead_code_candidate: bool = False
    last_modified: str = ""
    pagerank: float = 0.0
    in_cycle: bool = False
    language: str = "python"


class ModuleGraphSummary(BaseModel):
    """Summary metadata for a module dependency graph."""

    module_count: int = Field(0, ge=0)
    edge_count: int = Field(0, ge=0)
    cycle_count: int = Field(0, ge=0)
