from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from agents.hydrologist import build_lineage_graph
from agents.surveyor import build_module_graph
from graph.knowledge_graph import write_graph_json
from concurrent.futures import ThreadPoolExecutor, as_completed


@dataclass
class OrchestratorResult:
    module_graph_path: Path | None
    lineage_graph_path: Path | None
    module_error: str | None = None
    lineage_error: str | None = None


def run(repo_path: str | Path = "target_repo", parallel: bool = True) -> OrchestratorResult:
    """Run Surveyor and Hydrologist agents and persist outputs (optionally in parallel)."""
    repo_path = Path(repo_path)
    output_dir = Path(".cartography")
    output_dir.mkdir(parents=True, exist_ok=True)

    module_graph_path: Path | None = None
    lineage_graph_path: Path | None = None
    module_error: str | None = None
    lineage_error: str | None = None

    def _run_surveyor() -> Any:
        return build_module_graph(repo_path)

    def _run_hydrologist() -> Any:
        return build_lineage_graph(repo_path)

    if parallel:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = {
                pool.submit(_run_surveyor): "surveyor",
                pool.submit(_run_hydrologist): "hydrologist",
            }
            for future in as_completed(futures):
                name = futures[future]
                try:
                    result = future.result()
                except Exception as exc:  # pragma: no cover
                    if name == "surveyor":
                        module_error = str(exc)
                    else:
                        lineage_error = str(exc)
                    continue
                if name == "surveyor":
                    _, module_graph, _ = result
                    module_graph_path = output_dir / "module_graph.json"
                    write_graph_json(module_graph, module_graph_path)
                else:
                    _, lineage_graph, _ = result
                    lineage_graph_path = output_dir / "lineage_graph.json"
                    write_graph_json(lineage_graph.graph, lineage_graph_path)
    else:
        try:
            _, module_graph, _ = build_module_graph(repo_path)
            module_graph_path = output_dir / "module_graph.json"
            write_graph_json(module_graph, module_graph_path)
        except Exception as exc:  # pragma: no cover
            module_error = str(exc)

        try:
            _, lineage_graph, _ = build_lineage_graph(repo_path)
            lineage_graph_path = output_dir / "lineage_graph.json"
            write_graph_json(lineage_graph.graph, lineage_graph_path)
        except Exception as exc:  # pragma: no cover
            lineage_error = str(exc)

    return OrchestratorResult(
        module_graph_path=module_graph_path,
        lineage_graph_path=lineage_graph_path,
        module_error=module_error,
        lineage_error=lineage_error,
    )
