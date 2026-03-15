from __future__ import annotations

import json
from pathlib import Path


def test_dbt_manifest_lineage_analyzer_extracts_model_dependencies(tmp_path: Path) -> None:
    from analyzers.dbt_manifest_lineage import DbtManifestLineageAnalyzer

    manifest = {
        "nodes": {
            "model.proj.stg_orders": {
                "resource_type": "model",
                "name": "stg_orders",
                "alias": "stg_orders",
                "depends_on": {"nodes": ["source.proj.raw.raw_orders"]},
            }
        },
        "sources": {
            "source.proj.raw.raw_orders": {
                "resource_type": "source",
                "name": "raw_orders",
                "identifier": "raw_orders",
                "schema": "raw",
            }
        },
    }

    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")

    deps = DbtManifestLineageAnalyzer().analyze_manifest(path)
    assert len(deps) == 1
    assert deps[0].target == "stg_orders"
    assert deps[0].sources == ["raw.raw_orders"]
    assert deps[0].source_file == path

