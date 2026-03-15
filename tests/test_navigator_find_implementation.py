from __future__ import annotations

import json
from pathlib import Path

import networkx as nx

from agents.navigator import Navigator
from graph.knowledge_graph import write_graph_json


def _make_nav(tmp_path: Path, monkeypatch) -> Navigator:
    monkeypatch.chdir(tmp_path)
    carto = tmp_path / ".cartography"
    carto.mkdir()

    mg = nx.DiGraph()
    # Minimal module graph; find_implementation reads Semanticist report, but
    # Navigator expects artifacts to exist.
    mg.add_node("src/transforms/main.py", id="src/transforms/main.py", path="src/transforms/main.py", pagerank=1.0)
    write_graph_json(mg, carto / "module_graph.json")
    write_graph_json(nx.DiGraph(), carto / "lineage_graph.json")

    semantic = {
        "modules": [
            {
                "module_name": "src/transforms/main.py",
                "path": "src/transforms/main.py",
                "purpose": "Main transformation logic for analytics models.",
                "domain": "Transforms",
                "doc_drift": False,
                "evidence_symbols": [{"lineno": 1, "end_lineno": 20}],
            },
            {
                "module_name": "target_repo/models/staging/stg_orders.sql",
                "path": "target_repo/models/staging/stg_orders.sql",
                "purpose": "dbt staging model for orders (stg_* layer).",
                "domain": "dbt",
                "doc_drift": False,
                "evidence_symbols": [{"lineno": 1, "end_lineno": 80}],
            },
            {
                "module_name": "src/ingestion/extract_load.py",
                "path": "src/ingestion/extract_load.py",
                "purpose": "Ingestion / extract / load orchestration for raw data sources.",
                "domain": "Ingestion",
                "doc_drift": False,
                "evidence_symbols": [{"lineno": 1, "end_lineno": 120}],
            },
            {
                "module_name": "src/analyzers/sql_lineage.py",
                "path": "src/analyzers/sql_lineage.py",
                "purpose": "Lineage + SQL parsing logic (sqlglot-based) for Hydrologist.",
                "domain": "Lineage",
                "doc_drift": False,
                "evidence_symbols": [{"lineno": 1, "end_lineno": 260}],
            },
            {
                "module_name": "src/ol_superset/commands/promote.py",
                "path": "src/ol_superset/commands/promote.py",
                "purpose": "Superset promotion / refresh logic for dashboard assets.",
                "domain": "Superset",
                "doc_drift": False,
                "evidence_symbols": [{"lineno": 1, "end_lineno": 200}],
            },
        ],
        "domains": {
            "Transforms": ["src/transforms/main.py"],
            "dbt": ["target_repo/models/staging/stg_orders.sql"],
            "Ingestion": ["src/ingestion/extract_load.py"],
            "Lineage": ["src/analyzers/sql_lineage.py"],
            "Superset": ["src/ol_superset/commands/promote.py"],
        },
    }
    (carto / "semanticist_report.json").write_text(json.dumps(semantic), encoding="utf-8")

    return Navigator(
        module_graph_path=carto / "module_graph.json",
        lineage_graph_path=carto / "lineage_graph.json",
        semanticist_report_path=carto / "semanticist_report.json",
    )


def test_find_implementation_common_questions(tmp_path: Path, monkeypatch) -> None:
    nav = _make_nav(tmp_path, monkeypatch)

    cases = [
        ("main transformation logic", "src/transforms/main.py"),
        ("dbt model staging logic", "target_repo/models/staging/stg_orders.sql"),
        ("ingestion / extract / load", "src/ingestion/extract_load.py"),
        ("lineage or SQL parsing logic", "src/analyzers/sql_lineage.py"),
        ("Superset promotion / refresh logic", "src/ol_superset/commands/promote.py"),
    ]

    for concept, expected_path in cases:
        out = nav.find_implementation(concept)
        paths = [r.get("path") for r in (out.get("results") or [])]
        assert expected_path in paths

