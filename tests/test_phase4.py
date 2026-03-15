import json
from pathlib import Path

import networkx as nx

from graph.knowledge_graph import write_graph_json
from phase4 import generate_CODEBASE_md


def test_generate_codebase_md_writes_sections_and_trace(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    carto = tmp_path / ".cartography"
    carto.mkdir()

    mg = nx.DiGraph()
    mg.add_node("a.py", id="a.py", path="a.py", pagerank=0.9, change_velocity_30d=3)
    mg.add_node("b.py", id="b.py", path="b.py", pagerank=0.1, change_velocity_30d=1)
    mg.add_edge("a.py", "b.py")
    write_graph_json(mg, carto / "module_graph.json")

    lg = nx.DiGraph()
    lg.add_node("raw.table", id="raw.table", kind="table", source_files=["a.py"])
    lg.add_node("python:a.py", id="python:a.py", kind="script", source_files=["a.py"])
    lg.add_edge("raw.table", "python:a.py", source_file="a.py", transformation_type="python_read")
    write_graph_json(lg, carto / "lineage_graph.json")

    semantic = {
        "modules": [
            {
                "module_name": "a.py",
                "path": "a.py",
                "purpose": "Ingests data.",
                "docstring_flag": "matches",
                "docstring_reason": "",
                "doc_drift": True,
                "doc_similarity": 0.1,
                "domain": "ingestion",
                "evidence_symbols": [{"lineno": 1, "end_lineno": 2}],
            }
        ],
        "domains": {"ingestion": ["a.py"]},
    }
    (carto / "semanticist_report.json").write_text(json.dumps(semantic), encoding="utf-8")

    out = generate_CODEBASE_md(
        module_graph_path=carto / "module_graph.json",
        lineage_graph_path=carto / "lineage_graph.json",
        semanticist_report_path=carto / "semanticist_report.json",
        out_path=tmp_path / "CODEBASE.md",
        target_repo=tmp_path,
    )
    text = out.read_text(encoding="utf-8")
    assert "## Architecture Overview" in text
    assert "## Critical Path" in text
    assert "## Data Sources & Sinks" in text
    assert "## Known Debt" in text
    assert "## High-Velocity Files" in text

    trace = (tmp_path / "cartography_trace.jsonl").read_text(encoding="utf-8").splitlines()
    assert trace, "expected at least one trace entry"
    entry = json.loads(trace[0])
    for key in ["agent", "action", "evidence_source", "line_range", "method", "confidence", "timestamp"]:
        assert key in entry
    assert entry["agent"] == "Navigator"
