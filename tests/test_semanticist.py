from pathlib import Path

import pytest

from agents.semanticist import ContextWindowBudget, ModuleSemanticRecord, Semanticist


class FakeClient:
    def chat(self, model, messages, temperature=0.2):
        content = messages[-1]["content"]
        if "Infer a short business domain name" in content:
            return "ingestion"
        if "status:" in content and "Docstring:" in content:
            return "status: matches\nreason: ok"
        if "PURPOSE statement" in content:
            return "Loads raw data for downstream processing."
        return "ok"

    def embeddings(self, model, prompt):
        return [0.1, 0.2, 0.3]


def test_semanticist_basic_flow(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    mod = repo / "a.py"
    mod.write_text('"""doc"""\n\ndef f():\n    return 1\n')

    module_graph_json = {"nodes": [{"id": "a.py", "path": "a.py"}], "links": []}
    lineage_graph_json = {"nodes": [], "links": []}

    sem = Semanticist(client=FakeClient(), budget=ContextWindowBudget(max_total_tokens=10000), bulk_model="x", synth_model="y", embed_model="z")
    report = sem.run(repo, module_graph_json, lineage_graph_json)

    assert "modules" in report
    assert report["modules"][0]["docstring_flag"] == "matches"
    # Similarity-based drift flags trivial docstrings as drift.
    assert report["modules"][0]["doc_drift"] is True
    assert report["modules"][0]["doc_similarity"] is not None
    assert "evidence_symbols" in report["modules"][0]
    # With a single module, clustering falls back to a single domain label.
    assert report["modules"][0]["domain"] in {"uncategorized", "ingestion"}
    assert "fde_answers" in report
    assert "fde_questions" in report["fde_answers"]
    assert "1_primary_data_ingestion_path" in report["fde_answers"]["fde_questions"]


def test_answer_day_one_questions_is_grounded(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "ingest.py").write_text("def ingest():\n    return 1\n")
    (repo / "serve.py").write_text("def serve():\n    return 2\n")

    records = [
        ModuleSemanticRecord(
            module_name="ingest.py",
            path=str(repo / "ingest.py"),
            purpose="Ingests raw source data into staging tables.",
            docstring_flag="matches",
            domain="ingestion",
            evidence_symbols=[{"kind": "function", "name": "ingest", "lineno": 1, "end_lineno": 2}],
        ),
        ModuleSemanticRecord(
            module_name="serve.py",
            path=str(repo / "serve.py"),
            purpose="Serves curated outputs to downstream consumers.",
            docstring_flag="matches",
            domain="serving",
            evidence_symbols=[{"kind": "function", "name": "serve", "lineno": 1, "end_lineno": 2}],
        ),
    ]
    domain_map = {"ingestion": ["ingest.py"], "serving": ["serve.py"]}

    module_graph_json = {
        "nodes": [
            {"id": "ingest.py", "path": "ingest.py", "pagerank": 0.9, "change_velocity_30d": 3, "complexity_score": 10},
            {"id": "serve.py", "path": "serve.py", "pagerank": 0.3, "change_velocity_30d": 1, "complexity_score": 5},
        ],
        "links": [],
    }
    lineage_graph_json = {
        "nodes": [
            {"id": "raw.source", "kind": "dataset", "source_files": ["ingest.py"]},
            {"id": "dw.output", "kind": "table", "source_files": ["serve.py"]},
            {"id": "python:ingest.py", "kind": "script", "source_files": ["ingest.py"]},
        ],
        "links": [
            {"source": "raw.source", "target": "python:ingest.py", "transformation_type": "python_read", "source_file": "ingest.py", "line_range": ""},
            {"source": "python:ingest.py", "target": "dw.output", "transformation_type": "python_write", "source_file": "ingest.py", "line_range": ""},
        ],
    }

    sem = Semanticist(client=FakeClient(), budget=ContextWindowBudget(max_total_tokens=10000), bulk_model="x", synth_model="y", embed_model="z")
    out = sem.answer_day_one_questions(records, domain_map, module_graph_json, lineage_graph_json)

    q1 = out["fde_questions"]["1_primary_data_ingestion_path"]
    assert "ingest.py" in q1["ingestion_modules"]
    assert any(ev["file_path"].endswith("ingest.py") and ev["line_numbers"] == [1, 2] for ev in q1["evidence"])

    q2 = out["fde_questions"]["2_critical_outputs"]
    assert any(o.get("dataset_id") == "dw.output" for o in q2["outputs"])

    q5 = out["fde_questions"]["5_git_velocity"]
    assert q5["git_velocity"]["top_changed_30d"][0]["id"] == "ingest.py"
