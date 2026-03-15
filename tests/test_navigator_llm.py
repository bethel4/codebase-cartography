import json
from pathlib import Path

import networkx as nx

from agents.navigator import Navigator
from graph.knowledge_graph import write_graph_json


class FakeLlm:
    def chat(self, model, messages, temperature=0.2):
        assert model
        assert messages
        return "LLM explanation."


def test_explain_module_uses_llm_when_available(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    carto = tmp_path / ".cartography"
    carto.mkdir()

    mg = nx.DiGraph()
    mg.add_node("a.py", id="a.py", path="a.py", pagerank=1.0, change_velocity_30d=0)
    write_graph_json(mg, carto / "module_graph.json")
    write_graph_json(nx.DiGraph(), carto / "lineage_graph.json")

    semantic = {
        "modules": [
            {
                "module_name": "a.py",
                "path": "a.py",
                "purpose": "Does a thing.",
                "docstring_flag": "matches",
                "docstring_reason": "",
                "doc_drift": False,
                "doc_similarity": 0.9,
                "domain": "test",
                "evidence_symbols": [{"lineno": 1, "end_lineno": 2}],
            }
        ],
        "domains": {"test": ["a.py"]},
    }
    (carto / "semanticist_report.json").write_text(json.dumps(semantic), encoding="utf-8")

    nav = Navigator(
        module_graph_path=carto / "module_graph.json",
        lineage_graph_path=carto / "lineage_graph.json",
        semanticist_report_path=carto / "semanticist_report.json",
        llm_client=FakeLlm(),
        llm_model="openai/gpt-4o-mini",
    )
    out = nav.explain_module("a.py")
    assert out["explanation"] == "LLM explanation."
    assert any(e.get("method") == "llm_inference" for e in out["evidence"])

