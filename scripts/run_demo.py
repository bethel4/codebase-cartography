#!/usr/bin/env python3
"""
Run the Cartographer demo (Minutes 1-6) in sequence.

Usage (from repo root):
  uv run python scripts/run_demo.py [--repo-path target_repo] [--with-semantic]

Steps 1-3 (Required): Cold start, lineage query, blast radius — run automatically.
Steps 4-6 (Mastery): Day-One brief, living context, self-audit — instructions printed; onboarding_brief.md generated when semantic report exists.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _run_cli(*args: str) -> subprocess.CompletedProcess:
    root = _repo_root()
    cmd = [sys.executable, str(root / "src" / "cli.py"), *args]
    return subprocess.run(cmd, cwd=str(root), capture_output=False, text=True)


def _write_onboarding_brief(semantic_path: Path, out_path: Path) -> bool:
    """Generate onboarding_brief.md from semanticist_report.json fde_answers."""
    if not semantic_path.exists():
        return False
    try:
        data = json.loads(semantic_path.read_text(encoding="utf-8"))
        fde = data.get("fde_answers") or data.get("fde_day_one") or {}
        questions = fde.get("fde_questions") or {}
        lines = ["# Day-One Brief (FDE)", "", "Generated from Cartographer Semanticist output.", ""]
        for qid, content in sorted(questions.items()):
            if isinstance(content, dict):
                answer = content.get("answer") or content.get("summary") or str(content)
                evidence = content.get("evidence") or content.get("file_path")
                lines.append(f"## {qid}")
                lines.append("")
                lines.append(str(answer))
                if evidence:
                    lines.append("")
                    lines.append(f"*Evidence:* `{evidence}`")
                lines.append("")
            else:
                lines.append(f"## {qid}")
                lines.append("")
                lines.append(str(content))
                lines.append("")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
        return True
    except Exception:
        return False


def main() -> int:
    repo_root = _repo_root()
    repo_path = "target_repo"
    if "--repo-path" in sys.argv:
        i = sys.argv.index("--repo-path")
        if i + 1 < len(sys.argv):
            repo_path = sys.argv[i + 1]
    with_semantic = "--with-semantic" in sys.argv

    # All CLI commands run with cwd=repo_root; artifacts go to repo_root/.cartography/ and repo_root/CODEBASE.md
    carto = repo_root / ".cartography"
    carto.mkdir(parents=True, exist_ok=True)

    print("=== Step 1 -- Cold Start (analyze + CODEBASE.md) ===")
    t0 = time.perf_counter()
    _run_cli("survey", "--repo-path", repo_path)
    _run_cli("hydrology", "--repo-path", repo_path)
    if with_semantic:
        _run_cli("semantic", "--repo-path", repo_path, "--modules", ".cartography/module_graph.json", "--lineage", ".cartography/lineage_graph.json", "--out", ".cartography/semanticist_report.json")
    _run_cli("phase4", "--repo-path", repo_path, "--modules", ".cartography/module_graph.json", "--lineage", ".cartography/lineage_graph.json", "--semantic", ".cartography/semanticist_report.json", "--out", "CODEBASE.md")
    elapsed = time.perf_counter() - t0
    codebase_md = repo_root / "CODEBASE.md"
    print(f"  Done in {elapsed:.1f}s. CODEBASE.md: {codebase_md} (exists: {codebase_md.exists()})")
    print("")

    print("=== Step 2 -- Lineage Query (upstream sources for an output dataset) ===")
    dataset = "customers"
    _run_cli("navigate-tool", "trace_lineage", "--args-json", json.dumps({"dataset": dataset, "direction": "upstream"}), "--modules", ".cartography/module_graph.json", "--lineage", ".cartography/lineage_graph.json", "--semantic", ".cartography/semanticist_report.json", "--codebase", "CODEBASE.md")
    print("  (Above: graph traversal with file/line in edge source_file when present.)")
    print("")

    print("=== Step 3 -- Blast Radius (downstream dependency for a module) ===")
    module_path = "src/analyzers/sql_lineage.py"
    _run_cli("navigate-tool", "blast_radius", "--args-json", json.dumps({"module_path": module_path}), "--modules", ".cartography/module_graph.json", "--lineage", ".cartography/lineage_graph.json", "--semantic", ".cartography/semanticist_report.json", "--codebase", "CODEBASE.md")
    print("")

    semantic_path = repo_root / ".cartography" / "semanticist_report.json"
    brief_path = repo_root / "build" / "onboarding_brief.md"
    if _write_onboarding_brief(semantic_path, brief_path):
        print("=== Step 4 -- Day-One Brief ===")
        print(f"  onboarding_brief.md written: {brief_path}")
        print("  Open it and verify 2+ answers by navigating to the cited file and line.")
        print("")
    else:
        print("=== Step 4 -- Day-One Brief ===")
        print("  Run: uv run python src/cli.py semantic --repo-path target_repo")
        print("  Then re-run this script or run: uv run python scripts/run_demo.py --with-semantic")
        print("  Then open build/onboarding_brief.md and verify answers against cited file:line.")
        print("")

    print("=== Step 5 -- Living Context Injection ===")
    print("  1. Start a fresh AI coding agent session.")
    print("  2. Paste the contents of CODEBASE.md into the context (or attach it).")
    print("  3. Ask an architecture question (e.g. 'What is the critical path?' or 'Where are data sources?').")
    print("  4. Compare answer quality with vs without CODEBASE.md in context.")
    print("")

    print("=== Step 6 -- Self-Audit ===")
    print("  Run Cartographer on your own Week 1 repo (clone/survey/hydrology/semantic/phase4).")
    print("  Compare your existing docs with CODEBASE.md and semanticist_report.json.")
    print("  Explain any discrepancy (e.g. doc drift, missing modules, or outdated descriptions).")
    print("")

    return 0


if __name__ == "__main__":
    sys.exit(main())
