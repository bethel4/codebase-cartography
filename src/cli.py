from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

# Ensure `src` is on the path so `agents`, `graph`, etc. resolve when running
# as `python src/cli.py` or `python -m src.cli` from project root.
_src_dir = Path(__file__).resolve().parent
if str(_src_dir) not in sys.path:
    sys.path.insert(0, str(_src_dir))

from agents.semanticist import GeminiHttpClient, OllamaHttpClient, OpenRouterHttpClient, Semanticist
from agents.semanticist import ModuleSemanticRecord, ContextWindowBudget


def clone_repo(repo_url: str, target_dir: str = "target_repo") -> None:
    """Clone a repository into the local workspace."""
    target_path = Path(target_dir)
    target_path.mkdir(parents=True, exist_ok=True)
    if any(target_path.iterdir()):
        raise RuntimeError(f"Target directory is not empty: {target_path}")

    subprocess.run(["git", "clone", repo_url, str(target_path)], check=True)
    print(f"Repository cloned to {target_path}")
    # Optional: trace logging for auditability (do not break cloning if it fails).
    try:
        from datetime import datetime, timezone

        from cartography_trace import log_cartography_trace

        log_cartography_trace(
            {
                "agent": "Surveyor",
                "action": "clone_repo",
                "evidence_source": str(target_path),
                "line_range": None,
                "method": "static_analysis",
                "confidence": 1.0,
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            }
        )
    except Exception:
        pass


def build_languages_cmd(args: argparse.Namespace) -> None:
    """CLI handler for building tree-sitter libraries."""
    from analyzers.tree_sitter_analyzer import build_languages

    build_languages(
        vendor_dir=args.vendor_dir,
        out_path=args.out_path,
        languages=args.languages,
        force=args.force,
    )
    print(f"Tree-sitter library ready at {args.out_path}")


def list_languages_cmd(args: argparse.Namespace) -> None:
    """CLI handler that loads and prints the languages from a shared library."""
    from analyzers.tree_sitter_analyzer import DEFAULT_LANGUAGES
    from tree_sitter import Language

    lib_path = Path(args.lib_path)
    if not lib_path.exists():
        raise FileNotFoundError(f"Tree-sitter library not found at {lib_path}")

    languages = args.languages or DEFAULT_LANGUAGES
    for name in languages:
        try:
            language = Language(str(lib_path), name)
        except Exception as exc:
            print(f"{name}: {exc}", file=sys.stderr)
        else:
            print(language.name)


def survey_cmd(args: argparse.Namespace) -> None:
    """CLI handler for the Surveyor agent."""
    from agents.surveyor import build_module_graph
    from graph.knowledge_graph import write_graph_json

    _, graph, _ = build_module_graph(args.repo_path)
    write_graph_json(graph, Path(".cartography") / "module_graph.json")
    print("Surveyor analysis complete. Graph saved to .cartography/module_graph.json")
    from datetime import datetime, timezone

    from cartography_trace import log_cartography_trace

    log_cartography_trace(
        {
            "agent": "Surveyor",
            "action": "build_module_graph",
            "evidence_source": ".cartography/module_graph.json",
            "line_range": None,
            "method": "static_analysis",
            "confidence": 0.9,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
    )


def hydrology_cmd(args: argparse.Namespace) -> None:
    """CLI handler for the Hydrologist agent."""
    from agents.hydrologist import build_lineage_graph
    from graph.knowledge_graph import write_graph_json

    _, graph, _ = build_lineage_graph(
        args.repo_path,
        dbt_compile=not args.no_dbt_compile,
        dbt_profiles_dir=args.dbt_profiles_dir,
    )
    write_graph_json(graph.graph, Path(".cartography") / "lineage_graph.json")
    print("Hydrologist analysis complete. Graph saved to .cartography/lineage_graph.json")
    from datetime import datetime, timezone

    from cartography_trace import log_cartography_trace

    log_cartography_trace(
        {
            "agent": "Hydrologist",
            "action": "build_lineage_graph",
            "evidence_source": ".cartography/lineage_graph.json",
            "line_range": None,
            "method": "static_analysis",
            "confidence": 0.85,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
    )


def run_cmd(args: argparse.Namespace) -> None:
    """CLI handler to run the full cartography pipeline."""
    from orchestrator import run

    result = run(args.repo_path, parallel=args.parallel)
    if result.module_error:
        print(f"Surveyor failed: {result.module_error}", file=sys.stderr)
    if result.lineage_error:
        print(f"Hydrologist failed: {result.lineage_error}", file=sys.stderr)
    if result.module_graph_path and result.lineage_graph_path:
        print(f"Cartography complete. Graphs saved to {result.module_graph_path} and {result.lineage_graph_path}")
    else:
        print("Cartography completed with errors. Check stderr for details.", file=sys.stderr)

def semantic_cmd(args: argparse.Namespace) -> None:
    """CLI handler for the Semanticist agent (LLM-powered meaning extraction)."""
    module_graph_path = Path(args.modules)
    lineage_graph_path = Path(args.lineage)
    if not module_graph_path.exists():
        raise FileNotFoundError(module_graph_path)
    if not lineage_graph_path.exists():
        raise FileNotFoundError(lineage_graph_path)

    module_graph_json = json.loads(module_graph_path.read_text(encoding="utf-8"))
    lineage_graph_json = json.loads(lineage_graph_path.read_text(encoding="utf-8"))

    # .env already loaded in main(); openRoute / GemiBulk etc. are in os.environ when set.
    def make_client(provider: str, *, key_env: str | None = None, timeout_s: int | None = None):
        if provider == "openrouter":
            api_key = os.environ.get(args.openrouter_key_env, "")
            if not api_key:
                raise RuntimeError(
                    f"Missing OpenRouter API key in env var {args.openrouter_key_env!r}. Put it in .env as {args.openrouter_key_env}=..."
                )
            return OpenRouterHttpClient(api_key=api_key, base_url=args.openrouter_host, timeout_s=timeout_s or args.timeout_s)
        if provider == "gemini":
            env_name = key_env or args.gemini_key_env
            api_key = os.environ.get(env_name, "")
            if not api_key:
                raise RuntimeError(f"Missing Gemini API key in env var {env_name!r}. Put it in .env as {env_name}=...")
            return GeminiHttpClient(api_key=api_key, base_url=args.gemini_host, timeout_s=timeout_s or args.timeout_s)
        ollama_key_env = getattr(args, "ollama_key_env", "OLLAMA_API_KEY")
        api_key = os.environ.get(ollama_key_env, "")
        return OllamaHttpClient(base_url=args.ollama_host, timeout_s=timeout_s or args.timeout_s, api_key=api_key)

    bulk_provider = args.bulk_provider or args.provider
    synth_provider = args.synth_provider or args.provider
    bulk_client = make_client(bulk_provider, key_env=args.gemini_bulk_key_env, timeout_s=args.bulk_timeout_s or args.timeout_s)
    synth_client = make_client(synth_provider, key_env=args.gemini_synth_key_env, timeout_s=args.synth_timeout_s or args.timeout_s)

    bulk_model = args.bulk_model
    synth_model = args.synth_model
    # Tiered model selection: cheap/fast for bulk (purpose + drift), stronger for synthesis (domain names, day-one).
    if bulk_provider == "gemini" and not bulk_model:
        bulk_model = "gemini-2.0-flash-lite"
    if synth_provider == "gemini" and not synth_model:
        synth_model = "gemini-2.0-flash"
    # OpenRouter: default to broadly available OpenAI models to avoid 404s / region blocks.
    if bulk_provider == "openrouter" and not bulk_model:
        bulk_model = "openai/gpt-4o-mini"
    if synth_provider == "openrouter" and not synth_model:
        synth_model = "openai/gpt-4o-mini"

    budget = ContextWindowBudget(max_total_tokens=400_000)
    semanticist = Semanticist(
        bulk_client=bulk_client,
        synth_client=synth_client,
        embed_client=bulk_client,
        budget=budget,
        bulk_model=bulk_model or "mistral:latest",
        synth_model=synth_model or "mistral:latest",
        # For OpenRouter, embeddings are optional; if embed_model is empty, Semanticist
        # falls back to local hashing embeddings for clustering.
        embed_model=args.embed_model,
    )
    report = semanticist.run(Path(args.repo_path), module_graph_json, lineage_graph_json)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Semanticist complete. Report saved to {out_path}")
    from datetime import datetime, timezone

    from cartography_trace import log_cartography_trace

    log_cartography_trace(
        {
            "agent": "Semanticist",
            "action": "generate_semanticist_report",
            "evidence_source": str(out_path),
            "line_range": None,
            "method": "llm_inference",
            "confidence": 0.7,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
    )

def semantic_refresh_fde_cmd(args: argparse.Namespace) -> None:
    """
    Recompute only the Day-One (FDE) answers using existing semanticist artifacts.

    This avoids re-running expensive LLM steps over every module when you only want
    improved heuristics for `fde_day_one` / `fde_answers`.
    """

    module_graph_path = Path(args.modules)
    lineage_graph_path = Path(args.lineage)
    report_in_path = Path(args.in_path)
    if not module_graph_path.exists():
        raise FileNotFoundError(module_graph_path)
    if not lineage_graph_path.exists():
        raise FileNotFoundError(lineage_graph_path)
    if not report_in_path.exists():
        raise FileNotFoundError(report_in_path)

    module_graph_json = json.loads(module_graph_path.read_text(encoding="utf-8"))
    lineage_graph_json = json.loads(lineage_graph_path.read_text(encoding="utf-8"))
    report = json.loads(report_in_path.read_text(encoding="utf-8"))

    module_dicts = report.get("modules") or []
    records: list[ModuleSemanticRecord] = []
    for d in module_dicts if isinstance(module_dicts, list) else []:
        if not isinstance(d, dict):
            continue
        try:
            records.append(
                ModuleSemanticRecord(
                    module_name=str(d.get("module_name") or d.get("path") or ""),
                    path=str(d.get("path") or ""),
                    purpose=str(d.get("purpose") or ""),
                    docstring_flag=str(d.get("docstring_flag") or "unknown"),
                    docstring_reason=str(d.get("docstring_reason") or ""),
                    doc_drift=bool(d.get("doc_drift") or False),
                    doc_similarity=d.get("doc_similarity"),
                    domain=str(d.get("domain") or ""),
                    evidence_symbols=list(d.get("evidence_symbols") or []),
                )
            )
        except Exception:
            continue

    domain_map = report.get("domains")
    if not isinstance(domain_map, dict) or not domain_map:
        domain_map = {}
        for r in records:
            domain_map.setdefault(r.domain or "uncategorized", []).append(r.module_name)

    class _NoLlmClient:
        def chat(self, model, messages, temperature=0.2):
            return "{}"

        def embeddings(self, model, prompt):
            return [0.0]

    # Instantiate Semanticist purely to reuse its day-one heuristics; disable any optional LLM refinement.
    semanticist = Semanticist(client=_NoLlmClient(), budget=ContextWindowBudget(max_total_tokens=10_000), bulk_model="x", synth_model="y", embed_model="")
    semanticist._synth_llm_available = False
    semanticist._bulk_llm_available = False

    day_one = semanticist.answer_day_one_questions(records, domain_map, module_graph_json, lineage_graph_json)
    report["fde_day_one"] = day_one
    report["fde_answers"] = day_one

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Refreshed FDE answers. Report saved to {out_path}")

def phase4_cmd(args: argparse.Namespace) -> None:
    """CLI handler for Phase 4 (incremental update + CODEBASE.md generation)."""
    from phase4 import phase4_incremental_run

    out = phase4_incremental_run(
        target_repo=Path(args.repo_path),
        module_graph_path=Path(args.modules),
        lineage_graph_path=Path(args.lineage),
        semanticist_report_path=Path(args.semantic),
        codebase_md_path=Path(args.out),
    )
    print(f"Phase 4 complete. Generated {out}")


def navigate_cmd(args: argparse.Namespace) -> None:
    """CLI handler for Navigator Q&A (evidence-cited)."""
    from agents.navigator import Navigator
    from agents.semanticist import OpenRouterHttpClient

    # Optional: enable OpenRouter-backed rephrasing for explain_module.
    llm_client = None
    openrouter_key = os.environ.get("openRoute", "")
    if openrouter_key:
        llm_client = OpenRouterHttpClient(api_key=openrouter_key, base_url=os.environ.get("OPENROUTER_HOST", "https://openrouter.ai"))

    nav = Navigator(
        module_graph_path=Path(args.modules),
        lineage_graph_path=Path(args.lineage),
        semanticist_report_path=Path(args.semantic),
        codebase_md_path=Path(args.codebase),
        llm_client=llm_client,
        llm_model=os.environ.get("NAVIGATOR_OPENROUTER_MODEL", "openai/gpt-4o-mini"),
    )
    out = nav.answer(args.query)
    print(json.dumps(out, indent=2))

def navigate_tool_cmd(args: argparse.Namespace) -> None:
    """CLI handler for Navigator structured tools."""
    from agents.navigator import Navigator
    from agents.semanticist import OpenRouterHttpClient

    llm_client = None
    openrouter_key = os.environ.get("openRoute", "")
    if openrouter_key:
        llm_client = OpenRouterHttpClient(api_key=openrouter_key, base_url=os.environ.get("OPENROUTER_HOST", "https://openrouter.ai"))

    nav = Navigator(
        module_graph_path=Path(args.modules),
        lineage_graph_path=Path(args.lineage),
        semanticist_report_path=Path(args.semantic),
        codebase_md_path=Path(args.codebase),
        llm_client=llm_client,
        llm_model=os.environ.get("NAVIGATOR_OPENROUTER_MODEL", "openai/gpt-4o-mini"),
    )
    payload = json.loads(args.args_json) if args.args_json else {}
    if not isinstance(payload, dict):
        raise ValueError("args_json must be a JSON object")
    out = nav.run_tool(args.tool, payload)
    print(json.dumps(out, indent=2))


def bootstrap_cmd(args: argparse.Namespace) -> None:
    """
    One-command setup for a brand new repo:
    - clone
    - run Surveyor + Hydrologist
    - run Semanticist (optional)
    - run Phase 4 (CODEBASE.md + trace)
    """
    from phase4 import phase4_incremental_run
    from cartography_trace import log_cartography_trace
    from datetime import datetime, timezone

    target_dir = Path(args.target_dir)
    if args.repo_url:
        clone_repo(args.repo_url, str(target_dir))
        # clone_repo() logs its own trace entry.

    # Run static + lineage.
    from agents.surveyor import build_module_graph
    from agents.hydrologist import build_lineage_graph
    from graph.knowledge_graph import write_graph_json

    _, mg, _ = build_module_graph(target_dir)
    write_graph_json(mg, Path(args.modules))
    log_cartography_trace(
        {
            "agent": "Surveyor",
            "action": "build_module_graph",
            "evidence_source": str(args.modules),
            "line_range": None,
            "method": "static_analysis",
            "confidence": 0.9,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
    )

    _, lg, _ = build_lineage_graph(target_dir)
    write_graph_json(lg.graph, Path(args.lineage))
    log_cartography_trace(
        {
            "agent": "Hydrologist",
            "action": "build_lineage_graph",
            "evidence_source": str(args.lineage),
            "line_range": None,
            "method": "static_analysis",
            "confidence": 0.85,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
    )

    # Optional semantic run.
    if not args.skip_semantic:
        ns = argparse.Namespace(
            modules=args.modules,
            lineage=args.lineage,
            out=args.semantic_out,
            repo_path=str(target_dir),
            provider=args.provider,
            bulk_provider=args.bulk_provider,
            synth_provider=args.synth_provider,
            timeout_s=args.timeout_s,
            bulk_timeout_s=args.bulk_timeout_s,
            synth_timeout_s=args.synth_timeout_s,
            ollama_host=args.ollama_host,
            openrouter_host=args.openrouter_host,
            openrouter_key_env=args.openrouter_key_env,
            gemini_host=args.gemini_host,
            gemini_key_env=args.gemini_key_env,
            gemini_bulk_key_env=args.gemini_bulk_key_env,
            gemini_synth_key_env=args.gemini_synth_key_env,
            bulk_model=args.bulk_model,
            synth_model=args.synth_model,
            embed_model=args.embed_model,
        )
        semantic_cmd(ns)
        log_cartography_trace(
            {
                "agent": "Semanticist",
                "action": "generate_semanticist_report",
                "evidence_source": str(args.semantic_out),
                "line_range": None,
                "method": "llm_inference",
                "confidence": 0.7,
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            }
        )

    # Phase 4 output.
    out = phase4_incremental_run(
        target_repo=target_dir,
        module_graph_path=Path(args.modules),
        lineage_graph_path=Path(args.lineage),
        semanticist_report_path=Path(args.semantic_out),
        codebase_md_path=Path(args.codebase_out),
    )
    print(f"Bootstrap complete. Generated {out}")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Codebase Cartography CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    clone_parser = subparsers.add_parser("clone", help="Clone a target repository")
    clone_parser.add_argument("repo_url", help="Git repository URL")
    clone_parser.add_argument("--target-dir", default="target_repo")
    clone_parser.set_defaults(func=lambda args: clone_repo(args.repo_url, args.target_dir))

    build_parser = subparsers.add_parser("build-langs", help="Build tree-sitter languages")
    build_parser.add_argument("--vendor-dir", default="vendor")
    build_parser.add_argument("--out-path", default="build/my-languages.so")
    build_parser.add_argument(
        "--languages",
        nargs="+",
        default=None,
        help="Languages to build, e.g. python sql yaml",
    )
    build_parser.add_argument("--force", action="store_true")
    build_parser.set_defaults(func=build_languages_cmd)

    list_parser = subparsers.add_parser("list-langs", help="Load compiled tree-sitter languages")
    list_parser.add_argument("--lib-path", default="build/my-languages.so")
    list_parser.add_argument(
        "--languages",
        nargs="+",
        default=None,
        help="Language names to load (defaults to the built set)",
    )
    list_parser.set_defaults(func=list_languages_cmd)

    survey_parser = subparsers.add_parser("survey", help="Run the surveyor agent")
    survey_parser.add_argument("--repo-path", default="target_repo")
    survey_parser.set_defaults(func=survey_cmd)

    hydrology_parser = subparsers.add_parser("hydrology", help="Run the hydrologist agent")
    hydrology_parser.add_argument("--repo-path", default="target_repo")
    hydrology_parser.add_argument(
        "--no-dbt-compile",
        action="store_true",
        help="Skip `dbt compile` even if this looks like a dbt project.",
    )
    hydrology_parser.add_argument(
        "--dbt-profiles-dir",
        default=None,
        help="Optional DBT_PROFILES_DIR to use for `dbt compile` (default: ./dbt if present).",
    )
    hydrology_parser.set_defaults(func=hydrology_cmd)

    run_parser = subparsers.add_parser("run", help="Run the full cartography pipeline")
    run_parser.add_argument("--repo-path", default="target_repo")
    run_parser.add_argument("--parallel", action="store_true", default=True, help="Run Surveyor and Hydrologist concurrently")
    run_parser.add_argument("--no-parallel", dest="parallel", action="store_false", help="Run Surveyor and Hydrologist sequentially")
    run_parser.set_defaults(func=run_cmd)

    semantic_parser = subparsers.add_parser("semantic", help="Run the Semanticist agent (Ollama/Gemini/OpenRouter)")
    semantic_parser.add_argument("--repo-path", default="target_repo")
    semantic_parser.add_argument("--modules", default=".cartography/module_graph.json")
    semantic_parser.add_argument("--lineage", default=".cartography/lineage_graph.json")
    semantic_parser.add_argument("--out", default=".cartography/semanticist_report.json")
    semantic_parser.add_argument("--provider", choices=["ollama", "openrouter", "gemini"], default="openrouter", help="LLM provider; default openrouter (set openRoute in .env)")
    semantic_parser.add_argument(
        "--bulk-provider",
        choices=["ollama", "openrouter", "gemini"],
        default=None,
        help="Override provider for bulk calls (default: --provider)",
    )
    semantic_parser.add_argument(
        "--synth-provider",
        choices=["ollama", "openrouter", "gemini"],
        default=None,
        help="Override provider for synthesis calls (default: --provider)",
    )
    semantic_parser.add_argument("--timeout-s", type=int, default=300)
    semantic_parser.add_argument(
        "--bulk-timeout-s",
        type=int,
        default=None,
        help="Override timeout for bulk purpose extraction calls (seconds).",
    )
    semantic_parser.add_argument(
        "--synth-timeout-s",
        type=int,
        default=None,
        help="Override timeout for synthesis calls (seconds).",
    )
    semantic_parser.add_argument(
        "--ollama-host",
        default=None,
        help="Ollama server base URL (default: env OLLAMA_HOST or http://127.0.0.1:11434)",
    )
    semantic_parser.add_argument("--gemini-host", default="https://generativelanguage.googleapis.com")
    semantic_parser.add_argument("--gemini-key-env", default="GemiBulk", help="Default Gemini API key env var (default: GemiBulk)")
    semantic_parser.add_argument("--gemini-bulk-key-env", default="GemiBulk", help="Gemini bulk key env var (default: GemiBulk)")
    semantic_parser.add_argument("--gemini-synth-key-env", default="GemiPrompt", help="Gemini synth key env var (default: GemiPrompt)")
    semantic_parser.add_argument("--openrouter-host", default="https://openrouter.ai")
    semantic_parser.add_argument("--openrouter-key-env", default="openRoute", help="Env var name holding your OpenRouter API key (default: openRoute)")
    semantic_parser.add_argument("--ollama-key-env", default="OLLAMA_API_KEY", help="Env var name holding Ollama API key (for Ollama Cloud)")
    # No hard-coded OpenRouter defaults: pass any model IDs you want.
    semantic_parser.add_argument("--bulk-model", default=None, help="Bulk/cheap model id (Ollama tag or OpenRouter model id)")
    semantic_parser.add_argument("--synth-model", default=None, help="Synthesis model id (Ollama tag or OpenRouter model id)")
    semantic_parser.add_argument("--embed-model", default="", help="Embeddings model id (optional). If empty, uses local hashing embeddings for clustering.")
    semantic_parser.set_defaults(func=semantic_cmd)

    refresh_parser = subparsers.add_parser("semantic-refresh-fde", help="Recompute Day-One (FDE) answers in an existing Semanticist report")
    refresh_parser.add_argument("--modules", default=".cartography/module_graph.json")
    refresh_parser.add_argument("--lineage", default=".cartography/lineage_graph.json")
    refresh_parser.add_argument("--in", dest="in_path", default=".cartography/semanticist_report.json", help="Existing Semanticist report to refresh")
    refresh_parser.add_argument("--out", default=".cartography/semanticist_report.json", help="Output report path (default overwrites input)")
    refresh_parser.set_defaults(func=semantic_refresh_fde_cmd)

    phase4_parser = subparsers.add_parser("phase4", help="Run Phase 4: incremental update + generate CODEBASE.md")
    phase4_parser.add_argument("--repo-path", default="target_repo")
    phase4_parser.add_argument("--modules", default=".cartography/module_graph.json")
    phase4_parser.add_argument("--lineage", default=".cartography/lineage_graph.json")
    phase4_parser.add_argument("--semantic", default=".cartography/semanticist_report.json")
    phase4_parser.add_argument("--out", default="CODEBASE.md")
    phase4_parser.set_defaults(func=phase4_cmd)

    nav_parser = subparsers.add_parser("navigate", help="Ask Navigator a question (returns JSON with evidence)")
    nav_parser.add_argument("query")
    nav_parser.add_argument("--modules", default=".cartography/module_graph.json")
    nav_parser.add_argument("--lineage", default=".cartography/lineage_graph.json")
    nav_parser.add_argument("--semantic", default=".cartography/semanticist_report.json")
    nav_parser.add_argument("--codebase", default="CODEBASE.md")
    nav_parser.set_defaults(func=navigate_cmd)

    nav_tool_parser = subparsers.add_parser("navigate-tool", help="Call a Navigator tool with structured JSON args")
    nav_tool_parser.add_argument("tool", choices=["find_implementation", "trace_lineage", "blast_radius", "explain_module"])
    nav_tool_parser.add_argument("--args-json", default="{}", help='JSON object string, e.g. \'{"concept":"revenue"}\'')
    nav_tool_parser.add_argument("--modules", default=".cartography/module_graph.json")
    nav_tool_parser.add_argument("--lineage", default=".cartography/lineage_graph.json")
    nav_tool_parser.add_argument("--semantic", default=".cartography/semanticist_report.json")
    nav_tool_parser.add_argument("--codebase", default="CODEBASE.md")
    nav_tool_parser.set_defaults(func=navigate_tool_cmd)

    bootstrap_parser = subparsers.add_parser("bootstrap", help="One-command: clone + run cartography + (optional) semantic + phase4")
    bootstrap_parser.add_argument("repo_url", nargs="?", default="", help="Git URL to clone (optional if target-dir already exists)")
    bootstrap_parser.add_argument("--target-dir", default="target_repo")
    bootstrap_parser.add_argument("--modules", default=".cartography/module_graph.json")
    bootstrap_parser.add_argument("--lineage", default=".cartography/lineage_graph.json")
    bootstrap_parser.add_argument("--semantic-out", default=".cartography/semanticist_report.json")
    bootstrap_parser.add_argument("--codebase-out", default="CODEBASE.md")
    bootstrap_parser.add_argument("--skip-semantic", action="store_true", help="Skip Semanticist run")
    # Reuse Semanticist/provider args for semantic stage.
    bootstrap_parser.add_argument("--provider", choices=["ollama", "openrouter", "gemini"], default="openrouter", help="Default openrouter (set openRoute in .env)")
    bootstrap_parser.add_argument("--bulk-provider", choices=["ollama", "openrouter", "gemini"], default=None)
    bootstrap_parser.add_argument("--synth-provider", choices=["ollama", "openrouter", "gemini"], default=None)
    bootstrap_parser.add_argument("--timeout-s", type=int, default=300)
    bootstrap_parser.add_argument("--bulk-timeout-s", type=int, default=None)
    bootstrap_parser.add_argument("--synth-timeout-s", type=int, default=None)
    bootstrap_parser.add_argument("--ollama-host", default="http://127.0.0.1:11434")
    bootstrap_parser.add_argument("--ollama-key-env", default="OLLAMA_API_KEY")
    bootstrap_parser.add_argument("--gemini-host", default="https://generativelanguage.googleapis.com")
    bootstrap_parser.add_argument("--gemini-key-env", default="GemiBulk")
    bootstrap_parser.add_argument("--gemini-bulk-key-env", default="GemiBulk")
    bootstrap_parser.add_argument("--gemini-synth-key-env", default="GemiPrompt")
    bootstrap_parser.add_argument("--openrouter-host", default="https://openrouter.ai")
    bootstrap_parser.add_argument("--openrouter-key-env", default="openRoute")
    bootstrap_parser.add_argument("--bulk-model", default=None)
    bootstrap_parser.add_argument("--synth-model", default=None)
    bootstrap_parser.add_argument("--embed-model", default="")
    bootstrap_parser.set_defaults(func=bootstrap_cmd)

    return parser.parse_args(argv)


def _load_dotenv() -> None:
    """Load .env from current working directory so openRoute etc. are available for all commands."""
    dotenv = Path.cwd() / ".env"
    if not dotenv.exists():
        return
    for raw_line in dotenv.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def main(argv: list[str] | None = None) -> None:
    _load_dotenv()
    args = _parse_args(argv or sys.argv[1:])
    args.func(args)


if __name__ == "__main__":
    main()
