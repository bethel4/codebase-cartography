from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from agents.hydrologist import build_lineage_graph
from agents.surveyor import build_module_graph
from analyzers.tree_sitter_analyzer import DEFAULT_LANGUAGES, build_languages
from graph.knowledge_graph import write_graph_json
from orchestrator import run
from tree_sitter import Language


def clone_repo(repo_url: str, target_dir: str = "target_repo") -> None:
    """Clone a repository into the local workspace."""
    target_path = Path(target_dir)
    target_path.mkdir(parents=True, exist_ok=True)
    if any(target_path.iterdir()):
        raise RuntimeError(f"Target directory is not empty: {target_path}")

    subprocess.run(["git", "clone", repo_url, str(target_path)], check=True)
    print(f"Repository cloned to {target_path}")


def build_languages_cmd(args: argparse.Namespace) -> None:
    """CLI handler for building tree-sitter libraries."""
    build_languages(
        vendor_dir=args.vendor_dir,
        out_path=args.out_path,
        languages=args.languages,
        force=args.force,
    )
    print(f"Tree-sitter library ready at {args.out_path}")


def list_languages_cmd(args: argparse.Namespace) -> None:
    """CLI handler that loads and prints the languages from a shared library."""
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
    _, graph, _ = build_module_graph(args.repo_path)
    write_graph_json(graph, Path(".cartography") / "module_graph.json")
    print("Surveyor analysis complete. Graph saved to .cartography/module_graph.json")


def hydrology_cmd(args: argparse.Namespace) -> None:
    """CLI handler for the Hydrologist agent."""
    _, graph, _ = build_lineage_graph(args.repo_path)
    write_graph_json(graph.graph, Path(".cartography") / "lineage_graph.json")
    print("Hydrologist analysis complete. Graph saved to .cartography/lineage_graph.json")


def run_cmd(args: argparse.Namespace) -> None:
    """CLI handler to run the full cartography pipeline."""
    result = run(args.repo_path, parallel=args.parallel)
    if result.module_error:
        print(f"Surveyor failed: {result.module_error}", file=sys.stderr)
    if result.lineage_error:
        print(f"Hydrologist failed: {result.lineage_error}", file=sys.stderr)
    if result.module_graph_path and result.lineage_graph_path:
        print(f"Cartography complete. Graphs saved to {result.module_graph_path} and {result.lineage_graph_path}")
    else:
        print("Cartography completed with errors. Check stderr for details.", file=sys.stderr)


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
    hydrology_parser.set_defaults(func=hydrology_cmd)

    run_parser = subparsers.add_parser("run", help="Run the full cartography pipeline")
    run_parser.add_argument("--repo-path", default="target_repo")
    run_parser.add_argument("--parallel", action="store_true", default=True, help="Run Surveyor and Hydrologist concurrently")
    run_parser.add_argument("--no-parallel", dest="parallel", action="store_false", help="Run Surveyor and Hydrologist sequentially")
    run_parser.set_defaults(func=run_cmd)

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv or sys.argv[1:])
    args.func(args)


if __name__ == "__main__":
    main()
