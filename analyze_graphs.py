from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure `src/` is importable when running as a script.
sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from graph_analysis.analyzer import analyze_graph  # noqa: E402
from graph_analysis.loader import load_digraph, write_json  # noqa: E402
from graph_analysis.visualization import render_pyvis  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze cartography graphs and generate reports/visualizations.")
    parser.add_argument("--lineage", required=True, help="Path to lineage_graph.json (node-link)")
    parser.add_argument("--modules", required=True, help="Path to module_graph.json (node-link)")
    parser.add_argument("--out-dir", default="build/graph_analysis", help="Output directory")
    parser.add_argument("--critical-k", type=int, default=15, help="Top critical nodes to report")
    parser.add_argument("--cycle-limit", type=int, default=50, help="Max cycles to record (per graph)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    module_graph = load_digraph(args.modules)
    lineage_graph = load_digraph(args.lineage)

    report = {
        "module_graph": analyze_graph(module_graph, critical_k=args.critical_k, cycle_limit=args.cycle_limit).to_dict(),
        "lineage_graph": analyze_graph(lineage_graph, critical_k=args.critical_k, cycle_limit=args.cycle_limit).to_dict(),
    }

    report_path = write_json(report, out_dir / "analysis_report.json")
    print(f"Wrote: {report_path}")

    # Interactive HTML via PyVis (optional dependency).
    try:
        module_html = render_pyvis(module_graph, out_dir / "module_graph.html")
        lineage_html = render_pyvis(lineage_graph, out_dir / "lineage_graph.html")
    except ImportError as exc:
        print(str(exc), file=sys.stderr)
        print("Skipping HTML generation (missing dependency).", file=sys.stderr)
        return 0
    except Exception as exc:
        print(f"PyVis rendering failed: {exc}", file=sys.stderr)
        print("Skipping HTML generation.", file=sys.stderr)
        return 0

    print(f"Wrote: {module_html}")
    print(f"Wrote: {lineage_html}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
