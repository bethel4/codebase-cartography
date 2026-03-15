from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from reports.analytics_report import write_analytics_report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a Markdown analytics report from cartography artifacts.")
    parser.add_argument("--module-graph", default=".cartography/module_graph.json", help="Path to module_graph.json")
    parser.add_argument("--lineage-graph", default=".cartography/lineage_graph.json", help="Path to lineage_graph.json")
    parser.add_argument("--out", default="build/analytics_report.md", help="Output Markdown path")
    parser.add_argument("--top", type=int, default=10, help="Top-N items to list (default: 10)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    out_path = write_analytics_report(
        module_graph_json=Path(args.module_graph),
        lineage_graph_json=Path(args.lineage_graph),
        out_path=Path(args.out),
        top_n=args.top,
    )
    print(f"Wrote report: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
