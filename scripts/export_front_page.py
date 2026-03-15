from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from reports.front_page import write_front_page  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write a small interactive dashboard page for cartography graphs.")
    parser.add_argument("--out", default="build/index.html", help="Output HTML path (default: build/index.html)")
    parser.add_argument("--module-html", default="module_graph.html", help="Module graph HTML filename (relative to --out dir)")
    parser.add_argument("--lineage-html", default="lineage_graph.html", help="Lineage graph HTML filename (relative to --out dir)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    out_path = write_front_page(args.out, module_html=args.module_html, lineage_html=args.lineage_html)
    print(f"Wrote dashboard: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

