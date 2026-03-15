from __future__ import annotations

"""
Phase 4 audit logging (Week 1 pattern).

Every Phase 4 agent action MUST append a single JSON object per line to
`cartography_trace.jsonl`.

This module intentionally keeps the schema small and stable:
required fields:
- agent: str
- action: str
- evidence_source: str  (file path, dataset id, or graph artifact name)
- line_range: list[int] | None  (1-based line numbers, inclusive)
- method: str  (e.g., "static_analysis", "llm_inference", "graph_traversal", "git_diff")
- confidence: float  (0.0–1.0)
- timestamp: str  (ISO-8601, UTC recommended)

Evidence note:
The logging schema is separate from the "Navigator citations" schema. Navigator
answers should include explicit evidence blocks (file path + line range + method),
but each *action* that produced those blocks should also be traced here.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

TRACE_PATH = Path("cartography_trace.jsonl")

REQUIRED_KEYS = {
    "agent",
    "action",
    "evidence_source",
    "line_range",
    "method",
    "confidence",
    "timestamp",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def log_cartography_trace(entry: Mapping[str, Any], *, path: Path = TRACE_PATH) -> None:
    """
    Append a JSONL trace entry.

    This function enforces required keys and performs lightweight validation so the
    log remains machine-readable for audits.
    """
    missing = REQUIRED_KEYS.difference(entry.keys())
    if missing:
        raise ValueError(f"cartography_trace entry missing required keys: {sorted(missing)}")

    confidence = entry.get("confidence")
    if not isinstance(confidence, (int, float)) or not (0.0 <= float(confidence) <= 1.0):
        raise ValueError("cartography_trace entry `confidence` must be a float in [0, 1].")

    line_range = entry.get("line_range")
    if line_range is not None:
        if (
            not isinstance(line_range, list)
            or len(line_range) != 2
            or not all(isinstance(x, int) and x >= 1 for x in line_range)
        ):
            raise ValueError("cartography_trace entry `line_range` must be None or [start_line, end_line].")

    payload = dict(entry)
    payload.setdefault("timestamp", _utc_now_iso())

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")

