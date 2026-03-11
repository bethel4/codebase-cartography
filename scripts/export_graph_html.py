from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def load_node_link(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    nodes = data.get("nodes") or []
    links = data.get("links") or data.get("edges") or []
    if not isinstance(nodes, list) or not isinstance(links, list):
        raise ValueError(f"Unexpected node-link JSON in {path}")
    return nodes, links


def limit_by_degree(nodes: list[dict[str, Any]], links: list[dict[str, Any]], max_nodes: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if max_nodes <= 0 or len(nodes) <= max_nodes:
        return nodes, links

    degrees: dict[str, int] = {}
    for link in links:
        source = link.get("source")
        target = link.get("target")
        if source is None or target is None:
            continue
        degrees[str(source)] = degrees.get(str(source), 0) + 1
        degrees[str(target)] = degrees.get(str(target), 0) + 1

    def node_id(node: dict[str, Any]) -> str | None:
        value = node.get("id") or node.get("name")
        return str(value) if value is not None else None

    ranked = sorted(
        (nid for nid in (node_id(n) for n in nodes) if nid is not None),
        key=lambda nid: degrees.get(nid, 0),
        reverse=True,
    )
    keep = set(ranked[:max_nodes])
    filtered_nodes = [n for n in nodes if (node_id(n) in keep)]
    filtered_links = [
        l
        for l in links
        if (str(l.get("source")) in keep) and (str(l.get("target")) in keep)
    ]
    return filtered_nodes, filtered_links


HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Cartography Graph Viewer</title>
  <style>
    :root {{
      --bg: #0b1020;
      --panel: #111834;
      --text: #e7e9f3;
      --muted: #a6accd;
      --accent: #7aa2ff;
      --edge: rgba(166, 172, 205, 0.35);
      --node: #2a335f;
      --node-hi: #7aa2ff;
      --danger: #ff6b6b;
    }}
    html, body {{ height: 100%; margin: 0; background: var(--bg); color: var(--text); font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto; }}
    #wrap {{ display: grid; grid-template-columns: 360px 1fr; height: 100%; }}
    #panel {{ background: var(--panel); border-right: 1px solid rgba(255,255,255,0.08); padding: 14px; overflow: auto; }}
    #panel h1 {{ font-size: 16px; margin: 0 0 10px; }}
    #panel .hint {{ font-size: 12px; color: var(--muted); line-height: 1.4; }}
    #panel input {{ width: 100%; padding: 10px 12px; border-radius: 10px; border: 1px solid rgba(255,255,255,0.12); background: rgba(255,255,255,0.05); color: var(--text); outline: none; }}
    #panel input:focus {{ border-color: rgba(122,162,255,0.6); box-shadow: 0 0 0 3px rgba(122,162,255,0.15); }}
    #stats {{ margin-top: 10px; font-size: 12px; color: var(--muted); }}
    #details {{ margin-top: 12px; font-size: 12px; }}
    #details pre {{ white-space: pre-wrap; word-break: break-word; background: rgba(255,255,255,0.04); padding: 10px; border-radius: 10px; border: 1px solid rgba(255,255,255,0.08); }}
    #canvas {{ width: 100%; height: 100%; display: block; }}
    .badge {{ display: inline-block; padding: 2px 8px; border-radius: 999px; background: rgba(122,162,255,0.12); color: var(--accent); margin-left: 6px; font-size: 11px; }}
  </style>
</head>
<body>
<div id="wrap">
  <div id="panel">
    <h1>Graph Viewer <span class="badge">offline</span></h1>
    <div class="hint">Search a node id/name to highlight it. Drag to pan, scroll to zoom.</div>
    <div style="height:10px"></div>
    <input id="search" placeholder="Search node id..." />
    <div id="stats"></div>
    <div id="details"></div>
    <div class="hint" style="margin-top:10px">
      Tips: hover nodes for labels; click a node to pin/unpin it.
    </div>
  </div>
  <canvas id="canvas"></canvas>
</div>

<script>
// Embedded node-link data:
const DATA = __DATA__;

function pickNodeId(n) {{
  return (n.id ?? n.name ?? n.path ?? "").toString();
}}

// Build node/edge lists
const nodes = (DATA.nodes || []).map((n) => {{
  const id = pickNodeId(n);
  return {{
    id,
    raw: n,
    x: (Math.random() - 0.5) * 800,
    y: (Math.random() - 0.5) * 600,
    vx: 0,
    vy: 0,
    pinned: false,
    degree: 0,
  }};
}}).filter(n => n.id.length > 0);

const nodeIndex = new Map(nodes.map((n, i) => [n.id, i]));
const edges = (DATA.links || DATA.edges || []).map((e) => {{
  const source = (e.source ?? "").toString();
  const target = (e.target ?? "").toString();
  const ttype = e.transformation_type ?? "";
  return {{ source, target, ttype, raw: e }};
}}).filter(e => nodeIndex.has(e.source) && nodeIndex.has(e.target));

for (const e of edges) {{
  nodes[nodeIndex.get(e.source)].degree++;
  nodes[nodeIndex.get(e.target)].degree++;
}}

// Canvas setup
const canvas = document.getElementById("canvas");
const ctx = canvas.getContext("2d");
function resize() {{
  const dpr = window.devicePixelRatio || 1;
  canvas.width = Math.floor(canvas.clientWidth * dpr);
  canvas.height = Math.floor(canvas.clientHeight * dpr);
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
}}
window.addEventListener("resize", resize);
resize();

let camX = 0, camY = 0, camScale = 1;
let dragging = false;
let lastX = 0, lastY = 0;
let hoverNode = null;
let selectedNode = null;
let searchTerm = "";

function worldToScreen(x, y) {{
  return {{
    x: (x + camX) * camScale + canvas.clientWidth / 2,
    y: (y + camY) * camScale + canvas.clientHeight / 2,
  }};
}}
function screenToWorld(x, y) {{
  return {{
    x: (x - canvas.clientWidth / 2) / camScale - camX,
    y: (y - canvas.clientHeight / 2) / camScale - camY,
  }};
}}

canvas.addEventListener("mousedown", (ev) => {{
  dragging = true;
  lastX = ev.clientX;
  lastY = ev.clientY;
}});
window.addEventListener("mouseup", () => dragging = false);
window.addEventListener("mousemove", (ev) => {{
  if (dragging) {{
    const dx = ev.clientX - lastX;
    const dy = ev.clientY - lastY;
    camX += dx / camScale;
    camY += dy / camScale;
    lastX = ev.clientX;
    lastY = ev.clientY;
  }}
  const w = screenToWorld(ev.clientX, ev.clientY);
  hoverNode = hitTest(w.x, w.y);
}});

canvas.addEventListener("wheel", (ev) => {{
  ev.preventDefault();
  const zoom = Math.exp(-ev.deltaY * 0.0012);
  const mx = ev.clientX;
  const my = ev.clientY;
  const before = screenToWorld(mx, my);
  camScale = Math.max(0.12, Math.min(4.0, camScale * zoom));
  const after = screenToWorld(mx, my);
  camX += (after.x - before.x);
  camY += (after.y - before.y);
}}, {{ passive: false }});

canvas.addEventListener("click", (ev) => {{
  const w = screenToWorld(ev.clientX, ev.clientY);
  const node = hitTest(w.x, w.y);
  if (!node) {{
    selectedNode = null;
    renderDetails();
    return;
  }}
  node.pinned = !node.pinned;
  selectedNode = node;
  renderDetails();
}});

function hitTest(wx, wy) {{
  // check larger nodes first
  const ranked = nodes.slice().sort((a,b) => (b.degree - a.degree));
  for (const n of ranked) {{
    const r = nodeRadius(n);
    const dx = n.x - wx;
    const dy = n.y - wy;
    if ((dx*dx + dy*dy) <= r*r) return n;
  }}
  return null;
}}

function nodeRadius(n) {{
  return 6 + Math.min(18, Math.sqrt(n.degree + 1) * 2.2);
}}

function styleNode(n) {{
  const isSearch = searchTerm && n.id.toLowerCase().includes(searchTerm);
  const isSelected = selectedNode && selectedNode.id === n.id;
  return {{
    fill: (isSelected || isSearch) ? "rgba(122,162,255,0.90)" : "rgba(42,51,95,0.95)",
    stroke: (n.pinned) ? "rgba(255,107,107,0.95)" : "rgba(0,0,0,0.25)",
    strokeWidth: n.pinned ? 2.5 : 1.2,
  }};
}}

function renderDetails() {{
  const stats = document.getElementById("stats");
  stats.textContent = `nodes=${nodes.length}, edges=${edges.length} | zoom=${camScale.toFixed(2)}`;

  const details = document.getElementById("details");
  if (!selectedNode) {{
    details.innerHTML = "";
    return;
  }}
  const kind = selectedNode.raw.kind || selectedNode.raw.language || "";
  const title = kind ? `${selectedNode.id} (${kind})` : selectedNode.id;
  const payload = JSON.stringify(selectedNode.raw, null, 2);
  details.innerHTML = `<div style="font-weight:600; margin-bottom:6px">${title}</div><pre>${payload}</pre>`;
}}

document.getElementById("search").addEventListener("input", (ev) => {{
  searchTerm = (ev.target.value || "").trim().toLowerCase();
}});

// Simple force simulation (repulsion + springs)
function stepPhysics() {{
  const repulsion = 9000;
  const spring = 0.012;
  const damping = 0.84;
  const maxSpeed = 18;

  // Repulsion (O(n^2) but ok for a couple hundred nodes)
  for (let i = 0; i < nodes.length; i++) {{
    for (let j = i + 1; j < nodes.length; j++) {{
      const a = nodes[i], b = nodes[j];
      const dx = a.x - b.x;
      const dy = a.y - b.y;
      const dist2 = dx*dx + dy*dy + 0.01;
      const f = repulsion / dist2;
      const fx = dx * f;
      const fy = dy * f;
      if (!a.pinned) {{ a.vx += fx; a.vy += fy; }}
      if (!b.pinned) {{ b.vx -= fx; b.vy -= fy; }}
    }}
  }}

  // Springs along edges
  for (const e of edges) {{
    const a = nodes[nodeIndex.get(e.source)];
    const b = nodes[nodeIndex.get(e.target)];
    const dx = b.x - a.x;
    const dy = b.y - a.y;
    const dist = Math.sqrt(dx*dx + dy*dy) + 0.001;
    const desired = 120;
    const diff = (dist - desired);
    const fx = (dx / dist) * diff * spring;
    const fy = (dy / dist) * diff * spring;
    if (!a.pinned) {{ a.vx += fx; a.vy += fy; }}
    if (!b.pinned) {{ b.vx -= fx; b.vy -= fy; }}
  }}

  // Integrate
  for (const n of nodes) {{
    if (n.pinned) {{
      n.vx = 0; n.vy = 0;
      continue;
    }}
    n.vx *= damping;
    n.vy *= damping;
    const speed = Math.sqrt(n.vx*n.vx + n.vy*n.vy);
    if (speed > maxSpeed) {{
      n.vx = (n.vx / speed) * maxSpeed;
      n.vy = (n.vy / speed) * maxSpeed;
    }}
    n.x += n.vx * 0.015;
    n.y += n.vy * 0.015;
  }}
}}

function draw() {{
  ctx.clearRect(0, 0, canvas.clientWidth, canvas.clientHeight);

  // Edges
  ctx.strokeStyle = "rgba(166, 172, 205, 0.28)";
  ctx.lineWidth = 1;
  for (const e of edges) {{
    const a = nodes[nodeIndex.get(e.source)];
    const b = nodes[nodeIndex.get(e.target)];
    const p1 = worldToScreen(a.x, a.y);
    const p2 = worldToScreen(b.x, b.y);
    ctx.beginPath();
    ctx.moveTo(p1.x, p1.y);
    ctx.lineTo(p2.x, p2.y);
    ctx.stroke();
  }}

  // Nodes
  for (const n of nodes) {{
    const p = worldToScreen(n.x, n.y);
    const r = nodeRadius(n);
    const st = styleNode(n);
    ctx.beginPath();
    ctx.arc(p.x, p.y, r, 0, Math.PI * 2);
    ctx.fillStyle = st.fill;
    ctx.fill();
    ctx.strokeStyle = st.stroke;
    ctx.lineWidth = st.strokeWidth;
    ctx.stroke();
  }}

  // Hover label
  const h = hoverNode;
  if (h) {{
    const p = worldToScreen(h.x, h.y);
    const text = h.id;
    ctx.font = "12px ui-sans-serif, system-ui";
    const pad = 6;
    const w = ctx.measureText(text).width + pad * 2;
    const x = p.x + 12;
    const y = p.y - 10;
    ctx.fillStyle = "rgba(17, 24, 52, 0.95)";
    ctx.strokeStyle = "rgba(255,255,255,0.12)";
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.roundRect(x, y - 16, w, 22, 8);
    ctx.fill();
    ctx.stroke();
    ctx.fillStyle = "rgba(231,233,243,0.95)";
    ctx.fillText(text, x + pad, y);
  }}
}}

// Polyfill for roundRect (older canvases)
if (!CanvasRenderingContext2D.prototype.roundRect) {{
  CanvasRenderingContext2D.prototype.roundRect = function(x, y, w, h, r) {{
    const rr = Math.min(r, w/2, h/2);
    this.beginPath();
    this.moveTo(x + rr, y);
    this.arcTo(x + w, y, x + w, y + h, rr);
    this.arcTo(x + w, y + h, x, y + h, rr);
    this.arcTo(x, y + h, x, y, rr);
    this.arcTo(x, y, x + w, y, rr);
    this.closePath();
    return this;
  }}
}}

function tick() {{
  stepPhysics();
  draw();
  renderDetails();
  requestAnimationFrame(tick);
}}

renderDetails();
tick();
</script>
</body>
</html>
"""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export cartography graph JSON to a single-file HTML viewer.")
    parser.add_argument("--graph", required=True, help="Path to `.cartography/*_graph.json`")
    parser.add_argument("--out", default=None, help="Output HTML path (default: build/<name>.html)")
    parser.add_argument("--max-nodes", type=int, default=250, help="Keep only top-degree nodes (default: 250)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    in_path = Path(args.graph)
    if not in_path.exists():
        raise FileNotFoundError(in_path)

    nodes, links = load_node_link(in_path)
    nodes, links = limit_by_degree(nodes, links, args.max_nodes)
    data = {"nodes": nodes, "links": links, "directed": True}

    out_path = Path(args.out) if args.out else Path("build") / f"{in_path.stem}.html"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    html = HTML_TEMPLATE.replace("__DATA__", json.dumps(data))
    out_path.write_text(html, encoding="utf-8")
    print(f"Wrote HTML: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

