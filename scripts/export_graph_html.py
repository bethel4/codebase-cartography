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

    def should_always_keep(node: dict[str, Any]) -> bool:
        # Keep "dead/orphan" candidates even when limiting for performance.
        if node.get("is_orphan") is True:
            return True
        if node.get("unimported") is True:
            return True
        if node.get("is_dead_code_candidate") is True:
            return True
        dead_exports = node.get("dead_exports")
        if isinstance(dead_exports, list) and dead_exports:
            return True
        if node.get("dbt_resource_type") in {"model", "source"} and node.get("is_orphan") is True:
            return True
        return False

    ranked = sorted(
        (nid for nid in (node_id(n) for n in nodes) if nid is not None),
        key=lambda nid: degrees.get(nid, 0),
        reverse=True,
    )
    keep = set(ranked[:max_nodes])
    for n in nodes:
        if should_always_keep(n):
            nid = node_id(n)
            if nid:
                keep.add(nid)
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
      --bg: #f5f7fb;
      --panel: #ffffff;
      --text: #111827;
      --muted: #6b7280;
      --accent: #2563eb;
      --dead: #ef4444;
      --warn: #f59e0b;
      --orphan: #fb923c;
      --sink: #2563eb;
      --source: #22c55e;
      --edge: rgba(148, 163, 184, 0.45);
      --node: #e5e7eb;
      --node-hi: #2563eb;
      --danger: #ef4444;
    }}
    html, body {{ height: 100%; margin: 0; background: var(--bg); color: var(--text); font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto; }}
    #wrap {{ display: grid; grid-template-columns: 360px 1fr; height: 100%; }}
    #panel {{ background: var(--panel); border-right: 1px solid rgba(148,163,184,0.35); padding: 14px; overflow: auto; }}
    #panel h1 {{ font-size: 16px; margin: 0 0 10px; }}
    #panel .hint {{ font-size: 12px; color: var(--muted); line-height: 1.4; }}
    #panel input {{ width: 100%; padding: 10px 12px; border-radius: 10px; border: 1px solid rgba(148,163,184,0.45); background: #f9fafb; color: var(--text); outline: none; }}
    #panel input:focus {{ border-color: rgba(37,99,235,0.8); box-shadow: 0 0 0 3px rgba(37,99,235,0.25); }}
    #toolbar {{ display: flex; gap: 8px; flex-wrap: wrap; margin-top: 10px; }}
    #toolbar button {{
      border: 1px solid rgba(148,163,184,0.45);
      background: #f9fafb;
      color: var(--text);
      border-radius: 10px;
      padding: 8px 10px;
      font-size: 12px;
      cursor: pointer;
    }}
    #toolbar button:hover {{ border-color: rgba(37,99,235,0.8); background: #eff6ff; }}
    #toolbar .toggle {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 6px 10px;
      border: 1px solid rgba(148,163,184,0.45);
      border-radius: 10px;
      background: #f9fafb;
      font-size: 12px;
      color: var(--muted);
    }}
    #stats {{ margin-top: 10px; font-size: 12px; color: var(--muted); }}
    #details {{ margin-top: 12px; font-size: 12px; }}
    #details pre {{ white-space: pre-wrap; word-break: break-word; background: #f3f4f6; padding: 10px; border-radius: 10px; border: 1px solid rgba(148,163,184,0.35); }}
    #canvas {{ width: 100%; height: 100%; display: block; background: #f9fafb; }}
    #tooltip {{
      position: fixed;
      pointer-events: none;
      z-index: 10;
      max-width: 520px;
      background: rgba(255,255,255,0.98);
      border: 1px solid rgba(148,163,184,0.45);
      border-radius: 12px;
      padding: 10px 12px;
      box-shadow: 0 10px 30px rgba(0,0,0,0.35);
      display: none;
    }}
    #tooltip .title {{ font-weight: 650; margin-bottom: 6px; }}
    #tooltip .meta {{ color: var(--muted); font-size: 12px; line-height: 1.4; }}
	    #tooltip pre {{
	      margin: 8px 0 0;
	      font-size: 11px;
	      color: rgba(31,41,55,0.92);
	      white-space: pre-wrap;
	      word-break: break-word;
	      max-height: 220px;
	      overflow: auto;
	      border-top: 1px solid rgba(255,255,255,0.08);
	      padding-top: 8px;
	    }}
	    #emptyState {{
	      position: fixed;
	      inset: 0;
	      display: none;
	      align-items: center;
	      justify-content: center;
	      padding: 18px;
	      z-index: 60;
	      pointer-events: none;
	    }}
	    #emptyState .card {{
	      pointer-events: auto;
	      max-width: 720px;
	      width: calc(100vw - 36px);
	      border: 1px dashed rgba(255,255,255,0.22);
	      border-radius: 14px;
	      background: rgba(17, 24, 52, 0.92);
	      backdrop-filter: blur(10px);
	      padding: 14px 14px 12px;
	      box-shadow: 0 20px 60px rgba(0,0,0,0.45);
	    }}
	    #emptyState h3 {{ margin: 0 0 8px; font-size: 14px; color: var(--accent); }}
	    #emptyState p {{ margin: 0 0 8px; color: var(--muted); line-height: 1.45; font-size: 12px; }}
	    #emptyState code {{ font-size: 11px; }}
	    .badge {{ display: inline-block; padding: 2px 8px; border-radius: 999px; background: rgba(122,162,255,0.12); color: var(--accent); margin-left: 6px; font-size: 11px; }}
	    .kpi {{ display:flex; gap:8px; flex-wrap:wrap; margin: 8px 0 10px; }}
	    .kpi .chip {{ border: 1px solid rgba(255,255,255,0.12); background: rgba(255,255,255,0.04); border-radius: 999px; padding: 4px 10px; font-size: 12px; color: var(--muted); }}
	    .legend {{ display:flex; gap:8px; flex-wrap:wrap; margin-top: 10px; }}
    .legend .key {{ display:inline-flex; align-items:center; gap:8px; border: 1px solid rgba(255,255,255,0.12); background: rgba(255,255,255,0.04); border-radius: 999px; padding: 4px 10px; font-size: 12px; color: var(--muted); }}
    .legend .dot {{ width: 10px; height: 10px; border-radius: 999px; display:inline-block; }}
    .linkbtn {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      margin: 4px 6px 0 0;
      padding: 6px 10px;
      border: 1px solid rgba(255,255,255,0.12);
      border-radius: 999px;
      background: rgba(255,255,255,0.05);
      color: var(--text);
      cursor: pointer;
      font-size: 12px;
      text-decoration: none;
      max-width: 100%;
    }}
    .linkbtn:hover {{ border-color: rgba(122,162,255,0.65); }}
    .small {{ font-size: 11px; color: var(--muted); }}

    /* Embed mode: hide the side panel so the canvas can fill the page (useful for iframes). */
    body.embed #wrap {{ grid-template-columns: 1fr; }}
    body.embed #panel {{ display: none; }}
    #togglePanel {{
      position: fixed;
      top: 10px;
      left: 10px;
      z-index: 20;
      display: none;
      border: 1px solid rgba(255,255,255,0.12);
      background: rgba(17, 24, 52, 0.85);
      color: var(--text);
      border-radius: 10px;
      padding: 8px 10px;
      font-size: 12px;
      cursor: pointer;
      backdrop-filter: blur(8px);
    }}
    #togglePanel:hover {{ border-color: rgba(122,162,255,0.65); }}
    body.embed #togglePanel {{ display: inline-flex; }}
  </style>
</head>
<body>
<button id="togglePanel" title="Show/hide panel (p)">Panel</button>
<div id="wrap">
  <div id="panel">
    <h1>Graph Viewer <span class="badge">offline</span></h1>
    <div class="hint">Search a node id/name to highlight it. Drag to pan, scroll to zoom.</div>
    <div style="height:10px"></div>
    <input id="search" placeholder="Search node id..." />
    <div id="toolbar">
      <button id="zoomIn" title="Zoom in (+)">Zoom +</button>
      <button id="zoomOut" title="Zoom out (-)">Zoom -</button>
      <button id="fit" title="Fit to screen (f)">Fit</button>
      <button id="reset" title="Reset view (r)">Reset</button>
      <button id="pause" title="Pause/resume physics (space)">Pause</button>
      <span class="toggle">
        <input type="checkbox" id="labels" />
        <label for="labels">Show labels</label>
      </span>
      <span class="toggle">
        <input type="checkbox" id="neighbors" checked />
        <label for="neighbors">Neighbor focus</label>
      </span>
    </div>
    <div id="stats"></div>
    <div id="details"></div>
    <div class="legend" title="Node coloring legend">
      <span class="key"><span class="dot" style="background: var(--dead)"></span>orphan</span>
      <span class="key"><span class="dot" style="background: var(--orphan)"></span>unimported</span>
      <span class="key"><span class="dot" style="background: var(--warn)"></span>dead exports</span>
      <span class="key"><span class="dot" style="background: var(--node)"></span>normal</span>
    </div>
    <div class="hint" style="margin-top:10px">
      Tips: click a node to focus upstream/downstream; <code>shift+click</code> pins; press <code>f</code> to fit.
    </div>
  </div>
  <canvas id="canvas"></canvas>
	</div>
	<div id="tooltip"></div>
<div id="emptyState" aria-live="polite">
  <div class="card">
    <h3>Graph is empty</h3>
    <p>No nodes were found in the embedded graph data. This usually means the underlying JSON artifact is empty or missing.</p>
    <p class="small">Note: the <b>Module</b> graph is Python-import focused; if your repo is mostly SQL/dbt, it may legitimately be empty unless a dbt <code>target/manifest.json</code> is present.</p>
    <p>Try regenerating artifacts:</p>
    <p><code>uv run python src/cli.py survey --repo-path target_repo</code> (module graph) or <code>uv run python src/cli.py hydrology --repo-path target_repo</code> (lineage graph).</p>
  </div>
</div>

	<script>
	// Embedded node-link data:
	const DATA = __DATA__;
const params = new URLSearchParams(window.location.search);
const isEmbed = params.get("embed") === "1" || params.get("embed") === "true";
if (isEmbed) {{
  document.body.classList.add("embed");
}}

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

	// Empty-state guidance (common first-run issue).
	const emptyState = document.getElementById("emptyState");
	if (emptyState && nodes.length === 0) {{
	  emptyState.style.display = "flex";
	}}

// Adjacency maps for direction-aware focus.
const inNeighbors = new Map();
const outNeighbors = new Map();
for (const n of nodes) {{
  inNeighbors.set(n.id, new Set());
  outNeighbors.set(n.id, new Set());
}}
for (const e of edges) {{
  outNeighbors.get(e.source).add(e.target);
  inNeighbors.get(e.target).add(e.source);
}}

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
let showLabels = false;
let physicsPaused = false;
let mouseX = 0, mouseY = 0;
let neighborFocus = true;
const tooltip = document.getElementById("tooltip");
const togglePanel = document.getElementById("togglePanel");
let panelVisible = !isEmbed;

function setPanelVisible(visible) {{
  panelVisible = visible;
  const wrap = document.getElementById("wrap");
  const panel = document.getElementById("panel");
  if (!wrap || !panel) return;
  if (panelVisible) {{
    document.body.classList.remove("embed");
  }} else {{
    document.body.classList.add("embed");
  }}
}}

togglePanel.addEventListener("click", () => setPanelVisible(!panelVisible));

// neighbor index (undirected) for hover highlighting
const neighbors = new Map();
for (const n of nodes) neighbors.set(n.id, new Set());
for (const e of edges) {{
  neighbors.get(e.source).add(e.target);
  neighbors.get(e.target).add(e.source);
}}

// Precompute searchable text for attribute search/highlight.
for (const n of nodes) {{
  try {{
    n.searchText = (n.id + " " + JSON.stringify(n.raw)).toLowerCase();
  }} catch {{
    n.searchText = n.id.toLowerCase();
  }}
}}

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

function eventToCanvas(ev) {{
  const rect = canvas.getBoundingClientRect();
  return {{
    x: ev.clientX - rect.left,
    y: ev.clientY - rect.top,
  }};
}}

canvas.addEventListener("mousedown", (ev) => {{
  dragging = true;
  const p = eventToCanvas(ev);
  lastX = p.x;
  lastY = p.y;
}});
window.addEventListener("mouseup", () => dragging = false);
window.addEventListener("mousemove", (ev) => {{
  mouseX = ev.clientX;
  mouseY = ev.clientY;
  const p = eventToCanvas(ev);
  if (dragging) {{
    const dx = p.x - lastX;
    const dy = p.y - lastY;
    camX += dx / camScale;
    camY += dy / camScale;
    lastX = p.x;
    lastY = p.y;
  }}
  // Only attempt hover hit-test when cursor is inside the canvas.
  if (p.x >= 0 && p.y >= 0 && p.x <= canvas.clientWidth && p.y <= canvas.clientHeight) {{
    const w = screenToWorld(p.x, p.y);
    hoverNode = hitTest(w.x, w.y);
  }} else {{
    hoverNode = null;
  }}
  renderTooltip();
}});

canvas.addEventListener("wheel", (ev) => {{
  ev.preventDefault();
  const zoom = Math.exp(-ev.deltaY * 0.0012);
  const p = eventToCanvas(ev);
  const before = screenToWorld(p.x, p.y);
  camScale = Math.max(0.12, Math.min(4.0, camScale * zoom));
  const after = screenToWorld(p.x, p.y);
  camX += (after.x - before.x);
  camY += (after.y - before.y);
}}, {{ passive: false }});

canvas.addEventListener("click", (ev) => {{
  const p = eventToCanvas(ev);
  const w = screenToWorld(p.x, p.y);
  const node = hitTest(w.x, w.y);
  if (!node) {{
    selectedNode = null;
    renderDetails();
    return;
  }}
  // Shift-click toggles pin; plain click focuses upstream/downstream.
  if (ev.shiftKey) {{
    node.pinned = !node.pinned;
  }}
  selectedNode = node;
  renderDetails();

  // If embedded (e.g. inside the dashboard), tell the parent which node is focused.
  try {{
    window.parent?.postMessage?.({{
      type: "cartography:focus",
      node_id: node.id,
      upstream: Array.from(inNeighbors.get(node.id) || []),
      downstream: Array.from(outNeighbors.get(node.id) || []),
      raw: node.raw || {{}},
    }}, "*");
  }} catch {{}}
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
  const isSelected = selectedNode && selectedNode.id === n.id;
  const isSearch = searchTerm && matchesSearch(n, searchTerm);
  const isHover = hoverNode && hoverNode.id === n.id;
  const focusId = selectedNode?.id || (neighborFocus && hoverNode ? hoverNode.id : null);
  const isUp = focusId ? (inNeighbors.get(focusId)?.has(n.id) || false) : false;
  const isDown = focusId ? (outNeighbors.get(focusId)?.has(n.id) || false) : false;
  const isNeighbor = (neighborFocus && hoverNode) ? (neighbors.get(hoverNode.id)?.has(n.id) || false) : false;
  const isConnected = isHover || isNeighbor || isUp || isDown;
  const isHighlighted = isSelected || isSearch || isConnected;

  // Base (unhighlighted) color: encode "dead/unconnected" state directly in the graph.
  const hasDeadExports = Array.isArray(n.raw?.dead_exports) && n.raw.dead_exports.length > 0;
  const isOrphan = n.raw?.is_orphan === true;
  const isUnimported = n.raw?.unimported === true;

  let fill = "rgba(42,51,95,0.95)";
  if (isOrphan) fill = "rgba(255,107,107,0.88)";          // red
  else if (hasDeadExports) fill = "rgba(255,209,102,0.88)"; // yellow
  else if (isUnimported) fill = "rgba(255,159,28,0.88)";  // orange

  if (isHighlighted) {{
    if (isSelected) fill = "rgba(122,162,255,0.95)";
    else if (isDown) fill = "rgba(122,162,255,0.78)";
    else if (isUp) fill = "rgba(111,237,188,0.78)";
    else fill = "rgba(122,162,255,0.72)";
  }}
  return {{
    fill,
    stroke: (n.pinned)
      ? "rgba(255,107,107,0.95)"
      : (isNeighbor ? "rgba(122,162,255,0.70)" : (isOrphan ? "rgba(255,107,107,0.70)" : "rgba(0,0,0,0.25)")),
    strokeWidth: n.pinned ? 2.5 : 1.2,
    alpha: (focusId && !isHighlighted && !isSearch && !isSelected) ? 0.16 : 1.0,
  }};
}}

function matchesSearch(node, term) {{
  const t = term.trim().toLowerCase();
  if (!t) return false;

  // Support "key:value" searches against node.raw attributes.
  const sepIdx = t.indexOf(":");
  if (sepIdx > 0) {{
    const key = t.slice(0, sepIdx).trim();
    const value = t.slice(sepIdx + 1).trim();
    if (!value) return false;
    const rawVal = (node.raw || {{}})[key];
    if (rawVal === undefined || rawVal === null) return false;
    return String(rawVal).toLowerCase().includes(value);
  }}

  // Default: substring match on id + JSON metadata blob.
  return (node.searchText || node.id.toLowerCase()).includes(t);
}}

function zoomBy(factor, mx = canvas.clientWidth / 2, my = canvas.clientHeight / 2) {{
  const before = screenToWorld(mx, my);
  camScale = Math.max(0.12, Math.min(4.0, camScale * factor));
  const after = screenToWorld(mx, my);
  camX += (after.x - before.x);
  camY += (after.y - before.y);
}}

function resetView() {{
  camX = 0;
  camY = 0;
  camScale = 1;
}}

function fitView(padding = 40) {{
  if (nodes.length === 0) return;
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  for (const n of nodes) {{
    minX = Math.min(minX, n.x);
    minY = Math.min(minY, n.y);
    maxX = Math.max(maxX, n.x);
    maxY = Math.max(maxY, n.y);
  }}
  const w = (maxX - minX) || 1;
  const h = (maxY - minY) || 1;
  const scaleX = (canvas.clientWidth - padding * 2) / w;
  const scaleY = (canvas.clientHeight - padding * 2) / h;
  camScale = Math.max(0.12, Math.min(4.0, Math.min(scaleX, scaleY)));
  // center world bbox at origin
  const cx = (minX + maxX) / 2;
  const cy = (minY + maxY) / 2;
  camX = -cx;
  camY = -cy;
}}

function renderDetails() {{
  const stats = document.getElementById("stats");
  const paused = physicsPaused ? "paused" : "running";
  stats.textContent = `nodes=${nodes.length}, edges=${edges.length} | zoom=${camScale.toFixed(2)} | physics=${paused}`;

  const details = document.getElementById("details");
  if (!selectedNode) {{
    details.innerHTML = "";
    return;
  }}
  const kind = selectedNode.raw.kind || selectedNode.raw.language || "";
  const title = kind ? `${selectedNode.id} (${kind})` : selectedNode.id;

  const upstream = Array.from(inNeighbors.get(selectedNode.id) || []);
  const downstream = Array.from(outNeighbors.get(selectedNode.id) || []);

  const maxList = 45;
  function listButtons(label, ids) {{
    const shown = ids.slice(0, maxList);
    const more = ids.length - shown.length;
    const items = shown.map((id) => `
      <button class="linkbtn" data-focus-node="${escapeHtml(id)}" title="Focus ${escapeHtml(id)}">${escapeHtml(id)}</button>
    `).join("");
    const suffix = more > 0 ? `<div class="small">… +${more} more</div>` : "";
    return `
      <div style="margin-top:10px">
        <div style="font-weight:600">${escapeHtml(label)}</div>
        <div>${items || `<span class="small">none</span>`}</div>
        ${suffix}
      </div>
    `;
  }}

  let payload = "";
  try {{
    payload = JSON.stringify(selectedNode.raw, null, 2);
  }} catch {{
    payload = String(selectedNode.raw);
  }}

  details.innerHTML = `
    <div style="font-weight:650; margin-bottom:6px">${escapeHtml(title)}</div>
    <div class="kpi">
      <span class="chip">in=${upstream.length}</span>
      <span class="chip">out=${downstream.length}</span>
      <span class="chip">degree=${selectedNode.degree}</span>
      <span class="chip">${selectedNode.pinned ? "pinned" : "not pinned"}</span>
    </div>
    ${listButtons("Upstream (incoming)", upstream)}
    ${listButtons("Downstream (outgoing)", downstream)}
    <div style="margin-top:10px; font-weight:600">Node metadata</div>
    <pre>${escapeHtml(payload)}</pre>
  `;
}}

document.getElementById("search").addEventListener("input", (ev) => {{
  searchTerm = (ev.target.value || "").trim().toLowerCase();
}});

document.getElementById("labels").addEventListener("change", (ev) => {{
  showLabels = !!ev.target.checked;
}});

document.getElementById("neighbors").addEventListener("change", (ev) => {{
  neighborFocus = !!ev.target.checked;
}});

document.getElementById("zoomIn").addEventListener("click", () => zoomBy(1.15));
document.getElementById("zoomOut").addEventListener("click", () => zoomBy(1/1.15));
document.getElementById("reset").addEventListener("click", () => resetView());
document.getElementById("fit").addEventListener("click", () => fitView());
document.getElementById("pause").addEventListener("click", () => {{
  physicsPaused = !physicsPaused;
  document.getElementById("pause").textContent = physicsPaused ? "Resume" : "Pause";
}});

function escapeHtml(s) {{
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}}

document.getElementById("details").addEventListener("click", (ev) => {{
  const btn = ev.target?.closest?.("[data-focus-node]");
  if (!btn) return;
  const id = btn.getAttribute("data-focus-node");
  if (!id) return;
  const idx = nodeIndex.get(id);
  if (idx === undefined) return;
  selectedNode = nodes[idx];
  renderDetails();
}});

function renderTooltip() {{
  const n = hoverNode;
  if (!n) {{
    tooltip.style.display = "none";
    return;
  }}
  const kind = n.raw.kind || n.raw.language || "";
  const title = kind ? `${n.id} (${kind})` : n.id;
  const neighborCount = neighbors.get(n.id)?.size ?? 0;

  let payload = "";
  try {{
    payload = JSON.stringify(n.raw, null, 2);
  }} catch {{
    payload = String(n.raw);
  }}

  tooltip.innerHTML = `
    <div class="title">${escapeHtml(title)}</div>
    <div class="meta">degree=${n.degree}, neighbors=${neighborCount}</div>
    <pre>${escapeHtml(payload)}</pre>
  `;
  tooltip.style.display = "block";
  const pad = 14;
  const maxX = window.innerWidth - tooltip.offsetWidth - pad;
  const maxY = window.innerHeight - tooltip.offsetHeight - pad;
  const x = Math.max(pad, Math.min(maxX, mouseX + 16));
  const y = Math.max(pad, Math.min(maxY, mouseY + 16));
  tooltip.style.left = `${x}px`;
  tooltip.style.top = `${y}px`;
}}

window.addEventListener("keydown", (ev) => {{
  if (ev.key === "+" || ev.key === "=") zoomBy(1.15);
  else if (ev.key === "-" || ev.key === "_") zoomBy(1/1.15);
  else if (ev.key === "f" || ev.key === "F") fitView();
  else if (ev.key === "r" || ev.key === "R") resetView();
  else if (ev.key === "p" || ev.key === "P") setPanelVisible(!panelVisible);
  else if (ev.code === "Space") {{
    ev.preventDefault();
    physicsPaused = !physicsPaused;
    document.getElementById("pause").textContent = physicsPaused ? "Resume" : "Pause";
  }}
}});

// Simple force simulation (repulsion + springs)
function stepPhysics() {{
  if (physicsPaused) return;
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
  const focusId = selectedNode?.id || (neighborFocus && hoverNode ? hoverNode.id : null);
  const focusIn = focusId ? (inNeighbors.get(focusId) || new Set()) : new Set();
  const focusOut = focusId ? (outNeighbors.get(focusId) || new Set()) : new Set();
  for (const e of edges) {{
    const a = nodes[nodeIndex.get(e.source)];
    const b = nodes[nodeIndex.get(e.target)];
    const p1 = worldToScreen(a.x, a.y);
    const p2 = worldToScreen(b.x, b.y);

    let alpha = 0.22;
    let width = 1;
    let color = `rgba(166, 172, 205, ${alpha})`;

    if (focusId) {{
      const isOutgoing = (e.source === focusId) && focusOut.has(e.target);
      const isIncoming = (e.target === focusId) && focusIn.has(e.source);
      const isIncident = isOutgoing || isIncoming;
      if (isIncident) {{
        alpha = 0.92;
        width = 2.0;
        color = isOutgoing ? `rgba(122, 162, 255, ${alpha})` : `rgba(111, 237, 188, ${alpha})`;
      }} else {{
        alpha = 0.06;
        width = 1;
        color = `rgba(166, 172, 205, ${alpha})`;
      }}
    }}

    ctx.strokeStyle = color;
    ctx.lineWidth = width;
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
    ctx.globalAlpha = st.alpha ?? 1.0;
    ctx.beginPath();
    ctx.arc(p.x, p.y, r, 0, Math.PI * 2);
    ctx.fillStyle = st.fill;
    ctx.fill();
    ctx.strokeStyle = st.stroke;
    ctx.lineWidth = st.strokeWidth;
    ctx.stroke();
    ctx.globalAlpha = 1.0;
  }}

  // Labels: either hover-only, or always-on (for top-degree nodes).
  const labelNodes = showLabels
    ? nodes.slice().sort((a,b) => b.degree - a.degree).slice(0, 60)
    : (hoverNode ? [hoverNode] : []);

  for (const h of labelNodes) {{
    if (!h) continue;
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
    # The HTML template is a raw triple-quoted string (not `.format`), so the `{{` / `}}`
    # brace escaping isn't needed. Older versions of this script used `{{` widely, which
    # breaks CSS/JS in the browser. We normalize braces in the template *without touching*
    # the embedded graph JSON (which may legitimately contain `{{ ... }}` in dbt/Jinja).
    before, after = HTML_TEMPLATE.split("__DATA__", 1)
    before = before.replace("{{", "{").replace("}}", "}")
    after = after.replace("{{", "{").replace("}}", "}")
    html = before + json.dumps(data) + after
    out_path.write_text(html, encoding="utf-8")
    print(f"Wrote HTML: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
