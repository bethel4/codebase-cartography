from __future__ import annotations

from pathlib import Path


HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Cartography Dashboard</title>
  <style>
    :root {
      --bg: #f5f7fb;
      --panel: #ffffff;
      --text: #111827;
      --muted: #6b7280;
      --accent: #2563eb;
      --border: rgba(15,23,42,0.12);
    }
    html, body { height: 100%; margin: 0; background: var(--bg); color: var(--text);
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto; }
    #wrap {
      height: 100%;
      display: grid;
      grid-template-rows: 48px 1fr;
      grid-template-columns: 200px 1fr 280px;
    }
    #top {
      grid-column: 1 / -1;
      display: flex; align-items: center; justify-content: space-between;
      padding: 0 14px;
      border-bottom: 1px solid var(--border);
      background: rgba(255,255,255,0.9);
      backdrop-filter: blur(10px);
    }
    #brand { font-weight: 700; font-size: 15px; color: #0f172a; }
    #actions { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
    #sidebar {
      grid-row: 2;
      background: var(--panel);
      border-right: 1px solid var(--border);
      padding: 12px 0;
      overflow-y: auto;
    }
    #sidebar .phase {
      display: block;
      padding: 10px 14px;
      color: var(--muted);
      text-decoration: none;
      font-size: 13px;
      border-left: 3px solid transparent;
    }
    #sidebar .phase:hover {
      color: var(--text);
      background: rgba(37,99,235,0.06);
    }
    #sidebar .phase.active {
      color: var(--accent);
      border-left-color: var(--accent);
      background: rgba(37,99,235,0.10);
    }
    #inspector {
      grid-row: 2;
      background: var(--panel);
      border-left: 1px solid var(--border);
      padding: 12px;
      overflow-y: auto;
      font-size: 12px;
    }
    #inspector h3 { margin: 0 0 10px; font-size: 13px; color: var(--accent); }
    #inspectorContent { color: var(--muted); white-space: pre-wrap; word-break: break-word; font-size: 11px; line-height: 1.45; }
    button, a.btn {
      border: 1px solid var(--border);
      background: #f9fafb;
      color: var(--text);
      border-radius: 10px;
      padding: 8px 10px;
      font-size: 12px;
      cursor: pointer;
      text-decoration: none;
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }
    button:hover, a.btn:hover {
      border-color: rgba(37,99,235,0.65);
      background: #eff6ff;
    }
    .pill { color: var(--muted); font-size: 12px; }
    .pill.bad { color: #b91c1c; }
    .pill.good { color: #059669; }
    #main { grid-row: 2; height: 100%; min-height: 0; }

    /* Views */
    .view { height: 100%; display: none; }
    .view.active { display: block; }
    /*
      Non-JS fallback: use dedicated hash targets to show exactly one view.
      This avoids "split screen" when a view is `.active` AND another is `:target`.
    */
    #viewModule { display: block; } /* default */
    #navModule:target ~ #viewModule { display: block; }
    #navModule:target ~ #viewLineage { display: none; }
    #navLineage:target ~ #viewLineage { display: block; }
    #navLineage:target ~ #viewModule { display: none; }
    iframe { width: 100%; height: 100%; border: 0; background: #f9fafb; }

    /* Navigator panel */
    #navPanel {
      position: fixed;
      top: 56px;
      right: 16px;
      width: min(560px, calc(100vw - 32px));
      max-height: calc(100vh - 80px);
      border: 1px solid var(--border);
      border-radius: 14px;
      background: var(--panel);
      backdrop-filter: blur(12px);
      display: none;
      flex-direction: column;
      overflow: hidden;
      box-shadow: 0 20px 60px rgba(15,23,42,0.18);
    }
    #navPanel.open { display: flex; }
    /* Non-JS fallback: open Navigator via #navPanel hash. */
    #navPanel:target { display: flex; }
    #navHead { display:flex; align-items:center; justify-content: space-between; padding: 10px 12px; border-bottom: 1px solid var(--border); }
    #navBody { padding: 10px 12px; display:flex; flex-direction: column; gap: 8px; overflow: auto; }
    #navRow { display:flex; gap: 8px; align-items: center; flex-wrap: wrap; }
    select, input[type="text"], textarea {
      background: #f9fafb;
      border: 1px solid var(--border);
      border-radius: 10px;
      color: var(--text);
      padding: 8px 10px;
      font-size: 12px;
      outline: none;
    }
    textarea { width: 100%; height: 86px; resize: vertical; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }
    .navToolForm { display: none; margin-top: 6px; }
    .navToolForm.active { display: block; }
    .navToolForm label { display: block; color: var(--muted); font-size: 11px; margin-bottom: 2px; }
    .navToolForm input, .navToolForm select { width: 100%; margin-bottom: 8px; }
    #navOut {
      white-space: pre-wrap;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 12px;
      color: var(--text);
      padding: 10px;
      border: 1px solid var(--border);
      border-radius: 12px;
      background: #f3f4f6;
      overflow: auto;
      min-height: 140px;
    }
  </style>
</head>
<body>
<div id="wrap">
  <div id="top">
    <div id="brand">Codebase Cartography</div>
    <div id="actions">
      <a class="btn" href="analytics_report.md" title="Analytics report">Analytics report</a>
      <span class="pill" id="jsStatus">JS: loading…</span>
    </div>
  </div>

  <nav id="sidebar" aria-label="Workspace phases">
    <a class="phase" id="phaseOverview" href="#viewOverview">Overview</a>
    <a class="phase" id="phaseSurveyor" href="#navModule">Surveyor</a>
    <a class="phase" id="phaseHydrologist" href="#navLineage">Hydrologist</a>
    <a class="phase" id="phaseSemanticist" href="#viewOverview">Semanticist</a>
    <a class="phase" id="phaseArchivist" href="#viewOverview">Archivist</a>
    <a class="phase" id="phaseNavigator" href="#navPanel">Navigator</a>
  </nav>

  <div id="main">
    <!-- Hash targets used for non-JS navigation -->
    <span id="navModule"></span>
    <span id="navLineage"></span>

    <div id="viewModule" class="view active">
      <iframe id="frameModule" src="__MODULE_HTML__"></iframe>
    </div>
    <div id="viewLineage" class="view">
      <iframe id="frameLineage" src="__LINEAGE_HTML__"></iframe>
    </div>
    <div id="viewOverview" class="view" style="padding: 20px; overflow: auto;">
      <h2 style="margin-top:0; color: var(--accent);">Overview</h2>
      <p style="color: var(--muted); line-height: 1.6;">Codebase Cartography builds <strong>module dependency</strong> and <strong>data lineage</strong> graphs, then enriches them with semantic purpose and domain clusters. Use the phases in the sidebar to explore.</p>
      <ul style="color: var(--muted);">
        <li><strong>Surveyor</strong> — Module import graph, PageRank, change velocity.</li>
        <li><strong>Hydrologist</strong> — SQL/dbt and Python data flow, DAG topology.</li>
        <li><strong>Semanticist</strong> — Purpose statements, doc drift, domain map (run via CLI).</li>
        <li><strong>Archivist</strong> — CODEBASE.md and incremental updates (run via CLI).</li>
        <li><strong>Navigator</strong> — Query and tools: find implementation, trace lineage, blast radius, explain module.</li>
      </ul>
      <p style="color: var(--muted); font-size: 12px;">Artifacts: <code>.cartography/module_graph.json</code>, <code>.cartography/lineage_graph.json</code>, <code>.cartography/semanticist_report.json</code>, <code>CODEBASE.md</code>.</p>
    </div>
  </div>

  <aside id="inspector">
    <h3>Inspector</h3>
    <div id="inspectorContent">Run a Navigator query or select a phase. Results and evidence will appear here.</div>
  </aside>
</div>

<div id="navPanel" aria-label="Navigator">
  <div id="navHead">
    <div style="font-weight:650">Navigator</div>
    <a class="btn" id="btnNavClose" href="#navModule">Close</a>
  </div>
  <div id="navBody">
    <div class="pill">Uses <code>.cartography/*.json</code> and <code>CODEBASE.md</code> from the server CWD. Start with <code>scripts/serve_dashboard.py</code>.</div>
    <details>
      <summary class="pill" style="cursor:pointer">What is Navigator?</summary>
      <div style="margin-top:8px; color: var(--muted); font-size: 12px; line-height: 1.45">
        The <b>Navigator Agent</b> is the query interface for Cartographer. Think of your codebase as a city:
        modules/functions/datasets are buildings, and the Cartographer graphs capture the roads between them.
        Navigator is a smart tour guide that helps you find where logic lives and how changes propagate.
        <div style="margin-top:10px">
          <div><b>Tools</b></div>
          <ul style="margin:6px 0 0 18px; padding:0">
            <li><code>find_implementation(concept)</code>: semantic search (purpose/domain; falls back to lineage ids)</li>
            <li><code>trace_lineage(dataset, direction)</code>: walk lineage graph upstream/downstream</li>
            <li><code>blast_radius(module_path)</code>: importers/imports + relevant lineage edges</li>
            <li><code>explain_module(path)</code>: baseline from Semanticist; optional LLM rephrase</li>
          </ul>
        </div>
        <details style="margin-top:12px">
          <summary class="pill" style="cursor:pointer"><b>What do I ask? (examples per tool)</b></summary>
          <div style="margin-top:8px; color: var(--muted); font-size: 11px; line-height: 1.5">
            <p><b>find_implementation</b> — Ask for a <em>concept</em>: e.g. <code>revenue calculation logic</code>, <code>ingestion</code>, <code>dbt staging model</code>. Matches module purpose/domain and lineage table names.</p>
            <p><b>trace_lineage</b> — Use an exact <em>dataset/table id</em> from your lineage (e.g. <code>orders</code>, <code>raw.raw_customers</code>). Direction: <code>upstream</code> = what produces it, <code>downstream</code> = what it feeds.</p>
            <p><b>blast_radius</b> — Use a <em>module path</em>: e.g. <code>src/transforms/revenue.py</code>. Returns importers, imports, and lineage edges from that file.</p>
            <p><b>explain_module</b> — Use a <em>file path</em>: e.g. <code>src/ingestion/kafka_consumer.py</code>. Returns purpose, domain, and doc-drift from Semanticist (optional LLM rephrase if OpenRouter is set).</p>
            <p><b>Query (natural language)</b> — Ask in plain English: e.g. &quot;Where is the revenue calculation logic?&quot;, &quot;What produces the orders table?&quot;, &quot;Critical path / top modules&quot;. Navigator routes to the right tool.</p>
          </div>
        </details>
      </div>
    </details>
    <div id="navRow">
      <select id="navMode">
        <option value="query">Query (natural language)</option>
        <option value="tool">Tool (structured)</option>
      </select>
      <select id="navTool">
        <option value="find_implementation">find_implementation</option>
        <option value="trace_lineage">trace_lineage</option>
        <option value="blast_radius">blast_radius</option>
        <option value="explain_module">explain_module</option>
      </select>
      <button id="btnNavRun">Run</button>
    </div>
    <input id="navQuery" type="text" placeholder='e.g. "Where is the revenue calculation logic?"' />
    <div id="navToolFields">
      <div class="navToolForm" data-tool="find_implementation">
        <label for="argConcept">Concept (semantic search)</label>
        <input id="argConcept" type="text" placeholder="e.g. revenue calculation logic" />
      </div>
      <div class="navToolForm" data-tool="trace_lineage">
        <label for="argDataset">Dataset / table id</label>
        <input id="argDataset" type="text" placeholder="e.g. orders or raw.raw_customers" />
        <label for="argDirection">Direction</label>
        <select id="argDirection">
          <option value="upstream">upstream (what produces it)</option>
          <option value="downstream">downstream (what it feeds)</option>
        </select>
      </div>
      <div class="navToolForm" data-tool="blast_radius">
        <label for="argModulePath">Module path</label>
        <input id="argModulePath" type="text" placeholder="e.g. src/transforms/revenue.py" />
      </div>
      <div class="navToolForm" data-tool="explain_module">
        <label for="argPath">Module or file path</label>
        <input id="argPath" type="text" placeholder="e.g. src/ingestion/kafka_consumer.py" />
      </div>
    </div>
    <textarea id="navArgs" placeholder="Optional: paste JSON to override tool args (e.g. {\"dataset\":\"orders\",\"direction\":\"upstream\"})"></textarea>
    <div id="navOut">(output will appear here)</div>
  </div>
</div>

<script>
(function () {
  // ES5-friendly script so the dashboard works on older browsers.
  window.CARTOGRAPHY_DASHBOARD_VERSION = "2026-03-14-es5";
  var jsStatus = document.getElementById("jsStatus");
  if (jsStatus) {
    jsStatus.textContent = "JS: ok";
    jsStatus.className = jsStatus.className + " good";
  }

  function byId(id) { return document.getElementById(id); }

  var views = { overview: byId("viewOverview"), module: byId("viewModule"), lineage: byId("viewLineage") };
  var navPanel = byId("navPanel");
  var inspectorContent = byId("inspectorContent");

  function setNav(open) {
    if (!navPanel) return;
    if (open) navPanel.classList.add("open");
    else navPanel.classList.remove("open");
  }

  function setPhaseActive(phaseId) {
    var phases = document.querySelectorAll("#sidebar .phase");
    for (var i = 0; i < phases.length; i++) {
      phases[i].classList.toggle("active", phases[i].id === phaseId);
    }
  }

  function setView(name) {
    var viewNames = ["overview", "module", "lineage"];
    for (var j = 0; j < viewNames.length; j++) {
      var v = views[viewNames[j]];
      if (v) v.classList.toggle("active", viewNames[j] === name);
    }
    try { localStorage.setItem("cartography:view", name); } catch (e) {}
    if (name === "module") { setPhaseActive("phaseSurveyor"); window.location.hash = "navModule"; }
    else if (name === "lineage") { setPhaseActive("phaseHydrologist"); window.location.hash = "navLineage"; }
    else if (name === "overview") { setPhaseActive("phaseOverview"); window.location.hash = "viewOverview"; }
  }

  if (byId("phaseOverview")) byId("phaseOverview").onclick = function (ev) { ev.preventDefault(); setView("overview"); return false; };
  if (byId("phaseSurveyor")) byId("phaseSurveyor").onclick = function (ev) { ev.preventDefault(); setView("module"); return false; };
  if (byId("phaseHydrologist")) byId("phaseHydrologist").onclick = function (ev) { ev.preventDefault(); setView("lineage"); return false; };
  if (byId("phaseSemanticist")) byId("phaseSemanticist").onclick = function (ev) { ev.preventDefault(); setView("overview"); return false; };
  if (byId("phaseArchivist")) byId("phaseArchivist").onclick = function (ev) { ev.preventDefault(); setView("overview"); return false; };
  if (byId("phaseNavigator")) byId("phaseNavigator").onclick = function (ev) { ev.preventDefault(); setNav(true); window.location.hash = "navPanel"; return false; };
  var btnNavClose = byId("btnNavClose");
  if (btnNavClose) btnNavClose.onclick = function (ev) { ev.preventDefault(); setNav(false); setView("module"); return false; };

  function onHashChange() {
    var h = (window.location.hash || "").replace("#", "");
    if (h === "viewOverview") setView("overview");
    else if (h === "navLineage") setView("lineage");
    else if (h === "navPanel") setNav(true);
    else if (h === "navModule" || !h) setView("module");
  }
  window.onhashchange = onHashChange;

  var initial = "module";
  try { initial = localStorage.getItem("cartography:view") || initial; } catch (e) {}
  if (initial !== "module" && initial !== "lineage" && initial !== "overview") initial = "module";
  if (window.location.hash) onHashChange(); else setView(initial);

  // Cache-bust iframe URLs using fragments (no server-side involvement).
  var frameModule = byId("frameModule");
  var frameLineage = byId("frameLineage");
  function cacheBust(url) {
    var base = String(url || "").split("#")[0].split("?")[0];
    return base + "#ts=" + (new Date().getTime());
  }
  if (frameModule) frameModule.src = cacheBust(frameModule.getAttribute("src") || "module_graph.html");
  if (frameLineage) frameLineage.src = cacheBust(frameLineage.getAttribute("src") || "lineage_graph.html");

  // Navigator wiring (XHR; avoids fetch/async/await requirements).
  var navMode = byId("navMode");
  var navTool = byId("navTool");
  var navQuery = byId("navQuery");
  var navArgs = byId("navArgs");
  var navToolFields = byId("navToolFields");
  var navOut = byId("navOut");
  var btnNavRun = byId("btnNavRun");

  function safeJson(text) {
    if (!text) return {};
    var t = String(text);
    if (!t.replace(/\\s+/g, "")) return {};
    try { return JSON.parse(t); } catch (e) { return {}; }
  }

  function setModeUI() {
    if (!navMode || !navTool || !navQuery) return;
    var isTool = navMode.value === "tool";
    navTool.style.display = isTool ? "inline-block" : "none";
    if (navToolFields) navToolFields.style.display = isTool ? "block" : "none";
    if (navArgs) navArgs.style.display = isTool ? "block" : "none";
    navQuery.style.display = isTool ? "none" : "block";
    var tool = navTool ? navTool.value : "";
    var forms = navToolFields ? navToolFields.querySelectorAll(".navToolForm") : [];
    for (var i = 0; i < forms.length; i++) {
      forms[i].classList.toggle("active", forms[i].getAttribute("data-tool") === tool);
    }
  }
  if (navMode) navMode.onchange = setModeUI;
  if (navTool) navTool.onchange = setModeUI;
  setModeUI();

  function buildToolArgs() {
    var tool = navTool ? navTool.value : "";
    var args = {};
    if (tool === "find_implementation") {
      var concept = byId("argConcept");
      args.concept = concept ? String(concept.value || "").trim() : "";
    } else if (tool === "trace_lineage") {
      var dataset = byId("argDataset");
      var direction = byId("argDirection");
      args.dataset = dataset ? String(dataset.value || "").trim() : "";
      args.direction = direction ? String(direction.value || "upstream") : "upstream";
    } else if (tool === "blast_radius") {
      var modulePath = byId("argModulePath");
      args.module_path = modulePath ? String(modulePath.value || "").trim() : "";
    } else if (tool === "explain_module") {
      var pathEl = byId("argPath");
      args.path = pathEl ? String(pathEl.value || "").trim() : "";
    }
    return args;
  }

  function xhrJson(method, url, body, timeoutMs, cb) {
    var xhr = new XMLHttpRequest();
    xhr.open(method, url, true);
    xhr.timeout = timeoutMs || 0;
    xhr.onreadystatechange = function () {
      if (xhr.readyState !== 4) return;
      var text = xhr.responseText || "";
      var data = null;
      try { data = JSON.parse(text); } catch (e) { data = { raw: text }; }
      if (xhr.status >= 200 && xhr.status < 300) cb(null, data);
      else cb(new Error("HTTP " + xhr.status + ": " + text), data);
    };
    xhr.onerror = function () { cb(new Error("Network error"), null); };
    if (method !== "GET") xhr.setRequestHeader("Content-Type", "application/json");
    xhr.send(body ? JSON.stringify(body) : null);
  }

  function runNavigator() {
    if (!navOut) return;
    navOut.textContent = "Running...";
    if (window.location.protocol === "file:") {
      navOut.textContent = "Navigator requires the dashboard server. Run: `uv run python3 scripts/serve_dashboard.py --build-dir build --port 8000` and open http://127.0.0.1:8000/";
      return;
    }
    var mode = navMode ? navMode.value : "query";
    var payload = {};
    if (mode === "tool") {
      var rawArgs = navArgs ? String(navArgs.value || "").trim() : "";
      var args = rawArgs ? safeJson(navArgs.value) : buildToolArgs();
      payload = { tool: navTool ? navTool.value : "", args: args };
    } else {
      payload = { query: navQuery ? navQuery.value : "" };
    }
    xhrJson("GET", "/api/ping", null, 2500, function (err) {
      if (err) {
        navOut.textContent = String(err) + "\\n\\nStart server: `uv run python3 scripts/serve_dashboard.py --build-dir build --port 8000`";
        return;
      }
      xhrJson("POST", "/api/navigate", payload, 60000, function (err2, data) {
        if (err2) {
          navOut.textContent = String(err2);
          if (inspectorContent) inspectorContent.textContent = "Query failed: " + String(err2);
          return;
        }
        try { navOut.textContent = JSON.stringify(data, null, 2); }
        catch (e) { navOut.textContent = String(data); }
        if (inspectorContent && data) {
          var summary = [];
          if (data.answer) summary.push(data.answer.substring(0, 400));
          if (data.results && data.results.length) {
            data.results.slice(0, 3).forEach(function (r) {
              summary.push((r.path || "") + ": " + (r.purpose || r.domain || ""));
            });
          }
          if (data.resolved_node) summary.push("Resolved: " + data.resolved_node);
          if (data.edges && data.edges.length) summary.push("Edges: " + data.edges.length);
          if (data.path && data.purpose) summary.push(data.path + " — " + data.purpose);
          inspectorContent.textContent = summary.length ? summary.join("\\n\\n") : "(See output below)";
        }
      });
    });
  }

  if (btnNavRun) btnNavRun.onclick = runNavigator;
}());
</script>
</body>
</html>
"""


def write_front_page(
    out_path: str | Path = "build/index.html",
    module_html: str = "module_graph.html",
    lineage_html: str = "lineage_graph.html",
) -> Path:
    """
    Write an interactive dashboard page that links/embeds the module + lineage viewers.

    Assumes `out_path` is under the same directory as the referenced HTML files.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    html = (
        HTML.replace("__MODULE_HTML__", module_html)
        .replace("__LINEAGE_HTML__", lineage_html)
    )
    out_path.write_text(html, encoding="utf-8")
    return out_path
