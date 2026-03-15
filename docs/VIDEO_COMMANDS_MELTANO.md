# Full Video Demo Commands — Meltano Repo

Run these from the **Codebase Cartography repo root** (where `src/`, `scripts/`, `.cartography/` live). Ensure `.env` contains `openRoute=YOUR_OPENROUTER_API_KEY`.

---

## One-time setup (if not done)

```bash
uv sync
```

---

## Step 0: Clone Meltano into `target_repo`

```bash
uv run python src/cli.py clone https://github.com/meltano/meltano.git --target-dir target_repo
```

---

## Step 1 — Cold Start (analyze + CODEBASE.md, timed)

```bash
# Surveyor: module dependency graph
uv run python src/cli.py survey --repo-path target_repo

# Hydrologist: data lineage graph
uv run python src/cli.py hydrology --repo-path target_repo

# Semanticist: purpose statements, doc drift, domains, Day-One answers (OpenRouter)
uv run python src/cli.py semantic \
  --repo-path target_repo \
  --provider openrouter \
  --modules .cartography/module_graph.json \
  --lineage .cartography/lineage_graph.json \
  --out .cartography/semanticist_report.json

# Phase 4: CODEBASE.md
time uv run python src/cli.py phase4 \
  --repo-path target_repo \
  --modules .cartography/module_graph.json \
  --lineage .cartography/lineage_graph.json \
  --semantic .cartography/semanticist_report.json \
  --out CODEBASE.md
```

*(Note the `time` output for Cold Start duration.)*

---

## Step 2 — Lineage Query (upstream sources for an output dataset)

```bash
uv run python src/cli.py navigate-tool trace_lineage \
  --args-json '{"dataset": "customers", "direction": "upstream"}' \
  --modules .cartography/module_graph.json \
  --lineage .cartography/lineage_graph.json \
  --semantic .cartography/semanticist_report.json \
  --codebase CODEBASE.md
```

*(Replace `customers` with any table/dataset id from your lineage graph if different.)*

---

## Step 3 — Blast Radius (downstream dependency for a module)

```bash
uv run python src/cli.py navigate-tool blast_radius \
  --args-json '{"module_path": "target_repo/meltano/cli.py"}' \
  --modules .cartography/module_graph.json \
  --lineage .cartography/lineage_graph.json \
  --semantic .cartography/semanticist_report.json \
  --codebase CODEBASE.md
```

*(Adjust `module_path` to a real module in the Meltano repo if needed.)*

---

## Optional: Run full demo script (Steps 1–3 + onboarding brief)

```bash
uv run python scripts/run_demo.py --repo-path target_repo --with-semantic
```

---

## Optional: Regenerate graph HTML + serve dashboard

```bash
uv run python scripts/export_graph_html.py --graph .cartography/module_graph.json --out build/module_graph.html
uv run python scripts/export_graph_html.py --graph .cartography/lineage_graph.json --out build/lineage_graph.html
uv run python scripts/export_front_page.py
uv run python scripts/serve_dashboard.py --build-dir build --port 8000
```

Then open **http://127.0.0.1:8000/** for the UI (Surveyor / Hydrologist / Navigator).

---

## Steps 4–6 (narration only)

- **Step 4:** Open `build/onboarding_brief.md`; verify 2+ answers by navigating to cited file and line.
- **Step 5:** Inject `CODEBASE.md` into a fresh AI session; ask an architecture question; compare with/without context.
- **Step 6:** Use the Roo-Code run (see `docs/VIDEO_COMMANDS_ROO_CODE.md`) for self-audit; compare docs vs Cartographer output and explain discrepancies.
