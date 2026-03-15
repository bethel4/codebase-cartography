# Phase-by-phase commands (try each phase in repo)

## Video / report deliverables

- **Meltano repo (full video steps):** [VIDEO_COMMANDS_MELTANO.md](VIDEO_COMMANDS_MELTANO.md) — clone, survey, hydrology, semantic, phase4, lineage query, blast radius, optional UI.
- **Week 1 repo (Roo-Code) self-audit:** [VIDEO_COMMANDS_ROO_CODE.md](VIDEO_COMMANDS_ROO_CODE.md) — clone into `target_repo_week1`, run pipeline, compare with your docs.
- **Single PDF report (RECONNAISSANCE, architecture, accuracy, limitations, FDE, self-audit):** [FINAL_REPORT.md](FINAL_REPORT.md) — fill the tables and self-audit sections, then export to PDF.

---

## Run the full demo (Steps 1–6) at once

From the **repository root** (with `openRoute` in `.env` for OpenRouter):

```bash
# Minutes 1–3 (Required): Cold start + lineage query + blast radius
uv run python scripts/run_demo.py --repo-path target_repo

# Include Semanticist (purpose statements, doc drift, domain map, Day-One answers) for Step 4
uv run python scripts/run_demo.py --repo-path target_repo --with-semantic
```

The script runs **Step 1** (survey + hydrology + phase4, timed), **Step 2** (trace_lineage upstream for `customers`), **Step 3** (blast_radius for a module), then generates **build/onboarding_brief.md** from the semanticist report if present and prints instructions for **Steps 4–6**.

---

Run these from the **repository root** (where `src/`, `scripts/`, `.cartography/` live). Default target repo is `target_repo/` unless you pass `--repo-path` or `--target-dir`.

---

## Phase 0: Target selection & reconnaissance

- **Clone** a target repo (optional; you can use an existing directory):

  ```bash
  uv run python src/cli.py clone https://github.com/dbt-labs/jaffle_shop --target-dir target_repo
  ```

- Manually explore and write **RECONNAISSANCE.md** (ground truth for Day-One answers). No CLI for this.

---

## Phase 1: Surveyor (static structure)

- **Build tree-sitter languages** (once per environment):

  ```bash
  uv run python src/cli.py build-langs --vendor-dir vendor --out-path build/my-languages.so
  ```

- **Run Surveyor** (module import graph + PageRank + velocity):

  ```bash
  uv run python src/cli.py survey --repo-path target_repo
  ```

  Output: `.cartography/module_graph.json`

---

## Phase 2: Hydrologist (data lineage)

- **Run Hydrologist** (SQL + Python + DAG lineage):

  ```bash
  uv run python src/cli.py hydrology --repo-path target_repo
  ```

  For dbt projects, omit `--no-dbt-compile` so it runs `dbt compile` and uses the manifest. To skip dbt compile:

  ```bash
  uv run python src/cli.py hydrology --repo-path target_repo --no-dbt-compile
  ```

  Output: `.cartography/lineage_graph.json`

---

## Phase 3: Semanticist (LLM-powered analysis)

- **Run Semanticist** (purpose statements, doc drift, domain clustering, Day-One answers).

  **OpenRouter** (default; put `openRoute=your_key` in `.env` in the repo root; `.env` is loaded automatically by the CLI):

- `generate_purpose_statement` uses the **module’s code with the module docstring removed**; the docstring is sent only for cross-reference. The bulk model (e.g. `google/gemini-2.0-flash-exp:free`) produces a 2–3 sentence purpose; then `detect_docstring_drift` compares with the docstring and flags **Documentation Drift** when they disagree.

  ```bash
  uv run python src/cli.py semantic --repo-path target_repo --provider openrouter \\
    --modules .cartography/module_graph.json --lineage .cartography/lineage_graph.json \\
    --out .cartography/semanticist_report.json
  ```

  Tiered models (bulk = gemini-flash, synthesis = claude) are used automatically when `--provider openrouter`. Override:

  ```bash
  uv run python src/cli.py semantic --repo-path target_repo --provider openrouter \\
    --bulk-model "google/gemini-2.0-flash-exp:free" --synth-model "anthropic/claude-3.5-sonnet"
  ```

  **Ollama** (local):

  ```bash
  uv run python src/cli.py semantic --repo-path target_repo --provider ollama \\
    --modules .cartography/module_graph.json --lineage .cartography/lineage_graph.json \\
    --out .cartography/semanticist_report.json
  ```

  **Gemini** (set `GemiBulk` / `GemiPrompt` in `.env`):

  ```bash
  uv run python src/cli.py semantic --repo-path target_repo --provider gemini \\
    --modules .cartography/module_graph.json --lineage .cartography/lineage_graph.json \\
    --out .cartography/semanticist_report.json
  ```

  Output: `.cartography/semanticist_report.json` (modules, domains, `fde_day_one` / `fde_answers`).

- **Refresh only Day-One answers** (no re-run of purpose/embedding):

  ```bash
  uv run python src/cli.py semantic-refresh-fde \\
    --modules .cartography/module_graph.json --lineage .cartography/lineage_graph.json \\
    --in .cartography/semanticist_report.json --out .cartography/semanticist_report.json
  ```

---

## Phase 4: Archivist + Navigator

- **Generate CODEBASE.md + incremental update** (run after Surveyor/Hydrologist; optional Semanticist):

  ```bash
  uv run python src/cli.py phase4 --repo-path target_repo \\
    --modules .cartography/module_graph.json --lineage .cartography/lineage_graph.json \\
    --semantic .cartography/semanticist_report.json --out CODEBASE.md
  ```

- **Navigator: natural-language query** (evidence-cited answer):

  ```bash
  uv run python src/cli.py navigate "Where is the revenue calculation logic?" \\
    --modules .cartography/module_graph.json --lineage .cartography/lineage_graph.json \\
    --semantic .cartography/semanticist_report.json --codebase CODEBASE.md
  ```

- **Navigator: structured tools**

  - **find_implementation** (semantic search):

    ```bash
    uv run python src/cli.py navigate-tool find_implementation --args-json '{"concept": "revenue calculation"}'
    ```

  - **trace_lineage** (upstream/downstream):

    ```bash
    uv run python src/cli.py navigate-tool trace_lineage --args-json '{"dataset": "orders", "direction": "upstream"}'
    ```

  - **blast_radius** (impact of changing a module):

    ```bash
    uv run python src/cli.py navigate-tool blast_radius --args-json '{"module_path": "src/transforms/revenue.py"}'
    ```

  - **explain_module**:

    ```bash
    uv run python src/cli.py navigate-tool explain_module --args-json '{"path": "src/ingestion/kafka_consumer.py"}'
    ```

---

## One-command bootstrap

Clone + Surveyor + Hydrologist + (optional) Semanticist + Phase 4:

```bash
uv run python src/cli.py bootstrap https://github.com/dbt-labs/jaffle_shop \\
  --target-dir target_repo --skip-semantic
```

With Semanticist (OpenRouter):

```bash
# Ensure .env has openRoute=your_key
uv run python src/cli.py bootstrap https://github.com/dbt-labs/jaffle_shop \\
  --target-dir target_repo --provider openrouter
```

---

## Dashboard (Navigator + graphs in browser)

1. Generate graphs and front page:

   ```bash
   uv run python scripts/export_front_page.py
   uv run python scripts/export_graph_html.py --graph .cartography/module_graph.json --out build/module_graph.html
   uv run python scripts/export_graph_html.py --graph .cartography/lineage_graph.json --out build/lineage_graph.html
   ```

2. Start server (OpenRouter default; set `openRoute` in `.env`):

   ```bash
   uv run python scripts/serve_dashboard.py --build-dir build --port 8000
   ```

   Use Ollama instead:

   ```bash
   uv run python scripts/serve_dashboard.py --build-dir build --port 8000 --provider ollama
   ```

3. Open **http://127.0.0.1:8000/** and use the Navigator panel to run queries or tools.
