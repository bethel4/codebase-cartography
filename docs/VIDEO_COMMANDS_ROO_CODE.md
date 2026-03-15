# Full Video Demo Commands — Week 1 Repo (Roo-Code)

Run these from the **Codebase Cartography repo root**. Use a **separate target directory** (e.g. `target_repo_week1`) so Meltano and Roo-Code do not overwrite each other's `.cartography` artifacts. Ensure `.env` contains `openRoute=YOUR_OPENROUTER_API_KEY`.

---

## Step 0: Clone Roo-Code (Week 1 repo)

```bash
uv run python src/cli.py clone https://github.com/bethel4-b/Roo-Code.git --target-dir target_repo_week1
```

---

## Run full pipeline on Week 1 repo

```bash
# Surveyor
uv run python src/cli.py survey --repo-path target_repo_week1

# Hydrologist
uv run python src/cli.py hydrology --repo-path target_repo_week1

# Semanticist (OpenRouter)
uv run python src/cli.py semantic \
  --repo-path target_repo_week1 \
  --provider openrouter \
  --modules .cartography/module_graph.json \
  --lineage .cartography/lineage_graph.json \
  --out .cartography/semanticist_report.json

# Phase 4: CODEBASE.md (writes to repo root; optionally save a copy for Week 1)
uv run python src/cli.py phase4 \
  --repo-path target_repo_week1 \
  --modules .cartography/module_graph.json \
  --lineage .cartography/lineage_graph.json \
  --semantic .cartography/semanticist_report.json \
  --out CODEBASE.md
```

**Note:** Surveyor/Hydrologist/Phase4 write to `.cartography/` and `CODEBASE.md` in the **current working directory** (Cartography repo root). So after running on Roo-Code, your `.cartography/*` and `CODEBASE.md` will reflect **Roo-Code**, not Meltano. To compare:

- Run Meltano first, then copy artifacts:  
  `cp -r .cartography .cartography_meltano` and `cp CODEBASE.md CODEBASE_meltano.md`
- Then run the Roo-Code commands above; afterward you have `.cartography` for Roo-Code and `.cartography_meltano` / `CODEBASE_meltano.md` for Meltano.

---

## Self-audit: compare your docs vs Cartographer

1. Open your existing Roo-Code docs (e.g. `README.md`, `AGENTS.md`, or any architecture notes).
2. Open `CODEBASE.md` and `.cartography/semanticist_report.json` (after running the pipeline on `target_repo_week1`).
3. Compare:
   - **Critical path / main entry points** — do they match?
   - **Data sources and sinks** — does the lineage graph align with what you documented?
   - **Doc drift** — in `semanticist_report.json`, check `doc_drift` and `docstring_flag` per module; note any where the docstring contradicts the inferred purpose.
4. Document **discrepancies**: e.g. “Our README says X is the only consumer of Y; Cartographer shows Z also reads Y,” or “Module A’s docstring says it does X, but the purpose statement says it does Y (doc_drift: contradicts).”

---

## Optional: onboarding brief for Week 1

```bash
# After semantic + phase4 for target_repo_week1, generate brief
uv run python -c "
from pathlib import Path
import json
report = json.loads(Path('.cartography/semanticist_report.json').read_text())
fde = report.get('fde_answers') or report.get('fde_day_one') or {}
Path('build').mkdir(exist_ok=True)
lines = ['# Day-One Brief (Roo-Code / Week 1)', '']
for qid, c in sorted((fde.get('fde_questions') or {}).items()):
    lines.append(f'## {qid}')
    lines.append('')
    lines.append(str(c.get('answer', c)) if isinstance(c, dict) else str(c))
    lines.append('')
Path('build/onboarding_brief_week1.md').write_text('\n'.join(lines))
print('Written build/onboarding_brief_week1.md')
"
```

Use `build/onboarding_brief_week1.md` in your report for the self-audit section.
