## Codebase Cartography

Codebase Cartography builds **structural** and **data lineage** graphs for mixed codebases (Python + SQL/dbt + YAML-like configs) and exports them as NetworkX node-link JSON under `.cartography/`.

### What it produces

- `.cartography/module_graph.json` — Python module import graph + per-file metadata (imports, functions, classes, complexity, git velocity, etc.).
- `.cartography/lineage_graph.json` — Unified lineage graph merged from:
  - SQL/dbt dependencies (`sqlglot`)
  - Python data access patterns (tree-sitter Python)
  - DAG/config topology (best-effort parsing)

### Install (uv)

```bash
uv sync
```

### CLI usage

Run on an existing local repo directory (default is `target_repo`):

```bash
uv run python src/cli.py survey --repo-path target_repo
uv run python src/cli.py hydrology --repo-path target_repo
uv run python src/cli.py run --repo-path target_repo --parallel
```

Clone a repo into `target_repo/`:

```bash
uv run python src/cli.py clone <git_url> --target-dir target_repo
```

### Visualize outputs (DOT)

Generate a DOT graph from saved JSON:

```bash
uv run python scripts/visualize_graph.py --graph .cartography/lineage_graph.json --max-nodes 200
```

Outputs:
- `build/lineage_graph.dot`

Optional PNG rendering (requires a working Graphviz `dot`):

```bash
uv run python scripts/visualize_graph.py --graph .cartography/lineage_graph.json --render-png
```

### Memgraph Lab export

Export node/edge CSV + Cypher import script:

```bash
uv run python scripts/export_memgraph.py --graph .cartography/lineage_graph.json
```

Outputs:
- `build/memgraph/<graph>.nodes.csv`
- `build/memgraph/<graph>.edges.csv`
- `build/memgraph/import.cypher`

### Tests

```bash
python3 -m pytest
```

Notes:
- SQL lineage tests are skipped if `sqlglot` isn’t installed in the active interpreter.
- You may see a tree-sitter deprecation warning from the upstream `tree_sitter` package.
