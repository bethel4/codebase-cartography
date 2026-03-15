# Four-Agent Pipeline Architecture

This project is designed around four cooperating agents that analyze a target repository and write their findings into a shared **Knowledge Graph** layer (stored/serialized as NetworkX node-link JSON in `.cartography/`).

## Diagram (Mermaid) — End-to-End

```mermaid
flowchart LR
  %% Inputs
  TargetCodebase["Target codebase (Python/SQL/YAML/notebooks)"]

  %% Orchestration (parallel stage)
  Orchestrator["Orchestrator (runs agents, handles failures)"]

  subgraph P[Phase 1–2: Static & Lineage (parallel)]
    direction TB
    Surveyor["Surveyor (imports/defs, complexity, velocity)"]
    Hydrologist["Hydrologist (SQL/dbt + Python flows + DAG configs)"]
  end

  %% Shared store
  KnowledgeGraph[("Central Knowledge Graph (NetworkX + node-link JSON)")]
  CartographyStore[(".cartography (module_graph.json + lineage_graph.json)")]

  %% Semantic layer
  subgraph SemanticistLayer[Phase 3: Semanticist (LLM)]
    direction TB
    ContextWindowBudget["ContextWindowBudget (token/spend + tiered routing)"]
    PurposeStatements["Purpose statements + doc drift flags"]
    DomainArchitectureMap["Domain Architecture Map (embeddings + k-means + labels)"]
  end

  %% Archivist / audit layer
  subgraph ArchivistAndNavigator[Phase 4: Archivist + Navigator]
    direction TB
    TraceLog["cartography_trace.jsonl (actions + evidence + confidence)"]
    CodebaseMd["CODEBASE.md (living context for agents)"]
    Navigator["Navigator Agent (LangGraph) (query + citations)"]
  end

  %% Outputs / exports
  subgraph Exports[Exports / Visualizations]
    direction TB
    DotHtmlExports["DOT/HTML exports"]
    MemgraphExport["Memgraph export (CSV + Cypher)"]
  end

  %% Flow
  TargetCodebase --> Orchestrator

  %% Parallel execution (Surveyor + Hydrologist)
  Orchestrator -->|run in parallel| Surveyor
  Orchestrator -->|run in parallel| Hydrologist

  %% Write shared facts/edges into the KG and serialized store
  Surveyor -->|Module graph nodes/edges + metrics| KnowledgeGraph
  Hydrologist -->|Lineage nodes/edges + sources/sinks| KnowledgeGraph
  KnowledgeGraph --> CartographyStore

  %% Semanticist consumes static outputs + raw code text
  CartographyStore --> SemanticistLayer
  TargetCodebase --> SemanticistLayer
  SemanticistLayer -->|enrich nodes/edges + add drift/domain tags| KnowledgeGraph

  %% Archivist consumes everything and writes audit + deliverables
  KnowledgeGraph --> ArchivistAndNavigator
  CartographyStore --> ArchivistAndNavigator
  SemanticistLayer --> ArchivistAndNavigator
  ArchivistAndNavigator --> TraceLog
  ArchivistAndNavigator --> CodebaseMd

  %% Navigator queries the shared graph + evidence stores
  Navigator -->|graph queries| KnowledgeGraph
  Navigator -->|evidence + provenance| TraceLog
  Navigator -->|human-facing answers w/ citations| CodebaseMd

  %% Optional exports
  KnowledgeGraph --> Exports
  Exports --> DotHtmlExports
  Exports --> MemgraphExport

```

## Parallelism note (Surveyor + Hydrologist)

Surveyor and Hydrologist are intentionally independent and can run **in parallel** on the same repo path. Their outputs are merged via the shared Knowledge Graph layer (and persisted to `.cartography/`).

## What each agent contributes

- **Surveyor**: scans the repo, identifies modules/files, extracts imports/functions/classes, and builds the *structural* dependency graph.
- **Hydrologist**: extracts *data lineage* from SQL/dbt, Python data access patterns, and DAG configs to build a lineage graph.
- **Semanticist**: enriches nodes/edges with meaning (e.g., dataset descriptions, ownership, domain tags, LLM summaries).
- **Archivist**: persists snapshots over time and annotates changes (e.g., git velocity, churn, “what changed since last run”).
- **Navigator**: query interface over artifacts with evidence citations (see `docs/NAVIGATOR.md`).

## Inputs and outputs (what to check)

- **Inputs**: a local path to a cloned target repository.
- **Shared layer**: a central graph abstraction (in-memory NetworkX; exported as `.cartography/*.json`).
- **Outputs**: module graph JSON, lineage graph JSON, plus optional exports (DOT/HTML/Memgraph) for visualization.
