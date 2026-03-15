# Navigator Agent (Query Interface)

## What it is

The **Navigator Agent** is the query interface for Cartographer.

It’s implemented in a **LangGraph-style agent** (and can optionally be compiled into a real LangGraph graph when `langgraph` is installed), but the important thing is what it *uses*: the Cartographer artifacts (`.cartography/module_graph.json`, `.cartography/lineage_graph.json`, `.cartography/semanticist_report.json`).

Think of your codebase as a **city**:

- Every module, function, or dataset is a **building**.
- The Cartographer graphs capture the **roads** between buildings (imports, reads/writes, pipeline edges).
- The Navigator Agent is a **smart tour guide** that knows where things are and how they connect.

It comes with **four tools** that let you explore the city in different ways.

## The four tools

| Tool | Query Type | Example | What it does |
|---|---|---|---|
| `find_implementation(concept)` | Semantic | “Where is the revenue calculation logic?” | Searches the Semanticist purpose/domain map (and can fall back to lineage IDs) to locate where a concept likely lives. |
| `trace_lineage(dataset, direction)` | Graph | “What produces the `daily_active_users` table?” | Walks the lineage graph to see what produces/consumes a dataset. `upstream` = inputs, `downstream` = outputs affected. |
| `blast_radius(module_path)` | Graph | “What breaks if I change `src/transforms/revenue.py`?” | Finds modules that import a module (and relevant lineage edges) to estimate what might be impacted by a change. |
| `explain_module(path)` | Generative | “Explain what `src/ingestion/kafka_consumer.py` does.” | Uses Semanticist facts as a baseline; optionally uses an LLM (OpenRouter) to produce a concise explanation while keeping evidence citations. |

