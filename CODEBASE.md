# CODEBASE.md

_Generated: `2026-03-15T02:46:39.610407Z`_

## Architecture Overview

This repository implements a multi-agent codebase intelligence pipeline: Surveyor builds a module dependency graph (imports/defs + PageRank/velocity), Hydrologist builds a data lineage graph (SQL/dbt + Python dataset reads/writes + DAG configs), and Semanticist enriches modules with purpose, domain tags, and doc-drift signals. Primary semantic domains observed: unknown (Semanticist domains missing).

## Critical Path

- (no modules found)

## Data Sources & Sinks

- Sources:
- Sinks:

## Known Debt

- Import cycles detected: `0` (evidence: `.cartography/module_graph.json`; method=graph_traversal)
- Sample cycle (first): `[]` (evidence: `.cartography/module_graph.json`; method=graph_traversal)
- Doc drift flagged modules (top 20):

## High-Velocity Files

- (no velocity data found)
