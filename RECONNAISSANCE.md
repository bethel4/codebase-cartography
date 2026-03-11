# Brownfield Reconnaissance – jaffle_shop

Repository: mitodl/ol-data-platform

## Day-One FDE Findings (Second Pass)

This is the second repository I analyzed. After watching the technical tutorial, I started by checking `dbt_project.yml` to understand how the dbt project runs and where data originates. That file highlighted key project sections (models, seeds, macros), so I explored the `models/` folder next.

### 1) Primary ingestion path

- I used `dbt_project.yml` as the starting point to identify:
  - `models/` (transformations)
  - `seeds/` (static inputs)
  - `macros/` (templating + reusable logic)
- In the `models/` folder, I focused on how raw/staging sources become curated outputs (marts/external consumers).

### 2) Critical outputs (final datasets, reports, dashboards)

The most important outputs are the datasets and endpoints that other teams or external consumers rely on. These are the final materialized tables/views in the warehouse, plus dashboards or APIs that expose that data.

- **External consumer tables**
  - The `external/` folder contains models built specifically for external partners (e.g., IRX models mirroring outputs from `edx-analytics-exporter` for Simeon).
  - These are critical because external systems directly use them.
- **Analytics marts**
  - Tables under `marts/` aggregate and organize raw/staging data into reporting-friendly formats.
  - These are critical because BI dashboards and internal analyses depend on them.
- **Published dashboards / endpoints**
  - `target_repo/src/ol_superset/ol_superset/lib/superset_api.py` contains automation/scripts used to push or manage assets in Superset.
  - These endpoints are critical because they deliver key metrics to business users.

Each step relies on the previous step being consistent. A schema change (renaming/removing/changing a column type) can:

- break dbt model builds,
- prevent marts/external tables from being produced,
- cause dashboards to error or show blank data,
- break external consumers expecting specific columns.

### 3) Blast radius of the most critical module

I manually traced ingestion code under:

- `target_repo/dg_projects/edxorg/edxorg/assets/edxorg_archive.py`

Sensors detect new raw archives and tracking logs in GCS. The code extracts, normalizes, and enriches data, producing outputs in S3 that feed downstream analytics and dbt models. Based on observed asset keys, dynamic outputs, and partition dependencies, changes to this module can affect many downstream datasets, dashboards, and analytics pipelines.

### 4) Business logic concentration

Operational business logic is primarily concentrated in:

- **Dagster assets/ops** (data movement + transformation)
  - `target_repo/dg_projects/learning_resources/learning_resources/`
  - `target_repo/dg_projects/edxorg/edxorg/`
  - `target_repo/dg_projects/openedx/openedx/`
  - `target_repo/dg_projects/legacy_openedx/legacy_openedx/`
- **Superset automation/API tooling** (publishing/serving metrics)
  - `target_repo/src/ol_superset/ol_superset/`
- **Shared orchestration utilities**
  - `target_repo/packages/ol-orchestrate-lib/src/`
  - Example: `target_repo/packages/ol-orchestrate-lib/src/ol_orchestrate/lib/postgres/event_log.py`

These areas contain the “platform logic” that moves, transforms, and delivers data. Changes here are high-risk because they can impact downstream dashboards, warehouse tables, and external consumers.

### 5) Recent change velocity (risk hotspots)

High-activity areas in the last 30 days include:

- Dagster project assets:
  - `dg_projects/edxorg/edxorg/assets/edxorg_archive.py`
  - `dg_projects/learning_resources/learning_resources/assets/video_shorts.py`
- Shared orchestration libraries:
  - `packages/ol-orchestrate-lib/src/ol_orchestrate/lib/postgres/event_log.py`
- Superset API tooling:
  - `src/ol_superset/ol_superset/lib/superset_api.py`

These are actively modified “choke points”, making them higher risk for regressions (schemas or logic changes can cascade).

### Difficulty analysis

The most challenging part of the manual exploration was confirming the end-to-end ingestion path. While navigating the repository, I frequently encountered new files and folders that forced me to adjust my mental model of the data flow. As a result, the exact ingestion path was not immediately clear and would benefit from follow-up testing to fully confirm how data moves from upstream systems into the warehouse.

In addition, some folders/files were initially confusing, so I focused on the `models/` folder. In one pass, I found an `external/` README explaining that the repository uses **Airbyte** for ingestion and describing external consumers, warehouse transformations, and upstream systems (e.g., EdX exports).
