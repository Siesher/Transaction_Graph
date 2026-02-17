# Implementation Plan: Transaction Graph MVP

**Branch**: `001-transaction-graph-mvp` | **Date**: 2026-02-17 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-transaction-graph-mvp/spec.md`

## Summary

Build an MVP system that extracts banking transaction data from Hadoop Hive (database `s_dmrb`), constructs a heterogeneous corporate relationship graph, filters noise using the Serrano disparity filter, detects corporate groups via Leiden clustering, profiles nodes by centrality metrics, and renders an interactive visualization — all within JupyterLab notebooks on the MDP platform. The system uses a seed-based extraction strategy (start from one company, expand N hops) to produce manageable subgraphs from billion-row tables.

## Technical Context

**Language/Version**: Python 3.8+ (as available on MDP JupyterLab)
**Primary Dependencies**: pyspark (ETL from Hive), pandas (data manipulation), networkx (graph construction), python-igraph + leidenalg (community detection), pyvis (interactive visualization), numpy, matplotlib
**Storage**: Hadoop Hive (source: `s_dmrb`), Parquet files on HDFS (intermediate outputs)
**Testing**: Manual validation in notebooks; synthetic data generation for offline testing
**Target Platform**: JupyterLab on Hadoop cluster (MDP platform)
**Project Type**: Data pipeline + analytics (notebook-driven with reusable Python modules)
**Performance Goals**: Extract and visualize a 2-hop neighborhood (up to ~10K nodes, ~50K edges) within a single JupyterLab session
**Constraints**: Hive metastore partition limit of 3000 requires date-range filtering on large tables; `paymentcounteragent_stran` has ~5B rows; `account_sdim` has ~1B rows; no internet access assumed on cluster (all dependencies must be pre-installed or bundled)
**Scale/Scope**: Seed-based subgraph extraction — not full-graph processing. Target: under 100K nodes per analysis run.

## Constitution Check

*GATE: No constitution file exists for this project. Proceeding with standard engineering best practices.*

**Applied principles**:
- Simplicity: Notebooks as primary interface, minimal abstraction layers
- Reproducibility: All intermediate results saved as Parquet
- Modularity: Reusable `src/` modules imported by notebooks
- Data safety: Read-only access to Hive; no writes to source tables

## Project Structure

### Documentation (this feature)

```text
specs/001-transaction-graph-mvp/
├── plan.md              # This file
├── research.md          # Phase 0: technology decisions
├── data-model.md        # Phase 1: entity definitions
├── quickstart.md        # Phase 1: how to run the system
├── contracts/           # Phase 1: module interfaces
│   ├── etl.md
│   ├── graph_builder.md
│   ├── filters.md
│   ├── analysis.md
│   └── viz.md
└── tasks.md             # Phase 2: implementation tasks
```

### Source Code (repository root)

```text
notebooks/
├── 01_data_extraction.ipynb       # PySpark ETL from Hive → Parquet
├── 02_graph_construction.ipynb    # Build graph + disparity filter
├── 03_graph_analysis.ipynb        # Leiden clustering + centrality + patterns
└── 04_visualization.ipynb         # pyvis interactive graph + summary tables

src/
├── __init__.py
├── config.py                      # Configuration: DB name, table names, defaults
├── schema.py                      # Column mappings for each Hive table
├── etl.py                         # PySpark extraction functions
├── graph_builder.py               # NetworkX graph construction from DataFrames
├── filters.py                     # Disparity filter + pre-filters
├── analysis.py                    # Leiden, centrality, cycle/shell detection
└── viz.py                         # pyvis visualization + summary tables

data/                              # Parquet output directory (on HDFS or local)
└── .gitkeep

requirements.txt                   # Python dependencies
```

**Structure Decision**: Single project with `notebooks/` as the user-facing entry points and `src/` as the reusable library. No web frontend, no API, no separate backend. This is a data analysis workbench.
