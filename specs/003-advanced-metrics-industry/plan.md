# Implementation Plan: Advanced Graph Metrics, Industry Analytics & Notebook Documentation

**Branch**: `003-advanced-metrics-industry` | **Date**: 2026-02-20 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/003-advanced-metrics-industry/spec.md`

## Summary

Two-wave feature extending the Transaction Graph with advanced metrics and industry analytics:

**Wave 1** (no new data): Extended node metrics (unique counterparty count, top-K concentration, active months, hub flag), composite edge_score on every transaction edge, hub-aware filtering to prevent super-cluster formation, and external_counterparty_count in cluster summaries.

**Wave 2** (new data from Hive): ETL extension for OKVED/region codes, OKVED×OKVED cross-industry turnover matrix with heatmap, cross-industry hub detection via OKVED diversity/entropy, behavioral segmentation via K-Means clustering, and look-alike prospect scoring.

Plus: interactive notebook documentation with interpretation guidance for business users across all existing and new notebooks.

## Technical Context

**Language/Version**: Python 3.8+ (MDP JupyterLab)
**Primary Dependencies**: networkx, python-igraph, leidenalg, pandas, numpy, pyarrow, pyspark (ETL), scikit-learn (K-Means, StandardScaler), matplotlib (heatmaps)
**Storage**: Local filesystem, Parquet files, pickle (graph serialization)
**Testing**: pytest (38 existing tests passing)
**Target Platform**: MDP JupyterLab (Hadoop cluster), local development
**Project Type**: Single Python package + Jupyter notebooks
**Performance Goals**: Pipeline runs under 60s on graphs up to 10K nodes
**Constraints**: Python 3.8 compatibility, no internet access on MDP, CPU-only
**Scale/Scope**: Graphs with 100–10,000 nodes, 200–50,000 edges

## Constitution Check

*No constitution file found (`.specify/memory/constitution.md` does not exist). No gate violations to check.*

**Post-design re-check**: N/A — no constitution gates defined.

## Project Structure

### Documentation (this feature)

```text
specs/003-advanced-metrics-industry/
├── plan.md              # This file
├── spec.md              # Feature specification (5 user stories, 20 FRs)
├── research.md          # Phase 0: 6 technical decisions
├── data-model.md        # Phase 1: entity definitions
├── quickstart.md        # Phase 1: usage examples
├── contracts/
│   └── api.md           # Phase 1: function signatures
├── checklists/
│   └── requirements.md  # Spec quality checklist (16/16 pass)
└── tasks.md             # Phase 2 output (/speckit.tasks command)
```

### Source Code (repository root)

```text
src/
├── __init__.py          # Module docstring
├── config.py            # Configuration constants (MODIFIED: new Wave 1/2 params)
├── schema.py            # Hive column mappings (MODIFIED: OKVED/region fields)
├── etl.py               # PySpark ETL (MODIFIED: extract okved_code, region_code)
├── synthetic.py          # Synthetic data generator (MODIFIED: OKVED/region columns)
├── graph_builder.py     # Graph construction (MODIFIED: compute_extended_metrics, compute_edge_score)
├── filters.py           # Graph filtering (MODIFIED: hub_filter)
├── analysis.py          # Graph analysis (MODIFIED: external_counterparty_count, NEW: okved/behavioral funcs)
├── pipeline.py          # Pipeline orchestrator (MODIFIED: integrate new steps)
├── gnn.py               # GNN module (unchanged)
└── viz.py               # Visualization (unchanged)

tests/
├── conftest.py          # Shared fixtures (MODIFIED: OKVED/region data)
├── test_synthetic.py    # Synthetic data tests (MODIFIED: OKVED/region validation)
├── test_graph_builder.py # Graph construction tests (MODIFIED: extended metrics, edge_score)
├── test_filters.py      # Filter tests (MODIFIED: hub_filter tests)
├── test_analysis.py     # Analysis tests (MODIFIED: okved, behavioral, lookalike tests)
└── test_pipeline.py     # Pipeline tests (MODIFIED: full pipeline with new metrics)

notebooks/
├── 01_etl_extraction.ipynb       # MODIFIED: documentation + OKVED/region extraction
├── 02_graph_construction.ipynb   # MODIFIED: documentation + extended metrics + edge_score
├── 03_filtering.ipynb            # MODIFIED: documentation + hub filtering
├── 04_analysis.ipynb             # MODIFIED: documentation + interpretation guidance
├── 05_industry_analysis.ipynb    # NEW: OKVED matrix, heatmap, cross-industry hubs
└── 06_behavioral_segmentation.ipynb  # NEW: behavioral features, K-Means, look-alike
```

**Structure Decision**: Single-project Python package (existing structure). All new functions are added to existing modules following the established pattern. Two new notebooks are created for Wave 2 analytics. No new Python modules needed — all new functions fit naturally into `graph_builder.py`, `filters.py`, and `analysis.py`.

## Complexity Tracking

No constitution violations. No complexity justification needed.

## Design Artifacts

| Artifact | Status | Path |
|----------|--------|------|
| research.md | Complete | [research.md](research.md) |
| data-model.md | Complete | [data-model.md](data-model.md) |
| contracts/api.md | Complete | [contracts/api.md](contracts/api.md) |
| quickstart.md | Complete | [quickstart.md](quickstart.md) |

## Implementation Phases

### Phase 1: Config & Synthetic Data Extension
- Add new config constants (edge score weights, hub caps, OKVED defaults, behavioral k-range)
- Extend synthetic.py with okved_code and region_code columns
- Update conftest.py fixtures

### Phase 2: Wave 1 — Extended Node Metrics
- Implement `compute_extended_metrics()` in graph_builder.py
- Implement `compute_edge_score()` in graph_builder.py
- Write tests for both functions

### Phase 3: Wave 1 — Hub Filtering & Cluster Summary
- Implement `hub_filter()` in filters.py
- Extend `build_cluster_summary()` with external_counterparty_count
- Write tests

### Phase 4: Wave 2 — ETL Extension
- Add OKVED/region field mappings to schema.py
- Extend `extract_nodes()` in etl.py
- Update build_graph() to pass okved_code/region_code as node attributes

### Phase 5: Wave 2 — Industry Analytics
- Implement `build_okved_matrix()` in analysis.py
- Implement `compute_okved_diversity()` in analysis.py
- Write tests

### Phase 6: Wave 2 — Behavioral Segmentation & Look-Alike
- Implement `compute_behavioral_features()` in analysis.py
- Implement `cluster_behavioral_segments()` in analysis.py
- Implement `compute_lookalike_scores()` in analysis.py
- Write tests

### Phase 7: Pipeline Integration
- Extend `run_analysis_pipeline()` with new steps
- Extend PipelineResult with okved_matrix, behavioral_df
- Update pipeline tests

### Phase 8: Notebook Documentation
- Update notebooks 01–04 with markdown documentation
- Create notebook 05_industry_analysis.ipynb
- Create notebook 06_behavioral_segmentation.ipynb
- Add interpretation glossaries

### Phase 9: Polish
- Full test suite run
- Quickstart validation
- Update __init__.py docstring
