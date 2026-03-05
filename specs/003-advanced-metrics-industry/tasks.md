# Tasks: Advanced Graph Metrics, Industry Analytics & Notebook Documentation

**Input**: Design documents from `/specs/003-advanced-metrics-industry/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/api.md, quickstart.md

**Tests**: Included — SC-008 requires "Full test suite (existing + new tests) passes with 0 failures after all changes." Existing test suite has 38 passing tests.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

---

## Phase 1: Setup

**Purpose**: Extend configuration, synthetic data, and test fixtures with new parameters needed by all user stories.

- [x] T001 Extend src/config.py with Wave 1 parameters (EDGE_SCORE_W_BASE=0.30, EDGE_SCORE_W_BILATERAL=0.30, EDGE_SCORE_W_NODE=0.20, EDGE_SCORE_W_STABILITY=0.20, HUB_CAP_MIN=20, HUB_CAP_MAX=50, TOP_K_COUNTERPARTIES=5) and Wave 2 parameters (DEFAULT_OKVED_CODE="00", DEFAULT_REGION_CODE="00", BEHAVIORAL_K_RANGE=(3,10), LOOKALIKE_TOP_DECILE=0.1)
- [x] T002 Extend src/synthetic.py to add okved_code (random from 20 common OKVED codes per research.md Decision 3) and region_code (random from 10 region codes) columns to nodes_df for all node types. Companies and IPs get realistic codes; individuals get DEFAULT_OKVED_CODE/"00"
- [x] T003 Update tests/conftest.py fixtures: after loading nodes_df verify okved_code and region_code columns exist. Update tests/test_synthetic.py with validation that every node has okved_code and region_code, companies/IPs have non-"00" codes, individuals have "00"

**Checkpoint**: Config and synthetic data ready for all user stories

---

## Phase 2: User Story 1 — Extended Node & Edge Metrics (Priority: P1) MVP

**Goal**: Enrich every node with unique_counterparty_count, top_k_concentration, active_months, hub_flag. Add edge_score to every transaction edge. Hub-aware filtering. External counterparty count in cluster summaries.

**Independent Test**: Run pipeline on synthetic data, verify every node has the 4 new metrics, every transaction edge has edge_score > 0, hub filtering reduces edges for high-degree nodes, cluster summary includes external_counterparty_count.

### Implementation for User Story 1

- [x] T004 [US1] Implement compute_extended_metrics(G) in src/graph_builder.py — compute unique_counterparty_count (distinct tx neighbors in+out), top_k_concentration (top-5 outgoing amounts / total, 0.0 if no outgoing), active_months (distinct YYYY-MM from first_tx_date/last_tx_date across all tx edges), hub_flag (unique_counterparty_count > 2*median). Set as node attributes. See contracts/api.md and data-model.md
- [x] T005 [US1] Implement compute_edge_score(G, w_base, w_bilateral, w_node, w_stability) in src/graph_builder.py — rank-percentile norm_amount, bilateral_share (product of src outgoing share × tgt incoming share), node_importance (avg normalized pagerank), stability_factor (min active_months / max). Formula: weighted sum per research.md Decision 1. Requires compute_extended_metrics() to have run. See contracts/api.md
- [x] T006 [P] [US1] Implement hub_filter(G, membership, cap_min=20, cap_max=50) in src/filters.py — identify hubs (hub_flag=True), compute degree-proportional cap min(max(cap_min, ceil(sqrt(degree))), cap_max), rank tx edges by edge_score, remove below cap. Exempt non-tx edges, reciprocal tx edges, same-cluster edges. Verify no new isolated components. Returns filtered copy. See research.md Decision 2
- [x] T007 [P] [US1] Extend build_cluster_summary() in src/analysis.py — add external_counterparty_count column: count distinct nodes outside the cluster that have transaction edges with cluster members. Use single-pass edge iteration pattern (matching existing internal_turnover approach)
- [x] T008 [US1] Write tests for compute_extended_metrics() and compute_edge_score() in tests/test_graph_builder.py — test unique_counterparty_count matches manual count, top_k_concentration in [0,1], active_months >= 1, hub_flag correctness vs median, edge_score > 0 for all tx edges, edge_score ranking correctness. Test edge case: node with no outgoing edges → top_k_concentration=0.0
- [x] T009 [P] [US1] Write tests for hub_filter() in tests/test_filters.py — test hub edges reduced to cap, non-tx edges preserved, reciprocal edges preserved, no isolated components created. Test with membership=None (no cluster exemption)
- [x] T010 [P] [US1] Write tests for external_counterparty_count in tests/test_analysis.py — verify count is non-negative integer, cluster with no external edges has count=0, cluster with known external connections has correct count

**Checkpoint**: Wave 1 metrics complete — every node enriched, every tx edge scored, hubs filtered, cluster summary extended

---

## Phase 3: User Story 2 — OKVED & Region ETL Extension (Priority: P2)

**Goal**: ETL pipeline extracts okved_code and region_code from Hive client_sdim and stores them as node attributes in the graph.

**Independent Test**: Run ETL/synthetic generation, verify okved_code and region_code appear in nodes.parquet and as graph node attributes.

**Dependencies**: Phase 1 (synthetic data already has OKVED/region from T002)

### Implementation for User Story 2

- [x] T011 [P] [US2] Add OKVED/region field mappings to src/schema.py — add okved_code and region_code keys to CLIENT dict (field names to be verified via DESCRIBE TABLE on MDP, use placeholder names from assumption)
- [x] T012 [US2] Extend extract_nodes() in src/etl.py — add okved_code and region_code to SELECT query from client_sdim, with COALESCE to DEFAULT_OKVED_CODE/DEFAULT_REGION_CODE for nulls
- [x] T013 [US2] Extend build_graph() in src/graph_builder.py — read okved_code and region_code from nodes_df row and set as node attributes (default to config.DEFAULT_OKVED_CODE/DEFAULT_REGION_CODE if missing)
- [x] T014 [US2] Write tests for OKVED/region node attributes in tests/test_graph_builder.py — verify every node in graph has okved_code and region_code attributes, companies have non-"00" codes, individuals have "00"

**Checkpoint**: OKVED/region data flows from ETL/synthetic → parquet → graph node attributes

---

## Phase 4: User Story 3 — Industry Ecosystem Map (Priority: P2)

**Goal**: OKVED×OKVED cross-industry turnover matrix and per-node OKVED diversity metrics (count + Shannon entropy). Cross-industry hub detection.

**Independent Test**: Build OKVED matrix from synthetic data with diverse OKVED codes. Verify matrix is non-negative and covers all OKVED pairs. Verify OKVED diversity metrics on nodes.

**Dependencies**: Phase 3 (US2 — needs OKVED data on nodes)

### Implementation for User Story 3

- [x] T015 [US3] Implement build_okved_matrix(G) in src/analysis.py — iterate transaction edges, group by (source_okved, target_okved) excluding "00", aggregate total_turnover/edge_count/avg_amount. Return DataFrame with okved_source, okved_target, total_turnover, edge_count, avg_amount. See contracts/api.md
- [x] T016 [US3] Implement compute_okved_diversity(G) in src/analysis.py — for each node, collect counterparty OKVED codes (excluding "00"), compute okved_diversity_count (distinct count), okved_diversity_entropy (Shannon entropy: -sum(p*log2(p))), is_cross_industry_hub (top 10% by diversity count). Set as node attributes. See data-model.md
- [x] T017 [US3] Write tests for build_okved_matrix() and compute_okved_diversity() in tests/test_analysis.py — verify matrix has non-negative values, all OKVED pairs present in graph are covered, "00" excluded. Verify diversity count >= 0, entropy >= 0, is_cross_industry_hub is boolean. Test edge case: graph with single OKVED → 1×1 matrix, warning logged

**Checkpoint**: Industry ecosystem map and cross-industry hub detection working

---

## Phase 5: User Story 4 — Behavioral Segmentation & Look-Alike Scoring (Priority: P3)

**Goal**: Compute behavioral features per node, cluster via K-Means, score prospects by similarity to best clients.

**Independent Test**: Generate behavioral features for all nodes, run clustering, verify each node gets a segment label. Compute look-alike scores, verify known best clients score in top quartile.

**Dependencies**: Phase 2 (US1 — needs extended metrics for features), Phase 3 (US2 — needs OKVED for cross-tabulation)

### Implementation for User Story 4

- [x] T018 [US4] Implement compute_behavioral_features(G) in src/analysis.py — compute per-node: monthly_tx_count_avg, monthly_amount_avg, direction_ratio (outflow/(inflow+outflow)), counterparty_growth_rate (change over periods, 0.0 if single period), new_counterparty_share (fraction of counterparties first seen in latest period). Return DataFrame indexed by client_uk. See contracts/api.md
- [x] T019 [US4] Implement cluster_behavioral_segments(features_df, k_range, k_override) in src/analysis.py — StandardScaler normalization, remove zero-variance features (log warning), K-Means with k in k_range auto-selected by silhouette score (or k_override). Add behavioral_segment column. See research.md Decision 4
- [x] T020 [US4] Implement compute_lookalike_scores(features_df, G, top_decile_col) in src/analysis.py — define best clients as top decile by total turnover (total_in_flow + total_out_flow), compute centroid, Euclidean distance on StandardScaler-normalized features, score = 1/(1+distance). Return empty with warning if best group is empty. See research.md Decision 5
- [x] T021 [US4] Write tests for behavioral features, clustering, and look-alike in tests/test_analysis.py — verify all 5 features computed for nodes with tx edges, direction_ratio in [0,1], behavioral_segment is int in [0,k-1], every node assigned exactly one segment, lookalike_score in (0,1]. Test edge case: zero-variance feature → removed with warning. Test edge case: empty best-client group → empty result with warning

**Checkpoint**: Behavioral segmentation and look-alike scoring complete

---

## Phase 6: Pipeline Integration

**Purpose**: Wire all new Wave 1 + Wave 2 steps into the analysis pipeline orchestrator.

**Dependencies**: Phases 2–5 (all user story implementations)

- [x] T022 Extend run_analysis_pipeline() in src/pipeline.py — add compute_extended_metrics() and compute_edge_score() after centrality step, add optional hub_filter() after enrich step, add build_okved_matrix(), compute_okved_diversity(), compute_behavioral_features(), cluster_behavioral_segments(), compute_lookalike_scores() as new steps. Extend PipelineResult namedtuple with okved_matrix and behavioral_df fields
- [x] T023 Update tests/test_pipeline.py — verify pipeline result contains okved_matrix (DataFrame with expected columns), behavioral_df (DataFrame with behavioral_segment and lookalike_score columns), extended node metrics on graph, edge_score on tx edges. Verify all existing pipeline tests still pass

**Checkpoint**: Full pipeline runs end-to-end with all Wave 1 + Wave 2 analytics

---

## Phase 7: User Story 5 — Interactive Notebook Documentation (Priority: P2)

**Goal**: Every notebook has markdown explanations before each code cell, interpretation guidance, and a glossary. Two new notebooks for industry analysis and behavioral segmentation.

**Independent Test**: Open each notebook, verify every major code cell has a preceding markdown cell. Verify "Interpretation Guide" section exists at the end of each notebook.

**Dependencies**: Phases 2–6 (all code must be implemented before documenting)

### Implementation for User Story 5

- [x] T024 [P] [US5] Update notebooks/02_graph_construction.ipynb — add executive summary, markdown cells before each code section explaining graph building, edge metrics, extended metrics (unique_counterparty_count, top_k_concentration, active_months, hub_flag), edge_score computation. Add interpretation glossary at bottom. See research.md Decision 6 for sandwich structure
- [x] T025 [P] [US5] Update notebooks/03_filtering.ipynb — add documentation for pre-filter, disparity filter, hub filtering steps. Explain what each filter does, typical retention rates, how to adjust parameters. Add interpretation glossary
- [x] T026 [P] [US5] Update notebooks/04_analysis.ipynb — add documentation for Leiden clustering, centrality metrics, role classification, shell detection, cycle detection, cluster summary (including external_counterparty_count). Business-oriented explanations: "High betweenness = potential gatekeeper or transit company". Add interpretation glossary
- [x] T027 [P] [US5] Create notebooks/05_industry_analysis.ipynb — OKVED×OKVED matrix computation, heatmap visualization (matplotlib), cross-industry hub detection, OKVED diversity per node. Include visual examples with annotations, color scale legend, textual summary of key findings. Add interpretation glossary
- [x] T028 [P] [US5] Create notebooks/06_behavioral_segmentation.ipynb — behavioral feature computation, K-Means clustering with silhouette plot, segment distribution charts, look-alike scoring, OKVED×segment cross-tabulation. Include visual examples, interpretation of what each segment means. Add interpretation glossary
- [x] T029 [P] [US5] Update notebooks/01_etl_extraction.ipynb — add documentation for ETL steps, OKVED/region extraction, hop expansion. Explain what each step does, expected output shapes, typical values

**Checkpoint**: All notebooks are self-explanatory for non-technical users (relationship managers)

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Final validation, cleanup, and documentation

- [x] T030 Run full test suite — all existing (38) + new tests pass with 0 failures. Run: cd src && pytest -v
- [x] T031 Update src/__init__.py module docstring to include new analysis functions (compute_extended_metrics, compute_edge_score, build_okved_matrix, compute_behavioral_features, etc.)
- [x] T032 Validate quickstart.md examples — ensure code snippets in specs/003-advanced-metrics-industry/quickstart.md run correctly against synthetic data

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **US1 (Phase 2)**: Depends on Phase 1 — Wave 1 metrics on current graph
- **US2 (Phase 3)**: Depends on Phase 1 — ETL/synthetic extension for OKVED/region
- **US3 (Phase 4)**: Depends on Phase 3 (US2) — needs OKVED data on nodes
- **US4 (Phase 5)**: Depends on Phase 2 (US1) + Phase 3 (US2) — needs metrics + OKVED
- **Pipeline (Phase 6)**: Depends on Phases 2–5 — wires everything together
- **US5 (Phase 7)**: Depends on Phase 6 — all code must exist before documenting
- **Polish (Phase 8)**: Depends on all phases

### User Story Dependencies

```
Phase 1 (Setup)
    ├── Phase 2 (US1: Extended Metrics) ──────────┐
    │                                              ├── Phase 5 (US4: Behavioral)
    └── Phase 3 (US2: OKVED ETL) ─┬───────────────┘
                                   └── Phase 4 (US3: Industry Map)

Phases 2-5 ──→ Phase 6 (Pipeline) ──→ Phase 7 (US5: Notebooks) ──→ Phase 8 (Polish)
```

### Parallel Opportunities

**Within Phase 1** (Setup):
- T001 (config.py) and T002 (synthetic.py) touch different files → parallel

**Within Phase 2** (US1):
- T006 (filters.py) and T007 (analysis.py) touch different files from T004/T005 → parallel
- T009 (test_filters.py) and T010 (test_analysis.py) touch different files → parallel

**Across Phases**:
- Phase 2 (US1) and Phase 3 (US2) can run in parallel after Phase 1 completes
- All US5 notebook tasks (T024–T029) can run in parallel (different files)

---

## Parallel Example: Phase 2 (User Story 1)

```
# Step 1: Implementation (parallel where possible)
T004: compute_extended_metrics() in graph_builder.py
T006: hub_filter() in filters.py                        ← parallel with T004
T007: external_counterparty_count in analysis.py         ← parallel with T004

# Step 2: After T004 completes
T005: compute_edge_score() in graph_builder.py           ← depends on T004 (active_months)

# Step 3: Tests (parallel where possible)
T008: tests in test_graph_builder.py                     ← after T004, T005
T009: tests in test_filters.py                           ← parallel with T008
T010: tests in test_analysis.py                          ← parallel with T008
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (T001–T003)
2. Complete Phase 2: User Story 1 (T004–T010)
3. **STOP and VALIDATE**: Every node has 4 extended metrics, every tx edge has edge_score, hub filtering works
4. This delivers immediate value: enriched graph metrics on existing data without any ETL changes

### Incremental Delivery

1. Setup (Phase 1) → Foundation ready
2. US1: Extended Metrics (Phase 2) → Test independently → **MVP!**
3. US2: OKVED ETL (Phase 3) → Test independently
4. US3: Industry Map (Phase 4) → Test independently
5. US4: Behavioral (Phase 5) → Test independently
6. Pipeline (Phase 6) → Full integration
7. US5: Notebooks (Phase 7) → User-facing documentation
8. Polish (Phase 8) → Final validation

---

## Notes

- [P] tasks = different files, no dependencies on incomplete tasks in the same phase
- [Story] label maps task to specific user story for traceability
- Total tasks: 32
- Tasks per user story: US1=7, US2=4, US3=3, US4=4, US5=6
- Infrastructure tasks: Setup=3, Pipeline=2, Polish=3
- scikit-learn is a new dependency (K-Means, StandardScaler) — already in requirements.txt from feature 002
- All new functions follow existing patterns: modify graph in-place, return it; DataFrame operations use vectorized pandas/numpy
- Edge cases from spec.md are covered in test task descriptions (zero outgoing edges, zero-variance features, empty best-client group, single OKVED code)
