# Tasks: Transaction Graph MVP

**Input**: Design documents from `/specs/001-transaction-graph-mvp/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Not explicitly requested. Test tasks omitted.

**Organization**: Tasks grouped by user story for independent implementation and testing.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3, US4)
- Include exact file paths in descriptions

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization, directory structure, dependencies, and shared configuration modules

- [x] T001 Create project directory structure: `src/`, `notebooks/`, `data/` directories and `data/.gitkeep`
- [x] T002 Create `requirements.txt` with dependencies: pyspark, pandas, numpy, networkx, python-igraph, leidenalg, pyvis, matplotlib
- [x] T003 [P] Create `src/__init__.py` with package docstring
- [x] T004 [P] Create `src/config.py` with all configuration constants: HIVE_DATABASE, table names, default parameters (alpha=0.05, min_tx_count=3, gamma_values, shell_threshold, max_cycle_length), color maps for visualization, node type mappings
- [x] T005 [P] Create `src/schema.py` with column name mappings for all Hive tables: client_sdim (65 fields), account_sdim (47 fields), paymentcounteragent_stran (30 fields), clientauthority_shist (27 fields), clientauthority2clientrb_shist (12 fields), clnt2dealsalary_shist (27 fields), dealsalary_sdim, clienttype_ldim, clientstatus_ldim — each table maps assumed column names to configurable dict, with DESCRIBE TABLE helper function

**Checkpoint**: Project skeleton ready — all imports and configuration available for feature development

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Verify Hive connectivity and column name assumptions before building features

**Warning**: No user story work should proceed if column names are wrong

- [x] T006 Create `notebooks/00_verify_schema.ipynb` — a verification notebook that: (1) initializes SparkSession with Hive support, (2) runs DESCRIBE TABLE for all key tables in schema.py, (3) compares actual columns with assumed mappings, (4) prints mismatches and auto-generates corrected schema.py snippet. This notebook MUST be run first on MDP to validate assumptions.

**Checkpoint**: Foundation ready — column names verified, user story implementation can begin

---

## Phase 3: User Story 1 — Extract Company Neighborhood (Priority: P1) MVP

**Goal**: Extract a seed company's N-hop transaction neighborhood from Hive into Parquet files containing nodes, transaction edges, authority edges, and salary edges.

**Independent Test**: Run notebook 01 with a known seed company client_uk. Verify output Parquet files exist and contain expected columns. Check that hop-1 and hop-2 counterparties appear.

### Implementation for User Story 1

- [x] T007 [US1] Implement `src/etl.py` — PySpark extraction functions per contracts/etl.md: (1) `expand_hop()` — query paymentcounteragent_stran for counterparties of a client set with date_part filter, return expanded set; (2) `extract_nodes()` — query client_sdim JOIN clienttype_ldim JOIN clientstatus_ldim for given client_uks, add INN from account_sdim; (3) `extract_transaction_edges()` — query paymentcounteragent_stran with date filter and client filter, GROUP BY (source, target, quarter), compute SUM/COUNT/AVG/STDDEV/MAX/MIN; (4) `extract_authority_edges()` — JOIN clientauthority_shist with clientauthority2clientrb_shist for given clients; (5) `extract_salary_edges()` — JOIN clnt2dealsalary_shist with dealsalary_sdim for given clients; (6) `extract_seed_neighborhood()` — orchestrator that runs expand_hop N times then calls all extractors and saves Parquet. All queries must use schema.py column names and handle partition limit errors with date-range fallback.
- [x] T008 [US1] Create `notebooks/01_data_extraction.ipynb` — user-facing notebook with: (1) markdown cells explaining the extraction process, (2) configuration cell with SEED_CLIENT_UK, N_HOPS, START_DATE, END_DATE parameters, (3) SparkSession initialization, (4) call to extract_seed_neighborhood(), (5) summary statistics cell showing node count by type, edge count by type, and data sample previews, (6) save confirmation with output file paths

**Checkpoint**: User Story 1 complete — analyst can extract a company's neighborhood into Parquet files

---

## Phase 4: User Story 2 — Build and Filter the Relationship Graph (Priority: P2)

**Goal**: Construct a heterogeneous directed graph from Parquet files, compute edge metrics, and apply the Serrano disparity filter to extract the significant backbone.

**Independent Test**: Load sample Parquet files from US1 (or create synthetic test data), build graph, verify node/edge types are correct, apply disparity filter, verify edge count reduces by >50%.

### Implementation for User Story 2

- [x] T009 [P] [US2] Implement `src/graph_builder.py` per contracts/graph_builder.md: (1) `build_graph()` — load Parquet DataFrames, add nodes with attributes (client_uk, name, node_type, inn, status, hop_distance), add transaction edges with all metrics, add authority edges, add salary edges; (2) `compute_edge_metrics()` — for each transaction edge compute share_of_turnover, reciprocity, weight=log(1+amount); (3) `derive_shared_employees()` — from salary_edges_df find company pairs sharing employees, return DataFrame; (4) `get_graph_stats()` — return dict with counts by type, components, density
- [x] T010 [P] [US2] Implement `src/filters.py` per contracts/filters.md: (1) `pre_filter()` — remove transaction edges with tx_count < threshold or total_amount < threshold, keep non-transaction edges, return filtered copy; (2) `disparity_filter()` — Serrano algorithm: for each edge compute alpha from outgoing perspective (source) and incoming perspective (target), remove if insignificant from both perspectives, remove isolates, return backbone; (3) `apply_filter_pipeline()` — chain pre_filter → disparity_filter, return (filtered_graph, stats_dict with retention rates)
- [ ] T011 [US2] Create `notebooks/02_graph_construction.ipynb` — user-facing notebook: (1) load Parquet files from data/ directory, (2) build graph via build_graph() + compute_edge_metrics(), (3) display graph stats (nodes/edges by type, components), (4) derive shared_employees and add to graph, (5) apply filter pipeline with configurable parameters, (6) display before/after comparison (edge reduction %, weight retention %), (7) save filtered graph as pickle and metrics as Parquet

**Checkpoint**: User Story 2 complete — raw transaction data transformed into filtered backbone graph

---

## Phase 5: User Story 3 — Analyze Corporate Structure and Detect Patterns (Priority: P3)

**Goal**: Apply Leiden clustering to identify corporate groups, compute centrality metrics to profile node roles, and detect shell companies and circular payments.

**Independent Test**: Load pre-built graph from US2, run Leiden and verify clusters are found, verify centrality scores are computed, verify cycle detection works on a graph with known cycles.

### Implementation for User Story 3

- [ ] T012 [US3] Implement `src/analysis.py` per contracts/analysis.md: (1) `run_leiden_clustering()` — convert NetworkX→igraph via Graph.from_networkx(), run Leiden CPM at multiple gamma values, select best by quality, return membership dict + best_gamma; (2) `compute_centrality()` — weighted PageRank, betweenness centrality, clustering coefficient, in/out degree, total in/out flow, flow_through_ratio, return DataFrame; (3) `classify_node_roles()` — apply role classification rules from research.md (parent=high PR/moderate BC, shell=low PR/very high BC/low CC, subsidiary=moderate PR/low BC/high degree, conduit=low PR/high BC/low degree), add 'role' column; (4) `detect_shell_companies()` — compute shell_score from 5 signals (flow_through 30%, no_salary 25%, high_BC_low_CC 20%, low_counterparties 15%, bursty 10%), return nodes above threshold; (5) `detect_cycles()` — extract transaction subgraph, run nx.simple_cycles(), filter by length 3-5, compute total cycle amounts, sort by amount; (6) `build_cluster_summary()` — aggregate per cluster: member/company/individual count, internal turnover, lead company (max PR), has_cycles, shell_count, anomaly_flags
- [ ] T013 [US3] Create `notebooks/03_graph_analysis.ipynb` — user-facing notebook: (1) load filtered graph from pickle, (2) run Leiden with gamma sweep, display cluster count vs gamma plot, (3) compute centrality metrics for all nodes, (4) classify node roles and display role distribution table, (5) detect shell companies and display flagged nodes with scores, (6) detect cycles and display circular payment patterns, (7) build cluster summary table, (8) save all metrics to Parquet (graph_metrics.parquet, clusters.parquet)

**Checkpoint**: User Story 3 complete — corporate groups identified, nodes profiled, anomalies flagged

---

## Phase 6: User Story 4 — Visualize the Relationship Graph Interactively (Priority: P4)

**Goal**: Render an interactive HTML graph in JupyterLab with color-coded nodes, weighted edges, cluster grouping, and summary tables.

**Independent Test**: Load analyzed graph with cluster and centrality attributes, render visualization, verify nodes are colored by type, sized by PageRank, and tooltips show expected information.

### Implementation for User Story 4

- [ ] T014 [US4] Implement `src/viz.py` per contracts/viz.md: (1) `create_graph_visualization()` — create pyvis Network with notebook=True, cdn_resources='in_line', forceAtlas2Based physics; add nodes colored by node_type (company=blue, individual=green, IP=orange), sized by PageRank (10-50px), with tooltips (name, type, cluster, PR, BC, role); add edges colored by edge_type (transaction=gray, authority=red, salary=green, shared=purple), width by log(volume), with tooltips (type, volume, count, share); return Network object; (2) `create_cluster_visualization()` — extract single cluster subgraph + 1-hop external neighbors, internal=full color, external=faded, highlight shells/cycles; (3) `display_summary_table()` — format cluster_summary_df with pandas Styler: red for shells, yellow for cycles, bold lead companies; (4) `display_node_profile()` — display single node info card: basic info, centrality metrics, top counterparties, shell score
- [ ] T015 [US4] Create `notebooks/04_visualization.ipynb` — user-facing notebook: (1) load analyzed graph + metrics + cluster summary from Parquet/pickle, (2) create full graph visualization and display inline, (3) display cluster summary table, (4) loop over top clusters: create per-cluster visualization, (5) display node profile for seed company, (6) display node profiles for flagged shell companies, (7) add legend explaining color coding and size encoding

**Checkpoint**: User Story 4 complete — interactive visualization ready for demo/presentation

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect the entire system

- [ ] T016 Validate end-to-end workflow by running notebooks 00→01→02→03→04 sequentially with synthetic or real seed company, verify all outputs are generated
- [ ] T017 [P] Add error handling to `src/etl.py` for: Hive connection failures, empty result sets, partition limit exceeded (auto-narrow date range and retry)
- [ ] T018 [P] Add logging throughout all `src/*.py` modules using Python logging module with configurable level
- [ ] T019 Review and update `src/schema.py` with actual column names discovered during T006 verification
- [ ] T020 Final code cleanup: remove debug prints, ensure consistent docstrings, verify all imports

**Checkpoint**: System production-ready for demo

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — start immediately
- **Foundational (Phase 2)**: Depends on Setup (T004, T005 specifically) — BLOCKS user stories until schema verified
- **US1 (Phase 3)**: Depends on Phase 2 — requires config.py + schema.py + verified columns
- **US2 (Phase 4)**: Depends on Phase 1 completion — needs config.py. Can use US1 output OR synthetic data
- **US3 (Phase 5)**: Depends on Phase 1 completion — needs config.py. Can use US2 output OR synthetic graph
- **US4 (Phase 6)**: Depends on Phase 1 completion — needs config.py. Can use US3 output OR synthetic analyzed graph
- **Polish (Phase 7)**: Depends on all user stories being complete

### User Story Dependencies

- **US1 (P1)**: Standalone. Requires only config.py + schema.py
- **US2 (P2)**: Uses US1 Parquet output as input. Can also work with synthetic Parquet files
- **US3 (P3)**: Uses US2 graph as input. Can also work with synthetic NetworkX graph
- **US4 (P4)**: Uses US3 analyzed graph as input. Can also work with synthetic analyzed graph

### Within Each User Story

- src/ module implementation before notebook creation
- For US2: graph_builder.py and filters.py are independent (can be [P])
- Notebooks always last (they import from src/)

### Parallel Opportunities

Within Phase 1:
```
Parallel: T003 (init.py) + T004 (config.py) + T005 (schema.py)
```

Within Phase 4 (US2):
```
Parallel: T009 (graph_builder.py) + T010 (filters.py)
Then sequential: T011 (notebook, depends on T009 + T010)
```

Cross-story (if team capacity allows):
```
After Phase 2, US1-US4 src/ modules can be developed in parallel.
Notebooks must wait for their respective src/ modules.
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (T001-T005)
2. Complete Phase 2: Verify schema on MDP (T006)
3. Complete Phase 3: US1 Data Extraction (T007-T008)
4. **STOP and VALIDATE**: Run notebook 01 with a real seed company on MDP
5. If extraction works → continue to US2

### Incremental Delivery

1. Setup + Foundational → Skeleton ready
2. **US1** → Data extraction works → First demo (show extracted data)
3. **US2** → Graph built and filtered → Second demo (show backbone stats)
4. **US3** → Analysis complete → Third demo (show clusters and anomalies)
5. **US4** → Visualization ready → Final demo (interactive graph for stakeholders)

### Single Developer Strategy (Recommended for MVP)

Execute in strict order: Phase 1 → Phase 2 → Phase 3 → Phase 4 → Phase 5 → Phase 6 → Phase 7.
Each phase builds on the previous. Total estimated tasks: 20.

---

## Notes

- Schema verification (T006) is critical — run it on MDP before implementing etl.py
- All Parquet paths use `data/` directory relative to project root
- pyvis must use `cdn_resources='in_line'` for MDP offline compatibility
- For Hive tables exceeding 3000 partitions, always include `date_part` filter
- Column names in schema.py are assumptions from DDL screenshots — will be corrected in T006/T019
