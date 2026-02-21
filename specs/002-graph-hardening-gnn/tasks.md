# Tasks: Graph Hardening & GNN Integration

**Input**: Design documents from `specs/002-graph-hardening-gnn/`
**Prerequisites**: plan.md (required), spec.md (required), research.md, data-model.md, contracts/api.md

**Tests**: Included as a core user story (US1) per spec — tests are the highest priority deliverable.

**Organization**: Tasks grouped by user story to enable independent implementation and testing.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2)
- Include exact file paths in descriptions

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization, dependency file, test infrastructure

- [x] T001 Create requirements.txt at project root with all dependencies per research.md Decision 6 (pyspark>=3.0, pandas>=1.3, networkx>=2.6, python-igraph>=0.9, leidenalg>=0.8, pyvis>=0.3, numpy>=1.20, matplotlib>=3.4, torch>=1.13,<3.0, dgl>=1.1,<2.0, scikit-learn>=0.24, pytest>=7.0)
- [x] T002 Create tests/__init__.py as empty package marker
- [x] T003 Create tests/conftest.py with session-scoped pytest fixtures: `synthetic_data_dir` (generates synthetic data once via generate_synthetic_data to tmpdir), `nodes_df`/`tx_df`/`auth_df`/`sal_df`/`hop_df` (load parquets), `raw_graph` (build_graph + compute_edge_metrics + shared_employees), `filtered_graph` (apply_filter_pipeline on raw_graph), `metrics_df` (compute_centrality on filtered_graph), `membership` (run_leiden_clustering on filtered_graph)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Bug fixes and core functions that MUST be complete before user stories can proceed

**CRITICAL**: These changes fix broken behavior and add functions used by all subsequent stories

- [x] T004 Fix `import os` placement in src/config.py — move `import os as _os` from line 106 to top of file (after module docstring, before constants). Verify no behavioral change.
- [x] T005 Fix `is_liquidated` derivation in src/graph_builder.py — replace `is_liquidated=(str(row.get('liquidation_flag', '')).upper() == 'Y')` with logic: `deleted_flag == 'Y'` OR (`end_date` is not None/NaT AND `end_date < today`). Use `client_status_name` field and `deleted_flag` from the nodes DataFrame. Update synthetic.py to include `deleted_flag` column in generated nodes if missing.
- [x] T006 Fix float→int in src/etl.py — in `_register_client_temp_view()` change `pdf = pd.DataFrame({'uk': [float(c) for c in client_uks]})` to `pdf = pd.DataFrame({'uk': [int(c) for c in client_uks]})` to prevent type mismatch in Spark JOINs.
- [x] T007 Add `enrich_graph()` function in src/graph_builder.py — per contracts/api.md: accepts G, metrics_df, membership dict, optional shell_df. Writes pagerank, betweenness, clustering_coef, in_degree, out_degree, total_in_flow, total_out_flow, flow_through_ratio, role, cluster, shell_score as node attributes. Returns G (modified in-place).

**Checkpoint**: Foundation ready — all bug fixes applied, enrich_graph available for pipeline and tests

---

## Phase 3: User Story 1 — Automated Test Suite (Priority: P1)

**Goal**: Comprehensive pytest suite covering all modules using synthetic data only

**Independent Test**: Run `cd src && pytest` — all tests pass, no Spark/Hive needed

### Test Implementation

- [x] T008 [P] [US1] Create tests/test_synthetic.py — test generate_synthetic_data(): verify output files exist, node counts match (30 companies + 50 individuals + 5 IPs = 85), tx_df has expected columns, shell companies (client_uk 1024/1025) have no salary edges, cycle companies (1026/1027/1028) appear in tx edges
- [x] T009 [P] [US1] Create tests/test_graph_builder.py — test build_graph(): verify node count matches nodes_df, node_type mapping (company/individual/sole_proprietor), edge types present (transaction/authority/salary). Test compute_edge_metrics(): verify share_of_turnover sums to ~1.0 per node outgoing edges, reciprocity in [0,1]. Test derive_shared_employees(): verify shared pairs found. Test enrich_graph(): verify all expected attributes set on nodes after enrichment
- [x] T010 [P] [US1] Create tests/test_filters.py — test pre_filter(): verify edges below all thresholds removed, non-transaction edges preserved. Test disparity_filter(): verify edge count reduced, degree-1 node edges preserved. Test apply_filter_pipeline(): verify stats dict has expected keys, final < original edges
- [x] T011 [P] [US1] Create tests/test_analysis.py — test run_leiden_clustering(): verify returns dict mapping all nodes, at least 1 cluster. Test compute_centrality(): verify DataFrame has all expected columns, indexed by client_uk, all nodes present. Test classify_node_roles(): verify 'role' column added with valid values (parent/shell/subsidiary/conduit/regular). Test detect_shell_companies(): verify flagged nodes have shell_score >= threshold. Test detect_cycles(): verify cycle A→B→C detected (companies 1026→1027→1028). Test build_cluster_summary(): verify DataFrame has expected columns, no crash on single-cluster case
- [x] T012 [US1] Verify full test suite passes by running pytest from src/ directory — fix any import issues or fixture problems

**Checkpoint**: All tests pass. Safety net established for all further changes.

---

## Phase 4: User Story 2 — Bug Fixes and Data Integrity (Priority: P1)

**Goal**: Correct node status derivation and bridge analysis→visualization gap

**Independent Test**: Build graph from synthetic data with deleted nodes, verify is_liquidated correct. Run enrich_graph, verify viz attributes present.

*Note: Core fixes (T004-T007) already done in Phase 2. This phase adds tests validating those fixes.*

- [x] T013 [US2] Add tests for is_liquidated fix in tests/test_graph_builder.py — add synthetic node with deleted_flag='Y' to nodes_df, verify is_liquidated=True after build_graph. Add node with end_date in past, verify is_liquidated=True. Add active node, verify is_liquidated=False.
- [x] T014 [US2] Add tests for enrich_graph in tests/test_graph_builder.py — verify after enrich_graph(G, metrics_df, membership, shell_df): every node has 'cluster' attr, nodes in metrics_df have 'pagerank'/'betweenness'/'role', nodes in shell_df have 'shell_score'. Verify nodes NOT in metrics_df get sensible defaults.
- [x] T015 [US2] Update synthetic.py to add test data for is_liquidated — add 1-2 nodes with deleted_flag='Y' and 1 node with end_date in the past to the generated dataset, so tests can verify the fix

**Checkpoint**: Bug fixes validated by tests. Correct data flows through to visualization.

---

## Phase 5: User Story 3 — One-Command Analysis Pipeline (Priority: P2)

**Goal**: Single `run_analysis_pipeline()` function replacing manual 4-notebook workflow

**Independent Test**: Call pipeline on synthetic data dir, verify PipelineResult contains enriched graph with all attributes, metrics_df, cluster_summary, cycles.

- [x] T016 [US3] Create src/pipeline.py — define PipelineResult namedtuple (graph, metrics_df, cluster_summary, cycles, filter_stats, best_gamma). Implement run_analysis_pipeline() per contracts/api.md: load parquets (nodes, tx, auth, salary, hop_distances), merge hop_distances into nodes, call build_graph, compute_edge_metrics, derive_shared_employees (add edges), apply_filter_pipeline, run_leiden_clustering, compute_centrality, classify_node_roles, detect_shell_companies, detect_cycles, enrich_graph, build_cluster_summary. Accept optional override params for all thresholds. Return PipelineResult.
- [x] T017 [US3] Create tests/test_pipeline.py — test run_analysis_pipeline() on synthetic data: verify PipelineResult fields are populated, graph has enriched attributes (pagerank, cluster, role, shell_score on every node), metrics_df indexed by client_uk, cluster_summary has rows, cycles list not empty (cycle A→B→C should be detected), filter_stats has retention rates
- [x] T018 [US3] Update src/__init__.py — add pipeline module to package docstring imports list

**Checkpoint**: Pipeline orchestrator works end-to-end. Single function call replaces 4 notebooks.

---

## Phase 6: User Story 4 — Performance Optimization (Priority: P2)

**Goal**: Vectorize classify_node_roles, detect_shell_companies, build_cluster_summary

**Independent Test**: Run vectorized versions on synthetic data, compare results to pre-optimization baseline — must be identical.

- [x] T019 [US4] Vectorize classify_node_roles() in src/analysis.py — replace iterrows() loop with np.select(). Build boolean condition arrays for each role (parent, shell, subsidiary, conduit) matching the exact same logic as current if/elif chain. Use np.select(conditions, choices, default='regular'). Assign result to df['role']. Remove the old loop.
- [x] T020 [US4] Vectorize detect_shell_companies() in src/analysis.py — replace iterrows() loop with vectorized scoring. Compute each signal as a boolean Series: flow_through_ratio > 0.9, ~has_salary_payments, betweenness > median*2 & clustering_coef < 0.1, in_degree+out_degree <= 4. For bursty signal: pre-compute per-node period count from graph edges into a Series, then compare <= 1. Sum weighted booleans to get shell_score column. Filter by threshold.
- [x] T021 [US4] Optimize build_cluster_summary() in src/analysis.py — replace per-cluster edge iteration with single-pass approach: build edge list [(u, v, amount, cluster_u, cluster_v)], filter to internal edges (cluster_u == cluster_v), groupby cluster_id and sum. Merge with node-level cluster stats.
- [x] T022 [US4] Add backward-compatibility tests in tests/test_analysis.py — for each vectorized function: save output of old implementation on synthetic data as expected baseline, then verify new implementation produces identical DataFrame (use pd.testing.assert_frame_equal with check_like=True for column order independence)

**Checkpoint**: All optimizations produce identical results. Analysis runs faster on larger graphs.

---

## Phase 7: User Story 5 — GNN-Based Node Classification (Priority: P3)

**Goal**: R-GCN model for shell detection and role classification using DGL

**Independent Test**: Train on synthetic data heuristic labels, evaluate accuracy >= 70% shell / >= 60% role agreement.

### GNN Implementation

- [ ] T023 [US5] Create src/gnn.py — module-level try/except for `import torch` and `import dgl` with helpful ImportError message if missing. Define RGCN model class using dgl.nn.pytorch.HeteroGraphConv wrapping GraphConv per relation type. 2 layers, ReLU + dropout, configurable hidden_dim. Separate classification heads for shell (binary sigmoid) and role (multi-class softmax).
- [ ] T024 [US5] Implement prepare_gnn_data() in src/gnn.py — per contracts/api.md and research.md Decision 3: group nodes by node_type, assign local indices, build node_mapping dict. Group edges by edge_type, map to (src_type, relation, dst_type) tuples. Build dgl.heterograph(). Extract ~15 node features per data-model.md (pagerank, betweenness, clustering_coef, flows, degrees, type one-hot, hop_distance, has_salary, mean_tx_amount, total_tx_count). Fit StandardScaler, normalize features. Attach feature tensors and label tensors per node type. Return (dgl_graph, node_mapping, scaler).
- [ ] T025 [US5] Implement train_gnn() in src/gnn.py — per contracts/api.md: accept dgl_graph, target_task, hyperparams. Create train/val masks (80/20 split via scikit-learn). Instantiate RGCN model. Training loop: forward pass, compute loss (BCEWithLogitsLoss for shell with label_smoothing, CrossEntropyLoss for role), backward, optimizer step. Track train/val loss and val accuracy/f1. Return (model, train_info dict).
- [ ] T026 [US5] Implement predict_gnn() in src/gnn.py — per contracts/api.md: run model in eval mode, extract logits, apply sigmoid (shell) or softmax (role), map back to client_uk via node_mapping. Return DataFrame with client_uk, predicted_label, probability.
- [ ] T027 [US5] Implement save_gnn_model() and load_gnn_model() in src/gnn.py — per contracts/api.md: save dict containing model.state_dict(), node_mapping, scaler, label_encoder, metadata (hyperparams, metrics, date) via torch.save(). Load via torch.load() + model.load_state_dict(). Validate loaded structure.
- [ ] T028 [US5] Create tests/test_gnn.py — test prepare_gnn_data(): verify dgl_graph has correct node/edge types, feature tensors have expected shape. Test train_gnn(): verify training completes without error, val_accuracy > 0. Test predict_gnn(): verify output DataFrame has expected columns and all nodes present. Test save/load round-trip: save model, load, predict, verify identical results. Test with target_task='shell' and target_task='role'.

**Checkpoint**: GNN trains on synthetic data, achieves target accuracy, model persists correctly.

---

## Phase 8: User Story 6 — Dependency Management (Priority: P3)

**Goal**: requirements.txt with all pinned dependencies

*Note: requirements.txt already created in T001. This phase validates it.*

- [ ] T029 [US6] Validate requirements.txt — verify it includes all imports found across src/*.py files. Split into sections with comments: core (pandas, numpy, networkx, etc.), etl (pyspark), analysis (igraph, leidenalg), viz (pyvis, matplotlib), gnn (torch, dgl, scikit-learn), dev (pytest). Add comment noting GNN deps are optional.

**Checkpoint**: Dependencies documented. Environment reproducible.

---

## Phase 9: Polish & Cross-Cutting Concerns

**Purpose**: Final validation and cleanup

- [ ] T030 Run full pytest suite from src/ — verify ALL tests pass (test_synthetic, test_graph_builder, test_filters, test_analysis, test_pipeline, test_gnn)
- [ ] T031 Run quickstart.md validation — execute the quickstart code snippets (pipeline + GNN training) on synthetic data, verify they work as documented
- [ ] T032 Update src/__init__.py module docstring to mention new modules (pipeline, gnn)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — start immediately
- **Foundational (Phase 2)**: Depends on Phase 1 — BLOCKS all user stories
- **US1 Tests (Phase 3)**: Depends on Phase 2 — validates foundation
- **US2 Bug Fix Validation (Phase 4)**: Depends on Phase 2 + Phase 3 (test infra)
- **US3 Pipeline (Phase 5)**: Depends on Phase 2 (enrich_graph) — can run parallel with Phase 3/4
- **US4 Optimization (Phase 6)**: Depends on Phase 3 (tests exist to validate no regression)
- **US5 GNN (Phase 7)**: Depends on Phase 5 (pipeline produces enriched graph for training data)
- **US6 Dependencies (Phase 8)**: Independent — can run anytime
- **Polish (Phase 9)**: Depends on all prior phases

### User Story Dependencies

```
Phase 1 (Setup)
  └── Phase 2 (Foundational: bug fixes + enrich_graph)
        ├── Phase 3 (US1: Tests) ←── can start immediately after Phase 2
        │     └── Phase 4 (US2: Bug Fix Validation)
        │     └── Phase 6 (US4: Optimization) ←── needs tests for regression check
        ├── Phase 5 (US3: Pipeline) ←── can start parallel with US1
        │     └── Phase 7 (US5: GNN) ←── needs pipeline output
        └── Phase 8 (US6: Dependencies) ←── independent
```

### Parallel Opportunities

**Within Phase 1**: T001, T002, T003 can all run in parallel (different files)

**Within Phase 3**: T008, T009, T010, T011 can all run in parallel (different test files)

**Across Phases**: After Phase 2 completes:
- US1 (Phase 3) and US3 (Phase 5) can run in parallel
- US6 (Phase 8) can run anytime

**Within Phase 6**: T019, T020, T021 can run in parallel (different functions in same file, but non-overlapping code sections)

**Within Phase 7**: T023 must come first, then T024-T027 are sequential (each builds on prior)

---

## Parallel Example: User Story 1

```bash
# Launch all test files in parallel (different files, no dependencies):
Task: "Create tests/test_synthetic.py"
Task: "Create tests/test_graph_builder.py"
Task: "Create tests/test_filters.py"
Task: "Create tests/test_analysis.py"
```

## Parallel Example: After Phase 2

```bash
# Launch these stories in parallel:
Task: "US1 — Test suite" (Phase 3)
Task: "US3 — Pipeline orchestrator" (Phase 5)
Task: "US6 — Requirements validation" (Phase 8)
```

---

## Implementation Strategy

### MVP First (User Stories 1 + 2 Only)

1. Complete Phase 1: Setup (T001-T003)
2. Complete Phase 2: Foundational bug fixes (T004-T007)
3. Complete Phase 3: Test suite (T008-T012)
4. Complete Phase 4: Bug fix validation (T013-T015)
5. **STOP and VALIDATE**: All tests pass, bugs fixed, safety net in place
6. This alone is a significant quality improvement over MVP

### Incremental Delivery

1. Setup + Foundational → Foundation ready
2. Add US1 (Tests) + US2 (Bug fixes) → Quality baseline (MVP!)
3. Add US3 (Pipeline) → One-command analysis
4. Add US4 (Optimization) → Faster processing
5. Add US5 (GNN) → ML-powered classification
6. Add US6 (Dependencies) → Reproducible environment
7. Each story adds value without breaking previous stories

---

## Notes

- [P] tasks = different files, no dependencies on incomplete tasks
- [Story] label maps task to specific user story for traceability
- Python 3.8 compatibility required for all code
- GNN imports (torch, dgl) must be guarded with try/except — other modules must work without them
- Vectorized functions must produce identical output to row-by-row originals
- All tests use synthetic data only — no Spark, Hive, or network
