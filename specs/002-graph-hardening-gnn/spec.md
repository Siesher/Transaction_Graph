# Feature Specification: Graph Hardening & GNN Integration

**Feature Branch**: `002-graph-hardening-gnn`
**Created**: 2026-02-19
**Status**: Draft
**Input**: User description: "Масштабное улучшение проекта Transaction Graph MVP: тесты, багфиксы, оптимизация, pipeline-оркестратор, GNN для node classification"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Automated Test Suite for All Modules (Priority: P1)

As a developer, I want a comprehensive pytest test suite that validates graph construction, filtering, analysis, and synthetic data generation using synthetic data, so I can make changes with confidence and catch regressions automatically.

**Why this priority**: Without tests, every other improvement (bug fixes, refactoring, GNN) risks breaking existing functionality. Tests are the foundation for all further work.

**Independent Test**: Can be fully tested by running `cd src && pytest` and verifying all tests pass. Delivers immediate value by catching existing bugs and establishing a safety net.

**Acceptance Scenarios**:

1. **Given** the synthetic data generator, **When** `pytest` runs, **Then** tests verify that generated data contains the expected number of nodes, edges, and built-in patterns (shell companies, cycles, shared representatives).
2. **Given** a graph built from synthetic data, **When** graph construction tests run, **Then** tests verify correct node/edge counts, node type mapping, edge metric computation (share_of_turnover, reciprocity), and shared employee derivation.
3. **Given** a raw graph, **When** filter tests run, **Then** tests verify that pre-filter removes low-significance edges, disparity filter reduces edge count while preserving significant edges, and non-transaction edges are never removed.
4. **Given** a filtered graph, **When** analysis tests run, **Then** tests verify Leiden clustering produces at least one cluster, centrality metrics are computed for all nodes, shell detection flags nodes with known shell patterns, cycle detection finds the known A-B-C cycle, and role classification assigns valid roles.
5. **Given** no Hive access, **When** the full test suite runs, **Then** all tests complete using only synthetic data and local computation — no Spark or network dependencies.

---

### User Story 2 - Bug Fixes and Data Integrity (Priority: P1)

As a risk analyst, I want the system to correctly derive node status from actual database fields and to properly bridge analysis results to visualization, so I see accurate information in the graph display.

**Why this priority**: Tied with P1 because existing bugs produce incorrect data (is_liquidated always False, visualization missing metrics). Fixing these is prerequisite for trustworthy analysis.

**Independent Test**: Can be tested by building a graph from synthetic data with known deleted/ended nodes and verifying their status flags are correctly set. Visualization should show pagerank, cluster, role, and shell_score on each node.

**Acceptance Scenarios**:

1. **Given** a node with `deleted_flag='Y'` or `end_date` in the past, **When** the graph is built, **Then** the node's `is_liquidated` flag is derived from these real fields instead of the non-existent `liquidation_flag` column.
2. **Given** computed centrality metrics, cluster assignments, role classifications, and shell scores, **When** the analyst prepares for visualization, **Then** a single function writes all these results back as node attributes on the graph, so visualization reads them directly.
3. **Given** a set of client identifiers in the ETL temp view registration, **When** identifiers are written to Parquet, **Then** they are stored as integers (not floats), preventing type mismatch errors during Spark JOINs.

---

### User Story 3 - One-Command Analysis Pipeline (Priority: P2)

As a risk analyst, I want a single function that runs the entire analysis chain (graph construction, filtering, analysis, enrichment) from Parquet files, so I don't have to manually orchestrate four separate notebooks for routine analysis.

**Why this priority**: Reduces analyst effort from running 4 notebooks to calling one function. Enables batch analysis and is prerequisite for GNN training data preparation.

**Independent Test**: Can be tested by generating synthetic data, calling the pipeline function, and verifying it produces an enriched graph with all metrics, clusters, roles, shell scores, and a cluster summary.

**Acceptance Scenarios**:

1. **Given** Parquet files from ETL output (or synthetic data), **When** the analyst calls the analysis pipeline function, **Then** the system sequentially: loads data, builds graph, computes edge metrics, applies filter pipeline, runs Leiden clustering, computes centrality, classifies roles, detects shells, detects cycles, enriches graph with all results, and builds cluster summary.
2. **Given** the pipeline completes, **When** the analyst inspects the output, **Then** the enriched graph, metrics DataFrame, cluster summary, and cycle list are all returned as a structured result.
3. **Given** optional configuration parameters (filter thresholds, Leiden gammas), **When** the analyst passes them to the pipeline, **Then** they override defaults from config.

---

### User Story 4 - Performance Optimization (Priority: P2)

As a developer, I want the analysis functions to use vectorized operations instead of row-by-row iteration, so the system handles larger graphs efficiently and analysis completes faster.

**Why this priority**: Current row-by-row iteration in role classification, shell detection, and cluster summary becomes a bottleneck as graph size grows. Optimization enables scaling to production datasets.

**Independent Test**: Can be tested by running the analysis on synthetic data and verifying identical results to the pre-optimization version, with measurably shorter execution time on larger datasets.

**Acceptance Scenarios**:

1. **Given** a metrics DataFrame, **When** role classification runs, **Then** roles are assigned using vectorized conditional logic producing identical results to the previous row-by-row approach.
2. **Given** a metrics DataFrame and graph, **When** shell company detection runs, **Then** scores are computed using vectorized operations producing identical results to the previous approach.
3. **Given** a graph with cluster assignments, **When** cluster summary is built, **Then** internal turnover is computed in a single pass over all edges (not per-cluster iteration over all edges).

---

### User Story 5 - GNN-Based Node Classification (Priority: P3)

As a risk analyst, I want a trained Graph Neural Network model that classifies nodes (shell company, role) based on graph structure and node features, so I get more accurate predictions than heuristic rules and can classify new nodes without re-running the full analysis pipeline.

**Why this priority**: GNN leverages both topology and features for classification, potentially outperforming hand-crafted heuristics. However, it depends on all prior stories (tests, bug fixes, pipeline, correct data) for reliable training data.

**Independent Test**: Can be tested by training the GNN on synthetic data labels (from heuristic analysis), then evaluating classification accuracy on a held-out portion. The model should achieve measurably better or comparable accuracy to heuristics.

**Acceptance Scenarios**:

1. **Given** an enriched graph with heuristic labels (role, shell_score), **When** the GNN training module runs, **Then** it converts the NetworkX graph to a suitable format, prepares node feature vectors (centrality, flow metrics, degree, type encoding), and trains a node classification model.
2. **Given** a trained GNN model, **When** inference runs on the same or new graph, **Then** each node receives a predicted role and predicted shell probability, stored as node attributes.
3. **Given** a trained model, **When** the analyst saves and later loads the model, **Then** predictions can be reproduced without retraining.
4. **Given** synthetic data with known shell companies and known roles, **When** the GNN is evaluated, **Then** its accuracy on shell detection is at least comparable to the heuristic approach (shell_score), and role classification achieves at least 70% agreement with heuristic labels.

---

### User Story 6 - Dependency Management and Project Hygiene (Priority: P3)

As a developer, I want a requirements file listing all project dependencies with pinned versions, so the environment can be reliably reproduced on any machine or MDP instance.

**Why this priority**: Low effort, high value for reproducibility. No dependency on other stories.

**Independent Test**: Can be tested by creating a fresh virtual environment, installing from the requirements file, and verifying all imports succeed.

**Acceptance Scenarios**:

1. **Given** the project repository, **When** a developer looks at the root directory, **Then** a requirements file lists all runtime dependencies with version constraints.
2. **Given** the requirements file, **When** dependencies are installed in a fresh environment, **Then** all project modules import successfully.

---

### Edge Cases

- What happens when synthetic data generates zero transaction edges? Tests should handle empty DataFrames gracefully and analysis functions should return sensible defaults (empty results, not crashes).
- What happens when the GNN receives a graph with only one node type? The model should still train and produce predictions (degenerate case).
- What happens when all nodes are assigned to a single Leiden cluster? The pipeline should not crash; cluster summary should report one cluster.
- What happens when the pipeline is called with filter parameters so strict that all edges are removed? The system should warn the user and return the empty filtered graph without crashing downstream analysis.
- What happens when the GNN model file is corrupted or incompatible? Loading should raise a clear error message, not a cryptic stack trace.

## Requirements *(mandatory)*

### Functional Requirements

**Testing**
- **FR-001**: System MUST include a pytest test suite covering graph_builder, filters, analysis, and synthetic modules.
- **FR-002**: Tests MUST run without Spark, Hive, or network access — using only synthetic data.
- **FR-003**: Tests MUST verify that known synthetic patterns (shell companies, cycles, shared representatives) are correctly detected.

**Bug Fixes**
- **FR-004**: System MUST derive node liquidation/closure status from `deleted_flag` and `end_date` fields (not the non-existent `liquidation_flag`).
- **FR-005**: System MUST provide a function to enrich graph nodes with analysis results (centrality, cluster, role, shell_score) from DataFrames, bridging the gap between analysis output and visualization input.
- **FR-006**: System MUST store client identifiers as integers (not floats) when writing temp Parquet files for Spark JOINs.

**Pipeline**
- **FR-007**: System MUST provide an analysis pipeline function that chains: data loading, graph construction, edge metrics, filtering, clustering, centrality, role classification, shell detection, cycle detection, graph enrichment, and cluster summary.
- **FR-008**: Pipeline function MUST accept optional override parameters for all configurable thresholds.
- **FR-009**: Pipeline function MUST return a structured result containing the enriched graph, metrics DataFrame, cluster summary, and detected cycles.

**Optimization**
- **FR-010**: Role classification MUST use vectorized conditional logic instead of row-by-row iteration.
- **FR-011**: Shell company scoring MUST use vectorized operations instead of row-by-row iteration.
- **FR-012**: Cluster summary internal turnover computation MUST use a single pass over edges with grouping, not per-cluster iteration.
- **FR-013**: `import os` in config module MUST be placed at the top of the file following standard conventions.

**GNN**
- **FR-014**: System MUST include a GNN training module that accepts an enriched NetworkX graph and produces a trained node classification model.
- **FR-015**: GNN MUST use node features derived from: centrality metrics, flow metrics, degree statistics, and node type encoding.
- **FR-016**: GNN MUST support two classification tasks: shell company detection (binary) and role classification (multi-class).
- **FR-017**: System MUST support saving and loading trained GNN models for reuse without retraining.
- **FR-018**: System MUST include a GNN inference function that predicts labels for nodes in new or updated graphs.

**Dependencies**
- **FR-019**: Project MUST include a requirements file listing all runtime dependencies with version constraints.

### Key Entities

- **Node Feature Vector**: A numeric representation of a graph node combining centrality metrics (PageRank, betweenness, clustering coefficient), flow metrics (in/out flow, flow-through ratio), degree statistics (in/out degree), and encoded node type. Used as input to the GNN.
- **GNN Model**: A trained graph neural network that maps node feature vectors and graph topology to predicted labels (shell probability, role category). Can be saved, loaded, and applied to new graphs.
- **Analysis Pipeline Result**: A structured output containing: enriched graph (with all metrics as node/edge attributes), metrics DataFrame, cluster summary DataFrame, and list of detected cycles.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Full test suite passes with `pytest` — all modules have at least one test per public function, covering both normal and edge case inputs.
- **SC-002**: Node liquidation status matches the ground truth derived from `deleted_flag` and `end_date` for 100% of nodes in synthetic test data.
- **SC-003**: Visualization displays PageRank, cluster, role, and shell score for every node after running the analysis pipeline — no missing or default values for analyzed nodes.
- **SC-004**: Analysis pipeline produces identical results to running the four notebooks manually (same cluster assignments, same shell flags, same cycle list).
- **SC-005**: Vectorized role classification and shell detection produce identical results to the row-by-row versions on synthetic data.
- **SC-006**: GNN shell detection achieves at least 70% agreement with heuristic shell flags on synthetic test data.
- **SC-007**: GNN role classification achieves at least 60% agreement with heuristic role labels on synthetic test data.
- **SC-008**: A fresh environment can install all dependencies from the requirements file and successfully run the test suite.

## Assumptions

- The MDP JupyterLab environment can install PyTorch and PyTorch Geometric (or DGL) for GNN training. If GPU is unavailable, CPU training is acceptable for MVP-scale graphs (up to ~10K nodes).
- Heuristic labels from the existing analysis (shell_score, role) serve as training labels for the GNN. These are "silver standard" labels — not manually verified ground truth.
- The GNN module is an addition alongside existing heuristic analysis, not a replacement. Analysts can compare both approaches.
- Python 3.8+ compatibility is maintained for all new code, consistent with MDP platform constraints.
- The `itertuples()` or `to_dict('records')` pattern is acceptable for node/edge addition to NetworkX (no vectorized alternative exists for graph mutation).
