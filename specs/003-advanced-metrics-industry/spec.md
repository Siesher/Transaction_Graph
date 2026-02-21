# Feature Specification: Advanced Graph Metrics, Industry Analytics & Notebook Documentation

**Feature Branch**: `003-advanced-metrics-industry`
**Created**: 2026-02-20
**Status**: Draft
**Input**: User description: "Two-wave feature: Wave 1 — advanced node/edge metrics + hub management on current graph; Wave 2 — ETL extension for OKVED/region, industry ecosystem maps, behavioral segmentation, look-alike scoring. Plus interactive notebook documentation with interpretation guidance for business users."

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Extended Node & Edge Metrics (Priority: P1)

As a graph analyst, I want enriched node metrics (unique counterparty count, top-K concentration, active months, hub flag) and a combined edge_score on every edge so that I can identify the most important business relationships and key network hubs without manual inspection.

**Why this priority**: These metrics are the foundation for all downstream analytics — hub identification, ecosystem detection, filtering improvements, and industry analysis all depend on richer node/edge attributes. Works on current data with zero ETL changes.

**Independent Test**: Run the pipeline on synthetic data, verify every node has the new metrics and every transaction edge has an `edge_score`. Verify top-N hub filtering reduces edges for high-degree nodes while preserving connectivity.

**Acceptance Scenarios**:

1. **Given** an enriched graph, **When** I inspect any node, **Then** it has attributes: `unique_counterparty_count`, `top_k_concentration`, `active_months`, `hub_flag` with correct values.
2. **Given** an enriched graph, **When** I inspect any transaction edge, **Then** it has an `edge_score` combining base weight, bilateral importance, node importance, and stability.
3. **Given** a hub node (counterparty count >> median), **When** hub filtering is applied, **Then** only its top-N strongest edges by `edge_score` are retained, preventing super-cluster formation.
4. **Given** a cluster summary, **When** I view the results, **Then** each cluster shows `external_counterparty_count` alongside existing metrics.

---

### User Story 2 — OKVED & Region ETL Extension (Priority: P2)

As a data engineer, I want the ETL pipeline to extract `okved_code` and `region_code` from the Hive client table and store them as node attributes, so that industry-level and regional analyses become possible.

**Why this priority**: All Wave 2 analytics (industry maps, cross-industry hubs, behavioral segmentation) require OKVED and region data on nodes. This is a blocking prerequisite.

**Independent Test**: Run ETL extraction for a seed company, verify `okved_code` and `region_code` appear in nodes.parquet and as graph node attributes. Verify synthetic data generator produces these fields.

**Acceptance Scenarios**:

1. **Given** a Hive client_sdim table with OKVED/region columns, **When** ETL runs, **Then** nodes.parquet includes `okved_code` and `region_code` for every node.
2. **Given** synthetic data, **When** generated, **Then** nodes include realistic `okved_code` (from a predefined set of common codes) and `region_code` values.
3. **Given** a graph built from enriched parquet files, **When** I inspect any node, **Then** `okved_code` and `region_code` are present as node attributes.

---

### User Story 3 — Industry Ecosystem Map (Priority: P2)

As a relationship manager, I want to see a visual map showing which industries (OKVED codes) transact most with each other through the bank, so that I can understand cross-industry flows and identify partnership opportunities.

**Why this priority**: Directly addresses the "industry ecosystem map" hypothesis — high business value for strategic planning and client advisory.

**Independent Test**: Build an OKVED×OKVED turnover matrix from synthetic data with diverse OKVED codes. Verify the matrix is non-negative and visualizable as a heatmap.

**Acceptance Scenarios**:

1. **Given** a graph with OKVED-attributed nodes, **When** I build the industry matrix, **Then** I get a table with OKVED-source rows, OKVED-target columns, and turnover values.
2. **Given** the industry matrix, **When** I visualize it, **Then** a heatmap clearly shows which industry pairs have the strongest transaction flows.
3. **Given** a node, **When** I compute its OKVED diversity, **Then** I get a count of distinct OKVED codes among its counterparties and an entropy-based diversity score.

---

### User Story 4 — Behavioral Segmentation & Look-Alike Scoring (Priority: P3)

As a sales manager, I want clients and prospects clustered by their payment behavior patterns (frequency, volume, direction, counterparty diversity) and I want new prospects scored by similarity to our best converted clients, so that I can prioritize outreach.

**Why this priority**: Combines behavioral clustering with look-alike scoring for actionable sales intelligence. Depends on US1 (metrics) and US2 (OKVED data).

**Independent Test**: Generate behavioral features for all nodes, run clustering, verify each node gets a behavioral segment label. Compute look-alike scores for a subset of nodes against a "best client" profile.

**Acceptance Scenarios**:

1. **Given** monthly transaction aggregates, **When** behavioral features are computed, **Then** each node has: `monthly_tx_count_avg`, `monthly_amount_avg`, `direction_ratio`, `counterparty_growth_rate`, `new_counterparty_share`.
2. **Given** behavioral features, **When** clustering runs, **Then** each node is assigned a `behavioral_segment` label (integer cluster ID).
3. **Given** a profile of "best converted clients" (top decile by total turnover), **When** look-alike scoring runs on prospects, **Then** each prospect gets a `lookalike_score` (0–1) based on similarity to the best-client centroid.
4. **Given** behavioral segments, **When** intersected with OKVED codes, **Then** a cross-tabulation shows how behavioral segments distribute across industries.

---

### User Story 5 — Interactive Notebook Documentation (Priority: P2)

As a business user (relationship manager), I want Jupyter notebooks with clear markdown explanations at each analysis stage, describing what each metric means, how to interpret results, and what actions to take, so that I can use the analysis tool without deep technical knowledge.

**Why this priority**: Documentation is critical for adoption. The best analytics are useless if business users can't interpret or trust them. This story runs in parallel with US1–US4.

**Independent Test**: Open each notebook in Jupyter, verify every code cell has a preceding markdown cell explaining what it does and how to interpret its output. Verify an "Interpretation Guide" section exists at the end.

**Acceptance Scenarios**:

1. **Given** the graph construction notebook, **When** I open it, **Then** every major code cell has a preceding markdown explanation including: what the step does, what the output means, and typical values/ranges.
2. **Given** the analysis notebook, **When** I view the centrality/clustering sections, **Then** each section has an "Interpretation" block explaining what high/low values mean in business terms (e.g., "High betweenness = potential gatekeeper or transit company").
3. **Given** a new notebook for industry analytics, **When** I open it, **Then** it includes visual examples (heatmap, bar charts) with annotations explaining what business users should look for.
4. **Given** the visualization notebook, **When** I run it, **Then** the output includes a legend and a text summary of key findings (top hubs, largest clusters, flagged shells).

---

### Edge Cases

- What happens when a node has no OKVED code in the database? Default to "Unknown" with code "00.00"; exclude from industry matrix aggregation but keep in graph.
- What happens when behavioral features have zero variance (e.g., all nodes have identical monthly amounts)? Skip that feature in clustering or log a warning; clustering should handle uniform dimensions gracefully.
- How does hub filtering handle a node that is a hub AND part of a known cycle? Cycle edges are always preserved regardless of hub cap.
- What if the top-K concentration denominator is zero (node with no outgoing edges)? Return `top_k_concentration = 0.0`.
- What if the look-alike target group (best clients) is empty? Return empty results with a warning; do not crash.
- What if the graph has only one OKVED code across all nodes? The OKVED matrix collapses to a 1×1 cell; log a warning that industry diversity is insufficient.

## Requirements *(mandatory)*

### Functional Requirements

**Wave 1 — Extended Metrics (on current data)**

- **FR-001**: System MUST compute `unique_counterparty_count` for every node — count of distinct nodes connected by transaction edges (both in and out).
- **FR-002**: System MUST compute `top_k_concentration` for every node — share of total turnover going to the top-5 counterparties (0.0–1.0).
- **FR-003**: System MUST compute `active_months` for every node — number of distinct calendar months with at least one transaction.
- **FR-004**: System MUST flag `hub_flag = True` for nodes whose `unique_counterparty_count` exceeds twice the median across all nodes.
- **FR-005**: System MUST compute `edge_score` for every transaction edge combining: normalized total_amount, bilateral share_of_turnover (product of source's and target's shares), average node importance of both endpoints, and stability factor (active_months / max_months).
- **FR-006**: System MUST provide a hub-aware filtering function that limits high-degree nodes (hubs) to their top-N edges by `edge_score`, where N is configurable (default: 20).
- **FR-007**: System MUST extend `build_cluster_summary()` to include `external_counterparty_count` — number of distinct nodes outside the cluster that have transaction edges with cluster members.

**Wave 2 — Industry & Behavioral Analytics (new data)**

- **FR-008**: ETL MUST extract `okved_code` and `region_code` from the Hive client dimension table and include them in nodes.parquet.
- **FR-009**: Synthetic data generator MUST produce realistic `okved_code` (from a set of 15–20 common Russian OKVED codes) and `region_code` (from a set of 10 common region codes) for every generated node.
- **FR-010**: System MUST build an OKVED×OKVED turnover matrix showing total transaction volume between each pair of industry codes.
- **FR-011**: System MUST compute OKVED diversity per node — count of distinct OKVED codes among counterparties, plus Shannon entropy across those codes.
- **FR-012**: System MUST identify cross-industry hubs — nodes with OKVED diversity in the top 10% across the graph.
- **FR-013**: System MUST compute behavioral features per node: average monthly transaction count, average monthly amount, direction ratio (outflow / total flow), counterparty growth rate (change over available periods), new counterparty share (fraction of counterparties first seen in the latest period).
- **FR-014**: System MUST perform clustering on behavioral features to assign a `behavioral_segment` label to every node, with the number of clusters auto-selected via silhouette score or configurable by the user.
- **FR-015**: System MUST compute a look-alike score for each node based on cosine similarity between its feature vector and the centroid of a designated "best clients" group (top decile by total turnover).

**Notebooks — Documentation & Interpretation**

- **FR-016**: Each existing notebook (01–04) MUST be updated with markdown cells before every major code section explaining: purpose, expected output, and interpretation guidance.
- **FR-017**: A new notebook for industry analysis MUST be created covering: OKVED×OKVED matrix, industry heatmap, cross-industry hubs, and interpretation guidance.
- **FR-018**: A new notebook for behavioral segmentation MUST be created covering: behavioral feature computation, clustering, look-alike scoring, and interpretation of segments.
- **FR-019**: Every notebook MUST include an "Interpretation Guide" section at the end with a glossary of metrics and business-oriented explanations of what high/low values mean.
- **FR-020**: Visualization outputs in notebooks MUST include legends, color scales with labels, and a textual summary of key findings.

### Key Entities

- **Node Metrics (extended)**: unique_counterparty_count, top_k_concentration, active_months, hub_flag — new attributes added to each graph node.
- **Edge Score**: Combined metric on transaction edges incorporating base weight, bilateral importance, node importance, and stability factor.
- **OKVED×OKVED Matrix**: Table with OKVED-source rows, OKVED-target columns, and aggregated turnover values as cells.
- **Behavioral Feature Vector**: Per-node feature set for clustering — monthly transaction count average, monthly amount average, direction ratio, counterparty growth rate, new counterparty share.
- **Look-Alike Score**: Similarity measure (0–1) between a node's behavioral feature vector and a target centroid.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Every node in the graph has all 4 extended metrics (unique_counterparty_count, top_k_concentration, active_months, hub_flag) with values verifiable against manual calculation on synthetic data.
- **SC-002**: Every transaction edge has an `edge_score` > 0 that correctly ranks edges by their combined importance (top-scored edges correspond to highest-volume, most-stable connections between important nodes).
- **SC-003**: Hub filtering reduces the edge count of any hub node to at most N edges while preserving graph connectivity (no isolated components created by filtering alone).
- **SC-004**: OKVED×OKVED matrix covers all OKVED pairs present in the graph and the heatmap visualization renders without errors on a dataset with 10+ distinct OKVED codes.
- **SC-005**: Behavioral segmentation assigns every node to exactly one segment, with 3–10 clusters identified (auto-selected or configurable).
- **SC-006**: Look-alike scores for known "best clients" are in the top quartile of all scores (the scoring model correctly identifies similar nodes).
- **SC-007**: Every notebook cell with computation has a preceding markdown cell; notebooks are self-explanatory for non-technical users.
- **SC-008**: Full test suite (existing + new tests) passes with 0 failures after all changes.

## Assumptions

- `client_sdim` in Hive contains OKVED and region fields (not yet mapped in schema.py — will need field name verification via DESCRIBE TABLE on MDP, similar to how existing fields were verified).
- Model segment data is not available in the current Hive schema; behavioral segmentation is computed purely from transaction patterns and OKVED/region features. If model segment becomes available later, it can be joined.
- "Best clients" for look-alike scoring are defined by total turnover in the top decile. An alternative definition can be substituted when additional data becomes available.
- Revenue data per company is not available from the current Hive tables. We use total transaction turnover as a proxy.
- Growth rate metrics (FR-013) require at least two distinct time periods in the data. On single-period data, growth rate defaults to 0.0.
