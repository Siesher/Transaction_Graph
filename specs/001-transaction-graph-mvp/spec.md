# Feature Specification: Transaction Graph MVP

**Feature Branch**: `001-transaction-graph-mvp`
**Created**: 2026-02-17
**Status**: Draft
**Input**: User description: "MVP system for building a corporate relationship graph from banking transaction data in Hadoop Hive, running on JupyterLab (MDP platform)"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Extract Company Neighborhood from Banking Data (Priority: P1)

As a risk analyst, I want to specify a seed company (by name or identifier) and extract all its direct and indirect counterparties from the bank's data warehouse, so I can see who the company transacts with, who represents it, and where its employees also work.

**Why this priority**: Without data extraction, no other analysis is possible. This is the foundational data pipeline that feeds all downstream features.

**Independent Test**: Can be fully tested by running the extraction notebook with a known company identifier and verifying that output files contain nodes (clients) and edges (transactions, authorities, salary links) in the expected format.

**Acceptance Scenarios**:

1. **Given** access to the Hive database `s_dmrb` and a valid seed company identifier, **When** the analyst runs the extraction notebook with that identifier, **Then** the system produces Parquet files containing: (a) all clients within N hops of the seed company, (b) aggregated transaction edges between those clients, (c) authority/representative edges, and (d) salary project edges.
2. **Given** a seed company with known counterparties, **When** extraction completes for N=2 hops, **Then** all direct counterparties (1-hop) and their counterparties (2-hop) appear in the output.
3. **Given** tables with partition count exceeding the 3000 limit (e.g., transaction_stran), **When** the system attempts extraction, **Then** it applies date-range filters to stay within partition limits and logs a warning about the constraint.

---

### User Story 2 - Build and Filter the Relationship Graph (Priority: P2)

As a risk analyst, I want the extracted data to be assembled into a weighted, directed graph where noise is filtered out, so I only see structurally significant relationships rather than thousands of one-off transactions.

**Why this priority**: Raw transaction data contains enormous noise. Without filtering, the graph is unusable for analysis. The disparity filter transforms raw data into actionable intelligence.

**Independent Test**: Can be tested by loading sample Parquet files (from Story 1 or synthetic data), building the graph, and verifying that the disparity filter reduces edge count while preserving known significant relationships.

**Acceptance Scenarios**:

1. **Given** extracted Parquet files with nodes and edges, **When** the analyst runs the graph construction notebook, **Then** a heterogeneous directed graph is created with distinct node types (company, individual, sole proprietor) and edge types (transaction, authority, salary, shared employees).
2. **Given** a raw graph with transaction edges, **When** the disparity filter is applied with alpha=0.05, **Then** edges that are statistically insignificant for both endpoints are removed, and the remaining "backbone" retains edges where a counterparty represents a disproportionately large share of a node's activity.
3. **Given** transaction edges with fewer than 3 occurrences or below a minimum amount threshold, **When** pre-filtering runs, **Then** those edges are removed before the disparity filter is applied.

---

### User Story 3 - Analyze Corporate Structure and Detect Patterns (Priority: P3)

As a risk analyst, I want the system to automatically identify corporate groups (clusters of affiliated companies), rank companies by importance, and flag suspicious patterns (shell companies, circular payments), so I can focus my review on the most relevant findings.

**Why this priority**: This transforms the graph from a data structure into actionable insights. Clustering reveals hidden corporate groups; centrality metrics identify key players; pattern detection flags anomalies.

**Independent Test**: Can be tested by running analysis on a pre-built graph (from Story 2 or synthetic) and verifying that clusters are identified, centrality scores are computed, and known suspicious patterns (if present) are flagged.

**Acceptance Scenarios**:

1. **Given** a filtered graph, **When** community detection runs, **Then** the system assigns each node to a cluster, and clusters correspond to groups of companies with disproportionately high internal transaction volumes.
2. **Given** a filtered graph, **When** centrality analysis runs, **Then** each node receives PageRank and betweenness centrality scores, and nodes are classified into role profiles (parent company, shell, operating subsidiary, financial conduit).
3. **Given** a graph containing a circular payment pattern (A pays B pays C pays A within a short time window), **When** cycle detection runs, **Then** the cycle is identified and flagged with the involved nodes and total cycle amount.
4. **Given** a node where incoming flow approximately equals outgoing flow, with no salary payments and minimal operating expenses, **When** shell company detection runs, **Then** the node is flagged as a potential shell company.

---

### User Story 4 - Visualize the Relationship Graph Interactively (Priority: P4)

As a risk analyst, I want to see an interactive visual representation of the company's relationship graph, color-coded by entity type and cluster, with the ability to explore connections by clicking on nodes, so I can present findings to management and compliance teams.

**Why this priority**: Visualization makes the analysis accessible to non-technical stakeholders and is critical for the MVP demonstration. However, it depends on Stories 1-3 for meaningful data.

**Independent Test**: Can be tested by loading a pre-analyzed graph and generating an interactive HTML visualization within JupyterLab, then verifying that nodes are color-coded, edges show transaction volumes, and clusters are visually distinguishable.

**Acceptance Scenarios**:

1. **Given** an analyzed graph with clusters and centrality scores, **When** the analyst runs the visualization notebook, **Then** an interactive graph is rendered in JupyterLab where nodes are colored by type (company, individual, sole proprietor) and sized by importance (PageRank).
2. **Given** the interactive visualization, **When** the analyst hovers over a node, **Then** a tooltip displays the entity name, type, cluster, PageRank score, betweenness score, and role classification.
3. **Given** the interactive visualization, **When** the analyst hovers over an edge, **Then** a tooltip displays the relationship type, transaction volume, transaction count, and share of the sender's total turnover.
4. **Given** the visualization, **When** the analyst views the companion summary table, **Then** each cluster is listed with its member count, total internal turnover, lead company (highest PageRank), and any flagged anomalies.

---

### Edge Cases

- What happens when the seed company has no transactions in the selected time period? The system should report that no data was found and suggest expanding the time window.
- What happens when the N-hop expansion produces more than 100,000 nodes? The system should warn the analyst and offer to limit expansion to fewer hops or apply stricter filters.
- What happens when a Hive table is inaccessible due to partition limits? The system should automatically apply date-range partitioning and log the constraint.
- What happens when two companies have transactions in only one direction? The reciprocity metric should be 0, and the edge should still be retained if it passes the disparity filter.
- What happens when a company appears in salary data but not in transaction data? The node should still be included with available attributes and edges.
- What happens when the same physical person represents multiple companies? The system should create authority edges from each company to that person, enabling discovery of shared-representative relationships.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST allow the analyst to specify a seed company by identifier (client_uk) or by INN (taxpayer code) and extract its N-hop neighborhood from the bank's Hive data warehouse.
- **FR-002**: System MUST extract transaction relationships from `paymentcounteragent_stran`, aggregating by counterparty pair and time period (quarter), computing total volume, transaction count, average amount, and standard deviation.
- **FR-003**: System MUST extract authority/representative relationships by joining `clientauthority_shist` with `clientauthority2clientrb_shist` to identify which individuals represent which companies.
- **FR-004**: System MUST extract salary project relationships by joining `clnt2dealsalary_shist` with `dealsalary_sdim` to identify employer-employee links, and derive shared-employee edges between companies with overlapping workforces.
- **FR-005**: System MUST construct a heterogeneous directed graph with three node types (company, individual, sole proprietor) and four edge types (transaction, authority, salary, shared employees).
- **FR-006**: System MUST compute edge-level metrics for transaction edges: total volume, frequency, share of sender's turnover, reciprocity ratio, and regularity (coefficient of variation of inter-transaction intervals).
- **FR-007**: System MUST implement the Serrano disparity filter to extract the statistically significant backbone of the transaction network, with a configurable significance level (default alpha=0.05).
- **FR-008**: System MUST pre-filter transaction edges below configurable thresholds for minimum transaction count (default: 3) and minimum total amount.
- **FR-009**: System MUST detect communities using the Leiden algorithm with the Constant Potts Model (CPM) resolution function, supporting multiple resolution parameters for exploration.
- **FR-010**: System MUST compute weighted PageRank and betweenness centrality for all nodes, and classify nodes into role profiles (parent company, shell, operating subsidiary, financial conduit) based on the centrality signature table.
- **FR-011**: System MUST detect circular payment patterns (cycles of length 3-5) in the transaction graph.
- **FR-012**: System MUST detect potential shell companies based on: flow-through ratio close to 1.0, absence of salary payments, high betweenness centrality with low clustering coefficient.
- **FR-013**: System MUST generate an interactive graph visualization within JupyterLab, with nodes colored by type, sized by PageRank, and grouped by cluster.
- **FR-014**: System MUST produce a summary table listing all detected clusters with member counts, total turnover, lead company, and flagged anomalies.
- **FR-015**: System MUST handle Hive partition limit errors (>3000 partitions) by automatically applying date-range filters and logging warnings.
- **FR-016**: System MUST save all intermediate results (extracted nodes, edges, graph metrics, cluster assignments) as Parquet files for reproducibility.

### Key Entities

- **Client (Node)**: A bank client — company, individual, or sole proprietor. Key attributes: identifier, name, type, INN/taxpayer code, status, liquidation flag. Source: `client_sdim` joined with `clienttype_ldim` and `clientstatus_ldim`.
- **Account**: A bank account owned by a client. Key attributes: account number, owning client, currency, payroll company reference. Source: `account_sdim`. Acts as an intermediate entity linking clients to transactions.
- **Transaction Edge**: An aggregated financial flow between two clients over a time period. Key attributes: total volume, count, average amount, standard deviation, share of turnover, reciprocity, regularity. Derived from: `paymentcounteragent_stran`.
- **Authority Edge**: A representation/power-of-attorney relationship between an individual and a company. Source: `clientauthority_shist` joined with `clientauthority2clientrb_shist`.
- **Salary Edge**: An employer-employee relationship through a salary project. Source: `clnt2dealsalary_shist` joined with `dealsalary_sdim`.
- **Shared Employees Edge**: A derived relationship between two companies that share employees through overlapping salary projects.
- **Cluster**: A group of nodes identified by community detection as having disproportionately high internal connectivity. Attributes: member list, total internal turnover, lead company, anomaly flags.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Analyst can go from a seed company identifier to a fully visualized relationship graph within a single JupyterLab session (4 notebooks run sequentially).
- **SC-002**: The disparity filter reduces the number of transaction edges by at least 70% while preserving edges that represent more than 10% of any node's turnover.
- **SC-003**: Community detection identifies at least one multi-company cluster for a seed company known to be part of a corporate group.
- **SC-004**: Shell company detection flags nodes with flow-through ratio above 0.9 and absence of salary payments with 80% precision when validated against known cases.
- **SC-005**: The interactive visualization renders graphs of up to 500 nodes and 2000 edges within JupyterLab without freezing or becoming unusable.
- **SC-006**: All intermediate data outputs are saved as Parquet files and can be reloaded to resume analysis from any step without re-running previous steps.
- **SC-007**: The system handles Hive partition limit errors gracefully, completing extraction with date-filtered queries rather than failing.

## Assumptions

- The analyst has access to JupyterLab on the Hadoop cluster (MDP platform) with PySpark available.
- The analyst has read access to all referenced tables in `s_dmrb`.
- Column naming follows the bank's data warehouse conventions observed in DDL exports (e.g., `client_uk` for client identifier, `client_taxpayer_ccode` for INN).
- The `paymentcounteragent_stran` table contains sufficient information to identify payer and counterparty client identifiers and transaction amounts.
- Tables partitioned by `date_part` can be queried with date-range filters to stay within the 3000-partition Hive limit.
- For the MVP, the seed-based extraction approach (starting from one company, expanding N hops) produces a manageable subgraph (under 100K nodes) suitable for in-memory analysis.
- Python packages (networkx, igraph, leidenalg, pyvis, pandas) can be installed or are available on the MDP JupyterLab environment.
