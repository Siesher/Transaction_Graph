# Data Model: Transaction Graph MVP

**Date**: 2026-02-17

## Source Tables (Hive — read only)

### Primary Sources

| Table | Rows | Purpose | Partitioned |
|-------|------|---------|-------------|
| `s_dmrb.client_sdim` | 182M | Client master data (nodes) | No |
| `s_dmrb.account_sdim` | 1.07B | Account data (client-account links) | No |
| `s_dmrb.paymentcounteragent_stran` | 4.98B | Transaction counteragent data (edges) | `date_part` |
| `s_dmrb.clientauthority_shist` | 42.9M | Authority/representative history | No |
| `s_dmrb.clientauthority2clientrb_shist` | 35.2M | Authority-to-client relationships | No |
| `s_dmrb.clnt2dealsalary_shist` | 133M | Client-to-salary-deal links | No |
| `s_dmrb.dealsalary_sdim` | ~1M (est.) | Salary deal master data | No |

### Lookup Tables

| Table | Rows | Purpose |
|-------|------|---------|
| `s_dmrb.clienttype_ldim` | 67 | Client type dictionary (ЮЛ, ФЛ, ИП, etc.) |
| `s_dmrb.clientstatus_ldim` | 7 | Client status dictionary |
| `s_dmrb.c2clinkrole_sdim` | 47 | Client-to-client link role dictionary |
| `s_dmrb.sparkstructuresource_sdim` | 5 | SPARK data structure source types |

---

## Intermediate Entities (Parquet output)

### Entity: Node (Client)

Represents a bank client that becomes a graph node.

| Field | Type | Source | Description |
|-------|------|--------|-------------|
| `client_uk` | long | client_sdim.uk | Unique client identifier (surrogate key) |
| `client_name` | string | client_sdim.client_name | Full name (company) or last name (individual) |
| `first_name` | string | client_sdim.first_name | First name (individuals only) |
| `inn` | string | account_sdim.client_taxpayer_ccode | INN (taxpayer code) — via account join |
| `node_type` | string | clienttype_ldim.name | One of: `company`, `individual`, `sole_proprietor` |
| `client_type_uk` | long | client_sdim.clienttype_uk | FK to clienttype_ldim |
| `status` | string | clientstatus_ldim.name | Client status (active, closed, etc.) |
| `is_resident` | boolean | client_sdim.resident_flag | Y/N → true/false |
| `is_liquidated` | boolean | client_sdim.liquidation_flag | Y/N → true/false |
| `birth_date` | string | client_sdim.birth_date | Birth date (individuals) / registration date |
| `hop_distance` | int | computed | Distance from seed company (0 = seed) |

**Validation rules**:
- `client_uk` must be non-null and unique
- `node_type` must be one of the three defined types
- Liquidated companies (`is_liquidated=true`) are included but flagged

**Output**: `data/nodes.parquet`

---

### Entity: TransactionEdge

Aggregated financial flow between two clients over a time period.

| Field | Type | Source | Description |
|-------|------|--------|-------------|
| `source_client_uk` | long | paymentcounteragent_stran | Payer client FK |
| `target_client_uk` | long | paymentcounteragent_stran | Receiver/counteragent client FK |
| `period` | string | derived from date_part | Quarter (e.g., "2025-Q3") |
| `total_amount` | double | SUM(amount) | Total flow in period |
| `tx_count` | int | COUNT(*) | Number of transactions |
| `avg_amount` | double | AVG(amount) | Average transaction amount |
| `std_amount` | double | STDDEV(amount) | Standard deviation |
| `max_amount` | double | MAX(amount) | Largest single transaction |
| `min_amount` | double | MIN(amount) | Smallest single transaction |
| `first_tx_date` | string | MIN(date_part) | First transaction date in period |
| `last_tx_date` | string | MAX(date_part) | Last transaction date in period |
| `edge_type` | string | literal 'transaction' | Edge classification |

**Derived metrics** (computed during graph construction):
- `share_of_turnover`: total_amount / sender's total outgoing amount
- `reciprocity`: min(forward_flow, reverse_flow) / max(forward_flow, reverse_flow)
- `regularity_cv`: coefficient of variation of inter-transaction intervals
- `weight`: log(1 + total_amount) — for graph algorithms

**Validation rules**:
- `source_client_uk != target_client_uk` (no self-loops)
- `total_amount > 0`
- Both source and target must exist in nodes table

**Output**: `data/transaction_edges.parquet`

---

### Entity: AuthorityEdge

Representation/power-of-attorney relationship between a person and a company.

| Field | Type | Source | Description |
|-------|------|--------|-------------|
| `authority_uk` | long | clientauthority_shist | Authority record key |
| `company_client_uk` | long | clientauthority_shist | Company being represented |
| `representative_client_uk` | long | clientauthority2clientrb_shist | Individual representative |
| `authority_type` | string | clientauthoritytype_sdim.name | Type of authority |
| `start_date` | string | clientauthority_shist | Authority start date |
| `end_date` | string | clientauthority_shist | Authority end date |
| `is_active` | boolean | derived | end_date > current_date |
| `edge_type` | string | literal 'authority' | Edge classification |

**Validation rules**:
- `company_client_uk != representative_client_uk`
- Both endpoints must exist in nodes table
- Duplicate authority records (same company + representative) collapsed to single edge

**Output**: `data/authority_edges.parquet`

---

### Entity: SalaryEdge

Employer-employee relationship through a salary project.

| Field | Type | Source | Description |
|-------|------|--------|-------------|
| `deal_uk` | long | dealsalary_sdim | Salary deal key |
| `employer_client_uk` | long | dealsalary_sdim | Company (employer) |
| `employee_client_uk` | long | clnt2dealsalary_shist | Individual (employee) |
| `account_number` | string | clnt2dealsalary_shist | Salary account number |
| `start_date` | string | clnt2dealsalary_shist.start_date | Employment start |
| `end_date` | string | clnt2dealsalary_shist.end_date | Employment end |
| `is_active` | boolean | derived | end_date > current_date |
| `edge_type` | string | literal 'salary' | Edge classification |

**Validation rules**:
- `employer_client_uk != employee_client_uk`
- Employer must be a company node, employee must be an individual node

**Output**: `data/salary_edges.parquet`

---

### Entity: SharedEmployeesEdge

Derived relationship: two companies share employees (via overlapping salary projects).

| Field | Type | Source | Description |
|-------|------|--------|-------------|
| `company_a_uk` | long | derived | First company |
| `company_b_uk` | long | derived | Second company |
| `shared_count` | int | COUNT(DISTINCT employee) | Number of shared employees |
| `shared_employees` | array[long] | employee client_uk list | List of shared employee IDs |
| `edge_type` | string | literal 'shared_employees' | Edge classification |

**Derivation**: For each pair of companies (A, B), count individuals who have salary edges to both A and B within overlapping time periods.

**Validation rules**:
- `company_a_uk < company_b_uk` (canonical ordering, undirected)
- `shared_count >= 1`

**Output**: `data/shared_employees_edges.parquet`

---

### Entity: GraphMetrics

Per-node metrics computed during graph analysis.

| Field | Type | Description |
|-------|------|-------------|
| `client_uk` | long | Node identifier |
| `pagerank` | double | Weighted PageRank score |
| `betweenness` | double | Betweenness centrality |
| `clustering_coef` | double | Local clustering coefficient |
| `in_degree` | int | Incoming edge count |
| `out_degree` | int | Outgoing edge count |
| `total_in_flow` | double | Total incoming transaction volume |
| `total_out_flow` | double | Total outgoing transaction volume |
| `flow_through_ratio` | double | min(in, out) / max(in, out) |
| `has_salary_payments` | boolean | Node has outgoing salary edges |
| `cluster_id` | int | Leiden community assignment |
| `role` | string | Classified role: parent, shell, subsidiary, conduit |
| `shell_score` | double | Shell company suspicion score (0-1) |

**Output**: `data/graph_metrics.parquet`

---

### Entity: Cluster

Community detected by Leiden algorithm.

| Field | Type | Description |
|-------|------|-------------|
| `cluster_id` | int | Cluster identifier |
| `member_count` | int | Number of nodes in cluster |
| `company_count` | int | Number of company nodes |
| `individual_count` | int | Number of individual nodes |
| `total_internal_turnover` | double | Sum of transaction edges within cluster |
| `lead_company_uk` | long | Company with highest PageRank in cluster |
| `lead_company_name` | string | Name of lead company |
| `has_cycles` | boolean | Circular payments detected within cluster |
| `shell_count` | int | Number of flagged shell companies |
| `anomaly_flags` | array[string] | List of anomaly types found |

**Output**: `data/clusters.parquet`

---

## Entity Relationships

```
Client (Node)
  ├──→ TransactionEdge ──→ Client (payer → receiver)
  ├──→ AuthorityEdge ──→ Client (company → representative)
  ├──→ SalaryEdge ──→ Client (employer → employee)
  └──→ SharedEmployeesEdge ←──→ Client (company ↔ company)

Client → GraphMetrics (1:1)
Client → Cluster (many:1 via cluster_id)
```
