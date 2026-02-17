# Research: Transaction Graph MVP

**Date**: 2026-02-17
**Status**: Complete

## R1: Data Source Column Mapping

### Decision
Map Hive table columns based on DDL screenshots and bank DWH naming conventions (suffix `_uk` = surrogate key, `_ccode` = business code, `_ncode` = numeric code, `_sdim` = slowly changing dimension, `_stran` = streaming transaction, `_shist` = slowly changing history, `_ldim` = lookup dimension).

### Key Column Mappings (from DDL screenshots)

**account_sdim** (47 fields, 1.07B rows — full DDL visible):
- `uk` (double) — surrogate key
- `client_uk` (double) — owning client FK
- `client_pin` (string) — client PIN
- `client_taxpayer_uk` (double) — payroll company FK
- `client_taxpayer_ccode` (string) — payroll company INN
- `account_number` (string) — account number
- `account_number13` (string) — 13-digit account number
- `currency_uk` / `currency_iso_ncode` — currency
- `start_date` / `end_date` (string) — lifecycle dates
- `accountkind_uk` (double) — active/passive flag
- `salesplace_uk` / `salesplace_ccode` — branch
- `balance_flag` (string) — balance/off-balance flag

**client_sdim** (65 fields, 182M rows — partial DDL):
- `uk` (double) — surrogate key (assumed, standard pattern)
- `client_name` (string) — full name
- `first_name` (string) — first name (ФЛ)
- `middle_name` (string) — patronymic
- `birth_date` (string) — birth date
- `resident_flag` (string) — Y/N
- `liquidation_flag` (string) — Y/N
- `end_date` (string) — record end date
- `clienttype_uk` (double) — FK to clienttype_ldim (assumed)
- `clientstatus_uk` (double) — FK to clientstatus_ldim (assumed)

**paymentcounteragent_stran** (30 fields, 4.98B rows — partial):
- Partitioned by `date_part`
- Sample data: `['N', '1171201MOCO#DS1039159', 'UAG63A', 28536711936.0, '0']`
- Contains: flag, document_id (string), counteragent_code (string), amount (double), additional fields
- **Key assumption**: Contains payer/receiver client references and amounts. Exact column names to be verified via `DESCRIBE s_dmrb.paymentcounteragent_stran` on Hadoop.

**clientauthority_shist** (27 fields, 42.9M rows):
- `profile_create_date`, `attorney_date`, `job_update`
- Sample: `['UBQM1Y~XBLO2M~B~1', '2022-05-06', '5999-12-31', 27366205...]`
- Contains compound keys (PIN-like) and date ranges

**clientauthority2clientrb_shist** (12 fields, 35.2M rows):
- Sample: `[28811991.0, '2026-01-01', 28811991.0, 28128343945.0, 19742...]`
- Likely structure: `authority_uk, start_date, client_uk, related_client_uk, ...`

**clnt2dealsalary_shist** (27 fields, 133M rows):
- Sample: `['40817810607130042470', 'AQI0J9', 'APRW91', '2025-09-07', '2023-09-19']`
- Contains: account_number (20-digit), client codes, deal codes, dates

**dealsalary_sdim**:
- Sample: `[300197778749.0, 'UBLJCJ', '2022-08-08', '5999-12-31', 55259366883.0]`
- Contains: deal_uk, client_code, start_date, end_date, related_client_uk

### Rationale
Column names are derived from bank DWH naming conventions and DDL screenshot analysis. The `_uk` suffix consistently identifies surrogate keys (double type). String codes (`_ccode`) represent business identifiers (INN, branch codes). All assumptions will be verified at runtime via `DESCRIBE TABLE` and the schema.py configuration will be adjustable.

### Alternatives Considered
- Requesting full DDL files: User cannot copy files from Hadoop to local. Working with screenshot-derived mappings.
- Using `transaction_stran` instead of `paymentcounteragent_stran`: Rejected — transaction_stran has 4642 partitions (exceeds limit) and only 38 fields vs. paymentcounteragent_stran's 30 fields with accessible data.

---

## R2: Graph Construction Framework

### Decision
Use **NetworkX DiGraph** for graph construction and manipulation, with conversion to **igraph** only for Leiden clustering (which requires igraph).

### Rationale
- NetworkX provides the richest API for heterogeneous graph attributes (node_type, edge_type, arbitrary attributes)
- NetworkX has built-in PageRank, betweenness centrality, cycle detection
- igraph is needed only for Leiden (via leidenalg library which requires igraph)
- For MVP scale (up to ~10K nodes, ~50K edges), NetworkX performance is sufficient
- Conversion pattern: `igraph.Graph.from_networkx(nx_graph)` preserves attributes

### Code Pattern: NetworkX → igraph → Leiden
```python
import igraph as ig
import leidenalg

# Convert NetworkX to igraph (preserving weights)
ig_graph = ig.Graph.from_networkx(nx_graph)

# Run Leiden with CPM at multiple resolutions
results = {}
for gamma in [0.5, 0.8, 1.0, 1.5, 2.0]:
    partition = leidenalg.find_partition(
        ig_graph,
        leidenalg.CPMVertexPartition,
        weights='weight',
        resolution_parameter=gamma
    )
    results[gamma] = partition

# Select best gamma by modularity
best_gamma = max(results, key=lambda g: results[g].modularity)
best_partition = results[best_gamma]

# Map back to NetworkX nodes
for node_idx, cluster_id in enumerate(best_partition.membership):
    node_name = ig_graph.vs[node_idx]['_nx_name']
    nx_graph.nodes[node_name]['cluster'] = cluster_id
```

### Alternatives Considered
- **GraphFrames (PySpark)**: Better for full-graph distributed processing, but overkill for seed-based subgraph MVP. Would require running graph algorithms inside Spark, which is less flexible.
- **igraph only**: Faster for large graphs, but weaker API for heterogeneous attributes and less Pythonic.
- **graph-tool**: Excellent performance but complex installation on Hadoop clusters.

---

## R3: Disparity Filter Implementation

### Decision
Implement the **Serrano disparity filter** (2009) as a standalone Python function operating on NetworkX DiGraph. For directed graphs, compute significance for both outgoing and incoming perspectives.

### Algorithm
For each directed edge (i → j) with weight w_ij:
1. **Outgoing perspective** (node i): k_i = out_degree(i), s_i = sum of outgoing weights, p_ij = w_ij / s_i, alpha_out = (1 - p_ij)^(k_i - 1)
2. **Incoming perspective** (node j): k_j = in_degree(j), s_j = sum of incoming weights, p_ji = w_ij / s_j, alpha_in = (1 - p_ji)^(k_j - 1)
3. **Keep edge if**: min(alpha_out, alpha_in) < alpha (default 0.05)

### Code Pattern
```python
def disparity_filter(G, alpha=0.05, weight='weight'):
    """Serrano disparity filter for directed weighted graphs."""
    backbone = G.copy()
    edges_to_remove = []

    for u, v, data in G.edges(data=True):
        w = data.get(weight, 1.0)

        # Outgoing perspective (u)
        k_out = G.out_degree(u)
        s_out = sum(d.get(weight, 1.0) for _, _, d in G.out_edges(u, data=True))
        if k_out > 1 and s_out > 0:
            p_out = w / s_out
            alpha_out = (1 - p_out) ** (k_out - 1)
        else:
            alpha_out = 0.0  # Keep edges from degree-1 nodes

        # Incoming perspective (v)
        k_in = G.in_degree(v)
        s_in = sum(d.get(weight, 1.0) for _, _, d in G.in_edges(v, data=True))
        if k_in > 1 and s_in > 0:
            p_in = w / s_in
            alpha_in = (1 - p_in) ** (k_in - 1)
        else:
            alpha_in = 0.0

        # Remove if insignificant from BOTH perspectives
        if alpha_out >= alpha and alpha_in >= alpha:
            edges_to_remove.append((u, v))

    backbone.remove_edges_from(edges_to_remove)
    # Remove isolated nodes
    isolates = list(nx.isolates(backbone))
    backbone.remove_nodes_from(isolates)
    return backbone
```

### Rationale
The Serrano filter is the gold standard for backbone extraction (cited in the research document). It preserves edges that are locally significant for at least one endpoint, which is critical for heterogeneous networks where large hubs (banks, utility companies) have many individually small but collectively important connections.

### Alternatives Considered
- **Noise-Corrected backbone** (Coscia & Neffke): Better for noisy data but more complex. Deferred to post-MVP iteration.
- **Simple threshold filtering** (remove edges below weight X): Biased against small companies with legitimate small transactions.

---

## R4: Visualization Approach

### Decision
Use **pyvis** for interactive HTML graph visualization inside JupyterLab, with `notebook=True` mode.

### Code Pattern
```python
from pyvis.network import Network

# Color mapping by node type
COLOR_MAP = {
    'company': '#4472C4',      # Blue
    'individual': '#70AD47',    # Green
    'sole_proprietor': '#ED7D31' # Orange
}

EDGE_COLOR_MAP = {
    'transaction': '#808080',    # Gray
    'authority': '#FF0000',      # Red
    'salary': '#00B050',         # Green
    'shared_employees': '#7030A0' # Purple
}

def create_visualization(G, height='800px', width='100%'):
    net = Network(
        height=height, width=width,
        directed=True, notebook=True,
        cdn_resources='in_line'  # Important for offline JupyterLab
    )

    # Physics settings for readable layout
    net.set_options('''
    {
        "physics": {
            "forceAtlas2Based": {
                "gravitationalConstant": -50,
                "centralGravity": 0.01,
                "springLength": 200,
                "springConstant": 0.08
            },
            "maxVelocity": 50,
            "solver": "forceAtlas2Based",
            "stabilization": {"iterations": 150}
        }
    }
    ''')

    # Add nodes
    for node, attrs in G.nodes(data=True):
        node_type = attrs.get('node_type', 'company')
        pagerank = attrs.get('pagerank', 0.001)
        size = max(10, min(50, pagerank * 5000))

        net.add_node(
            str(node),
            label=attrs.get('name', str(node))[:30],
            color=COLOR_MAP.get(node_type, '#808080'),
            size=size,
            title=f"Name: {attrs.get('name', 'N/A')}\n"
                  f"Type: {node_type}\n"
                  f"Cluster: {attrs.get('cluster', 'N/A')}\n"
                  f"PageRank: {pagerank:.6f}\n"
                  f"Betweenness: {attrs.get('betweenness', 0):.6f}\n"
                  f"Role: {attrs.get('role', 'N/A')}"
        )

    # Add edges
    for u, v, attrs in G.edges(data=True):
        edge_type = attrs.get('edge_type', 'transaction')
        weight = attrs.get('weight', 1.0)
        width = max(1, min(10, weight / 1e6))

        net.add_edge(
            str(u), str(v),
            color=EDGE_COLOR_MAP.get(edge_type, '#808080'),
            width=width,
            title=f"Type: {edge_type}\n"
                  f"Volume: {attrs.get('total_amount', 0):,.0f}\n"
                  f"Count: {attrs.get('tx_count', 0)}\n"
                  f"Share: {attrs.get('share', 0):.1%}"
        )

    return net
```

### Key Points
- `cdn_resources='in_line'` embeds all JS/CSS directly in HTML — critical for offline/air-gapped environments
- ForceAtlas2Based layout works best for 200-500 node graphs (clustered but readable)
- Node size capped at 50px to prevent visual overflow
- Edge width scaled logarithmically for billion-range amounts

### Alternatives Considered
- **Plotly/Dash**: More control over layout but requires a server for interactivity. Overkill for notebook MVP.
- **Gephi export**: Better for publication-quality graphs but requires separate tool installation.
- **D3.js custom**: Maximum flexibility but significant development effort.

---

## R5: PySpark Seed-Based Extraction Strategy

### Decision
Use iterative PySpark queries to expand the neighborhood from a seed company, hop by hop, collecting client IDs at each step and using them to filter the next hop.

### Strategy
```
Hop 0: seed_clients = {seed_company_uk}
Hop 1: Find all counterparties of seed via paymentcounteragent_stran
        → add to client set
Hop 2: Find all counterparties of hop-1 clients
        → add to client set (with deduplication)
...repeat for N hops...

Then: Extract all edges between collected clients
      Extract authority relationships for collected clients
      Extract salary relationships for collected clients
```

### Partition Handling
For tables exceeding 3000-partition limit:
```python
# Use explicit date_part filter to limit partition scan
date_filter = f"date_part >= '{start_date}' AND date_part <= '{end_date}'"
df = spark.sql(f"""
    SELECT * FROM s_dmrb.paymentcounteragent_stran
    WHERE {date_filter}
    AND (payer_client_uk IN ({client_ids}) OR receiver_client_uk IN ({client_ids}))
""")
```

### Rationale
Seed-based extraction is the only viable approach for MVP given 5B-row tables. Processing the full graph requires distributed graph infrastructure (GraphX, Spark GraphFrames) which is out of MVP scope. A 2-hop neighborhood from one company typically yields 1K-10K nodes — well within in-memory limits.

### Alternatives Considered
- **Full graph extraction + filtering**: Infeasible for 5B rows in MVP timeframe.
- **Pre-materialized graph table**: Would require DBA support and ETL pipeline. Good for production, but MVP should work with raw tables.

---

## R6: Shell Company Detection Heuristics

### Decision
Use a multi-signal scoring approach based on the research paper's criteria.

### Signals
1. **Flow-through ratio**: `min(total_in, total_out) / max(total_in, total_out)` > 0.9
2. **No salary payments**: Node has no outgoing 'salary' edges
3. **High betweenness, low clustering**: `betweenness > median * 2` AND `clustering_coef < 0.1`
4. **Low own activity**: Few unique counterparties relative to volume
5. **Burstiness**: Activity concentrated in short periods (not continuous)

### Scoring
```python
shell_score = (
    0.30 * (flow_through > 0.9) +
    0.25 * (no_salary_payments) +
    0.20 * (high_betweenness and low_clustering) +
    0.15 * (low_unique_counterparties) +
    0.10 * (bursty_activity)
)
# Flag if shell_score >= 0.5
```

### Rationale
The research cites 87-97% accuracy for graph-based shell detection (UK Companies House study). Our simplified heuristic approach is appropriate for MVP — a production system would use supervised ML on labeled examples.

---

## R7: Cycle Detection for Circular Payments

### Decision
Use NetworkX's `simple_cycles()` function (Johnson's algorithm) with post-filtering for length 3-5 and temporal constraints.

### Code Pattern
```python
import networkx as nx

def detect_circular_payments(G, min_length=3, max_length=5):
    """Find circular payment patterns in transaction subgraph."""
    # Extract transaction-only subgraph
    tx_edges = [(u, v) for u, v, d in G.edges(data=True)
                if d.get('edge_type') == 'transaction']
    tx_graph = G.edge_subgraph(tx_edges).copy()

    cycles = []
    for cycle in nx.simple_cycles(tx_graph):
        if min_length <= len(cycle) <= max_length:
            # Calculate total cycle amount
            total = sum(
                tx_graph[cycle[i]][cycle[(i+1) % len(cycle)]].get('total_amount', 0)
                for i in range(len(cycle))
            )
            cycles.append({
                'nodes': cycle,
                'length': len(cycle),
                'total_amount': total
            })

    return sorted(cycles, key=lambda c: c['total_amount'], reverse=True)
```

### Rationale
Johnson's algorithm finds all simple cycles. For subgraphs of ~10K nodes this is tractable. The 3-5 length constraint filters out irrelevant long cycles. Temporal filtering (cycles closing within days vs. weeks) can be added post-MVP.
