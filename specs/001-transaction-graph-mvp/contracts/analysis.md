# Contract: src/analysis.py

Module for graph analytics: clustering, centrality, pattern detection.

## Functions

### `run_leiden_clustering`
```python
def run_leiden_clustering(
    G: nx.DiGraph,
    gamma_values: list[float] = [0.5, 0.8, 1.0, 1.5, 2.0],
    weight_attr: str = 'weight'
) -> tuple[dict[int, int], float]:
    """
    Run Leiden CPM at multiple resolutions, select best by modularity.

    Converts NetworkX → igraph internally.
    Returns (membership_dict, best_gamma) where membership_dict maps
    node_id → cluster_id.
    """
```

### `compute_centrality`
```python
def compute_centrality(G: nx.DiGraph) -> pd.DataFrame:
    """
    Compute centrality metrics for all nodes:
    - pagerank (weighted)
    - betweenness_centrality (weighted)
    - clustering_coefficient
    - in_degree, out_degree
    - total_in_flow, total_out_flow
    - flow_through_ratio

    Returns DataFrame indexed by node_id.
    """
```

### `classify_node_roles`
```python
def classify_node_roles(
    metrics_df: pd.DataFrame,
    G: nx.DiGraph
) -> pd.DataFrame:
    """
    Classify nodes into roles based on centrality profile:
    - 'parent': high pagerank, moderate betweenness, high in-degree
    - 'shell': low pagerank, very high betweenness, low clustering
    - 'subsidiary': moderate pagerank, low betweenness, high degree
    - 'conduit': low pagerank, high betweenness, low degree
    - 'regular': default

    Adds 'role' column to metrics_df. Returns updated DataFrame.
    """
```

### `detect_shell_companies`
```python
def detect_shell_companies(
    metrics_df: pd.DataFrame,
    G: nx.DiGraph,
    threshold: float = 0.5
) -> pd.DataFrame:
    """
    Score nodes on shell company indicators:
    - flow_through_ratio > 0.9 (30%)
    - no salary payments (25%)
    - high betweenness + low clustering (20%)
    - low unique counterparties (15%)
    - bursty activity (10%)

    Adds 'shell_score' column. Returns flagged nodes (score >= threshold).
    """
```

### `detect_cycles`
```python
def detect_cycles(
    G: nx.DiGraph,
    min_length: int = 3,
    max_length: int = 5,
    edge_type: str = 'transaction'
) -> list[dict]:
    """
    Detect circular payment patterns.

    Returns list of dicts: {nodes, length, total_amount}.
    Sorted by total_amount descending.
    """
```

### `build_cluster_summary`
```python
def build_cluster_summary(
    G: nx.DiGraph,
    metrics_df: pd.DataFrame,
    cycles: list[dict]
) -> pd.DataFrame:
    """
    Build summary table for each cluster:
    - cluster_id, member_count, company_count, individual_count
    - total_internal_turnover
    - lead_company_uk, lead_company_name
    - has_cycles, shell_count, anomaly_flags

    Returns DataFrame with one row per cluster.
    """
```
