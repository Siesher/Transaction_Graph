# Contract: src/graph_builder.py

Module for constructing a NetworkX heterogeneous directed graph from extracted DataFrames.

## Functions

### `build_graph`
```python
def build_graph(
    nodes_df: pd.DataFrame,
    transaction_edges_df: pd.DataFrame,
    authority_edges_df: pd.DataFrame,
    salary_edges_df: pd.DataFrame
) -> nx.DiGraph:
    """
    Build heterogeneous directed graph from extracted data.

    Node attributes: client_uk, name, node_type, inn, status, is_liquidated, hop_distance.
    Edge attributes: edge_type, weight, + type-specific attributes.

    Returns NetworkX DiGraph with all nodes and edges.
    """
```

### `compute_edge_metrics`
```python
def compute_edge_metrics(G: nx.DiGraph) -> nx.DiGraph:
    """
    Compute derived metrics for transaction edges:
    - share_of_turnover: edge weight / total outgoing weight of source
    - reciprocity: min(forward, reverse) / max(forward, reverse)
    - weight: log(1 + total_amount) for algorithm use

    Modifies graph in-place and returns it.
    """
```

### `derive_shared_employees`
```python
def derive_shared_employees(
    salary_edges_df: pd.DataFrame,
    min_shared: int = 1
) -> pd.DataFrame:
    """
    Derive shared-employee edges between companies.
    For each pair of companies, count employees appearing in both.

    Returns DataFrame: company_a_uk, company_b_uk, shared_count, shared_employees.
    """
```

### `get_graph_stats`
```python
def get_graph_stats(G: nx.DiGraph) -> dict:
    """
    Return basic graph statistics:
    - node_count, edge_count, by node_type, by edge_type
    - connected_components count
    - density
    """
```
