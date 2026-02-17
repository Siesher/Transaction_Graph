# Contract: src/filters.py

Module for graph noise reduction and backbone extraction.

## Functions

### `pre_filter`
```python
def pre_filter(
    G: nx.DiGraph,
    min_tx_count: int = 3,
    min_total_amount: float = 0.0,
    min_periods: int = 2
) -> nx.DiGraph:
    """
    Remove low-significance transaction edges.

    Criteria (all must fail for removal):
    - tx_count < min_tx_count
    - total_amount < min_total_amount
    - present in fewer than min_periods quarters

    Non-transaction edges (authority, salary) are always preserved.
    Returns filtered copy of graph (original unchanged).
    """
```

### `disparity_filter`
```python
def disparity_filter(
    G: nx.DiGraph,
    alpha: float = 0.05,
    weight_attr: str = 'weight'
) -> nx.DiGraph:
    """
    Serrano disparity filter for directed weighted graphs.

    For each edge (u→v):
    - Compute alpha_out from u's outgoing perspective
    - Compute alpha_in from v's incoming perspective
    - Remove edge if alpha >= threshold for BOTH perspectives

    Removes isolated nodes after filtering.
    Returns backbone subgraph (copy).
    """
```

### `apply_filter_pipeline`
```python
def apply_filter_pipeline(
    G: nx.DiGraph,
    min_tx_count: int = 3,
    min_total_amount: float = 0.0,
    min_periods: int = 2,
    alpha: float = 0.05
) -> tuple[nx.DiGraph, dict]:
    """
    Run full filtering pipeline: pre-filter → disparity filter.

    Returns (filtered_graph, stats_dict) where stats_dict contains:
    - original_nodes, original_edges
    - after_prefilter_nodes, after_prefilter_edges
    - final_nodes, final_edges
    - edge_retention_rate, weight_retention_rate
    """
```
