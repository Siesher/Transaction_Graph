# Contract: src/viz.py

Module for interactive graph visualization in JupyterLab.

## Functions

### `create_graph_visualization`
```python
def create_graph_visualization(
    G: nx.DiGraph,
    height: str = '800px',
    width: str = '100%',
    output_file: str = 'graph.html'
) -> Network:
    """
    Create interactive pyvis visualization from analyzed graph.

    Visual encoding:
    - Node color by node_type (company=blue, individual=green, sole_proprietor=orange)
    - Node size proportional to PageRank
    - Edge color by edge_type (transaction=gray, authority=red, salary=green, shared=purple)
    - Edge width proportional to log(transaction volume)
    - Node tooltips: name, type, cluster, pagerank, betweenness, role
    - Edge tooltips: type, volume, count, share_of_turnover

    Uses cdn_resources='in_line' for offline compatibility.
    Physics: forceAtlas2Based for 200-500 node graphs.

    Returns pyvis Network object (call .show() to display).
    """
```

### `create_cluster_visualization`
```python
def create_cluster_visualization(
    G: nx.DiGraph,
    cluster_id: int,
    output_file: str = 'cluster.html'
) -> Network:
    """
    Visualize a single cluster and its immediate external connections.

    Internal nodes: full color. External nodes: faded/gray.
    Highlights shell companies and cycles if present.

    Returns pyvis Network object.
    """
```

### `display_summary_table`
```python
def display_summary_table(
    cluster_summary_df: pd.DataFrame
) -> None:
    """
    Display formatted cluster summary table in Jupyter.
    Uses pandas Styler for highlighting:
    - Red background for clusters with shell companies
    - Yellow for clusters with cycles
    - Bold for lead company names
    """
```

### `display_node_profile`
```python
def display_node_profile(
    G: nx.DiGraph,
    node_id: int,
    metrics_df: pd.DataFrame
) -> None:
    """
    Display detailed profile for a single node:
    - Basic info (name, type, INN, status)
    - Centrality metrics
    - Role classification
    - Top counterparties (by volume)
    - Shell score breakdown (if applicable)
    """
```
