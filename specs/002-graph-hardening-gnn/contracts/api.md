# API Contracts: Graph Hardening & GNN Integration

**Feature**: 002-graph-hardening-gnn
**Date**: 2026-02-19

This project is a Python library (not a web service), so "contracts" are Python function signatures with type hints, input/output specifications, and invariants.

---

## Module: `src/pipeline.py` (new)

### `run_analysis_pipeline()`

```
run_analysis_pipeline(
    data_dir: str,
    min_tx_count: int = 3,
    min_total_amount: float = 0.0,
    min_periods: int = 2,
    alpha: float = 0.05,
    gamma_values: list[float] | None = None,
    shell_threshold: float | None = None,
) -> PipelineResult
```

**Input**: Path to directory containing Parquet files (nodes, transaction_edges, authority_edges, salary_edges, hop_distances).

**Output**: `PipelineResult` (namedtuple or dataclass):
- `graph`: nx.DiGraph — enriched with all metrics as node attributes
- `metrics_df`: pd.DataFrame — indexed by client_uk
- `cluster_summary`: pd.DataFrame — one row per cluster
- `cycles`: list[dict] — detected circular patterns
- `filter_stats`: dict — retention rates
- `best_gamma`: float — selected Leiden resolution

**Invariants**:
- Every node in `graph` has attributes: pagerank, betweenness, cluster, role, shell_score
- `metrics_df` index matches `graph.nodes()`
- Non-transaction edges are never removed by filtering

---

## Module: `src/graph_builder.py` (modified)

### `enrich_graph()` (new function)

```
enrich_graph(
    G: nx.DiGraph,
    metrics_df: pd.DataFrame,
    membership: dict[int, int],
    shell_df: pd.DataFrame | None = None,
) -> nx.DiGraph
```

**Input**:
- `G`: graph to enrich (modified in-place)
- `metrics_df`: must have columns: pagerank, betweenness, clustering_coef, in_degree, out_degree, role
- `membership`: {node_id: cluster_id}
- `shell_df`: optional, must have column: shell_score

**Output**: Same graph G with node attributes set.

**Invariants**:
- Every node in G gets `cluster` attribute (from membership, or -1 if missing)
- Every node in G with a row in metrics_df gets: pagerank, betweenness, clustering_coef, role
- Every node in G with a row in shell_df gets: shell_score

---

## Module: `src/gnn.py` (new)

### `prepare_gnn_data()`

```
prepare_gnn_data(
    G: nx.DiGraph,
    target_task: str = 'shell',  # 'shell' or 'role'
) -> tuple[dgl.DGLHeteroGraph, dict, object]
```

**Input**: Enriched NetworkX DiGraph with node attributes (from pipeline).

**Output**:
- `dgl_graph`: DGL heterogeneous graph with node features and labels
- `node_mapping`: {nx_node_id: (node_type, local_index)}
- `scaler`: fitted feature scaler

**Invariants**:
- All node types present in G are represented in dgl_graph
- All edge types present in G are represented as relation tuples
- Feature tensor shape: (n_nodes_per_type, n_features)

### `train_gnn()`

```
train_gnn(
    dgl_graph: dgl.DGLHeteroGraph,
    target_task: str = 'shell',
    hidden_dim: int = 64,
    n_layers: int = 2,
    epochs: int = 200,
    lr: float = 0.01,
    label_smoothing: float = 0.1,
    val_ratio: float = 0.2,
) -> tuple[torch.nn.Module, dict]
```

**Input**: DGL heterogeneous graph with features and labels.

**Output**:
- `model`: trained R-GCN model
- `train_info`: {train_loss, val_loss, val_accuracy, val_f1, epochs_run}

### `predict_gnn()`

```
predict_gnn(
    model: torch.nn.Module,
    dgl_graph: dgl.DGLHeteroGraph,
    node_mapping: dict,
) -> pd.DataFrame
```

**Output**: DataFrame with columns: client_uk, predicted_label, probability

### `save_gnn_model()` / `load_gnn_model()`

```
save_gnn_model(model, node_mapping, scaler, metadata, path: str) -> None
load_gnn_model(path: str) -> tuple[torch.nn.Module, dict, object, dict]
```

**Invariants**:
- Saved file is self-contained (model weights + mapping + scaler + metadata)
- `load` followed by `predict` reproduces same results as immediately after training

---

## Module: `src/analysis.py` (modified)

### `classify_node_roles()` — vectorized

Same signature. Same output columns. Internal implementation changed from `iterrows()` to `np.select()`.

**Invariant**: Output is identical to previous row-by-row implementation for the same input.

### `detect_shell_companies()` — vectorized

Same signature. Same output columns. Internal implementation changed from `iterrows()` to vectorized boolean masks.

**Invariant**: Output is identical to previous row-by-row implementation for the same input.

### `build_cluster_summary()` — optimized

Same signature. Same output columns. Internal implementation changed from per-cluster edge scan to single-pass groupby.

**Invariant**: Output is identical to previous implementation for the same input.
