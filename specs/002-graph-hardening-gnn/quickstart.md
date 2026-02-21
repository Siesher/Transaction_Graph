# Quickstart: Graph Hardening & GNN Integration

**Feature**: 002-graph-hardening-gnn
**Date**: 2026-02-19

## Prerequisites

- Python 3.8+
- All dependencies from `requirements.txt` installed
- For GNN: PyTorch and DGL (CPU-only is sufficient)

## 1. Run Tests

```bash
cd src
pytest
```

All tests use synthetic data — no Spark or Hive needed.

## 2. Generate Synthetic Data & Run Pipeline

```python
from src.synthetic import generate_synthetic_data
from src.pipeline import run_analysis_pipeline

# Generate test data
paths = generate_synthetic_data(output_dir='data/', seed=42)

# Run full analysis in one call
result = run_analysis_pipeline(data_dir='data/')

# Inspect results
print(f"Nodes: {result.graph.number_of_nodes()}")
print(f"Clusters: {len(result.cluster_summary)}")
print(f"Cycles: {len(result.cycles)}")
print(result.metrics_df.head())
```

## 3. Visualize

```python
from src.viz import create_graph_visualization

net = create_graph_visualization(result.graph)
net.show('graph.html')
```

All node attributes (pagerank, cluster, role, shell_score) are already on the graph — no manual enrichment needed.

## 4. Train GNN (optional)

```python
from src.gnn import prepare_gnn_data, train_gnn, predict_gnn, save_gnn_model

# Prepare data for shell detection
dgl_graph, node_mapping, scaler = prepare_gnn_data(result.graph, target_task='shell')

# Train
model, info = train_gnn(dgl_graph, target_task='shell', epochs=200)
print(f"Validation accuracy: {info['val_accuracy']:.2%}")

# Predict
predictions = predict_gnn(model, dgl_graph, node_mapping)
print(predictions.head())

# Save for later
save_gnn_model(model, node_mapping, scaler, info, 'data/gnn_shell_model.pt')
```

## 5. Use GNN on New Data

```python
from src.gnn import load_gnn_model, prepare_gnn_data, predict_gnn

# Load saved model
model, node_mapping, scaler, metadata = load_gnn_model('data/gnn_shell_model.pt')

# Run pipeline on new data
new_result = run_analysis_pipeline(data_dir='new_data/')

# Prepare and predict
new_dgl, new_mapping, _ = prepare_gnn_data(new_result.graph, target_task='shell')
predictions = predict_gnn(model, new_dgl, new_mapping)
```

## Key Changes from MVP

| Before (MVP) | After (This Feature) |
|---|---|
| Manual enrichment in each notebook cell | `enrich_graph()` + `run_analysis_pipeline()` |
| `is_liquidated` always False | Derived from `deleted_flag` + `end_date` |
| No tests | Full pytest suite on synthetic data |
| Row-by-row role/shell classification | Vectorized with numpy |
| Heuristic-only shell detection | Heuristic + GNN predictions |
| No `requirements.txt` | All dependencies documented |
