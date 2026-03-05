# Quickstart: Advanced Graph Metrics, Industry Analytics & Notebook Documentation

**Feature**: 003-advanced-metrics-industry
**Date**: 2026-02-20

## Prerequisites

```bash
pip install pandas numpy networkx python-igraph leidenalg pyarrow scikit-learn matplotlib
```

## Quick Start: Full Pipeline

```python
from src.pipeline import run_analysis_pipeline

# Run full pipeline (includes Wave 1 + Wave 2 metrics)
result = run_analysis_pipeline('data/')

# Access results
G = result.graph                    # NetworkX DiGraph with all metrics
metrics_df = result.metrics_df      # Node-level metrics DataFrame
clusters = result.cluster_summary   # Cluster summary with external_counterparty_count
okved_matrix = result.okved_matrix  # OKVED×OKVED turnover matrix
behavioral_df = result.behavioral_df  # Behavioral features + segments + lookalike scores
```

## Wave 1: Extended Metrics (on existing data)

### Extended Node Metrics

```python
from src.graph_builder import build_graph, compute_edge_metrics, compute_extended_metrics

G = build_graph(nodes_df, tx_df, auth_df, sal_df)
G = compute_edge_metrics(G)
G = compute_extended_metrics(G)

# Check a node's extended metrics
node = 1000
print(f"Counterparties: {G.nodes[node]['unique_counterparty_count']}")
print(f"Top-5 concentration: {G.nodes[node]['top_k_concentration']:.2f}")
print(f"Active months: {G.nodes[node]['active_months']}")
print(f"Is hub: {G.nodes[node]['hub_flag']}")
```

### Edge Score

```python
from src.graph_builder import compute_edge_score

G = compute_edge_score(G)

# Inspect top edges by score
edges = [(u, v, d['edge_score']) for u, v, d in G.edges(data=True)
         if d.get('edge_type') == 'transaction']
edges.sort(key=lambda x: x[2], reverse=True)
print("Top 5 edges by score:")
for u, v, score in edges[:5]:
    print(f"  {u} → {v}: edge_score={score:.4f}")
```

### Hub Filtering

```python
from src.filters import hub_filter

# Apply hub-aware filtering (after Leiden clustering)
filtered_G = hub_filter(G, membership=membership, cap_min=20, cap_max=50)
```

## Wave 2: Industry & Behavioral Analytics (requires OKVED/region data)

### Generate Synthetic Data with OKVED/Region

```python
from src.synthetic import generate_synthetic_data

paths = generate_synthetic_data('data/', seed=42)
# nodes.parquet now includes okved_code and region_code columns
```

### OKVED Matrix & Heatmap

```python
from src.analysis import build_okved_matrix, compute_okved_diversity
import matplotlib.pyplot as plt

# Build cross-industry turnover matrix
okved_matrix = build_okved_matrix(G)

# Pivot for heatmap
pivot = okved_matrix.pivot(index='okved_source', columns='okved_target', values='total_turnover').fillna(0)
plt.figure(figsize=(12, 10))
plt.imshow(pivot.values, cmap='YlOrRd', aspect='auto')
plt.xticks(range(len(pivot.columns)), pivot.columns, rotation=90)
plt.yticks(range(len(pivot.index)), pivot.index)
plt.colorbar(label='Total Turnover')
plt.title('OKVED×OKVED Industry Turnover Matrix')
plt.tight_layout()
plt.show()

# OKVED diversity per node
G = compute_okved_diversity(G)
```

### Behavioral Segmentation

```python
from src.analysis import compute_behavioral_features, cluster_behavioral_segments

features_df = compute_behavioral_features(G)
features_df = cluster_behavioral_segments(features_df, k_range=(3, 10))

# View segment distribution
print(features_df['behavioral_segment'].value_counts())
```

### Look-Alike Scoring

```python
from src.analysis import compute_lookalike_scores

features_df = compute_lookalike_scores(features_df, G)

# Top prospects
top_prospects = features_df.nlargest(10, 'lookalike_score')
print(top_prospects[['lookalike_score', 'behavioral_segment']])
```

## Notebooks

| Notebook | Description |
|----------|-------------|
| 01_data_extraction.ipynb | ETL from Hive (now includes OKVED/region) |
| 02_graph_construction.ipynb | Graph building + extended metrics + edge_score |
| 03_graph_analysis.ipynb | Clustering, centrality, edge_score, roles, shells, cycles |
| 04_visualization.ipynb | Interactive visualization, cluster maps, node profiles |
| 05_industry_analysis.ipynb | **NEW**: OKVED matrix, heatmap, cross-industry hubs |
| 06_behavioral_segmentation.ipynb | **NEW**: Behavioral features, clustering, look-alike |

Each notebook includes:
- Executive summary at the top
- Markdown explanations before every code cell
- Interpretation glossary at the bottom

## Testing

```bash
cd src && pytest
```
