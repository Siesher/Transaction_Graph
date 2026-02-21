# Transaction_Graph Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-02-17

## Active Technologies
- Python 3.8+ (MDP JupyterLab) + networkx, igraph, leidenalg, pandas, numpy, pyvis, pyspark (existing); torch 1.13+, dgl 1.1.x, scikit-learn (new for GNN) (002-graph-hardening-gnn)
- Local filesystem, Parquet files, pickle (graph serialization) (002-graph-hardening-gnn)
- Python 3.8+ (MDP JupyterLab) + networkx, python-igraph, leidenalg, pandas, numpy, pyarrow, pyspark (ETL), scikit-learn (K-Means, StandardScaler), matplotlib (heatmaps) (003-advanced-metrics-industry)

- Python 3.8+ (as available on MDP JupyterLab) + pyspark (ETL from Hive), pandas (data manipulation), networkx (graph construction), python-igraph + leidenalg (community detection), pyvis (interactive visualization), numpy, matplotlib (001-transaction-graph-mvp)

## Project Structure

```text
src/
tests/
```

## Commands

cd src; pytest; ruff check .

## Code Style

Python 3.8+ (as available on MDP JupyterLab): Follow standard conventions

## Recent Changes
- 003-advanced-metrics-industry: Added Python 3.8+ (MDP JupyterLab) + networkx, python-igraph, leidenalg, pandas, numpy, pyarrow, pyspark (ETL), scikit-learn (K-Means, StandardScaler), matplotlib (heatmaps)
- 002-graph-hardening-gnn: Added Python 3.8+ (MDP JupyterLab) + networkx, igraph, leidenalg, pandas, numpy, pyvis, pyspark (existing); torch 1.13+, dgl 1.1.x, scikit-learn (new for GNN)

- 001-transaction-graph-mvp: Added Python 3.8+ (as available on MDP JupyterLab) + pyspark (ETL from Hive), pandas (data manipulation), networkx (graph construction), python-igraph + leidenalg (community detection), pyvis (interactive visualization), numpy, matplotlib

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
