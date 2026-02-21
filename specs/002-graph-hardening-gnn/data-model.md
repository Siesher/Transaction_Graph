# Data Model: Graph Hardening & GNN Integration

**Feature**: 002-graph-hardening-gnn
**Date**: 2026-02-19

## Entities

### Node Feature Vector (new)

Numeric representation of a graph node used as GNN input.

| Field                | Type    | Source                          | Description                         |
|----------------------|---------|---------------------------------|-------------------------------------|
| pagerank             | float   | compute_centrality()            | Weighted PageRank score             |
| betweenness          | float   | compute_centrality()            | Betweenness centrality              |
| clustering_coef      | float   | compute_centrality()            | Local clustering coefficient        |
| total_in_flow        | float   | compute_edge_metrics()          | Total incoming transaction volume   |
| total_out_flow       | float   | compute_edge_metrics()          | Total outgoing transaction volume   |
| flow_through_ratio   | float   | compute_centrality()            | min(in,out)/max(in,out)             |
| in_degree            | int     | compute_centrality()            | Number of incoming edges            |
| out_degree           | int     | compute_centrality()            | Number of outgoing edges            |
| is_company           | bool    | node_type encoding              | One-hot: company                    |
| is_individual        | bool    | node_type encoding              | One-hot: individual                 |
| is_sole_proprietor   | bool    | node_type encoding              | One-hot: sole_proprietor            |
| hop_distance         | int     | ETL hop expansion               | Distance from seed company          |
| has_salary_payments  | bool    | compute_centrality()            | Has outgoing salary edges           |
| mean_tx_amount       | float   | Derived from edge data          | Avg transaction amount across edges |
| total_tx_count       | int     | Derived from edge data          | Total transactions across all edges |

### GNN Model Artifact (new)

A trained model persisted for reuse.

| Field             | Type   | Description                                      |
|-------------------|--------|--------------------------------------------------|
| model_state_dict  | dict   | PyTorch state dictionary (weights)               |
| node_id_mapping   | dict   | {nx_node_id: (node_type, local_index)} mapping   |
| feature_scaler    | object | Fitted scaler for feature normalization           |
| label_encoder     | dict   | {task: {label: int}} mapping for classification  |
| metadata          | dict   | Training hyperparameters, metrics, date           |

### Analysis Pipeline Result (new)

Structured output from the orchestrator function.

| Field            | Type          | Description                                       |
|------------------|---------------|---------------------------------------------------|
| graph            | nx.DiGraph    | Enriched graph with all metrics as node attributes |
| metrics_df       | pd.DataFrame  | Node-level metrics (centrality, roles, scores)     |
| cluster_summary  | pd.DataFrame  | One row per cluster with summary statistics        |
| cycles           | list[dict]    | Detected circular payment patterns                 |
| filter_stats     | dict          | Edge/node retention rates from filtering           |
| best_gamma       | float         | Selected Leiden resolution parameter               |

## Modified Entities

### Graph Node (existing, modified)

Changes to node attributes on `nx.DiGraph`:

| Attribute     | Change      | Old Source           | New Source                          |
|---------------|-------------|----------------------|-------------------------------------|
| is_liquidated | **Fixed**   | `liquidation_flag`   | `deleted_flag='Y'` OR `end_date < today` |
| pagerank      | **Enriched**| Not set on graph     | Set by `enrich_graph()` from metrics_df  |
| betweenness   | **Enriched**| Not set on graph     | Set by `enrich_graph()` from metrics_df  |
| cluster       | **Enriched**| Set manually in notebook | Set by `enrich_graph()` from membership |
| role          | **Enriched**| Not set on graph     | Set by `enrich_graph()` from metrics_df  |
| shell_score   | **Enriched**| Not set on graph     | Set by `enrich_graph()` from shell_df    |
| gnn_shell_prob| **New**     | N/A                  | GNN inference output                     |
| gnn_role      | **New**     | N/A                  | GNN inference output                     |

## Relationships

```
NetworkX DiGraph
  └── nodes (with enriched attributes)
  └── edges (transaction, authority, salary, shared_employees)
        │
        ▼ (conversion)
DGL HeteroGraph
  └── node types: company, individual, sole_proprietor
  └── relation types:
        ├── (company, transaction, company)
        ├── (company, transaction, individual)
        ├── (individual, authority, company)
        ├── (company, salary, individual)
        └── (company, shared_employees, company)
        │
        ▼ (R-GCN inference)
Node Predictions
  └── shell_probability (float 0-1)
  └── predicted_role (category)
```
