# Research: Graph Hardening & GNN Integration

**Feature**: 002-graph-hardening-gnn
**Date**: 2026-02-19

## Decision 1: GNN Framework

**Decision**: DGL (Deep Graph Library) 1.1.x with PyTorch backend.

**Rationale**:
- Explicit Python 3.8 support (MDP platform constraint)
- CPU-only wheel builds as first-class option — no GPU required
- Native heterogeneous graph API via `dgl.heterograph()` and `HeteroGraphConv`
- Works with PyTorch 1.13.x and 2.0.x (flexible for whatever MDP has)
- Strong documentation and examples for RGCN node classification

**Alternatives considered**:
- **PyTorch Geometric (PyG)**: Better API ergonomics, richer model zoo. Rejected because modern PyG requires PyTorch 2.x which dropped Python 3.8. Pairing with PyTorch 1.13 is fragile and unsupported.
- **StellarGraph**: Abandoned since mid-2020. No new releases. Eliminated immediately.

**Fallback**: If MDP has Python 3.9+ and PyTorch 2.0+, PyG 2.3-2.4 becomes viable.

## Decision 2: GNN Architecture

**Decision**: R-GCN (Relational Graph Convolutional Network) via `HeteroGraphConv` wrapping `GraphConv` per relation type.

**Rationale**:
- 4 distinct edge types (transaction, authority, salary, shared_employees) require relation-specific weight matrices
- R-GCN is validated on financial fraud/AML heterogeneous networks in literature
- 2-layer depth: prevents oversmoothing on small graphs, fast CPU training
- Hidden dimension 64: sufficient for ~15 input features and ~10K nodes
- Full-batch training (no mini-batching): entire graph fits in memory

**Alternatives considered**:
- **Plain GCN**: Cannot distinguish edge types. Destroys relational signal critical for shell detection.
- **GAT**: Attention is homogeneous by default; requires conversion to HAN/HGT for heterogeneous graphs, adding complexity.
- **GraphSAGE**: Good inductive properties but same heterogeneous limitation as GAT. Could be used as base conv inside HeteroGraphConv as alternative to GraphConv.

## Decision 3: NetworkX → DGL Conversion

**Decision**: Manual conversion (no automatic converter exists for heterogeneous graphs).

**Procedure**:
1. Group nodes by `node_type`, assign local integer index per type
2. Group edges by `edge_type`, map to `(src_type, relation, dst_type)` tuples with local indices
3. Build `dgl.heterograph({relation_tuple: (src_list, dst_list), ...})`
4. Attach node feature tensors and label tensors per node type

**Rationale**: Neither DGL nor PyG provides automatic `from_networkx()` for heterogeneous graphs. Manual conversion is straightforward given that `graph_builder.py` already stores `node_type` and `edge_type` as attributes.

## Decision 4: Label Strategy

**Decision**: Use heuristic labels as "silver standard" with label smoothing (0.1).

**Rationale**:
- No manually annotated ground truth available
- Label smoothing accounts for heuristic noise without discarding labels
- GNN is positioned as complement to heuristics, not replacement
- 80/20 train/validation split; report agreement with heuristics + divergence cases

## Decision 5: Node Feature Vector Design

**Decision**: ~15 features per node:
- Centrality: pagerank, betweenness, clustering_coef (3)
- Flow: total_in_flow, total_out_flow, flow_through_ratio (3)
- Degree: in_degree, out_degree (2)
- Type: one-hot encoding of node_type (3: company, individual, sole_proprietor)
- Graph: hop_distance, has_salary_payments (2)
- Edge stats: mean_tx_amount, tx_count_total (2)

**Rationale**: All features are already computed by the analysis pipeline. No additional computation needed. Features are normalized (min-max or standard scaling) before training.

## Decision 6: Dependency Management

**Decision**: `requirements.txt` with pinned major.minor versions.

**New dependencies for GNN**:
```
torch>=1.13,<3.0
dgl>=1.1,<2.0
scikit-learn>=0.24
```

**Existing dependencies (to document)**:
```
pyspark>=3.0
pandas>=1.3
networkx>=2.6
python-igraph>=0.9
leidenalg>=0.8
pyvis>=0.3
numpy>=1.20
matplotlib>=3.4
```

## Decision 7: Vectorization Approach

**Decision**: Replace `iterrows()` loops in `classify_node_roles` and `detect_shell_companies` with `np.select()` / vectorized boolean masks.

**Rationale**:
- `np.select(conditions, choices, default)` maps directly to the if/elif/else chain in role classification
- Shell score can be computed as sum of boolean columns multiplied by weights
- Cluster summary internal turnover: pre-compute edge-to-cluster mapping, then `groupby().sum()`

## Decision 8: enrich_graph Function Design

**Decision**: Single function `enrich_graph(G, metrics_df, membership, shell_df, cycles)` that writes all results as node attributes.

**Rationale**: Bridges the gap between analysis (DataFrame outputs) and visualization (reads node attributes). Currently this is done manually in each notebook cell. Centralizing ensures consistency.
