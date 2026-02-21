# API Contracts: Advanced Graph Metrics, Industry Analytics & Notebook Documentation

**Feature**: 003-advanced-metrics-industry
**Date**: 2026-02-20

## Wave 1: Extended Metrics (src/graph_builder.py, src/analysis.py, src/filters.py)

### compute_extended_metrics(G) → nx.DiGraph

**Module**: `src/graph_builder.py`

Computes extended node metrics on a graph with edge metrics already applied.

```python
def compute_extended_metrics(G: nx.DiGraph) -> nx.DiGraph:
    """
    Compute extended node metrics: unique_counterparty_count, top_k_concentration,
    active_months, hub_flag. Modifies graph in-place and returns it.

    Prerequisites: compute_edge_metrics() must have been called.
    Sets node attributes: unique_counterparty_count, top_k_concentration,
                          active_months, hub_flag.
    """
```

**Behavior**:
- `unique_counterparty_count`: count distinct nodes connected by transaction edges (in + out, deduplicated)
- `top_k_concentration`: sum of top-5 amounts / total amount for outgoing tx edges; 0.0 if no outgoing
- `active_months`: count distinct YYYY-MM from first_tx_date and last_tx_date across all tx edges
- `hub_flag`: True if unique_counterparty_count > 2 * median(all unique_counterparty_counts)

### compute_edge_score(G) → nx.DiGraph

**Module**: `src/graph_builder.py`

Computes edge_score for every transaction edge.

```python
def compute_edge_score(
    G: nx.DiGraph,
    w_base: float = 0.30,
    w_bilateral: float = 0.30,
    w_node: float = 0.20,
    w_stability: float = 0.20,
) -> nx.DiGraph:
    """
    Compute composite edge_score for every transaction edge.
    Modifies graph in-place and returns it.

    Prerequisites: compute_edge_metrics() and compute_extended_metrics() must have been called.
    Sets edge attribute: edge_score (float).
    """
```

**Behavior**:
- Rank-percentile normalization for total_amount across all tx edges
- bilateral_share = share_of_turnover_src * (edge_amount / target_in_total)
- node_importance = (pagerank_src + pagerank_tgt) / (2 * max_pagerank)
- stability_factor = min(active_months_src, active_months_tgt) / max_active_months
- edge_score = w_base*norm_amount + w_bilateral*bilateral_share + w_node*node_importance + w_stability*stability_factor

### hub_filter(G, membership) → nx.DiGraph

**Module**: `src/filters.py`

Hub-aware edge filtering that limits high-degree nodes to top-N edges.

```python
def hub_filter(
    G: nx.DiGraph,
    membership: dict = None,
    cap_min: int = 20,
    cap_max: int = 50,
) -> nx.DiGraph:
    """
    Filter hub nodes to retain only top-N edges by edge_score.

    Cap formula: min(max(cap_min, ceil(sqrt(degree))), cap_max)

    Exemptions (always preserved):
    - Non-transaction edges (authority, salary, shared_employees)
    - Reciprocal transaction edges
    - Edges within the same Leiden cluster (if membership provided)

    Returns filtered copy; verifies no new isolated components.
    """
```

### build_cluster_summary(G, metrics_df, cycles) → pd.DataFrame [MODIFIED]

**Module**: `src/analysis.py`

Extended to include `external_counterparty_count`.

```python
# Existing signature unchanged. New column in output:
# external_counterparty_count: int — count of distinct nodes outside the cluster
#   that have transaction edges with cluster members.
```

## Wave 2: Industry & Behavioral Analytics (src/etl.py, src/synthetic.py, src/analysis.py)

### extract_nodes(spark, client_uks) → DataFrame [MODIFIED]

**Module**: `src/etl.py`

Extended to extract `okved_code` and `region_code` from client_sdim.

```python
# Existing signature unchanged. New columns in output:
# okved_code: str — OKVED code from client_sdim (nullable, default "00")
# region_code: str — region code from client_sdim (nullable, default "00")
```

### generate_synthetic_data(...) [MODIFIED]

**Module**: `src/synthetic.py`

Extended to produce `okved_code` and `region_code` for every node.

```python
# Existing signature unchanged. New columns in nodes_df:
# okved_code: str — random from 20 common OKVED codes
# region_code: str — random from 10 common region codes
```

### build_okved_matrix(G) → pd.DataFrame

**Module**: `src/analysis.py`

Builds OKVED×OKVED cross-industry turnover matrix.

```python
def build_okved_matrix(G: nx.DiGraph) -> pd.DataFrame:
    """
    Build OKVED×OKVED turnover matrix from transaction edges.

    Groups transaction edges by (source_okved, target_okved) and sums total_amount.
    Excludes nodes with okved_code "00" (unknown).

    Returns DataFrame with columns: okved_source, okved_target, total_turnover,
    edge_count, avg_amount.
    """
```

### compute_okved_diversity(G) → nx.DiGraph

**Module**: `src/analysis.py`

Computes OKVED diversity metrics per node.

```python
def compute_okved_diversity(G: nx.DiGraph) -> nx.DiGraph:
    """
    Compute OKVED diversity for each node:
    - okved_diversity_count: distinct OKVED codes among counterparties
    - okved_diversity_entropy: Shannon entropy across counterparty OKVED codes
    - is_cross_industry_hub: True if okved_diversity_count in top 10%

    Modifies graph in-place and returns it.
    """
```

### compute_behavioral_features(G) → pd.DataFrame

**Module**: `src/analysis.py`

Computes behavioral features from monthly transaction aggregates.

```python
def compute_behavioral_features(G: nx.DiGraph) -> pd.DataFrame:
    """
    Compute behavioral feature vector for every node with transaction edges.

    Features:
    - monthly_tx_count_avg: average monthly transaction count
    - monthly_amount_avg: average monthly total amount
    - direction_ratio: outflow / (inflow + outflow)
    - counterparty_growth_rate: change over periods (0.0 if single period)
    - new_counterparty_share: fraction of counterparties first seen in latest period

    Returns DataFrame indexed by client_uk with feature columns.
    """
```

### cluster_behavioral_segments(features_df, k_range, k_override) → pd.DataFrame

**Module**: `src/analysis.py`

K-Means clustering on behavioral features.

```python
def cluster_behavioral_segments(
    features_df: pd.DataFrame,
    k_range: tuple = (3, 10),
    k_override: int = None,
) -> pd.DataFrame:
    """
    Cluster nodes by behavioral features using K-Means.

    Auto-selects k by silhouette score within k_range, unless k_override is set.
    Applies StandardScaler normalization. Removes zero-variance features.

    Returns features_df with added 'behavioral_segment' column (int).
    """
```

### compute_lookalike_scores(features_df, top_decile_col) → pd.DataFrame

**Module**: `src/analysis.py`

Look-alike scoring against best-client centroid.

```python
def compute_lookalike_scores(
    features_df: pd.DataFrame,
    G: nx.DiGraph,
    top_decile_col: str = 'total_turnover',
) -> pd.DataFrame:
    """
    Score each node by similarity to the best-client centroid.

    Best clients = top decile by total_turnover (total_in_flow + total_out_flow).
    Uses Euclidean distance on StandardScaler-normalized features.
    Score = 1 / (1 + distance), range (0, 1].

    Returns features_df with added 'lookalike_score' column (float).
    Returns empty if best-client group is empty (with warning).
    """
```

## Config Extensions (src/config.py)

```python
# --- Wave 1: Extended Metrics ---
EDGE_SCORE_W_BASE = 0.30
EDGE_SCORE_W_BILATERAL = 0.30
EDGE_SCORE_W_NODE = 0.20
EDGE_SCORE_W_STABILITY = 0.20
HUB_CAP_MIN = 20
HUB_CAP_MAX = 50
TOP_K_COUNTERPARTIES = 5  # for top_k_concentration

# --- Wave 2: Industry & Behavioral ---
DEFAULT_OKVED_CODE = "00"
DEFAULT_REGION_CODE = "00"
BEHAVIORAL_K_RANGE = (3, 10)
LOOKALIKE_TOP_DECILE = 0.1  # top 10% by turnover
```

## Pipeline Integration (src/pipeline.py)

```python
# run_analysis_pipeline() extended with new steps:
# After Step 5 (centrality): compute_extended_metrics(), compute_edge_score()
# After Step 9 (enrich): hub_filter() (optional, configurable)
# New Steps: build_okved_matrix(), compute_okved_diversity(),
#            compute_behavioral_features(), cluster_behavioral_segments(),
#            compute_lookalike_scores()
# PipelineResult extended with: okved_matrix, behavioral_df
```
