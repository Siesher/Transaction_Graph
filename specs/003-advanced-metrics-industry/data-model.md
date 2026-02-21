# Data Model: Advanced Graph Metrics, Industry Analytics & Notebook Documentation

**Feature**: 003-advanced-metrics-industry
**Date**: 2026-02-20

## New Entities

### Extended Node Metrics (Wave 1)

New attributes added to every graph node by `compute_extended_metrics()`.

| Field | Type | Source | Description |
|-------|------|--------|-------------|
| unique_counterparty_count | int | Transaction edges | Count of distinct nodes connected by transaction edges (both in and out) |
| top_k_concentration | float | Transaction edges | Share of total turnover going to top-5 counterparties (0.0–1.0) |
| active_months | int | Transaction edge dates | Number of distinct calendar months with at least one transaction |
| hub_flag | bool | Derived | True if unique_counterparty_count > 2 * median across all nodes |

### Edge Score (Wave 1)

New attribute on every transaction edge computed by `compute_edge_score()`.

| Field | Type | Source | Description |
|-------|------|--------|-------------|
| edge_score | float | Composite | Weighted sum of normalized amount, bilateral share, node importance, stability (0–1+) |
| norm_amount | float | rank-percentile | Rank-percentile of total_amount across all tx edges |
| bilateral_share | float | share_of_turnover | Product of source's outgoing share × target's incoming share |
| node_importance | float | pagerank | Average normalized PageRank of both endpoints |
| stability_factor | float | active_months | min(active_months_src, active_months_tgt) / max_months |

### OKVED/Region Node Attributes (Wave 2)

New fields on nodes.parquet and graph node attributes.

| Field | Type | Source | Description |
|-------|------|--------|-------------|
| okved_code | str | Hive client_sdim / synthetic | Two-digit OKVED code (e.g., "46" for wholesale trade) |
| region_code | str | Hive client_sdim / synthetic | Two-digit region code (e.g., "77" for Moscow) |

### OKVED×OKVED Turnover Matrix (Wave 2)

Aggregated cross-industry transaction flows.

| Field | Type | Description |
|-------|------|-------------|
| okved_source | str | OKVED code of source node |
| okved_target | str | OKVED code of target node |
| total_turnover | float | Sum of total_amount for all transaction edges between nodes with these OKVED codes |
| edge_count | int | Number of transaction edges in this OKVED pair |
| avg_amount | float | Mean transaction amount for this OKVED pair |

### OKVED Diversity per Node (Wave 2)

| Field | Type | Description |
|-------|------|-------------|
| okved_diversity_count | int | Count of distinct OKVED codes among counterparties |
| okved_diversity_entropy | float | Shannon entropy across counterparty OKVED codes |
| is_cross_industry_hub | bool | True if okved_diversity_count in top 10% |

### Behavioral Feature Vector (Wave 2)

Per-node features for clustering, computed from monthly transaction aggregates.

| Field | Type | Description |
|-------|------|-------------|
| monthly_tx_count_avg | float | Average monthly transaction count |
| monthly_amount_avg | float | Average monthly total amount |
| direction_ratio | float | outflow / (inflow + outflow); 0.5 = balanced |
| counterparty_growth_rate | float | (latest_counterparties - earliest_counterparties) / earliest_counterparties; 0.0 if single period |
| new_counterparty_share | float | Fraction of counterparties first seen in latest period |

### Behavioral Segment (Wave 2)

| Field | Type | Description |
|-------|------|-------------|
| behavioral_segment | int | Cluster ID from K-Means (0 to k-1) |

### Look-Alike Score (Wave 2)

| Field | Type | Description |
|-------|------|-------------|
| lookalike_score | float | 1/(1+distance) similarity to best-client centroid (0–1] |

## Modified Entities

### Graph Node (existing, modified)

| Attribute | Change | Description |
|-----------|--------|-------------|
| unique_counterparty_count | **New** | FR-001: distinct transaction counterparties |
| top_k_concentration | **New** | FR-002: top-5 counterparty turnover share |
| active_months | **New** | FR-003: distinct active months |
| hub_flag | **New** | FR-004: hub identification flag |
| okved_code | **New** | FR-008: industry code from ETL |
| region_code | **New** | FR-008: region code from ETL |
| okved_diversity_count | **New** | FR-011: counterparty OKVED diversity |
| okved_diversity_entropy | **New** | FR-011: Shannon entropy of counterparty OKVEDs |
| is_cross_industry_hub | **New** | FR-012: top 10% OKVED diversity |
| behavioral_segment | **New** | FR-014: K-Means cluster assignment |
| lookalike_score | **New** | FR-015: similarity to best clients |

### Transaction Edge (existing, modified)

| Attribute | Change | Description |
|-----------|--------|-------------|
| edge_score | **New** | FR-005: composite edge importance metric |

### Cluster Summary (existing, modified)

| Attribute | Change | Description |
|-----------|--------|-------------|
| external_counterparty_count | **New** | FR-007: distinct nodes outside cluster with tx edges to cluster members |

### nodes.parquet (existing, modified)

| Column | Change | Description |
|--------|--------|-------------|
| okved_code | **New** | FR-008/FR-009: OKVED code from Hive or synthetic |
| region_code | **New** | FR-008/FR-009: region code from Hive or synthetic |

## Relationships

```
Graph Node (enriched)
  ├── Extended Metrics (unique_counterparty_count, top_k_concentration, active_months, hub_flag)
  ├── OKVED/Region (okved_code, region_code)
  ├── OKVED Diversity (okved_diversity_count, okved_diversity_entropy, is_cross_industry_hub)
  ├── Behavioral (behavioral_segment, lookalike_score)
  └── Transaction Edges → edge_score

OKVED×OKVED Matrix
  └── Aggregated from Transaction Edges grouped by source/target okved_code

Behavioral Features
  └── Aggregated from monthly Transaction Edge data per node
  └── Input to K-Means → behavioral_segment

Look-Alike Score
  └── Distance from behavioral features to best-client centroid
```
