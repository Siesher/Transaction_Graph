# Research: Advanced Graph Metrics, Industry Analytics & Notebook Documentation

**Feature**: 003-advanced-metrics-industry
**Date**: 2026-02-20

## Decision 1: Edge Score Formula

**Decision**: Hybrid additive formula with rank-percentile normalization.

**Formula**:
```
edge_score = w_base * norm_amount
           + w_bilateral * bilateral_share
           + w_node * node_importance
           + w_stability * stability_factor
```

**Components**:
- `norm_amount`: rank-percentile of `total_amount` across all transaction edges (0–1)
- `bilateral_share`: `share_of_turnover_src * share_of_turnover_tgt` — product of source's outgoing share and target's incoming share (0–1)
- `node_importance`: `(pagerank_src + pagerank_tgt) / (2 * max_pagerank)` — average normalized PageRank of endpoints (0–1)
- `stability_factor`: `min(active_months_src, active_months_tgt) / max_months` — stability proxy (0–1)

**Weights**: `w_base=0.30, w_bilateral=0.30, w_node=0.20, w_stability=0.20`

**Rationale**: Additive formula is transparent, debuggable, and each component is independently interpretable. Rank-percentile normalization avoids outlier domination. Weights are configurable in `config.py`.

**Alternatives considered**:
- Multiplicative formula (product of components): single zero factor kills entire score; hard to debug
- Pure volume-based: ignores structural importance and stability
- PCA-based composite: opaque, hard to explain to business users

## Decision 2: Hub Filtering Strategy

**Decision**: Degree-proportional cap with edge exemptions.

**Cap formula**: `cap = min(max(20, ceil(sqrt(degree))), 50)` where `degree` is the number of transaction edges.

**Exemptions** (always preserved regardless of cap):
- Non-transaction edges (authority, salary, shared_employees)
- Reciprocal transaction edges (if edge u→v exists and v→u also exists)
- Edges connecting nodes within the same Leiden cluster

**Process**:
1. Identify hubs: nodes with `unique_counterparty_count > 2 * median`
2. For each hub, compute degree-proportional cap
3. Rank hub's transaction edges by `edge_score` descending
4. Remove edges below the cap threshold (except exempted ones)
5. Verify no new isolated components are created; if so, restore the cut edge with the highest `edge_score`

**Rationale**: Fixed cap penalizes all hubs equally; degree-proportional cap lets larger hubs retain proportionally more edges while still preventing super-cluster formation. Exemptions preserve structural connectivity.

**Alternatives considered**:
- Fixed cap (N=20 for all hubs): too aggressive for large hubs, too lenient for small ones
- Disparity filter alone: already applied but doesn't specifically target hub explosion
- k-core decomposition: removes entire periphery, not just hub excess

## Decision 3: OKVED & Region Codes for Synthetic Data

**Decision**: 20 common two-digit OKVED codes + 10 Russian region codes.

**OKVED codes** (two-digit, ОКВЭД-2):
| Code | Description |
|------|-------------|
| 01 | Растениеводство и животноводство |
| 10 | Производство пищевых продуктов |
| 14 | Производство одежды |
| 20 | Производство химических веществ |
| 23 | Производство прочей неметаллической продукции |
| 25 | Производство металлических изделий |
| 41 | Строительство зданий |
| 43 | Работы строительные специализированные |
| 45 | Торговля автотранспортными средствами |
| 46 | Торговля оптовая |
| 47 | Торговля розничная |
| 49 | Деятельность сухопутного транспорта |
| 52 | Складское хозяйство |
| 62 | Разработка компьютерного ПО |
| 64 | Финансовая деятельность |
| 68 | Операции с недвижимостью |
| 69 | Деятельность в области права и бухучёта |
| 70 | Деятельность головных офисов; консультирование |
| 71 | Архитектура и инженерные изыскания |
| 86 | Деятельность в области здравоохранения |

**Region codes** (10 common Russian regions):
| Code | Region |
|------|--------|
| 77 | Москва |
| 78 | Санкт-Петербург |
| 50 | Московская область |
| 23 | Краснодарский край |
| 16 | Республика Татарстан |
| 54 | Новосибирская область |
| 66 | Свердловская область |
| 63 | Самарская область |
| 74 | Челябинская область |
| 52 | Нижегородская область |

**Missing OKVED handling**: Default to code `"00"` (Unknown), description "Не указан". Exclude from OKVED×OKVED matrix aggregation but keep in graph.

**Rationale**: Two-digit codes balance granularity (enough to show industry diversity) with synthetic data simplicity. The 20 codes cover major sectors of the Russian economy. Region codes cover ~60% of corporate registrations.

## Decision 4: Behavioral Clustering Approach

**Decision**: K-Means with silhouette-score auto-selection, StandardScaler normalization, zero-variance feature removal.

**Features** (5 per node):
1. `monthly_tx_count_avg`: average monthly transaction count
2. `monthly_amount_avg`: average monthly total amount
3. `direction_ratio`: outflow / (inflow + outflow), 0.5 = balanced
4. `counterparty_growth_rate`: (counterparties_latest - counterparties_earliest) / counterparties_earliest; 0.0 if single period
5. `new_counterparty_share`: fraction of counterparties first seen in latest period

**Pipeline**:
1. Compute features for all nodes with transaction edges
2. Remove zero-variance features (log warning)
3. StandardScaler normalization (zero mean, unit variance)
4. K-Means with k ∈ [3, 10], select best by silhouette score
5. Allow user override of k via config parameter
6. Assign `behavioral_segment` label (integer cluster ID) to each node

**Rationale**: K-Means is fast, interpretable, well-supported by scikit-learn. Silhouette score balances between too few (loss of segment differentiation) and too many (overfitting) clusters. StandardScaler handles different feature scales.

**Alternatives considered**:
- DBSCAN: no natural cluster count selection, sensitive to epsilon
- Gaussian Mixture Models: more flexible but harder to interpret for business users
- Hierarchical clustering: O(n²) memory, not suitable for large graphs

## Decision 5: Look-Alike Scoring Method

**Decision**: Euclidean distance on StandardScaler-normalized features, `1/(1+d)` score transformation.

**Process**:
1. Define "best clients" as top-decile nodes by total turnover (configurable)
2. Compute centroid of best clients' feature vectors (same 5 behavioral features)
3. For each prospect node, compute Euclidean distance to centroid
4. Transform: `lookalike_score = 1 / (1 + distance)` → range (0, 1]
5. Score of 1.0 = identical to centroid; scores decrease with distance

**Rationale**: Euclidean distance on normalized features is the simplest interpretable metric. `1/(1+d)` gives bounded scores in (0,1] without hard thresholds. The centroid approach is robust to outliers in the "best" group.

**Alternatives considered**:
- Cosine similarity: ignores magnitude differences, which matter for amount-based features
- Mahalanobis distance: requires non-singular covariance matrix, fails with collinear features
- k-NN scoring: O(n*k) per query, harder to interpret a single score

## Decision 6: Notebook Documentation Structure

**Decision**: "Sandwich" documentation pattern for each notebook.

**Structure per notebook**:
1. **Executive summary** (top): 3–5 bullet points explaining what this notebook does and what outputs to expect
2. **Inline section headers** (per code cell): markdown cell before each major code section with:
   - What this step does (1–2 sentences)
   - What the output means (expected shape, typical values)
   - How to interpret results (business-oriented guidance)
3. **Interpretation glossary** (bottom): table of metrics with business-oriented explanations

**Metric interpretation examples**:
| Metric | High Value Means | Low Value Means | Business Action |
|--------|-----------------|-----------------|-----------------|
| PageRank | Central company in payment flows | Peripheral/isolated | Key client for RM attention |
| Betweenness | Gateway/broker between groups | Within-group transactor | Check for transit/shell risk |
| edge_score | Critical business relationship | Weak/occasional link | Prioritize for cross-sell |
| hub_flag | Major hub node | Normal node | Apply hub filtering |

**Rationale**: Business users (relationship managers) need context at every step. The sandwich structure ensures they can understand purpose before seeing code and interpret results after. Glossary serves as reference during meetings.
