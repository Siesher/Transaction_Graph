"""
Анализ графа: кластеризация Leiden, центральность, обнаружение паттернов.

Работает с NetworkX DiGraph, конвертирует в igraph для Leiden.
"""

import logging

import igraph as ig
import leidenalg
import networkx as nx
import numpy as np
import pandas as pd

from src import config

logger = logging.getLogger(__name__)


# =============================================================================
# Leiden Clustering
# =============================================================================

def run_leiden_clustering(
    G: nx.DiGraph,
    gamma_values: list = None,
    weight_attr: str = 'weight',
) -> tuple:
    """
    Run Leiden CPM at multiple resolutions, select best by modularity.

    Converts NetworkX -> igraph internally.
    Returns (membership_dict, best_gamma) where membership_dict maps
    node_id -> cluster_id.
    """
    if gamma_values is None:
        gamma_values = config.DEFAULT_GAMMA_VALUES

    # Extract transaction subgraph for clustering (ignore direction for community detection)
    tx_edges = [(u, v, d) for u, v, d in G.edges(data=True)
                if d.get('edge_type') == 'transaction']
    if not tx_edges:
        logger.warning("No transaction edges for clustering")
        return {node: 0 for node in G.nodes()}, 0.0

    tx_graph = nx.Graph()
    for u, v, d in tx_edges:
        w = d.get(weight_attr, 1.0)
        if tx_graph.has_edge(u, v):
            tx_graph[u][v]['weight'] += w
        else:
            tx_graph.add_edge(u, v, weight=w)

    # Add isolated nodes from original graph
    for node in G.nodes():
        if node not in tx_graph:
            tx_graph.add_node(node)

    # Convert to igraph
    ig_graph = ig.Graph.from_networkx(tx_graph)

    # Run Leiden at multiple gammas
    results = {}
    for gamma in gamma_values:
        try:
            partition = leidenalg.find_partition(
                ig_graph,
                leidenalg.CPMVertexPartition,
                weights='weight',
                resolution_parameter=gamma,
            )
            results[gamma] = partition
            n_clusters = len(set(partition.membership))
            logger.info(f"gamma={gamma}: {n_clusters} clusters, quality={partition.quality():.4f}")
        except Exception as e:
            logger.warning(f"Leiden failed at gamma={gamma}: {e}")

    if not results:
        logger.error("All Leiden runs failed")
        return {node: 0 for node in G.nodes()}, 0.0

    # Select best by modularity
    best_gamma = max(results, key=lambda g: results[g].quality())
    best_partition = results[best_gamma]

    # Map back to NetworkX node IDs
    membership = {}
    for node_idx, cluster_id in enumerate(best_partition.membership):
        node_name = ig_graph.vs[node_idx]['_nx_name']
        membership[node_name] = cluster_id

    # Add membership for nodes not in transaction graph
    for node in G.nodes():
        if node not in membership:
            membership[node] = -1  # unassigned

    logger.info(
        f"Best clustering: gamma={best_gamma}, "
        f"{len(set(membership.values()))} clusters"
    )
    return membership, best_gamma


# =============================================================================
# Centrality Metrics
# =============================================================================

def compute_centrality(G: nx.DiGraph) -> pd.DataFrame:
    """
    Compute centrality metrics for all nodes.

    Returns DataFrame indexed by node_id with columns:
    pagerank, betweenness, clustering_coef, in_degree, out_degree,
    total_in_flow, total_out_flow, flow_through_ratio, has_salary_payments.
    """
    nodes = list(G.nodes())
    if not nodes:
        return pd.DataFrame()

    # Weighted PageRank
    try:
        pagerank = nx.pagerank(G, weight='weight')
    except Exception:
        pagerank = {n: 1.0 / len(nodes) for n in nodes}

    # Betweenness centrality (weighted)
    try:
        betweenness = nx.betweenness_centrality(G, weight='weight')
    except Exception:
        betweenness = {n: 0.0 for n in nodes}

    # Clustering coefficient (on undirected view)
    try:
        undirected = G.to_undirected()
        clustering = nx.clustering(undirected, weight='weight')
    except Exception:
        clustering = {n: 0.0 for n in nodes}

    # Build metrics DataFrame
    records = []
    for node in nodes:
        attrs = G.nodes[node]
        in_flow = attrs.get('total_in_flow', 0)
        out_flow = attrs.get('total_out_flow', 0)
        max_flow = max(in_flow, out_flow)
        min_flow = min(in_flow, out_flow)
        ft_ratio = min_flow / max_flow if max_flow > 0 else 0.0

        has_salary = any(
            d.get('edge_type') == 'salary'
            for _, _, d in G.out_edges(node, data=True)
        )

        records.append({
            'client_uk': node,
            'pagerank': pagerank.get(node, 0),
            'betweenness': betweenness.get(node, 0),
            'clustering_coef': clustering.get(node, 0),
            'in_degree': G.in_degree(node),
            'out_degree': G.out_degree(node),
            'total_in_flow': in_flow,
            'total_out_flow': out_flow,
            'flow_through_ratio': ft_ratio,
            'has_salary_payments': has_salary,
        })

    df = pd.DataFrame(records).set_index('client_uk')
    logger.info(f"Centrality computed for {len(df)} nodes")
    return df


# =============================================================================
# Node Role Classification
# =============================================================================

def classify_node_roles(
    metrics_df: pd.DataFrame,
    G: nx.DiGraph,
) -> pd.DataFrame:
    """
    Classify nodes into roles based on centrality profile.

    Roles:
    - 'parent': high pagerank, moderate betweenness, high in-degree
    - 'shell': low pagerank, very high betweenness, low clustering
    - 'subsidiary': moderate pagerank, low betweenness, high degree
    - 'conduit': low pagerank, high betweenness, low degree
    - 'regular': default

    Adds 'role' column. Returns updated DataFrame.
    """
    df = metrics_df.copy()

    # Compute percentile thresholds
    pr_high = df['pagerank'].quantile(0.8)
    pr_med = df['pagerank'].quantile(0.5)
    bc_high = df['betweenness'].quantile(0.9)
    bc_med = df['betweenness'].quantile(0.7)
    cc_low = 0.1
    deg_high = df['in_degree'].quantile(0.8) + df['out_degree'].quantile(0.8)
    deg_low = df['in_degree'].quantile(0.3) + df['out_degree'].quantile(0.3)

    total_degree = df['in_degree'] + df['out_degree']

    # Conditions in priority order (first match wins, matching original if/elif)
    conditions = [
        (df['pagerank'] >= pr_high) & (df['betweenness'] >= bc_med),
        (df['pagerank'] < pr_med) & (df['betweenness'] >= bc_high) & (df['clustering_coef'] < cc_low),
        (df['pagerank'] >= pr_med) & (df['betweenness'] < bc_med) & (total_degree >= deg_high),
        (df['pagerank'] < pr_med) & (df['betweenness'] >= bc_med) & (total_degree < deg_low),
    ]
    choices = ['parent', 'shell', 'subsidiary', 'conduit']

    df['role'] = np.select(conditions, choices, default='regular')
    role_counts = df['role'].value_counts().to_dict()
    logger.info(f"Role classification: {role_counts}")
    return df


# =============================================================================
# Shell Company Detection
# =============================================================================

def detect_shell_companies(
    metrics_df: pd.DataFrame,
    G: nx.DiGraph,
    threshold: float = None,
) -> pd.DataFrame:
    """
    Score nodes on shell company indicators.

    Signals:
    - flow_through_ratio > 0.9 (30%)
    - no salary payments (25%)
    - high betweenness + low clustering (20%)
    - low unique counterparties (15%)
    - bursty activity (10%)

    Adds 'shell_score' column. Returns flagged nodes (score >= threshold).
    """
    if threshold is None:
        threshold = config.SHELL_SCORE_THRESHOLD

    df = metrics_df.copy()

    # Medians for thresholds
    bc_median = df['betweenness'].median()

    # Vectorized boolean signals
    sig_flow = (df['flow_through_ratio'] > 0.9).astype(float) * config.SHELL_WEIGHT_FLOW_THROUGH
    sig_salary = (~df['has_salary_payments']).astype(float) * config.SHELL_WEIGHT_NO_SALARY
    sig_bc_cc = ((df['betweenness'] > bc_median * 2) & (df['clustering_coef'] < 0.1)).astype(float) * config.SHELL_WEIGHT_HIGH_BC_LOW_CC
    sig_degree = ((df['in_degree'] + df['out_degree']) <= 4).astype(float) * config.SHELL_WEIGHT_LOW_COUNTERPARTIES

    # Bursty signal: pre-compute per-node period count from graph edges
    period_counts = {}
    for u, v, d in G.edges(data=True):
        if d.get('edge_type') != 'transaction':
            continue
        first_date = d.get('first_tx_date', '')
        if first_date:
            period_month = first_date[:7]
            if u not in period_counts:
                period_counts[u] = set()
            period_counts[u].add(period_month)

    sig_bursty = pd.Series(0.0, index=df.index)
    for node in df.index:
        periods = period_counts.get(node, set())
        if len(periods) >= 1 and len(periods) <= 1:
            sig_bursty.loc[node] = config.SHELL_WEIGHT_BURSTY

    df['shell_score'] = sig_flow + sig_salary + sig_bc_cc + sig_degree + sig_bursty

    flagged = df[df['shell_score'] >= threshold].sort_values('shell_score', ascending=False)
    logger.info(
        f"Shell detection: {len(flagged)} flagged out of {len(df)} "
        f"(threshold={threshold})"
    )
    return flagged


# =============================================================================
# Cycle Detection
# =============================================================================

def detect_cycles(
    G: nx.DiGraph,
    min_length: int = None,
    max_length: int = None,
    edge_type: str = 'transaction',
) -> list:
    """
    Detect circular payment patterns.

    Returns list of dicts: {nodes, length, total_amount}.
    Sorted by total_amount descending.
    """
    if min_length is None:
        min_length = config.MIN_CYCLE_LENGTH
    if max_length is None:
        max_length = config.MAX_CYCLE_LENGTH

    # Extract subgraph of specified edge type
    type_edges = [(u, v) for u, v, d in G.edges(data=True)
                  if d.get('edge_type') == edge_type]

    if not type_edges:
        logger.info(f"No {edge_type} edges for cycle detection")
        return []

    subgraph = G.edge_subgraph(type_edges).copy()

    cycles = []
    try:
        for cycle in nx.simple_cycles(subgraph, length_bound=max_length):
            if len(cycle) < min_length:
                continue

            # Calculate total cycle amount
            total = 0
            for i in range(len(cycle)):
                u = cycle[i]
                v = cycle[(i + 1) % len(cycle)]
                if subgraph.has_edge(u, v):
                    total += subgraph[u][v].get('total_amount', 0)

            cycles.append({
                'nodes': cycle,
                'length': len(cycle),
                'total_amount': total,
            })
    except Exception as e:
        logger.warning(f"Cycle detection error (may be too many cycles): {e}")

    cycles.sort(key=lambda c: c['total_amount'], reverse=True)
    logger.info(f"Detected {len(cycles)} cycles (length {min_length}-{max_length})")
    return cycles


# =============================================================================
# Cluster Summary
# =============================================================================

def build_cluster_summary(
    G: nx.DiGraph,
    metrics_df: pd.DataFrame,
    cycles: list,
) -> pd.DataFrame:
    """
    Build summary table for each cluster.

    Returns DataFrame with one row per cluster:
    cluster_id, member_count, company_count, individual_count,
    total_internal_turnover, lead_company_uk, lead_company_name,
    has_cycles, shell_count, anomaly_flags.
    """
    if 'cluster' not in G.nodes[list(G.nodes())[0]] if G.nodes() else True:
        # No cluster assignments
        logger.warning("No cluster assignments found on graph nodes")
        return pd.DataFrame()

    # Build node→cluster mapping and group nodes by cluster
    node_cluster = {}
    clusters = {}
    for node, attrs in G.nodes(data=True):
        cid = attrs.get('cluster', -1)
        node_cluster[node] = cid
        if cid not in clusters:
            clusters[cid] = []
        clusters[cid].append(node)

    # Determine which clusters have cycles
    cycle_clusters = set()
    for cyc in cycles:
        for node in cyc['nodes']:
            if node in node_cluster:
                cycle_clusters.add(node_cluster[node])

    # Single-pass: compute internal turnover and external counterparties per cluster
    internal_turnover = {}
    external_counterparties = {}  # {cluster_id: set of external node ids}
    for u, v, d in G.edges(data=True):
        if d.get('edge_type') != 'transaction':
            continue
        cid_u = node_cluster.get(u, -1)
        cid_v = node_cluster.get(v, -1)
        if cid_u == cid_v and cid_u != -1:
            internal_turnover[cid_u] = internal_turnover.get(cid_u, 0) + d.get('total_amount', 0)
        elif cid_u != -1 and cid_v != cid_u:
            external_counterparties.setdefault(cid_u, set()).add(v)
        if cid_v != -1 and cid_u != cid_v:
            external_counterparties.setdefault(cid_v, set()).add(u)

    # Pre-check shell_score availability
    has_shell_col = 'shell_score' in metrics_df.columns

    records = []
    for cid, members in clusters.items():
        if cid == -1:
            continue

        company_count = 0
        individual_count = 0
        lead_uk = None
        lead_name = ''
        max_pr = -1.0
        shell_count = 0

        for n in members:
            ntype = G.nodes[n].get('node_type', '')
            if ntype == 'company':
                company_count += 1
                # Lead company (max PageRank)
                if n in metrics_df.index:
                    pr = metrics_df.loc[n, 'pagerank']
                    if pr > max_pr:
                        max_pr = pr
                        lead_uk = n
                        lead_name = G.nodes[n].get('name', '')
            elif ntype == 'individual':
                individual_count += 1

            # Shell count
            if has_shell_col and n in metrics_df.index:
                if metrics_df.loc[n, 'shell_score'] >= config.SHELL_SCORE_THRESHOLD:
                    shell_count += 1

        # Anomaly flags
        flags = []
        if shell_count > 0:
            flags.append(f'{shell_count}_shells')
        if cid in cycle_clusters:
            flags.append('has_cycles')

        records.append({
            'cluster_id': cid,
            'member_count': len(members),
            'company_count': company_count,
            'individual_count': individual_count,
            'total_internal_turnover': internal_turnover.get(cid, 0),
            'external_counterparty_count': len(external_counterparties.get(cid, set())),
            'lead_company_uk': lead_uk,
            'lead_company_name': lead_name,
            'has_cycles': cid in cycle_clusters,
            'shell_count': shell_count,
            'anomaly_flags': ', '.join(flags) if flags else '',
        })

    df = pd.DataFrame(records).sort_values('total_internal_turnover', ascending=False)
    logger.info(f"Cluster summary: {len(df)} clusters")
    return df


# =============================================================================
# OKVED Matrix & Diversity (Wave 2)
# =============================================================================

def build_okved_matrix(G: nx.DiGraph) -> pd.DataFrame:
    """
    Build OKVED×OKVED cross-industry turnover matrix from transaction edges.

    Groups transaction edges by (source_okved, target_okved) and aggregates
    total_turnover, edge_count, avg_amount. Excludes nodes with okved_code "00".

    Returns DataFrame with columns: okved_source, okved_target, total_turnover,
    edge_count, avg_amount.
    """
    records = {}
    for u, v, d in G.edges(data=True):
        if d.get('edge_type') != 'transaction':
            continue
        okved_u = G.nodes[u].get('okved_code', '00')
        okved_v = G.nodes[v].get('okved_code', '00')
        if okved_u == '00' or okved_v == '00':
            continue
        key = (okved_u, okved_v)
        if key not in records:
            records[key] = {'total_turnover': 0.0, 'edge_count': 0}
        records[key]['total_turnover'] += d.get('total_amount', 0)
        records[key]['edge_count'] += 1

    rows = []
    for (src, tgt), vals in records.items():
        rows.append({
            'okved_source': src,
            'okved_target': tgt,
            'total_turnover': vals['total_turnover'],
            'edge_count': vals['edge_count'],
            'avg_amount': vals['total_turnover'] / vals['edge_count'] if vals['edge_count'] > 0 else 0.0,
        })

    df = pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=['okved_source', 'okved_target', 'total_turnover', 'edge_count', 'avg_amount']
    )
    logger.info(f"OKVED matrix: {len(df)} OKVED pairs")
    return df


def compute_okved_diversity(G: nx.DiGraph) -> nx.DiGraph:
    """
    Compute OKVED diversity for each node:
    - okved_diversity_count: distinct OKVED codes among transaction counterparties
    - okved_diversity_entropy: Shannon entropy across counterparty OKVED codes
    - is_cross_industry_hub: True if okved_diversity_count in top 10%

    Modifies graph in-place and returns it.
    """
    for node in G.nodes():
        # Collect counterparty OKVED codes (excluding "00")
        okved_counts = {}
        for _, tgt, d in G.out_edges(node, data=True):
            if d.get('edge_type') == 'transaction':
                code = G.nodes[tgt].get('okved_code', '00')
                if code != '00':
                    okved_counts[code] = okved_counts.get(code, 0) + 1
        for src, _, d in G.in_edges(node, data=True):
            if d.get('edge_type') == 'transaction':
                code = G.nodes[src].get('okved_code', '00')
                if code != '00':
                    okved_counts[code] = okved_counts.get(code, 0) + 1

        diversity_count = len(okved_counts)
        G.nodes[node]['okved_diversity_count'] = diversity_count

        # Shannon entropy
        if diversity_count > 0:
            total = sum(okved_counts.values())
            entropy = 0.0
            for count in okved_counts.values():
                p = count / total
                if p > 0:
                    entropy -= p * np.log2(p)
            G.nodes[node]['okved_diversity_entropy'] = entropy
        else:
            G.nodes[node]['okved_diversity_entropy'] = 0.0

    # Cross-industry hub: top 10% by diversity count
    counts = [G.nodes[n]['okved_diversity_count'] for n in G.nodes()]
    if counts:
        threshold = float(np.percentile(counts, 90))
        for node in G.nodes():
            G.nodes[node]['is_cross_industry_hub'] = G.nodes[node]['okved_diversity_count'] > threshold
    else:
        for node in G.nodes():
            G.nodes[node]['is_cross_industry_hub'] = False

    logger.info(f"OKVED diversity computed for {G.number_of_nodes()} nodes")
    return G


# =============================================================================
# Behavioral Segmentation & Look-Alike (Wave 2)
# =============================================================================

def compute_behavioral_features(G: nx.DiGraph) -> pd.DataFrame:
    """
    Compute behavioral feature vector for every node with transaction edges.

    Features:
    - monthly_tx_count_avg: average monthly transaction count
    - monthly_amount_avg: average monthly total amount
    - direction_ratio: outflow / (inflow + outflow); 0.5 if total is 0
    - counterparty_growth_rate: 0.0 (simplified — single-period data)
    - new_counterparty_share: fraction of counterparties in latest period

    Returns DataFrame indexed by client_uk.
    """
    records = []
    for node in G.nodes():
        # Collect monthly tx data
        monthly_counts = {}
        monthly_amounts = {}
        outflow = 0.0
        inflow = 0.0
        counterparties_by_period = {}

        for _, tgt, d in G.out_edges(node, data=True):
            if d.get('edge_type') != 'transaction':
                continue
            outflow += d.get('total_amount', 0)
            # Extract months from dates
            for key in ('first_tx_date', 'last_tx_date'):
                dt = str(d.get(key, ''))
                if len(dt) >= 7 and dt[4] == '-':
                    month = dt[:7]
                    monthly_counts[month] = monthly_counts.get(month, 0) + d.get('tx_count', 1)
                    monthly_amounts[month] = monthly_amounts.get(month, 0) + d.get('total_amount', 0)
                    counterparties_by_period.setdefault(month, set()).add(tgt)

        for src, _, d in G.in_edges(node, data=True):
            if d.get('edge_type') != 'transaction':
                continue
            inflow += d.get('total_amount', 0)
            for key in ('first_tx_date', 'last_tx_date'):
                dt = str(d.get(key, ''))
                if len(dt) >= 7 and dt[4] == '-':
                    month = dt[:7]
                    monthly_counts[month] = monthly_counts.get(month, 0) + d.get('tx_count', 1)
                    monthly_amounts[month] = monthly_amounts.get(month, 0) + d.get('total_amount', 0)
                    counterparties_by_period.setdefault(month, set()).add(src)

        if not monthly_counts:
            continue  # Skip nodes with no tx edges

        n_months = max(len(monthly_counts), 1)
        total_flow = inflow + outflow

        # Counterparty growth rate
        sorted_periods = sorted(counterparties_by_period.keys())
        if len(sorted_periods) >= 2:
            earliest = len(counterparties_by_period[sorted_periods[0]])
            latest = len(counterparties_by_period[sorted_periods[-1]])
            growth_rate = (latest - earliest) / max(earliest, 1)
        else:
            growth_rate = 0.0

        # New counterparty share
        if len(sorted_periods) >= 2:
            all_prev = set()
            for p in sorted_periods[:-1]:
                all_prev |= counterparties_by_period[p]
            latest_cp = counterparties_by_period[sorted_periods[-1]]
            new_cp = latest_cp - all_prev
            new_share = len(new_cp) / max(len(latest_cp), 1)
        else:
            new_share = 0.0

        records.append({
            'client_uk': node,
            'monthly_tx_count_avg': sum(monthly_counts.values()) / n_months,
            'monthly_amount_avg': sum(monthly_amounts.values()) / n_months,
            'direction_ratio': outflow / total_flow if total_flow > 0 else 0.5,
            'counterparty_growth_rate': growth_rate,
            'new_counterparty_share': new_share,
        })

    df = pd.DataFrame(records).set_index('client_uk') if records else pd.DataFrame(
        columns=['monthly_tx_count_avg', 'monthly_amount_avg', 'direction_ratio',
                 'counterparty_growth_rate', 'new_counterparty_share']
    )
    logger.info(f"Behavioral features computed for {len(df)} nodes")
    return df


def cluster_behavioral_segments(
    features_df: pd.DataFrame,
    k_range: tuple = None,
    k_override: int = None,
) -> pd.DataFrame:
    """
    Cluster nodes by behavioral features using K-Means.

    Auto-selects k by silhouette score within k_range, unless k_override is set.
    Applies StandardScaler normalization. Removes zero-variance features with warning.

    Returns features_df with added 'behavioral_segment' column (int).
    """
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import KMeans
    from sklearn.metrics import silhouette_score

    if k_range is None:
        k_range = config.BEHAVIORAL_K_RANGE

    if features_df.empty:
        features_df['behavioral_segment'] = pd.Series(dtype=int)
        return features_df

    feature_cols = [c for c in features_df.columns if c != 'behavioral_segment']
    X = features_df[feature_cols].values.copy()

    # Remove zero-variance features
    variances = np.var(X, axis=0)
    zero_var_mask = variances == 0
    if zero_var_mask.any():
        removed = [feature_cols[i] for i in range(len(feature_cols)) if zero_var_mask[i]]
        logger.warning(f"Removing zero-variance features: {removed}")
        X = X[:, ~zero_var_mask]

    if X.shape[1] == 0:
        logger.warning("All features have zero variance; assigning segment 0")
        features_df['behavioral_segment'] = 0
        return features_df

    # Scale
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Auto-select k or use override
    if k_override is not None:
        best_k = k_override
    else:
        max_k = min(k_range[1], len(features_df) - 1)
        min_k = min(k_range[0], max_k)
        if max_k < 2:
            features_df['behavioral_segment'] = 0
            return features_df

        best_k = min_k
        best_score = -1
        for k in range(min_k, max_k + 1):
            km = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = km.fit_predict(X_scaled)
            if len(set(labels)) < 2:
                continue
            score = silhouette_score(X_scaled, labels)
            if score > best_score:
                best_score = score
                best_k = k

    km = KMeans(n_clusters=best_k, random_state=42, n_init=10)
    features_df['behavioral_segment'] = km.fit_predict(X_scaled)

    logger.info(f"Behavioral segmentation: k={best_k}, {len(features_df)} nodes")
    return features_df


def compute_lookalike_scores(
    features_df: pd.DataFrame,
    G: nx.DiGraph,
    top_decile_col: str = 'total_turnover',
) -> pd.DataFrame:
    """
    Score each node by similarity to the best-client centroid.

    Best clients = top decile by total turnover (total_in_flow + total_out_flow).
    Uses Euclidean distance on StandardScaler-normalized features.
    Score = 1 / (1 + distance), range (0, 1].

    Returns features_df with added 'lookalike_score' column.
    """
    from sklearn.preprocessing import StandardScaler

    if features_df.empty:
        features_df['lookalike_score'] = pd.Series(dtype=float)
        return features_df

    # Compute total turnover for each node in features_df
    turnovers = {}
    for node in features_df.index:
        if node in G.nodes():
            in_flow = G.nodes[node].get('total_in_flow', 0)
            out_flow = G.nodes[node].get('total_out_flow', 0)
            turnovers[node] = in_flow + out_flow
        else:
            turnovers[node] = 0

    turnover_series = pd.Series(turnovers)
    threshold = turnover_series.quantile(1.0 - config.LOOKALIKE_TOP_DECILE)

    best_mask = turnover_series >= threshold
    best_nodes = best_mask[best_mask].index

    if len(best_nodes) == 0:
        logger.warning("No best clients found for look-alike scoring")
        features_df['lookalike_score'] = 0.0
        return features_df

    feature_cols = [c for c in features_df.columns
                    if c not in ('behavioral_segment', 'lookalike_score')]

    X = features_df[feature_cols].values.copy()
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Centroid of best clients
    best_indices = [list(features_df.index).index(n) for n in best_nodes if n in features_df.index]
    centroid = X_scaled[best_indices].mean(axis=0)

    # Euclidean distance to centroid
    distances = np.sqrt(((X_scaled - centroid) ** 2).sum(axis=1))
    features_df['lookalike_score'] = 1.0 / (1.0 + distances)

    logger.info(
        f"Look-alike scores computed: {len(best_nodes)} best clients, "
        f"top score={features_df['lookalike_score'].max():.4f}"
    )
    return features_df
