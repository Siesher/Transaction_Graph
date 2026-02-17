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

    roles = []
    for idx, row in df.iterrows():
        total_degree = row['in_degree'] + row['out_degree']

        if row['pagerank'] >= pr_high and row['betweenness'] >= bc_med:
            role = 'parent'
        elif (row['pagerank'] < pr_med
              and row['betweenness'] >= bc_high
              and row['clustering_coef'] < cc_low):
            role = 'shell'
        elif (row['pagerank'] >= pr_med
              and row['betweenness'] < bc_med
              and total_degree >= deg_high):
            role = 'subsidiary'
        elif (row['pagerank'] < pr_med
              and row['betweenness'] >= bc_med
              and total_degree < df['in_degree'].quantile(0.3) + df['out_degree'].quantile(0.3)):
            role = 'conduit'
        else:
            role = 'regular'

        roles.append(role)

    df['role'] = roles
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

    scores = []
    for idx, row in df.iterrows():
        score = 0.0

        # 1. Flow-through ratio > 0.9
        if row['flow_through_ratio'] > 0.9:
            score += config.SHELL_WEIGHT_FLOW_THROUGH

        # 2. No salary payments
        if not row['has_salary_payments']:
            score += config.SHELL_WEIGHT_NO_SALARY

        # 3. High betweenness + low clustering
        if row['betweenness'] > bc_median * 2 and row['clustering_coef'] < 0.1:
            score += config.SHELL_WEIGHT_HIGH_BC_LOW_CC

        # 4. Low unique counterparties
        total_degree = row['in_degree'] + row['out_degree']
        if total_degree <= 4:
            score += config.SHELL_WEIGHT_LOW_COUNTERPARTIES

        # 5. Bursty activity (simplified: check if node has edges in few periods)
        node_edges = [d for _, _, d in G.edges(idx, data=True)
                      if d.get('edge_type') == 'transaction']
        if node_edges:
            periods = set()
            for e in node_edges:
                if 'first_tx_date' in e and 'last_tx_date' in e:
                    periods.add(e.get('first_tx_date', '')[:7])
            if len(periods) <= 1:
                score += config.SHELL_WEIGHT_BURSTY

        scores.append(score)

    df['shell_score'] = scores

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

    # Group nodes by cluster
    clusters = {}
    for node, attrs in G.nodes(data=True):
        cid = attrs.get('cluster', -1)
        if cid not in clusters:
            clusters[cid] = []
        clusters[cid].append(node)

    # Determine which clusters have cycles
    cycle_clusters = set()
    for cyc in cycles:
        for node in cyc['nodes']:
            if node in G:
                cid = G.nodes[node].get('cluster', -1)
                cycle_clusters.add(cid)

    records = []
    for cid, members in clusters.items():
        if cid == -1:
            continue

        companies = [n for n in members if G.nodes[n].get('node_type') == 'company']
        individuals = [n for n in members if G.nodes[n].get('node_type') == 'individual']

        # Internal turnover
        internal_turnover = 0
        for u, v, d in G.edges(data=True):
            if (d.get('edge_type') == 'transaction'
                    and u in members and v in members):
                internal_turnover += d.get('total_amount', 0)

        # Lead company (max PageRank)
        lead_uk = None
        lead_name = ''
        max_pr = -1
        for n in companies:
            if n in metrics_df.index:
                pr = metrics_df.loc[n, 'pagerank']
                if pr > max_pr:
                    max_pr = pr
                    lead_uk = n
                    lead_name = G.nodes[n].get('name', '')

        # Shell count
        shell_count = 0
        if 'shell_score' in metrics_df.columns:
            for n in members:
                if n in metrics_df.index:
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
            'company_count': len(companies),
            'individual_count': len(individuals),
            'total_internal_turnover': internal_turnover,
            'lead_company_uk': lead_uk,
            'lead_company_name': lead_name,
            'has_cycles': cid in cycle_clusters,
            'shell_count': shell_count,
            'anomaly_flags': ', '.join(flags) if flags else '',
        })

    df = pd.DataFrame(records).sort_values('total_internal_turnover', ascending=False)
    logger.info(f"Cluster summary: {len(df)} clusters")
    return df
