"""
Построение гетерогенного направленного графа из Parquet-данных.

Создаёт NetworkX DiGraph с типизированными узлами и рёбрами,
вычисляет метрики рёбер (доля оборота, реципрокность),
выявляет общих сотрудников между компаниями.
"""

import logging
import math
from datetime import date

import networkx as nx
import numpy as np
import pandas as pd

from src import config

logger = logging.getLogger(__name__)


def _is_liquidated(row) -> bool:
    """Derive liquidation/closure status from deleted_flag and end_date."""
    if str(row.get('deleted_flag', '')).upper() == 'Y':
        return True
    end_date = row.get('end_date', None)
    if end_date is not None and end_date is not pd.NaT:
        try:
            if isinstance(end_date, str):
                if end_date.startswith('5999'):
                    return False
                end_date = pd.Timestamp(end_date)
            if pd.notna(end_date) and end_date.date() < date.today():
                return True
        except (ValueError, TypeError, AttributeError):
            pass
    return False


def build_graph(
    nodes_df: pd.DataFrame,
    transaction_edges_df: pd.DataFrame,
    authority_edges_df: pd.DataFrame,
    salary_edges_df: pd.DataFrame,
) -> nx.DiGraph:
    """
    Build heterogeneous directed graph from extracted data.

    Node attributes: client_uk, name, node_type, inn, status, is_liquidated, hop_distance.
    Edge attributes: edge_type, weight, + type-specific attributes.

    Returns NetworkX DiGraph with all nodes and edges.
    """
    G = nx.DiGraph()

    # --- Add nodes ---
    for _, row in nodes_df.iterrows():
        client_uk = int(row['client_uk'])

        # Map client type name to internal type
        type_name = str(row.get('client_type_name', ''))
        node_type = config.CLIENT_TYPE_MAP.get(type_name, config.DEFAULT_NODE_TYPE)

        name = row.get('client_name', '') or ''
        first = row.get('first_name', '') or ''
        if first and node_type == 'individual':
            name = f"{name} {first}".strip()

        G.add_node(client_uk,
            client_uk=client_uk,
            name=name,
            node_type=node_type,
            inn=row.get('inn', ''),
            status=row.get('client_status_name', ''),
            is_liquidated=_is_liquidated(row),
            is_resident=(str(row.get('resident_flag', '')).upper() == 'Y'),
            hop_distance=int(row.get('hop_distance', -1)),
            okved_code=str(row.get('okved_code', config.DEFAULT_OKVED_CODE) or config.DEFAULT_OKVED_CODE),
            region_code=str(row.get('region_code', config.DEFAULT_REGION_CODE) or config.DEFAULT_REGION_CODE),
        )

    logger.info(f"Added {G.number_of_nodes()} nodes")

    # --- Add transaction edges ---
    # Aggregate across periods: sum amounts, sum counts per (source, target)
    if len(transaction_edges_df) > 0:
        tx_agg = transaction_edges_df.groupby(
            ['source_client_uk', 'target_client_uk']
        ).agg({
            'total_amount': 'sum',
            'tx_count': 'sum',
            'avg_amount': 'mean',
            'std_amount': 'mean',
            'max_amount': 'max',
            'min_amount': 'min',
            'first_tx_date': 'min',
            'last_tx_date': 'max',
            'period': 'nunique',
        }).reset_index()
        tx_agg.rename(columns={'period': 'n_periods'}, inplace=True)

        for _, row in tx_agg.iterrows():
            src = int(row['source_client_uk'])
            tgt = int(row['target_client_uk'])
            if src not in G or tgt not in G:
                continue
            amount = float(row['total_amount'])
            G.add_edge(src, tgt,
                edge_type='transaction',
                total_amount=amount,
                tx_count=int(row['tx_count']),
                avg_amount=float(row['avg_amount']),
                std_amount=float(row['std_amount']) if not pd.isna(row['std_amount']) else 0.0,
                max_amount=float(row['max_amount']),
                min_amount=float(row['min_amount']),
                first_tx_date=str(row['first_tx_date']),
                last_tx_date=str(row['last_tx_date']),
                n_periods=int(row['n_periods']),
                weight=math.log1p(amount),
            )

    tx_count = sum(1 for _, _, d in G.edges(data=True) if d.get('edge_type') == 'transaction')
    logger.info(f"Added {tx_count} transaction edges")

    # --- Add authority edges ---
    if len(authority_edges_df) > 0:
        # Deduplicate: one edge per (company, representative)
        auth_dedup = authority_edges_df.drop_duplicates(
            subset=['company_client_uk', 'representative_client_uk']
        )
        for _, row in auth_dedup.iterrows():
            company = int(row['company_client_uk'])
            rep = int(row['representative_client_uk'])
            if company not in G or rep not in G:
                continue
            G.add_edge(company, rep,
                edge_type='authority',
                authority_uk=int(row.get('authority_uk', 0)),
                is_active=bool(row.get('is_active', False)),
                weight=1.0,
            )

    auth_count = sum(1 for _, _, d in G.edges(data=True) if d.get('edge_type') == 'authority')
    logger.info(f"Added {auth_count} authority edges")

    # --- Add salary edges ---
    if len(salary_edges_df) > 0:
        # Deduplicate: one edge per (employer, employee)
        sal_dedup = salary_edges_df.drop_duplicates(
            subset=['employer_client_uk', 'employee_client_uk']
        )
        for _, row in sal_dedup.iterrows():
            emp = int(row['employer_client_uk'])
            ee = int(row['employee_client_uk'])
            if emp not in G or ee not in G:
                continue
            G.add_edge(emp, ee,
                edge_type='salary',
                deal_uk=int(row.get('deal_uk', 0)),
                is_active=bool(row.get('is_active', False)),
                weight=1.0,
            )

    sal_count = sum(1 for _, _, d in G.edges(data=True) if d.get('edge_type') == 'salary')
    logger.info(f"Added {sal_count} salary edges")

    return G


def compute_edge_metrics(G: nx.DiGraph) -> nx.DiGraph:
    """
    Compute derived metrics for transaction edges:
    - share_of_turnover: edge weight / total outgoing weight of source
    - reciprocity: min(forward, reverse) / max(forward, reverse)
    - weight: log(1 + total_amount) for algorithm use

    Modifies graph in-place and returns it.
    """
    # Compute total outgoing amount per node (transaction edges only)
    out_totals = {}
    in_totals = {}
    for u, v, data in G.edges(data=True):
        if data.get('edge_type') != 'transaction':
            continue
        amount = data.get('total_amount', 0)
        out_totals[u] = out_totals.get(u, 0) + amount
        in_totals[v] = in_totals.get(v, 0) + amount

    for u, v, data in G.edges(data=True):
        if data.get('edge_type') != 'transaction':
            continue

        amount = data.get('total_amount', 0)

        # Share of turnover (outgoing perspective)
        s_out = out_totals.get(u, 0)
        data['share_of_turnover'] = amount / s_out if s_out > 0 else 0.0

        # Reciprocity
        if G.has_edge(v, u):
            reverse_data = G[v][u]
            if reverse_data.get('edge_type') == 'transaction':
                rev_amount = reverse_data.get('total_amount', 0)
                max_val = max(amount, rev_amount)
                data['reciprocity'] = min(amount, rev_amount) / max_val if max_val > 0 else 0.0
            else:
                data['reciprocity'] = 0.0
        else:
            data['reciprocity'] = 0.0

    # Store totals on nodes
    for node in G.nodes():
        G.nodes[node]['total_out_flow'] = out_totals.get(node, 0)
        G.nodes[node]['total_in_flow'] = in_totals.get(node, 0)

    logger.info("Edge metrics computed (share_of_turnover, reciprocity)")
    return G


def derive_shared_employees(
    salary_edges_df: pd.DataFrame,
    min_shared: int = 1,
) -> pd.DataFrame:
    """
    Derive shared-employee edges between companies.

    For each pair of companies, count employees appearing in both.
    Returns DataFrame: company_a_uk, company_b_uk, shared_count, shared_employees.
    """
    if len(salary_edges_df) == 0:
        return pd.DataFrame(
            columns=['company_a_uk', 'company_b_uk', 'shared_count', 'shared_employees']
        )

    # Group employees by employer
    employer_employees = (
        salary_edges_df
        .groupby('employer_client_uk')['employee_client_uk']
        .apply(set)
        .to_dict()
    )

    employers = list(employer_employees.keys())
    results = []

    for i in range(len(employers)):
        for j in range(i + 1, len(employers)):
            a, b = employers[i], employers[j]
            shared = employer_employees[a] & employer_employees[b]
            if len(shared) >= min_shared:
                results.append({
                    'company_a_uk': min(int(a), int(b)),
                    'company_b_uk': max(int(a), int(b)),
                    'shared_count': len(shared),
                    'shared_employees': list(shared),
                })

    df = pd.DataFrame(results)
    logger.info(f"Found {len(df)} shared-employee pairs")
    return df


def enrich_graph(
    G: nx.DiGraph,
    metrics_df: pd.DataFrame,
    membership: dict,
    shell_df: pd.DataFrame = None,
) -> nx.DiGraph:
    """
    Write analysis results back as node attributes on the graph.

    Bridges the gap between analysis (DataFrame outputs) and visualization
    (reads node attributes). Modifies graph in-place and returns it.

    Args:
        G: graph to enrich
        metrics_df: DataFrame indexed by client_uk with centrality/role columns
        membership: {node_id: cluster_id} from Leiden clustering
        shell_df: optional DataFrame with shell_score column
    """
    # Write cluster assignments
    for node in G.nodes():
        G.nodes[node]['cluster'] = membership.get(node, -1)

    # Write centrality metrics and role from metrics_df
    metric_cols = [
        'pagerank', 'betweenness', 'clustering_coef',
        'in_degree', 'out_degree',
        'total_in_flow', 'total_out_flow', 'flow_through_ratio',
        'has_salary_payments', 'role',
    ]
    for node in G.nodes():
        if node in metrics_df.index:
            row = metrics_df.loc[node]
            for col in metric_cols:
                if col in metrics_df.columns:
                    val = row[col]
                    if isinstance(val, (np.bool_, np.integer, np.floating)):
                        val = val.item()
                    G.nodes[node][col] = val
        else:
            # Defaults for nodes not in metrics
            G.nodes[node].setdefault('pagerank', 0.0)
            G.nodes[node].setdefault('betweenness', 0.0)
            G.nodes[node].setdefault('role', 'regular')

    # Write shell scores
    if shell_df is not None and 'shell_score' in shell_df.columns:
        for node in G.nodes():
            if node in shell_df.index:
                G.nodes[node]['shell_score'] = float(shell_df.loc[node, 'shell_score'])
            else:
                G.nodes[node].setdefault('shell_score', 0.0)
    else:
        for node in G.nodes():
            G.nodes[node].setdefault('shell_score', 0.0)

    logger.info(f"Enriched {G.number_of_nodes()} nodes with analysis results")
    return G


def compute_extended_metrics(G: nx.DiGraph) -> nx.DiGraph:
    """
    Compute extended node metrics on a graph with edge metrics already applied.

    Sets node attributes:
    - unique_counterparty_count: distinct transaction counterparties (in + out)
    - top_k_concentration: share of top-K outgoing amounts / total outgoing
    - active_months: distinct YYYY-MM from tx edge dates
    - hub_flag: True if unique_counterparty_count > 2 * median
    """
    k = config.TOP_K_COUNTERPARTIES

    for node in G.nodes():
        # Unique counterparties via transaction edges (both directions)
        counterparties = set()
        out_amounts = []
        months = set()

        for _, tgt, d in G.out_edges(node, data=True):
            if d.get('edge_type') == 'transaction':
                counterparties.add(tgt)
                out_amounts.append(d.get('total_amount', 0))
                _collect_months(d, months)

        for src, _, d in G.in_edges(node, data=True):
            if d.get('edge_type') == 'transaction':
                counterparties.add(src)
                _collect_months(d, months)

        G.nodes[node]['unique_counterparty_count'] = len(counterparties)

        # Top-K concentration
        if out_amounts:
            total_out = sum(out_amounts)
            top_k_sum = sum(sorted(out_amounts, reverse=True)[:k])
            G.nodes[node]['top_k_concentration'] = top_k_sum / total_out if total_out > 0 else 0.0
        else:
            G.nodes[node]['top_k_concentration'] = 0.0

        G.nodes[node]['active_months'] = max(len(months), 0)

    # Hub flag: > 2 * median counterparty count
    counts = [G.nodes[n]['unique_counterparty_count'] for n in G.nodes()]
    median_count = float(np.median(counts)) if counts else 0.0
    hub_threshold = 2 * median_count

    for node in G.nodes():
        G.nodes[node]['hub_flag'] = G.nodes[node]['unique_counterparty_count'] > hub_threshold

    hub_count = sum(1 for n in G.nodes() if G.nodes[n]['hub_flag'])
    logger.info(
        f"Extended metrics computed: {hub_count} hubs "
        f"(threshold > {hub_threshold:.0f} counterparties)"
    )
    return G


def _collect_months(edge_data: dict, months: set):
    """Extract YYYY-MM from first_tx_date and last_tx_date into months set."""
    for key in ('first_tx_date', 'last_tx_date'):
        dt_str = str(edge_data.get(key, ''))
        if len(dt_str) >= 7 and dt_str[4] == '-':
            months.add(dt_str[:7])


def compute_edge_score(
    G: nx.DiGraph,
    w_base: float = None,
    w_bilateral: float = None,
    w_node: float = None,
    w_stability: float = None,
) -> nx.DiGraph:
    """
    Compute composite edge_score for every transaction edge.

    Prerequisites: compute_edge_metrics() and compute_extended_metrics()
    must have been called (needs share_of_turnover, active_months, pagerank).
    """
    if w_base is None:
        w_base = config.EDGE_SCORE_W_BASE
    if w_bilateral is None:
        w_bilateral = config.EDGE_SCORE_W_BILATERAL
    if w_node is None:
        w_node = config.EDGE_SCORE_W_NODE
    if w_stability is None:
        w_stability = config.EDGE_SCORE_W_STABILITY

    # Collect all tx edge amounts for rank-percentile normalization
    tx_edges = []
    for u, v, d in G.edges(data=True):
        if d.get('edge_type') == 'transaction':
            tx_edges.append((u, v, d.get('total_amount', 0)))

    if not tx_edges:
        logger.warning("No transaction edges for edge_score computation")
        return G

    # Rank-percentile for amounts
    amounts = np.array([e[2] for e in tx_edges])
    ranks = np.argsort(np.argsort(amounts)).astype(float)
    norm_amounts = ranks / max(len(ranks) - 1, 1)
    amount_rank = {(tx_edges[i][0], tx_edges[i][1]): norm_amounts[i]
                   for i in range(len(tx_edges))}

    # Max pagerank for normalization
    max_pr = max(
        (G.nodes[n].get('pagerank', 0) for n in G.nodes()),
        default=1.0,
    )
    if max_pr == 0:
        max_pr = 1.0

    # Max active_months for stability normalization
    max_months = max(
        (G.nodes[n].get('active_months', 1) for n in G.nodes()),
        default=1,
    )
    if max_months == 0:
        max_months = 1

    # Compute incoming totals for bilateral_share (target perspective)
    in_totals = {}
    for u, v, d in G.edges(data=True):
        if d.get('edge_type') == 'transaction':
            in_totals[v] = in_totals.get(v, 0) + d.get('total_amount', 0)

    for u, v, d in G.edges(data=True):
        if d.get('edge_type') != 'transaction':
            continue

        norm_amount = amount_rank.get((u, v), 0.0)

        # bilateral_share: product of source outgoing share × target incoming share
        src_share = d.get('share_of_turnover', 0.0)
        tgt_in_total = in_totals.get(v, 0)
        tgt_share = d.get('total_amount', 0) / tgt_in_total if tgt_in_total > 0 else 0.0
        bilateral = src_share * tgt_share

        # node_importance: average normalized pagerank of both endpoints
        pr_u = G.nodes[u].get('pagerank', 0)
        pr_v = G.nodes[v].get('pagerank', 0)
        node_imp = (pr_u + pr_v) / (2 * max_pr)

        # stability_factor
        am_u = G.nodes[u].get('active_months', 0)
        am_v = G.nodes[v].get('active_months', 0)
        stability = min(am_u, am_v) / max_months

        d['edge_score'] = (
            w_base * norm_amount
            + w_bilateral * bilateral
            + w_node * node_imp
            + w_stability * stability
        )

    logger.info(f"Edge scores computed for {len(tx_edges)} transaction edges")
    return G


def get_graph_stats(G: nx.DiGraph) -> dict:
    """
    Return basic graph statistics:
    - node_count, edge_count, by node_type, by edge_type
    - connected_components count
    - density
    """
    # Node type counts
    node_types = {}
    for _, data in G.nodes(data=True):
        t = data.get('node_type', 'unknown')
        node_types[t] = node_types.get(t, 0) + 1

    # Edge type counts
    edge_types = {}
    for _, _, data in G.edges(data=True):
        t = data.get('edge_type', 'unknown')
        edge_types[t] = edge_types.get(t, 0) + 1

    # Components (undirected view)
    n_components = nx.number_weakly_connected_components(G)

    return {
        'node_count': G.number_of_nodes(),
        'edge_count': G.number_of_edges(),
        'nodes_by_type': node_types,
        'edges_by_type': edge_types,
        'weakly_connected_components': n_components,
        'density': nx.density(G),
    }
