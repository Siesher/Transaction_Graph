"""
Построение гетерогенного направленного графа из Parquet-данных.

Создаёт NetworkX DiGraph с типизированными узлами и рёбрами,
вычисляет метрики рёбер (доля оборота, реципрокность),
выявляет общих сотрудников между компаниями.
"""

import logging
import math

import networkx as nx
import numpy as np
import pandas as pd

from src import config

logger = logging.getLogger(__name__)


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
            is_liquidated=(str(row.get('liquidation_flag', '')).upper() == 'Y'),
            is_resident=(str(row.get('resident_flag', '')).upper() == 'Y'),
            hop_distance=int(row.get('hop_distance', -1)),
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
