"""
Shared pytest fixtures for Transaction Graph tests.

All fixtures use synthetic data — no Spark, Hive, or network access.
Session-scoped to generate data once per test run.
"""

import os
import sys

import pytest

# Ensure src/ is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


@pytest.fixture(scope='session')
def synthetic_data_dir(tmp_path_factory):
    """Generate synthetic data once per session, return path to output dir."""
    from src.synthetic import generate_synthetic_data

    out = str(tmp_path_factory.mktemp('synth_data'))
    generate_synthetic_data(output_dir=out, seed=42)
    return out


@pytest.fixture(scope='session')
def nodes_df(synthetic_data_dir):
    import pandas as pd
    nodes = pd.read_parquet(os.path.join(synthetic_data_dir, 'nodes.parquet'))
    hop = pd.read_parquet(os.path.join(synthetic_data_dir, 'hop_distances.parquet'))
    return nodes.merge(hop, on='client_uk', how='left')


@pytest.fixture(scope='session')
def tx_df(synthetic_data_dir):
    import pandas as pd
    return pd.read_parquet(os.path.join(synthetic_data_dir, 'transaction_edges.parquet'))


@pytest.fixture(scope='session')
def auth_df(synthetic_data_dir):
    import pandas as pd
    return pd.read_parquet(os.path.join(synthetic_data_dir, 'authority_edges.parquet'))


@pytest.fixture(scope='session')
def sal_df(synthetic_data_dir):
    import pandas as pd
    return pd.read_parquet(os.path.join(synthetic_data_dir, 'salary_edges.parquet'))


@pytest.fixture(scope='session')
def raw_graph(nodes_df, tx_df, auth_df, sal_df):
    """Build raw graph with edge metrics and shared employees."""
    from src.graph_builder import build_graph, compute_edge_metrics, derive_shared_employees

    G = build_graph(nodes_df, tx_df, auth_df, sal_df)
    G = compute_edge_metrics(G)

    shared_df = derive_shared_employees(sal_df)
    for _, row in shared_df.iterrows():
        a, b = int(row['company_a_uk']), int(row['company_b_uk'])
        if a in G and b in G:
            G.add_edge(a, b, edge_type='shared_employees',
                       shared_count=int(row['shared_count']),
                       weight=float(row['shared_count']))
            G.add_edge(b, a, edge_type='shared_employees',
                       shared_count=int(row['shared_count']),
                       weight=float(row['shared_count']))
    return G


@pytest.fixture(scope='session')
def filtered_graph(raw_graph):
    """Apply filter pipeline to raw graph."""
    from src.filters import apply_filter_pipeline

    filtered, stats = apply_filter_pipeline(raw_graph, min_tx_count=2, alpha=0.05)
    return filtered


@pytest.fixture(scope='session')
def filter_stats(raw_graph):
    """Filter stats from pipeline."""
    from src.filters import apply_filter_pipeline

    _, stats = apply_filter_pipeline(raw_graph, min_tx_count=2, alpha=0.05)
    return stats


@pytest.fixture(scope='session')
def membership(filtered_graph):
    """Leiden clustering membership dict."""
    from src.analysis import run_leiden_clustering

    mem, gamma = run_leiden_clustering(filtered_graph)
    return mem


@pytest.fixture(scope='session')
def best_gamma(filtered_graph):
    """Best gamma from Leiden clustering."""
    from src.analysis import run_leiden_clustering

    _, gamma = run_leiden_clustering(filtered_graph)
    return gamma


@pytest.fixture(scope='session')
def metrics_df(filtered_graph):
    """Centrality metrics DataFrame."""
    from src.analysis import compute_centrality

    return compute_centrality(filtered_graph)
