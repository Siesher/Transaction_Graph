"""
Tests for src.pipeline module.

Validates the end-to-end analysis pipeline on synthetic data.
"""

import networkx as nx
import pandas as pd
import pytest

from src.pipeline import PipelineResult, run_analysis_pipeline


def test_pipeline_returns_result(synthetic_data_dir):
    """run_analysis_pipeline should return a PipelineResult namedtuple."""
    result = run_analysis_pipeline(synthetic_data_dir, min_tx_count=2)
    assert isinstance(result, PipelineResult)


def test_pipeline_graph_enriched(synthetic_data_dir):
    """Pipeline graph should have enriched attributes on every node."""
    result = run_analysis_pipeline(synthetic_data_dir, min_tx_count=2)
    G = result.graph

    assert isinstance(G, nx.DiGraph)
    assert G.number_of_nodes() > 0

    for node, data in G.nodes(data=True):
        assert 'cluster' in data, f"Node {node} missing 'cluster'"
        assert 'shell_score' in data, f"Node {node} missing 'shell_score'"


def test_pipeline_metrics_df(synthetic_data_dir):
    """metrics_df should be indexed by client_uk with centrality columns."""
    result = run_analysis_pipeline(synthetic_data_dir, min_tx_count=2)
    df = result.metrics_df

    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert df.index.name == 'client_uk'

    for col in ('pagerank', 'betweenness', 'role'):
        assert col in df.columns, f"Missing column: {col}"


def test_pipeline_cluster_summary(synthetic_data_dir):
    """cluster_summary should have rows."""
    result = run_analysis_pipeline(synthetic_data_dir, min_tx_count=2)
    assert isinstance(result.cluster_summary, pd.DataFrame)
    assert len(result.cluster_summary) > 0


def test_pipeline_cycles(synthetic_data_dir):
    """Cycles list should be a list (may be empty if disparity filter removes cycle edges)."""
    result = run_analysis_pipeline(synthetic_data_dir, min_tx_count=2)
    assert isinstance(result.cycles, list)
    # Cycle structure validated if any found
    for cyc in result.cycles:
        assert 'nodes' in cyc
        assert 'length' in cyc
        assert 'total_amount' in cyc


def test_pipeline_filter_stats(synthetic_data_dir):
    """filter_stats should have retention rates."""
    result = run_analysis_pipeline(synthetic_data_dir, min_tx_count=2)
    stats = result.filter_stats

    assert 'edge_retention_rate' in stats
    assert 0 <= stats['edge_retention_rate'] <= 1
    assert stats['final_edges'] < stats['original_edges']


# -- Wave 1: Extended Metrics on Pipeline Graph (T023) -------------------------


def test_pipeline_extended_metrics(synthetic_data_dir):
    """Pipeline graph should have extended metrics on nodes."""
    result = run_analysis_pipeline(synthetic_data_dir, min_tx_count=2)
    G = result.graph

    for node, data in G.nodes(data=True):
        assert 'unique_counterparty_count' in data, f"Node {node} missing unique_counterparty_count"
        assert 'hub_flag' in data, f"Node {node} missing hub_flag"


def test_pipeline_edge_score(synthetic_data_dir):
    """Every transaction edge in pipeline graph should have edge_score."""
    result = run_analysis_pipeline(synthetic_data_dir, min_tx_count=2)
    G = result.graph

    for u, v, d in G.edges(data=True):
        if d.get('edge_type') == 'transaction':
            assert 'edge_score' in d, f"Edge ({u},{v}) missing edge_score"
            assert d['edge_score'] >= 0


# -- Wave 2: OKVED Matrix & Behavioral (T023) ---------------------------------


def test_pipeline_okved_matrix(synthetic_data_dir):
    """Pipeline should return an okved_matrix DataFrame."""
    result = run_analysis_pipeline(synthetic_data_dir, min_tx_count=2)
    assert isinstance(result.okved_matrix, pd.DataFrame)
    if not result.okved_matrix.empty:
        for col in ('okved_source', 'okved_target', 'total_turnover'):
            assert col in result.okved_matrix.columns


def test_pipeline_behavioral_df(synthetic_data_dir):
    """Pipeline should return behavioral_df with segments and lookalike scores."""
    result = run_analysis_pipeline(synthetic_data_dir, min_tx_count=2)
    assert isinstance(result.behavioral_df, pd.DataFrame)
    if not result.behavioral_df.empty:
        assert 'behavioral_segment' in result.behavioral_df.columns
        assert 'lookalike_score' in result.behavioral_df.columns


def test_pipeline_cluster_summary_external(synthetic_data_dir):
    """Cluster summary should include external_counterparty_count."""
    result = run_analysis_pipeline(synthetic_data_dir, min_tx_count=2)
    if not result.cluster_summary.empty:
        assert 'external_counterparty_count' in result.cluster_summary.columns
