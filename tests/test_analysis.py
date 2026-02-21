"""
Tests for src.analysis module.

Covers: Leiden clustering, centrality metrics, node role classification,
shell company detection, cycle detection, and cluster summary.

Uses session-scoped fixtures from conftest.py (filtered_graph, metrics_df, membership).
"""

import copy

import pandas as pd
import pytest

from src.analysis import (
    build_cluster_summary,
    build_okved_matrix,
    classify_node_roles,
    cluster_behavioral_segments,
    compute_behavioral_features,
    compute_centrality,
    compute_lookalike_scores,
    compute_okved_diversity,
    detect_cycles,
    detect_shell_companies,
    run_leiden_clustering,
)
from src import config


# -- Leiden Clustering ---------------------------------------------------------


def test_leiden_returns_all_nodes(membership, filtered_graph):
    """Every node in filtered_graph must appear in the membership dict."""
    for node in filtered_graph.nodes():
        assert node in membership, f"Node {node} missing from membership dict"


def test_leiden_at_least_one_cluster(membership):
    """At least one non-negative cluster id must exist."""
    positive_clusters = {v for v in membership.values() if v >= 0}
    assert len(positive_clusters) >= 1, "Expected at least 1 cluster with id >= 0"


# -- Centrality Metrics -------------------------------------------------------


EXPECTED_CENTRALITY_COLUMNS = [
    "pagerank",
    "betweenness",
    "clustering_coef",
    "in_degree",
    "out_degree",
    "total_in_flow",
    "total_out_flow",
    "flow_through_ratio",
    "has_salary_payments",
]


def test_centrality_columns(metrics_df):
    """Verify all expected centrality columns are present."""
    for col in EXPECTED_CENTRALITY_COLUMNS:
        assert col in metrics_df.columns, f"Missing column: {col}"


def test_centrality_all_nodes(metrics_df, filtered_graph):
    """metrics_df index must cover every node in filtered_graph."""
    graph_nodes = set(filtered_graph.nodes())
    index_nodes = set(metrics_df.index)
    missing = graph_nodes - index_nodes
    assert not missing, f"Nodes missing from metrics_df: {missing}"


# -- Node Role Classification -------------------------------------------------

VALID_ROLES = {"parent", "shell", "subsidiary", "conduit", "regular"}


def test_classify_roles_valid(metrics_df, filtered_graph):
    """classify_node_roles must add a 'role' column with only valid values."""
    roles_df = classify_node_roles(metrics_df, filtered_graph)

    assert "role" in roles_df.columns, "'role' column not added"
    invalid = set(roles_df["role"].unique()) - VALID_ROLES
    assert not invalid, f"Invalid roles found: {invalid}"


# -- Shell Company Detection ---------------------------------------------------


def test_detect_shell_companies(metrics_df, filtered_graph):
    """detect_shell_companies must return a DataFrame with shell_score >= threshold."""
    roles_df = classify_node_roles(metrics_df, filtered_graph)
    flagged = detect_shell_companies(roles_df, filtered_graph)

    assert isinstance(flagged, pd.DataFrame), "Expected a DataFrame"
    assert "shell_score" in flagged.columns, "Missing 'shell_score' column"

    if not flagged.empty:
        assert (flagged["shell_score"] >= config.SHELL_SCORE_THRESHOLD).all(), (
            "All returned rows must have shell_score >= threshold"
        )


# -- Cycle Detection -----------------------------------------------------------


def test_detect_cycles(filtered_graph):
    """detect_cycles must return a list of dicts with expected keys."""
    cycles = detect_cycles(filtered_graph)

    assert isinstance(cycles, list), "Expected a list"
    for cyc in cycles:
        assert "nodes" in cyc, "Cycle dict missing 'nodes' key"
        assert "length" in cyc, "Cycle dict missing 'length' key"
        assert "total_amount" in cyc, "Cycle dict missing 'total_amount' key"


# -- Cluster Summary -----------------------------------------------------------

EXPECTED_SUMMARY_COLUMNS = [
    "cluster_id",
    "member_count",
    "company_count",
    "individual_count",
    "total_internal_turnover",
    "external_counterparty_count",
    "lead_company_uk",
    "lead_company_name",
    "has_cycles",
    "shell_count",
    "anomaly_flags",
]


def test_build_cluster_summary(filtered_graph, metrics_df, membership):
    """build_cluster_summary must produce a DataFrame with expected columns."""
    G = copy.deepcopy(filtered_graph)

    # Enrich graph nodes with cluster assignments
    for node, cid in membership.items():
        if node in G:
            G.nodes[node]["cluster"] = cid

    # Prepare inputs
    roles_df = classify_node_roles(metrics_df, G)
    flagged = detect_shell_companies(roles_df, G)

    # Merge shell_score back into roles_df so build_cluster_summary can see it
    if not flagged.empty and "shell_score" in flagged.columns:
        roles_df = roles_df.copy()
        roles_df["shell_score"] = 0.0
        for idx in flagged.index:
            if idx in roles_df.index:
                roles_df.loc[idx, "shell_score"] = flagged.loc[idx, "shell_score"]

    cycles = detect_cycles(G)
    summary = build_cluster_summary(G, roles_df, cycles)

    assert isinstance(summary, pd.DataFrame), "Expected a DataFrame"

    # If there are clusters, check columns
    if not summary.empty:
        for col in EXPECTED_SUMMARY_COLUMNS:
            assert col in summary.columns, f"Missing summary column: {col}"


# -- External Counterparty Count (T010) ----------------------------------------


def test_external_counterparty_count_nonnegative(filtered_graph, metrics_df, membership):
    """external_counterparty_count should be non-negative for all clusters."""
    import copy
    G = copy.deepcopy(filtered_graph)
    for node, cid in membership.items():
        if node in G:
            G.nodes[node]["cluster"] = cid
    roles_df = classify_node_roles(metrics_df, G)
    cycles = detect_cycles(G)
    summary = build_cluster_summary(G, roles_df, cycles)

    if not summary.empty:
        assert (summary['external_counterparty_count'] >= 0).all(), \
            "external_counterparty_count should be non-negative"


# -- OKVED Matrix & Diversity (T017) -------------------------------------------


def test_build_okved_matrix(raw_graph):
    """OKVED matrix should have non-negative values and expected columns."""
    from src.graph_builder import compute_extended_metrics
    G = raw_graph.copy()
    matrix = build_okved_matrix(G)

    assert isinstance(matrix, pd.DataFrame)
    for col in ('okved_source', 'okved_target', 'total_turnover', 'edge_count', 'avg_amount'):
        assert col in matrix.columns, f"Missing column: {col}"

    if not matrix.empty:
        assert (matrix['total_turnover'] >= 0).all()
        assert (matrix['edge_count'] > 0).all()
        # "00" should not appear
        assert '00' not in matrix['okved_source'].values
        assert '00' not in matrix['okved_target'].values


def test_okved_diversity(raw_graph):
    """OKVED diversity metrics should be computed for all nodes."""
    G = raw_graph.copy()
    G = compute_okved_diversity(G)

    for node, data in G.nodes(data=True):
        assert 'okved_diversity_count' in data, f"Node {node} missing okved_diversity_count"
        assert 'okved_diversity_entropy' in data, f"Node {node} missing okved_diversity_entropy"
        assert 'is_cross_industry_hub' in data, f"Node {node} missing is_cross_industry_hub"
        assert data['okved_diversity_count'] >= 0
        assert data['okved_diversity_entropy'] >= 0
        assert isinstance(data['is_cross_industry_hub'], bool)


# -- Behavioral Features, Clustering, Look-Alike (T021) -----------------------


def test_behavioral_features(raw_graph):
    """Behavioral features should be computed for nodes with tx edges."""
    features = compute_behavioral_features(raw_graph)
    assert isinstance(features, pd.DataFrame)
    assert len(features) > 0, "Should have features for at least some nodes"

    expected_cols = [
        'monthly_tx_count_avg', 'monthly_amount_avg', 'direction_ratio',
        'counterparty_growth_rate', 'new_counterparty_share',
    ]
    for col in expected_cols:
        assert col in features.columns, f"Missing feature: {col}"

    # direction_ratio in [0, 1]
    assert (features['direction_ratio'] >= 0).all()
    assert (features['direction_ratio'] <= 1).all()


def test_cluster_behavioral_segments(raw_graph):
    """K-Means clustering should assign a segment to every node."""
    features = compute_behavioral_features(raw_graph)
    result = cluster_behavioral_segments(features, k_override=3)

    assert 'behavioral_segment' in result.columns
    assert result['behavioral_segment'].notna().all()
    # Segments should be 0, 1, or 2
    assert set(result['behavioral_segment'].unique()).issubset({0, 1, 2})


def test_lookalike_scores(raw_graph):
    """Look-alike scores should be in (0, 1] for all nodes."""
    from src.graph_builder import compute_edge_metrics
    G = raw_graph.copy()
    G = compute_edge_metrics(G)

    features = compute_behavioral_features(G)
    features = compute_lookalike_scores(features, G)

    assert 'lookalike_score' in features.columns
    if not features.empty:
        assert (features['lookalike_score'] > 0).all(), "lookalike_score should be > 0"
        assert (features['lookalike_score'] <= 1.0).all(), "lookalike_score should be <= 1"
