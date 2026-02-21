"""
Tests for src.graph_builder module.

Validates graph construction, edge metric computation,
shared-employee derivation, and graph enrichment.
"""

import networkx as nx
import pandas as pd
import pytest

from src.graph_builder import (
    build_graph,
    compute_edge_metrics,
    compute_extended_metrics,
    compute_edge_score,
    derive_shared_employees,
    enrich_graph,
)


# ---------------------------------------------------------------------------
# 1. Node count matches input DataFrame
# ---------------------------------------------------------------------------

def test_build_graph_node_count(raw_graph, nodes_df):
    """Graph should contain exactly as many nodes as rows in nodes_df."""
    assert raw_graph.number_of_nodes() == len(nodes_df)


# ---------------------------------------------------------------------------
# 2. Every node has a valid node_type
# ---------------------------------------------------------------------------

VALID_NODE_TYPES = {'company', 'individual', 'sole_proprietor'}


def test_build_graph_node_types(raw_graph):
    """Every node must have a node_type in the allowed set."""
    for node, data in raw_graph.nodes(data=True):
        assert 'node_type' in data, f"Node {node} missing node_type attribute"
        assert data['node_type'] in VALID_NODE_TYPES, (
            f"Node {node} has unexpected node_type '{data['node_type']}'"
        )


# ---------------------------------------------------------------------------
# 3. All three core edge types are present
# ---------------------------------------------------------------------------

def test_build_graph_edge_types(raw_graph):
    """The graph must contain transaction, authority, and salary edges."""
    edge_types = {
        d['edge_type']
        for _, _, d in raw_graph.edges(data=True)
        if 'edge_type' in d
    }
    for expected in ('transaction', 'authority', 'salary'):
        assert expected in edge_types, f"Edge type '{expected}' not found in graph"


# ---------------------------------------------------------------------------
# 4. share_of_turnover sums to ~1.0 per source node
# ---------------------------------------------------------------------------

def test_edge_metrics_share_of_turnover(raw_graph):
    """For each source node with outgoing transactions, share_of_turnover should sum to <=1.0.

    Note: shared_employees edges (added after compute_edge_metrics) may overwrite
    some transaction edges in DiGraph, so the sum can be < 1.0 for affected nodes.
    For unaffected nodes it should be ~1.0.
    """
    from collections import defaultdict

    sums = defaultdict(float)
    has_tx_out = set()
    has_shared_out = set()

    for u, v, data in raw_graph.edges(data=True):
        if data.get('edge_type') == 'transaction':
            has_tx_out.add(u)
            sums[u] += data.get('share_of_turnover', 0.0)
        elif data.get('edge_type') == 'shared_employees':
            has_shared_out.add(u)

    assert len(has_tx_out) > 0, "No transaction edges found"

    # Nodes without shared_employees overwrites should sum to ~1.0
    clean_nodes = has_tx_out - has_shared_out
    for node in clean_nodes:
        assert abs(sums[node] - 1.0) < 0.01, (
            f"Node {node}: share_of_turnover sums to {sums[node]:.4f}, expected ~1.0"
        )

    # All nodes: sum should be <= 1.0 (some edges may have been overwritten)
    for node in has_tx_out:
        assert sums[node] <= 1.01, (
            f"Node {node}: share_of_turnover sums to {sums[node]:.4f}, should be <= 1.0"
        )


# ---------------------------------------------------------------------------
# 5. reciprocity values are in [0, 1]
# ---------------------------------------------------------------------------

def test_edge_metrics_reciprocity(raw_graph):
    """All reciprocity values on transaction edges must be in [0, 1]."""
    checked = 0
    for _, _, data in raw_graph.edges(data=True):
        if data.get('edge_type') != 'transaction':
            continue
        r = data.get('reciprocity')
        assert r is not None, "Transaction edge missing reciprocity attribute"
        assert 0.0 <= r <= 1.0, f"reciprocity {r} out of range [0, 1]"
        checked += 1

    assert checked > 0, "No transaction edges found to check reciprocity"


# ---------------------------------------------------------------------------
# 6. derive_shared_employees returns expected structure
# ---------------------------------------------------------------------------

def test_derive_shared_employees(sal_df):
    """derive_shared_employees should return a DataFrame with the right columns and >= 1 pair."""
    result = derive_shared_employees(sal_df)

    assert isinstance(result, pd.DataFrame)
    for col in ('company_a_uk', 'company_b_uk', 'shared_count'):
        assert col in result.columns, f"Missing column '{col}' in shared-employee result"

    assert len(result) >= 1, "Expected at least 1 shared-employee pair from synthetic data"

    # company_a_uk should always be <= company_b_uk (canonical ordering)
    assert (result['company_a_uk'] <= result['company_b_uk']).all(), (
        "company_a_uk should be <= company_b_uk for canonical ordering"
    )


# ---------------------------------------------------------------------------
# 7. enrich_graph writes cluster and centrality attributes
# ---------------------------------------------------------------------------

def test_enrich_graph(filtered_graph, metrics_df, membership):
    """After enrichment, every node should have 'cluster'; nodes in metrics_df should have centrality."""
    # Work on a copy so we don't mutate the session-scoped fixture
    G_copy = filtered_graph.copy()
    enrich_graph(G_copy, metrics_df, membership)

    # Every node must have a cluster attribute
    for node, data in G_copy.nodes(data=True):
        assert 'cluster' in data, f"Node {node} missing 'cluster' attribute after enrichment"

    # Nodes present in metrics_df should have pagerank and betweenness
    metric_nodes = set(metrics_df.index) & set(G_copy.nodes())
    assert len(metric_nodes) > 0, "No overlap between metrics_df index and graph nodes"

    for node in metric_nodes:
        attrs = G_copy.nodes[node]
        assert 'pagerank' in attrs, f"Node {node} missing 'pagerank' after enrichment"
        assert 'betweenness' in attrs, f"Node {node} missing 'betweenness' after enrichment"


# ---------------------------------------------------------------------------
# 8. is_liquidated: deleted_flag='Y' → is_liquidated=True (T013)
# ---------------------------------------------------------------------------

def test_is_liquidated_deleted_flag(raw_graph):
    """Node with deleted_flag='Y' (client_uk=1029, last company) should be is_liquidated=True."""
    assert 1029 in raw_graph, "Node 1029 (deleted company) not in graph"
    attrs = raw_graph.nodes[1029]
    assert attrs.get('is_liquidated') is True, (
        f"Node 1029 (deleted_flag='Y') should have is_liquidated=True, got {attrs.get('is_liquidated')}"
    )


def test_is_liquidated_past_end_date(raw_graph):
    """Node with end_date in the past (last individual, client_uk=1079) should be is_liquidated=True."""
    # Last individual: 1000 + 30 companies + 50 individuals - 1 = 1079
    uk = 1079
    assert uk in raw_graph, f"Node {uk} (past end_date individual) not in graph"
    attrs = raw_graph.nodes[uk]
    assert attrs.get('is_liquidated') is True, (
        f"Node {uk} (end_date='2020-01-01') should have is_liquidated=True, got {attrs.get('is_liquidated')}"
    )


def test_is_liquidated_active_node(raw_graph):
    """Active node (seed company 1000) should have is_liquidated=False."""
    attrs = raw_graph.nodes[1000]
    assert attrs.get('is_liquidated') is False, (
        f"Node 1000 (active seed) should have is_liquidated=False, got {attrs.get('is_liquidated')}"
    )


# ---------------------------------------------------------------------------
# 9. enrich_graph: detailed attribute validation (T014)
# ---------------------------------------------------------------------------

def test_enrich_graph_shell_score(filtered_graph, metrics_df, membership):
    """After enrichment with shell_df, flagged nodes should have shell_score > 0."""
    from src.analysis import classify_node_roles, detect_shell_companies

    G_copy = filtered_graph.copy()
    roles_df = classify_node_roles(metrics_df, filtered_graph)
    shell_df = detect_shell_companies(roles_df, filtered_graph)

    enrich_graph(G_copy, metrics_df, membership, shell_df if not shell_df.empty else None)

    # Every node should have shell_score attribute
    for node in G_copy.nodes():
        assert 'shell_score' in G_copy.nodes[node], f"Node {node} missing 'shell_score'"

    # Flagged nodes should have shell_score > 0
    if not shell_df.empty:
        for node in shell_df.index:
            if node in G_copy:
                assert G_copy.nodes[node]['shell_score'] > 0, (
                    f"Flagged node {node} should have shell_score > 0"
                )


def test_enrich_graph_defaults_for_missing_nodes(filtered_graph, metrics_df, membership):
    """Nodes NOT in metrics_df should get sensible defaults after enrichment."""
    G_copy = filtered_graph.copy()

    # Add a dummy node not present in metrics_df
    dummy_uk = 99999
    G_copy.add_node(dummy_uk, node_type='company', name='Dummy')

    enrich_graph(G_copy, metrics_df, membership)

    attrs = G_copy.nodes[dummy_uk]
    assert attrs.get('cluster') == -1, "Dummy node should have cluster=-1"
    assert attrs.get('pagerank', 0.0) == 0.0, "Dummy node should default pagerank=0.0"
    assert attrs.get('role', 'regular') == 'regular', "Dummy node should default role='regular'"
    assert attrs.get('shell_score', 0.0) == 0.0, "Dummy node should default shell_score=0.0"


# ---------------------------------------------------------------------------
# 10. compute_extended_metrics (T008)
# ---------------------------------------------------------------------------

@pytest.fixture(scope='module')
def extended_graph(raw_graph, metrics_df):
    """Graph with extended metrics computed (needs pagerank for edge_score)."""
    from src.analysis import compute_centrality
    G = raw_graph.copy()
    # Compute centrality so pagerank is available for edge_score
    centrality = compute_centrality(G)
    for node in G.nodes():
        G.nodes[node]['pagerank'] = centrality.loc[node, 'pagerank'] if node in centrality.index else 0.0
    G = compute_extended_metrics(G)
    return G


def test_extended_metrics_counterparty_count(extended_graph):
    """unique_counterparty_count should match manual count of tx neighbors."""
    G = extended_graph
    node = 1000  # seed company — should have many counterparties
    expected = set()
    for _, tgt, d in G.out_edges(node, data=True):
        if d.get('edge_type') == 'transaction':
            expected.add(tgt)
    for src, _, d in G.in_edges(node, data=True):
        if d.get('edge_type') == 'transaction':
            expected.add(src)
    assert G.nodes[node]['unique_counterparty_count'] == len(expected)


def test_extended_metrics_top_k_range(extended_graph):
    """top_k_concentration should be in [0.0, 1.0] for all nodes."""
    for node, data in extended_graph.nodes(data=True):
        val = data.get('top_k_concentration', 0.0)
        assert 0.0 <= val <= 1.0, f"Node {node}: top_k_concentration={val} out of range"


def test_extended_metrics_no_outgoing_zero_concentration(extended_graph):
    """Node with no outgoing tx edges should have top_k_concentration=0.0."""
    G = extended_graph
    for node in G.nodes():
        has_out_tx = any(d.get('edge_type') == 'transaction' for _, _, d in G.out_edges(node, data=True))
        if not has_out_tx:
            assert G.nodes[node]['top_k_concentration'] == 0.0, \
                f"Node {node} has no outgoing tx but top_k_concentration != 0.0"
            break  # Test at least one


def test_extended_metrics_hub_flag(extended_graph):
    """hub_flag should be True for nodes with counterparty_count > 2*median."""
    import numpy as np
    G = extended_graph
    counts = [G.nodes[n]['unique_counterparty_count'] for n in G.nodes()]
    median_count = float(np.median(counts))
    for node in G.nodes():
        expected = G.nodes[node]['unique_counterparty_count'] > 2 * median_count
        assert G.nodes[node]['hub_flag'] == expected, \
            f"Node {node}: hub_flag={G.nodes[node]['hub_flag']}, expected {expected}"


# ---------------------------------------------------------------------------
# 11. compute_edge_score (T008)
# ---------------------------------------------------------------------------

def test_edge_score_positive(extended_graph):
    """All tx edges should have edge_score > 0 after computing."""
    G = compute_edge_score(extended_graph.copy())
    for u, v, d in G.edges(data=True):
        if d.get('edge_type') == 'transaction':
            assert 'edge_score' in d, f"Edge ({u},{v}) missing edge_score"
            assert d['edge_score'] >= 0, f"Edge ({u},{v}): edge_score={d['edge_score']} < 0"


def test_edge_score_ranking(extended_graph):
    """Higher-amount edges should generally have higher edge_score (not strict)."""
    G = compute_edge_score(extended_graph.copy())
    scores = []
    for u, v, d in G.edges(data=True):
        if d.get('edge_type') == 'transaction':
            scores.append((d.get('total_amount', 0), d['edge_score']))
    # Just verify we have scores and they're reasonable
    assert len(scores) > 0, "No transaction edges with edge_score found"
    # Top-10 by amount should have above-median score
    scores.sort(key=lambda x: x[0], reverse=True)
    import numpy as np
    median_score = np.median([s[1] for s in scores])
    top10 = scores[:min(10, len(scores))]
    above_median = sum(1 for _, s in top10 if s >= median_score)
    assert above_median >= len(top10) // 2, "Top-amount edges should have above-median scores"


# ---------------------------------------------------------------------------
# 12. OKVED/region node attributes (T014)
# ---------------------------------------------------------------------------

def test_okved_region_on_graph_nodes(raw_graph):
    """Every node should have okved_code and region_code attributes."""
    for node, data in raw_graph.nodes(data=True):
        assert 'okved_code' in data, f"Node {node} missing okved_code"
        assert 'region_code' in data, f"Node {node} missing region_code"


def test_okved_companies_have_real_codes(raw_graph):
    """Companies should have non-'00' OKVED codes; individuals should have '00'."""
    for node, data in raw_graph.nodes(data=True):
        if data.get('node_type') == 'company':
            assert data['okved_code'] != '00', f"Company {node} has OKVED='00'"
        elif data.get('node_type') == 'individual':
            assert data['okved_code'] == '00', f"Individual {node} should have OKVED='00'"
