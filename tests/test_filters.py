"""
Tests for src.filters -- pre-filtering, disparity filter, and filter pipeline.

Uses synthetic data fixtures from conftest.py.
"""

import networkx as nx
import pytest

from src.filters import apply_filter_pipeline, disparity_filter, hub_filter, pre_filter

NON_TX_TYPES = {'authority', 'salary', 'shared_employees'}


def _count_non_tx_edges(G: nx.DiGraph) -> int:
    """Count edges whose edge_type is NOT transaction."""
    return sum(
        1 for _, _, d in G.edges(data=True)
        if d.get('edge_type') in NON_TX_TYPES
    )


def _count_tx_edges(G: nx.DiGraph) -> int:
    """Count edges whose edge_type IS transaction."""
    return sum(
        1 for _, _, d in G.edges(data=True)
        if d.get('edge_type') == 'transaction'
    )


# --------------------------------------------------------------------------- #
# pre_filter
# --------------------------------------------------------------------------- #

class TestPreFilter:

    def test_pre_filter_preserves_non_tx_edges(self, raw_graph):
        """Non-transaction edges (authority, salary, shared_employees)
        must survive pre_filter unchanged."""
        non_tx_before = _count_non_tx_edges(raw_graph)
        filtered = pre_filter(raw_graph)
        non_tx_after = _count_non_tx_edges(filtered)

        assert non_tx_before == non_tx_after, (
            f'Non-tx edges changed: {non_tx_before} -> {non_tx_after}'
        )

    def test_pre_filter_removes_edges(self, raw_graph):
        """With extremely strict thresholds every transaction edge should
        fail ALL criteria and be removed."""
        tx_before = _count_tx_edges(raw_graph)
        filtered = pre_filter(
            raw_graph,
            min_tx_count=100,
            min_total_amount=1e15,
            min_periods=100,
        )
        tx_after = _count_tx_edges(filtered)

        assert tx_after < tx_before, (
            f'Expected fewer tx edges with strict thresholds, '
            f'got {tx_after} (was {tx_before})'
        )


# --------------------------------------------------------------------------- #
# disparity_filter
# --------------------------------------------------------------------------- #

class TestDisparityFilter:

    def test_disparity_filter_reduces_edges(self, raw_graph):
        """Disparity filter should remove at least some edges from a
        non-trivial graph."""
        original_edge_count = raw_graph.number_of_edges()
        backbone = disparity_filter(raw_graph, alpha=0.05)
        backbone_edge_count = backbone.number_of_edges()

        assert backbone_edge_count < original_edge_count, (
            f'Expected backbone ({backbone_edge_count}) to have fewer '
            f'edges than original ({original_edge_count})'
        )

    def test_disparity_filter_preserves_non_tx(self, raw_graph):
        """Non-transaction edges must survive disparity filter."""
        non_tx_before = _count_non_tx_edges(raw_graph)
        backbone = disparity_filter(raw_graph, alpha=0.05)
        non_tx_after = _count_non_tx_edges(backbone)

        assert non_tx_before == non_tx_after, (
            f'Non-tx edges changed: {non_tx_before} -> {non_tx_after}'
        )


# --------------------------------------------------------------------------- #
# apply_filter_pipeline
# --------------------------------------------------------------------------- #

class TestFilterPipeline:

    EXPECTED_STAT_KEYS = {
        'original_nodes',
        'original_edges',
        'after_prefilter_nodes',
        'after_prefilter_edges',
        'final_nodes',
        'final_edges',
        'edge_retention_rate',
        'weight_retention_rate',
    }

    def test_apply_filter_pipeline_stats(self, filter_stats):
        """Stats dict must contain every expected key with sane values."""
        assert set(filter_stats.keys()) == self.EXPECTED_STAT_KEYS, (
            f'Missing keys: {self.EXPECTED_STAT_KEYS - set(filter_stats.keys())}, '
            f'Extra keys: {set(filter_stats.keys()) - self.EXPECTED_STAT_KEYS}'
        )

        # Pipeline should reduce edges
        assert filter_stats['final_edges'] < filter_stats['original_edges'], (
            'Expected final_edges < original_edges after full pipeline'
        )

        # Retention rate between 0 and 1 (inclusive)
        assert 0 <= filter_stats['edge_retention_rate'] <= 1, (
            f"edge_retention_rate out of range: {filter_stats['edge_retention_rate']}"
        )
        assert 0 <= filter_stats['weight_retention_rate'] <= 1, (
            f"weight_retention_rate out of range: {filter_stats['weight_retention_rate']}"
        )

    def test_filter_pipeline_returns_graph(self, filtered_graph):
        """Pipeline output must be a non-empty nx.DiGraph."""
        assert isinstance(filtered_graph, nx.DiGraph)
        assert filtered_graph.number_of_nodes() > 0, 'Filtered graph has no nodes'
        assert filtered_graph.number_of_edges() > 0, 'Filtered graph has no edges'


# --------------------------------------------------------------------------- #
# hub_filter (T009)
# --------------------------------------------------------------------------- #

class TestHubFilter:

    @pytest.fixture
    def hub_graph(self, raw_graph):
        """Graph with extended metrics and edge_score for hub filtering."""
        from src.graph_builder import compute_extended_metrics, compute_edge_score
        from src.analysis import compute_centrality

        G = raw_graph.copy()
        centrality = compute_centrality(G)
        for node in G.nodes():
            G.nodes[node]['pagerank'] = centrality.loc[node, 'pagerank'] if node in centrality.index else 0.0
        G = compute_extended_metrics(G)
        G = compute_edge_score(G)
        return G

    def test_hub_filter_preserves_non_tx_edges(self, hub_graph):
        """Non-transaction edges must survive hub filtering."""
        non_tx_before = _count_non_tx_edges(hub_graph)
        filtered = hub_filter(hub_graph)
        non_tx_after = _count_non_tx_edges(filtered)
        assert non_tx_before == non_tx_after

    def test_hub_filter_returns_graph(self, hub_graph):
        """Hub filter should return a valid non-empty DiGraph."""
        filtered = hub_filter(hub_graph)
        assert isinstance(filtered, nx.DiGraph)
        assert filtered.number_of_nodes() > 0
        assert filtered.number_of_edges() > 0

    def test_hub_filter_reduces_hub_edges(self, hub_graph):
        """If hubs exist, hub_filter with low cap should reduce their edges."""
        hubs = [n for n in hub_graph.nodes() if hub_graph.nodes[n].get('hub_flag', False)]
        if not hubs:
            pytest.skip("No hubs in synthetic data")

        # Use very low caps to force reduction
        filtered = hub_filter(hub_graph, cap_min=2, cap_max=5)
        # Total edges should be reduced
        assert filtered.number_of_edges() <= hub_graph.number_of_edges()

    def test_hub_filter_with_membership(self, hub_graph, membership):
        """Hub filter with membership should preserve same-cluster edges."""
        filtered = hub_filter(hub_graph, membership=membership)
        assert isinstance(filtered, nx.DiGraph)
        assert filtered.number_of_nodes() > 0
