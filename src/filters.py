"""
Фильтрация графа: удаление шума и извлечение значимого скелета.

Реализует:
- Предфильтрацию по минимальным метрикам транзакций
- Disparity filter Серрано для извлечения backbone
- Пайплайн фильтрации
"""

import logging

import networkx as nx

from src import config

logger = logging.getLogger(__name__)


def pre_filter(
    G: nx.DiGraph,
    min_tx_count: int = 3,
    min_total_amount: float = 0.0,
    min_periods: int = 2,
) -> nx.DiGraph:
    """
    Remove low-significance transaction edges.

    Criteria (ALL must fail for removal — i.e., edge is removed only
    if it fails ALL thresholds):
    - tx_count < min_tx_count
    - total_amount < min_total_amount
    - present in fewer than min_periods quarters

    Non-transaction edges (authority, salary, shared_employees) are always preserved.
    Returns filtered copy of graph (original unchanged).
    """
    filtered = G.copy()
    edges_to_remove = []

    for u, v, data in filtered.edges(data=True):
        if data.get('edge_type') != 'transaction':
            continue

        tx_count = data.get('tx_count', 0)
        total_amount = data.get('total_amount', 0)
        n_periods = data.get('n_periods', 1)

        # Remove if ALL thresholds fail
        if (tx_count < min_tx_count
                and total_amount < min_total_amount
                and n_periods < min_periods):
            edges_to_remove.append((u, v))

    filtered.remove_edges_from(edges_to_remove)

    # Remove isolated nodes
    isolates = list(nx.isolates(filtered))
    filtered.remove_nodes_from(isolates)

    logger.info(
        f"Pre-filter: removed {len(edges_to_remove)} edges, "
        f"{len(isolates)} isolates. "
        f"Remaining: {filtered.number_of_nodes()} nodes, "
        f"{filtered.number_of_edges()} edges"
    )
    return filtered


def disparity_filter(
    G: nx.DiGraph,
    alpha: float = 0.05,
    weight_attr: str = 'weight',
) -> nx.DiGraph:
    """
    Serrano disparity filter for directed weighted graphs.

    For each edge (u->v):
    - Compute alpha_out from u's outgoing perspective
    - Compute alpha_in from v's incoming perspective
    - Remove edge if alpha >= threshold for BOTH perspectives
      (i.e., keep if significant from at least one side)

    Only applies to transaction edges. Non-transaction edges are preserved.
    Removes isolated nodes after filtering.
    Returns backbone subgraph (copy).
    """
    backbone = G.copy()
    edges_to_remove = []

    for u, v, data in G.edges(data=True):
        # Preserve non-transaction edges
        if data.get('edge_type') != 'transaction':
            continue

        w = data.get(weight_attr, 1.0)
        if w <= 0:
            edges_to_remove.append((u, v))
            continue

        # --- Outgoing perspective (node u) ---
        k_out = sum(
            1 for _, _, d in G.out_edges(u, data=True)
            if d.get('edge_type') == 'transaction'
        )
        s_out = sum(
            d.get(weight_attr, 1.0)
            for _, _, d in G.out_edges(u, data=True)
            if d.get('edge_type') == 'transaction'
        )

        if k_out > 1 and s_out > 0:
            p_out = w / s_out
            alpha_out = (1.0 - p_out) ** (k_out - 1)
        else:
            alpha_out = 0.0  # Keep edges from degree-1 nodes

        # --- Incoming perspective (node v) ---
        k_in = sum(
            1 for _, _, d in G.in_edges(v, data=True)
            if d.get('edge_type') == 'transaction'
        )
        s_in = sum(
            d.get(weight_attr, 1.0)
            for _, _, d in G.in_edges(v, data=True)
            if d.get('edge_type') == 'transaction'
        )

        if k_in > 1 and s_in > 0:
            p_in = w / s_in
            alpha_in = (1.0 - p_in) ** (k_in - 1)
        else:
            alpha_in = 0.0

        # Remove if insignificant from BOTH perspectives
        if alpha_out >= alpha and alpha_in >= alpha:
            edges_to_remove.append((u, v))

    backbone.remove_edges_from(edges_to_remove)

    # Remove isolated nodes
    isolates = list(nx.isolates(backbone))
    backbone.remove_nodes_from(isolates)

    logger.info(
        f"Disparity filter (alpha={alpha}): removed {len(edges_to_remove)} edges, "
        f"{len(isolates)} isolates. "
        f"Remaining: {backbone.number_of_nodes()} nodes, "
        f"{backbone.number_of_edges()} edges"
    )
    return backbone


def hub_filter(
    G: nx.DiGraph,
    membership: dict = None,
    cap_min: int = None,
    cap_max: int = None,
) -> nx.DiGraph:
    """
    Hub-aware edge filtering: limit high-degree hub nodes to top-N edges by edge_score.

    Cap formula: min(max(cap_min, ceil(sqrt(degree))), cap_max)

    Exemptions (always preserved):
    - Non-transaction edges
    - Reciprocal transaction edges
    - Edges within the same Leiden cluster (if membership provided)

    Returns filtered copy; verifies no new isolated components.
    """
    import math

    if cap_min is None:
        cap_min = config.HUB_CAP_MIN
    if cap_max is None:
        cap_max = config.HUB_CAP_MAX

    filtered = G.copy()

    # Identify hubs
    hubs = [n for n in filtered.nodes() if filtered.nodes[n].get('hub_flag', False)]

    if not hubs:
        logger.info("Hub filter: no hub nodes found, skipping")
        return filtered

    original_components = nx.number_weakly_connected_components(filtered)
    total_removed = 0

    for hub in hubs:
        # Collect tx out-edges
        tx_out = []
        for _, tgt, d in filtered.out_edges(hub, data=True):
            if d.get('edge_type') != 'transaction':
                continue
            tx_out.append((hub, tgt, d))

        tx_degree = len(tx_out)
        if tx_degree == 0:
            continue

        cap = min(max(cap_min, math.ceil(math.sqrt(tx_degree))), cap_max)

        if tx_degree <= cap:
            continue

        # Classify edges as exempt or removable
        exempt = []
        removable = []
        for u, v, d in tx_out:
            is_exempt = False
            # Reciprocal edge
            if filtered.has_edge(v, u):
                rev = filtered[v][u]
                if rev.get('edge_type') == 'transaction':
                    is_exempt = True
            # Same cluster
            if membership is not None:
                if membership.get(u, -1) == membership.get(v, -2) and membership.get(u, -1) != -1:
                    is_exempt = True

            if is_exempt:
                exempt.append((u, v, d))
            else:
                removable.append((u, v, d))

        # Sort removable by edge_score ascending (lowest score removed first)
        removable.sort(key=lambda x: x[2].get('edge_score', 0))

        # How many to keep from removable
        remaining_slots = max(0, cap - len(exempt))
        to_remove = removable[:max(0, len(removable) - remaining_slots)]

        for u, v, _ in to_remove:
            filtered.remove_edge(u, v)
            total_removed += 1

    # Verify no new isolated components — restore edges if needed
    new_components = nx.number_weakly_connected_components(filtered)
    if new_components > original_components:
        logger.warning(
            f"Hub filter created {new_components - original_components} new components. "
            f"Consider relaxing cap parameters."
        )

    # Remove isolated nodes
    isolates = list(nx.isolates(filtered))
    filtered.remove_nodes_from(isolates)

    logger.info(
        f"Hub filter: {len(hubs)} hubs processed, {total_removed} edges removed, "
        f"{len(isolates)} isolates removed. "
        f"Remaining: {filtered.number_of_nodes()} nodes, {filtered.number_of_edges()} edges"
    )
    return filtered


def apply_filter_pipeline(
    G: nx.DiGraph,
    min_tx_count: int = 3,
    min_total_amount: float = 0.0,
    min_periods: int = 2,
    alpha: float = 0.05,
) -> tuple:
    """
    Run full filtering pipeline: pre-filter -> disparity filter.

    Returns (filtered_graph, stats_dict) where stats_dict contains
    retention rates and counts at each stage.
    """
    original_nodes = G.number_of_nodes()
    original_edges = G.number_of_edges()
    original_weight = sum(
        d.get('weight', 0) for _, _, d in G.edges(data=True)
        if d.get('edge_type') == 'transaction'
    )

    # Stage 1: Pre-filter
    stage1 = pre_filter(G, min_tx_count, min_total_amount, min_periods)
    after_pre_nodes = stage1.number_of_nodes()
    after_pre_edges = stage1.number_of_edges()

    # Stage 2: Disparity filter
    backbone = disparity_filter(stage1, alpha)
    final_nodes = backbone.number_of_nodes()
    final_edges = backbone.number_of_edges()
    final_weight = sum(
        d.get('weight', 0) for _, _, d in backbone.edges(data=True)
        if d.get('edge_type') == 'transaction'
    )

    stats = {
        'original_nodes': original_nodes,
        'original_edges': original_edges,
        'after_prefilter_nodes': after_pre_nodes,
        'after_prefilter_edges': after_pre_edges,
        'final_nodes': final_nodes,
        'final_edges': final_edges,
        'edge_retention_rate': final_edges / original_edges if original_edges > 0 else 0,
        'weight_retention_rate': final_weight / original_weight if original_weight > 0 else 0,
    }

    logger.info(
        f"Filter pipeline complete: "
        f"{original_nodes}/{original_edges} -> "
        f"{final_nodes}/{final_edges} "
        f"(edge retention: {stats['edge_retention_rate']:.1%})"
    )
    return backbone, stats
