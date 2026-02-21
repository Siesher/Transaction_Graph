"""
Analysis pipeline orchestrator: one-command graph->filter->analyze->viz.

Replaces manual 4-notebook workflow with a single function call.
"""

import logging
import os
from collections import namedtuple

import pandas as pd

from src import config
from src.graph_builder import (
    build_graph, compute_edge_metrics, compute_extended_metrics,
    compute_edge_score, derive_shared_employees, enrich_graph,
)
from src.filters import apply_filter_pipeline
from src.analysis import (
    run_leiden_clustering, compute_centrality, classify_node_roles,
    detect_shell_companies, detect_cycles, build_cluster_summary,
    build_okved_matrix, compute_okved_diversity,
    compute_behavioral_features, cluster_behavioral_segments,
    compute_lookalike_scores,
)

logger = logging.getLogger(__name__)

PipelineResult = namedtuple('PipelineResult', [
    'graph',
    'metrics_df',
    'cluster_summary',
    'cycles',
    'filter_stats',
    'best_gamma',
    'okved_matrix',
    'behavioral_df',
])


def run_analysis_pipeline(
    data_dir,
    min_tx_count=3,
    min_total_amount=0.0,
    min_periods=2,
    alpha=0.05,
    gamma_values=None,
    shell_threshold=None,
):
    """
    Run full analysis pipeline from Parquet files to enriched graph.

    Args:
        data_dir: path to directory with parquet files from ETL or synthetic
        min_tx_count: pre-filter minimum transaction count
        min_total_amount: pre-filter minimum total amount
        min_periods: pre-filter minimum quarters
        alpha: disparity filter significance level
        gamma_values: Leiden CPM resolution values (None = config defaults)
        shell_threshold: shell detection threshold (None = config default)

    Returns:
        PipelineResult namedtuple with enriched graph, metrics, summary, cycles,
        okved_matrix, and behavioral_df.
    """
    # --- Step 1: Load data ---
    logger.info(f"Loading data from {data_dir}")
    nodes_df = pd.read_parquet(os.path.join(data_dir, 'nodes.parquet'))
    tx_df = pd.read_parquet(os.path.join(data_dir, 'transaction_edges.parquet'))
    auth_df = pd.read_parquet(os.path.join(data_dir, 'authority_edges.parquet'))
    sal_df = pd.read_parquet(os.path.join(data_dir, 'salary_edges.parquet'))

    # Merge hop distances if available
    hop_path = os.path.join(data_dir, 'hop_distances.parquet')
    if os.path.exists(hop_path):
        hop_df = pd.read_parquet(hop_path)
        nodes_df = nodes_df.merge(hop_df, on='client_uk', how='left')

    # --- Step 2: Build graph ---
    logger.info("Building graph...")
    G = build_graph(nodes_df, tx_df, auth_df, sal_df)
    G = compute_edge_metrics(G)

    # Shared employees
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

    # --- Step 3: Filter ---
    logger.info("Filtering graph...")
    filtered_G, filter_stats = apply_filter_pipeline(
        G, min_tx_count=min_tx_count, min_total_amount=min_total_amount,
        min_periods=min_periods, alpha=alpha,
    )

    # --- Step 4: Cluster ---
    logger.info("Running Leiden clustering...")
    membership, best_gamma = run_leiden_clustering(
        filtered_G, gamma_values=gamma_values,
    )

    # --- Step 5: Centrality ---
    logger.info("Computing centrality metrics...")
    metrics_df = compute_centrality(filtered_G)

    # --- Step 5b: Extended metrics & edge score ---
    logger.info("Computing extended metrics and edge scores...")
    for node in filtered_G.nodes():
        filtered_G.nodes[node]['pagerank'] = metrics_df.loc[node, 'pagerank'] if node in metrics_df.index else 0.0
    filtered_G = compute_extended_metrics(filtered_G)
    filtered_G = compute_edge_score(filtered_G)

    # --- Step 6: Classify roles ---
    logger.info("Classifying node roles...")
    metrics_df = classify_node_roles(metrics_df, filtered_G)

    # --- Step 7: Detect shells ---
    logger.info("Detecting shell companies...")
    shell_df = detect_shell_companies(metrics_df, filtered_G, threshold=shell_threshold)

    # Merge shell scores back into metrics_df
    if len(shell_df) > 0 and 'shell_score' in shell_df.columns:
        metrics_df['shell_score'] = 0.0
        for idx in shell_df.index:
            if idx in metrics_df.index:
                metrics_df.loc[idx, 'shell_score'] = shell_df.loc[idx, 'shell_score']
    else:
        metrics_df['shell_score'] = 0.0

    # --- Step 8: Detect cycles ---
    logger.info("Detecting cycles...")
    cycles = detect_cycles(filtered_G)

    # --- Step 9: Enrich graph ---
    logger.info("Enriching graph with analysis results...")
    enrich_graph(filtered_G, metrics_df, membership, shell_df if len(shell_df) > 0 else None)

    # --- Step 10: Cluster summary ---
    logger.info("Building cluster summary...")
    cluster_summary = build_cluster_summary(filtered_G, metrics_df, cycles)

    # --- Step 11: OKVED analytics ---
    logger.info("Computing OKVED analytics...")
    okved_matrix = build_okved_matrix(filtered_G)
    compute_okved_diversity(filtered_G)

    # --- Step 12: Behavioral segmentation & look-alike ---
    logger.info("Computing behavioral segmentation...")
    behavioral_df = compute_behavioral_features(filtered_G)
    if not behavioral_df.empty:
        behavioral_df = cluster_behavioral_segments(behavioral_df)
        behavioral_df = compute_lookalike_scores(behavioral_df, filtered_G)

    logger.info("Pipeline complete.")
    return PipelineResult(
        graph=filtered_G,
        metrics_df=metrics_df,
        cluster_summary=cluster_summary,
        cycles=cycles,
        filter_stats=filter_stats,
        best_gamma=best_gamma,
        okved_matrix=okved_matrix,
        behavioral_df=behavioral_df,
    )
