"""
Визуализация графа: интерактивный HTML через pyvis, сводные таблицы.

Работает в JupyterLab на MDP (notebook=True, cdn_resources='in_line' для offline).
"""

import logging
import math

import networkx as nx
import pandas as pd
from IPython.display import display, HTML
from pyvis.network import Network

from src import config

logger = logging.getLogger(__name__)


# =============================================================================
# Full Graph Visualization
# =============================================================================

def create_graph_visualization(
    G: nx.DiGraph,
    height: str = '800px',
    width: str = '100%',
    output_file: str = 'graph.html',
) -> Network:
    """
    Create interactive pyvis visualization from analyzed graph.

    Visual encoding:
    - Node color by node_type (company=blue, individual=green, sole_proprietor=orange)
    - Node size proportional to PageRank
    - Edge color by edge_type (transaction=gray, authority=red, salary=green, shared=purple)
    - Edge width proportional to log(transaction volume)
    - Tooltips on nodes and edges

    Uses cdn_resources='in_line' for offline MDP compatibility.
    Returns pyvis Network object (call .show() to display).
    """
    net = Network(
        height=height, width=width,
        directed=True, notebook=True,
        cdn_resources='in_line',
    )

    # Physics for readable layout
    net.set_options('''
    {
        "physics": {
            "forceAtlas2Based": {
                "gravitationalConstant": -50,
                "centralGravity": 0.01,
                "springLength": 200,
                "springConstant": 0.08
            },
            "maxVelocity": 50,
            "solver": "forceAtlas2Based",
            "stabilization": {"iterations": 150}
        }
    }
    ''')

    # Add nodes
    for node, attrs in G.nodes(data=True):
        node_type = attrs.get('node_type', 'company')
        pagerank = attrs.get('pagerank', 0.001)
        size = max(
            config.NODE_SIZE_MIN,
            min(config.NODE_SIZE_MAX, pagerank * 5000)
        )

        color = config.NODE_COLOR_MAP.get(node_type, '#808080')

        # Highlight shells
        role = attrs.get('role', '')
        if role == 'shell':
            color = '#FF6B6B'  # Red-ish for shell companies

        label = str(attrs.get('name', str(node)))[:30]

        tooltip = (
            f"<b>{attrs.get('name', 'N/A')}</b><br>"
            f"ID: {node}<br>"
            f"Type: {node_type}<br>"
            f"INN: {attrs.get('inn', 'N/A')}<br>"
            f"Cluster: {attrs.get('cluster', 'N/A')}<br>"
            f"PageRank: {pagerank:.6f}<br>"
            f"Betweenness: {attrs.get('betweenness', 0):.6f}<br>"
            f"Role: {role or 'N/A'}"
        )

        net.add_node(
            str(node),
            label=label,
            color=color,
            size=size,
            title=tooltip,
        )

    # Add edges
    for u, v, attrs in G.edges(data=True):
        edge_type = attrs.get('edge_type', 'transaction')
        color = config.EDGE_COLOR_MAP.get(edge_type, '#808080')

        if edge_type == 'transaction':
            amount = attrs.get('total_amount', 0)
            width = max(
                config.EDGE_WIDTH_MIN,
                min(config.EDGE_WIDTH_MAX, math.log1p(amount) / 3)
            )
            tooltip = (
                f"Type: {edge_type}<br>"
                f"Volume: {amount:,.0f}<br>"
                f"Count: {attrs.get('tx_count', 0)}<br>"
                f"Share: {attrs.get('share_of_turnover', 0):.1%}"
            )
        else:
            width = 1
            tooltip = f"Type: {edge_type}"
            if edge_type == 'shared_employees':
                tooltip += f"<br>Shared: {attrs.get('shared_count', 0)}"

        net.add_edge(str(u), str(v), color=color, width=width, title=tooltip)

    logger.info(
        f"Visualization created: {G.number_of_nodes()} nodes, "
        f"{G.number_of_edges()} edges"
    )
    return net


# =============================================================================
# Cluster Visualization
# =============================================================================

def create_cluster_visualization(
    G: nx.DiGraph,
    cluster_id: int,
    output_file: str = 'cluster.html',
) -> Network:
    """
    Visualize a single cluster and its immediate external connections.

    Internal nodes: full color. External nodes: faded/gray.
    Highlights shell companies and cycles if present.
    """
    # Get cluster members
    internal = {n for n, d in G.nodes(data=True) if d.get('cluster') == cluster_id}

    if not internal:
        logger.warning(f"No nodes in cluster {cluster_id}")
        return None

    # Add 1-hop external neighbors
    external = set()
    for node in internal:
        for neighbor in G.successors(node):
            if neighbor not in internal:
                external.add(neighbor)
        for neighbor in G.predecessors(node):
            if neighbor not in internal:
                external.add(neighbor)

    # Build subgraph
    all_nodes = internal | external
    subgraph = G.subgraph(all_nodes).copy()

    # Mark internal/external
    for node in subgraph.nodes():
        subgraph.nodes[node]['_internal'] = node in internal

    net = Network(
        height='600px', width='100%',
        directed=True, notebook=True,
        cdn_resources='in_line',
    )

    net.set_options('''
    {
        "physics": {
            "forceAtlas2Based": {
                "gravitationalConstant": -30,
                "centralGravity": 0.02,
                "springLength": 150,
                "springConstant": 0.1
            },
            "solver": "forceAtlas2Based",
            "stabilization": {"iterations": 100}
        }
    }
    ''')

    for node, attrs in subgraph.nodes(data=True):
        is_internal = attrs.get('_internal', False)
        node_type = attrs.get('node_type', 'company')
        pagerank = attrs.get('pagerank', 0.001)

        if is_internal:
            color = config.NODE_COLOR_MAP.get(node_type, '#808080')
            if attrs.get('role') == 'shell':
                color = '#FF6B6B'
            opacity = 1.0
        else:
            color = '#D3D3D3'  # Light gray for external
            opacity = 0.5

        size = max(8, min(40, pagerank * 5000))
        label = str(attrs.get('name', str(node)))[:25]

        net.add_node(
            str(node), label=label, color=color, size=size,
            title=f"{attrs.get('name', 'N/A')}<br>{'Internal' if is_internal else 'External'}",
            opacity=opacity,
        )

    for u, v, attrs in subgraph.edges(data=True):
        edge_type = attrs.get('edge_type', 'transaction')
        color = config.EDGE_COLOR_MAP.get(edge_type, '#C0C0C0')
        width = 1 if edge_type != 'transaction' else max(1, min(5, math.log1p(attrs.get('total_amount', 0)) / 4))
        net.add_edge(str(u), str(v), color=color, width=width)

    logger.info(f"Cluster {cluster_id} visualization: {len(internal)} internal + {len(external)} external nodes")
    return net


# =============================================================================
# Summary Table
# =============================================================================

def display_summary_table(cluster_summary_df: pd.DataFrame) -> None:
    """
    Display formatted cluster summary table in Jupyter.

    Uses pandas Styler for highlighting:
    - Red background for clusters with shell companies
    - Yellow for clusters with cycles
    - Bold for lead company names
    """
    if cluster_summary_df.empty:
        print("No clusters to display.")
        return

    df = cluster_summary_df.copy()

    # Format columns for display
    display_cols = [
        'cluster_id', 'member_count', 'company_count', 'individual_count',
        'total_internal_turnover', 'lead_company_name',
        'has_cycles', 'shell_count', 'anomaly_flags',
    ]
    display_df = df[[c for c in display_cols if c in df.columns]].copy()

    if 'total_internal_turnover' in display_df.columns:
        display_df['total_internal_turnover'] = display_df['total_internal_turnover'].apply(
            lambda x: f'{x:,.0f}'
        )

    def highlight_row(row):
        styles = [''] * len(row)
        if row.get('shell_count', 0) > 0:
            styles = ['background-color: #FFB3B3'] * len(row)
        elif row.get('has_cycles', False):
            styles = ['background-color: #FFFACD'] * len(row)
        return styles

    styler = display_df.style.apply(highlight_row, axis=1)
    display(styler)


# =============================================================================
# Node Profile
# =============================================================================

def display_node_profile(
    G: nx.DiGraph,
    node_id: int,
    metrics_df: pd.DataFrame,
) -> None:
    """
    Display detailed profile for a single node.

    Shows: basic info, centrality metrics, role, top counterparties, shell score.
    """
    if node_id not in G:
        print(f"Node {node_id} not found in graph.")
        return

    attrs = G.nodes[node_id]

    html = f"""
    <div style="border: 1px solid #ccc; padding: 15px; border-radius: 8px; max-width: 600px;">
        <h3 style="margin-top: 0;">{attrs.get('name', 'N/A')}</h3>
        <table style="width: 100%; border-collapse: collapse;">
            <tr><td><b>Client UK</b></td><td>{node_id}</td></tr>
            <tr><td><b>Type</b></td><td>{attrs.get('node_type', 'N/A')}</td></tr>
            <tr><td><b>INN</b></td><td>{attrs.get('inn', 'N/A')}</td></tr>
            <tr><td><b>Status</b></td><td>{attrs.get('status', 'N/A')}</td></tr>
            <tr><td><b>Cluster</b></td><td>{attrs.get('cluster', 'N/A')}</td></tr>
            <tr><td><b>Role</b></td><td>{attrs.get('role', 'N/A')}</td></tr>
    """

    if node_id in metrics_df.index:
        m = metrics_df.loc[node_id]
        html += f"""
            <tr><td colspan="2"><hr></td></tr>
            <tr><td><b>PageRank</b></td><td>{m.get('pagerank', 0):.6f}</td></tr>
            <tr><td><b>Betweenness</b></td><td>{m.get('betweenness', 0):.6f}</td></tr>
            <tr><td><b>Clustering Coef</b></td><td>{m.get('clustering_coef', 0):.4f}</td></tr>
            <tr><td><b>In-degree</b></td><td>{m.get('in_degree', 0)}</td></tr>
            <tr><td><b>Out-degree</b></td><td>{m.get('out_degree', 0)}</td></tr>
            <tr><td><b>Total In Flow</b></td><td>{m.get('total_in_flow', 0):,.0f}</td></tr>
            <tr><td><b>Total Out Flow</b></td><td>{m.get('total_out_flow', 0):,.0f}</td></tr>
            <tr><td><b>Flow-Through Ratio</b></td><td>{m.get('flow_through_ratio', 0):.2f}</td></tr>
        """
        if 'shell_score' in m:
            score = m['shell_score']
            color = '#FF0000' if score >= config.SHELL_SCORE_THRESHOLD else '#333'
            html += f'<tr><td><b>Shell Score</b></td><td style="color:{color}"><b>{score:.2f}</b></td></tr>'

    html += "</table>"

    # Top counterparties
    counterparties = []
    for _, v, d in G.out_edges(node_id, data=True):
        if d.get('edge_type') == 'transaction':
            counterparties.append({
                'name': G.nodes[v].get('name', str(v))[:30],
                'amount': d.get('total_amount', 0),
                'share': d.get('share_of_turnover', 0),
            })
    counterparties.sort(key=lambda x: x['amount'], reverse=True)

    if counterparties:
        html += "<h4>Top Counterparties (outgoing)</h4><table style='width:100%;'>"
        html += "<tr><th>Name</th><th>Amount</th><th>Share</th></tr>"
        for cp in counterparties[:5]:
            html += f"<tr><td>{cp['name']}</td><td>{cp['amount']:,.0f}</td><td>{cp['share']:.1%}</td></tr>"
        html += "</table>"

    html += "</div>"
    display(HTML(html))
