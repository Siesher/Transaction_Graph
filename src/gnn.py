"""
GNN-based node classification for shell detection and role classification.

Uses DGL with R-GCN (Relational Graph Convolutional Network) for
heterogeneous graph learning. All dependencies (torch, dgl, sklearn)
are optional — other modules work without them.

Usage:
    from src.gnn import prepare_gnn_data, train_gnn, predict_gnn
    dgl_graph, mapping, scaler = prepare_gnn_data(enriched_graph, 'shell')
    model, info = train_gnn(dgl_graph, 'shell')
    predictions = predict_gnn(model, dgl_graph, mapping)
"""

import logging
import os
from collections import defaultdict

import numpy as np
import pandas as pd

from src import config

logger = logging.getLogger(__name__)

# Guard imports for optional GNN dependencies
try:
    import torch
    import torch.nn as nn
    import torch.nn.functional as F
    import dgl
    import dgl.nn.pytorch as dglnn
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import train_test_split
    HAS_GNN_DEPS = True
except ImportError as e:
    HAS_GNN_DEPS = False
    _IMPORT_ERROR = str(e)


def _check_deps():
    """Raise ImportError with helpful message if GNN deps are missing."""
    if not HAS_GNN_DEPS:
        raise ImportError(
            f"GNN dependencies not available: {_IMPORT_ERROR}. "
            "Install with: pip install torch dgl scikit-learn"
        )


# =============================================================================
# Constants
# =============================================================================

ROLE_LABELS = {'parent': 0, 'shell': 1, 'subsidiary': 2, 'conduit': 3, 'regular': 4}
ROLE_LABELS_INV = {v: k for k, v in ROLE_LABELS.items()}

FEATURE_NAMES = [
    'pagerank', 'betweenness', 'clustering_coef',
    'total_in_flow', 'total_out_flow', 'flow_through_ratio',
    'in_degree', 'out_degree',
    'is_company', 'is_individual', 'is_sole_proprietor',
    'hop_distance', 'has_salary_payments',
    'mean_tx_amount', 'total_tx_count',
]


# =============================================================================
# Model
# =============================================================================

if HAS_GNN_DEPS:
    class RGCNModel(nn.Module):
        """R-GCN model using HeteroGraphConv with classification head.

        Architecture:
        - Input: node features (15 dims)
        - N HeteroGraphConv layers with GraphConv per relation type
        - ReLU activation + dropout between layers
        - Classification head: Linear → sigmoid (shell) or softmax (role)
        """

        def __init__(self, in_feats, hidden_dim, n_classes, rel_names,
                     n_layers=2, dropout=0.5, task='shell'):
            super().__init__()
            self.task = task
            self.n_layers = n_layers
            self.hidden_dim = hidden_dim

            self.convs = nn.ModuleList()

            # First layer: in_feats → hidden_dim
            self.convs.append(dglnn.HeteroGraphConv({
                rel: dglnn.GraphConv(in_feats, hidden_dim)
                for rel in rel_names
            }, aggregate='sum'))

            # Additional hidden layers
            for _ in range(max(0, n_layers - 1)):
                self.convs.append(dglnn.HeteroGraphConv({
                    rel: dglnn.GraphConv(hidden_dim, hidden_dim)
                    for rel in rel_names
                }, aggregate='sum'))

            self.dropout = nn.Dropout(dropout)

            # Classification head
            if task == 'shell':
                self.classifier = nn.Linear(hidden_dim, 1)
            else:
                self.classifier = nn.Linear(hidden_dim, n_classes)

        def forward(self, g, inputs):
            h = inputs
            for i, conv in enumerate(self.convs):
                h = conv(g, h)
                h = {k: F.relu(v) for k, v in h.items()}
                if i < len(self.convs) - 1:
                    h = {k: self.dropout(v) for k, v in h.items()}

            # Apply classifier to each node type
            out = {}
            for ntype, feat in h.items():
                out[ntype] = self.classifier(feat)
            return out


# =============================================================================
# Data Preparation
# =============================================================================

def prepare_gnn_data(G, target_task='shell'):
    """
    Convert enriched NetworkX graph to DGL heterograph with features and labels.

    Args:
        G: NetworkX DiGraph with enriched node attributes (from pipeline)
        target_task: 'shell' (binary) or 'role' (multi-class)

    Returns:
        (dgl_graph, node_mapping, scaler) where:
        - dgl_graph: DGL heterogeneous graph with 'feat' and 'label' per node type
        - node_mapping: {nx_node_id: (node_type, local_index)}
        - scaler: fitted StandardScaler for feature normalization
    """
    _check_deps()

    # 1. Group nodes by type, assign local indices
    node_type_map = {}
    type_nodes = defaultdict(list)

    for node, data in G.nodes(data=True):
        ntype = data.get('node_type', 'company')
        node_type_map[node] = ntype
        type_nodes[ntype].append(node)

    # Sort for determinism
    for ntype in type_nodes:
        type_nodes[ntype].sort()

    # node_id → (node_type, local_idx)
    node_mapping = {}
    for ntype, nodes in type_nodes.items():
        for local_idx, node_id in enumerate(nodes):
            node_mapping[node_id] = (ntype, local_idx)

    # 2. Group edges by canonical type (src_type, edge_type, dst_type)
    edge_dict = defaultdict(lambda: ([], []))

    for u, v, data in G.edges(data=True):
        etype = data.get('edge_type', 'unknown')
        src_type = node_type_map.get(u, 'company')
        dst_type = node_type_map.get(v, 'company')
        canonical = (src_type, etype, dst_type)

        src_local = node_mapping[u][1]
        dst_local = node_mapping[v][1]
        edge_dict[canonical][0].append(src_local)
        edge_dict[canonical][1].append(dst_local)

    # 3. Build DGL heterograph
    graph_data = {}
    for canonical, (src_ids, dst_ids) in edge_dict.items():
        graph_data[canonical] = (
            torch.tensor(src_ids, dtype=torch.long),
            torch.tensor(dst_ids, dtype=torch.long),
        )

    if not graph_data:
        raise ValueError("No edges found in graph for GNN conversion")

    num_nodes_dict = {ntype: len(nodes) for ntype, nodes in type_nodes.items()}
    dgl_graph = dgl.heterograph(graph_data, num_nodes_dict=num_nodes_dict)

    # 4. Extract and normalize features
    # Pre-compute per-node edge stats
    node_tx_amounts = defaultdict(list)
    node_tx_counts = defaultdict(int)
    for u, v, data in G.edges(data=True):
        if data.get('edge_type') == 'transaction':
            amt = data.get('total_amount', 0)
            cnt = data.get('tx_count', 0)
            node_tx_amounts[u].append(amt)
            node_tx_amounts[v].append(amt)
            node_tx_counts[u] += cnt
            node_tx_counts[v] += cnt

    scaler = StandardScaler()
    all_features = []
    feature_order = []  # track (ntype, local_idx) to reassign later

    for ntype in sorted(type_nodes.keys()):
        for node_id in type_nodes[ntype]:
            attrs = G.nodes[node_id]
            feats = [
                float(attrs.get('pagerank', 0)),
                float(attrs.get('betweenness', 0)),
                float(attrs.get('clustering_coef', 0)),
                float(attrs.get('total_in_flow', 0)),
                float(attrs.get('total_out_flow', 0)),
                float(attrs.get('flow_through_ratio', 0)),
                float(G.in_degree(node_id)),
                float(G.out_degree(node_id)),
                1.0 if ntype == 'company' else 0.0,
                1.0 if ntype == 'individual' else 0.0,
                1.0 if ntype == 'sole_proprietor' else 0.0,
                float(attrs.get('hop_distance', -1)),
                1.0 if attrs.get('has_salary_payments', False) else 0.0,
                float(np.mean(node_tx_amounts[node_id])) if node_tx_amounts[node_id] else 0.0,
                float(node_tx_counts[node_id]),
            ]
            all_features.append(feats)
            feature_order.append((ntype, node_id))

    all_features_arr = np.array(all_features, dtype=np.float32)
    scaler.fit(all_features_arr)
    all_features_scaled = scaler.transform(all_features_arr)

    # Assign features per node type
    type_features = defaultdict(list)
    for i, (ntype, node_id) in enumerate(feature_order):
        type_features[ntype].append(all_features_scaled[i])

    for ntype in type_nodes:
        feat_tensor = torch.tensor(np.array(type_features[ntype]), dtype=torch.float32)
        dgl_graph.nodes[ntype].data['feat'] = feat_tensor

    # 5. Assign labels per node type
    for ntype in type_nodes:
        if target_task == 'shell':
            labels = []
            for node_id in type_nodes[ntype]:
                shell_score = G.nodes[node_id].get('shell_score', 0.0)
                labels.append(1.0 if shell_score >= config.SHELL_SCORE_THRESHOLD else 0.0)
            dgl_graph.nodes[ntype].data['label'] = torch.tensor(labels, dtype=torch.float32)
        else:
            labels = []
            for node_id in type_nodes[ntype]:
                role = G.nodes[node_id].get('role', 'regular')
                labels.append(ROLE_LABELS.get(role, ROLE_LABELS['regular']))
            dgl_graph.nodes[ntype].data['label'] = torch.tensor(labels, dtype=torch.long)

    logger.info(
        f"GNN data prepared: {dgl_graph.ntypes} node types, "
        f"{len(dgl_graph.canonical_etypes)} relation types, "
        f"{len(FEATURE_NAMES)} features, task={target_task}"
    )
    return dgl_graph, node_mapping, scaler


# =============================================================================
# Training
# =============================================================================

def train_gnn(dgl_graph, target_task='shell', hidden_dim=64, n_layers=2,
              epochs=200, lr=0.01, label_smoothing=0.1, val_ratio=0.2):
    """
    Train R-GCN model on heterogeneous graph.

    Args:
        dgl_graph: DGL heterograph with 'feat' and 'label' data
        target_task: 'shell' or 'role'
        hidden_dim: hidden layer dimension
        n_layers: number of R-GCN layers
        epochs: training epochs
        lr: learning rate
        label_smoothing: smoothing factor for labels
        val_ratio: validation split ratio

    Returns:
        (model, train_info) where train_info has keys:
        train_losses, val_losses, val_accuracy, val_f1, epochs_run
    """
    _check_deps()

    rel_names = dgl_graph.canonical_etypes
    in_feats = len(FEATURE_NAMES)
    n_classes = 1 if target_task == 'shell' else len(ROLE_LABELS)

    model = RGCNModel(in_feats, hidden_dim, n_classes, rel_names,
                      n_layers=n_layers, task=target_task)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)

    # Create train/val masks per node type
    for ntype in dgl_graph.ntypes:
        n_nodes = dgl_graph.num_nodes(ntype)
        if n_nodes == 0:
            continue
        indices = np.arange(n_nodes)
        if n_nodes >= 5:
            train_idx, val_idx = train_test_split(
                indices, test_size=val_ratio, random_state=42
            )
        else:
            train_idx = indices
            val_idx = indices

        train_mask = torch.zeros(n_nodes, dtype=torch.bool)
        val_mask = torch.zeros(n_nodes, dtype=torch.bool)
        train_mask[train_idx] = True
        val_mask[val_idx] = True

        dgl_graph.nodes[ntype].data['train_mask'] = train_mask
        dgl_graph.nodes[ntype].data['val_mask'] = val_mask

    # Training loop
    train_info = {
        'train_losses': [], 'val_losses': [],
        'val_accuracy': 0.0, 'val_f1': 0.0, 'epochs_run': 0,
    }

    for epoch in range(epochs):
        model.train()

        inputs = {
            ntype: dgl_graph.nodes[ntype].data['feat']
            for ntype in dgl_graph.ntypes
            if dgl_graph.num_nodes(ntype) > 0
        }
        logits = model(dgl_graph, inputs)

        # Compute loss across all node types
        total_loss = torch.tensor(0.0, requires_grad=True)
        for ntype in dgl_graph.ntypes:
            if ntype not in logits or dgl_graph.num_nodes(ntype) == 0:
                continue
            mask = dgl_graph.nodes[ntype].data.get('train_mask')
            if mask is None or mask.sum() == 0:
                continue

            labels = dgl_graph.nodes[ntype].data['label']
            pred = logits[ntype][mask]

            if target_task == 'shell':
                target = labels[mask].unsqueeze(1)
                if label_smoothing > 0:
                    target = target * (1 - label_smoothing) + 0.5 * label_smoothing
                loss = F.binary_cross_entropy_with_logits(pred, target)
            else:
                target = labels[mask]
                loss = F.cross_entropy(pred, target, label_smoothing=label_smoothing)

            total_loss = total_loss + loss

        optimizer.zero_grad()
        total_loss.backward()
        optimizer.step()

        train_info['epochs_run'] = epoch + 1

    # Final validation
    model.eval()
    val_acc = 0.0
    with torch.no_grad():
        inputs = {
            ntype: dgl_graph.nodes[ntype].data['feat']
            for ntype in dgl_graph.ntypes
            if dgl_graph.num_nodes(ntype) > 0
        }
        val_logits = model(dgl_graph, inputs)
        correct = 0
        total = 0

        for ntype in dgl_graph.ntypes:
            if ntype not in val_logits or dgl_graph.num_nodes(ntype) == 0:
                continue
            mask = dgl_graph.nodes[ntype].data.get('val_mask')
            if mask is None or mask.sum() == 0:
                continue

            labels = dgl_graph.nodes[ntype].data['label']
            pred = val_logits[ntype][mask]

            if target_task == 'shell':
                predicted = (torch.sigmoid(pred) >= 0.5).squeeze().long()
                if predicted.dim() == 0:
                    predicted = predicted.unsqueeze(0)
                correct += (predicted == labels[mask].long()).sum().item()
            else:
                predicted = pred.argmax(dim=1)
                correct += (predicted == labels[mask]).sum().item()

            total += mask.sum().item()

        val_acc = correct / total if total > 0 else 0.0

    train_info['val_accuracy'] = val_acc
    train_info['train_losses'].append(total_loss.item())

    logger.info(f"GNN training complete: {epochs} epochs, val_accuracy={val_acc:.3f}")
    return model, train_info


# =============================================================================
# Prediction
# =============================================================================

def predict_gnn(model, dgl_graph, node_mapping):
    """
    Run GNN prediction on all nodes.

    Args:
        model: trained RGCNModel
        dgl_graph: DGL heterograph with features
        node_mapping: {nx_node_id: (node_type, local_index)}

    Returns:
        DataFrame with columns: client_uk, predicted_label, probability
    """
    _check_deps()

    model.eval()
    inputs = {
        ntype: dgl_graph.nodes[ntype].data['feat']
        for ntype in dgl_graph.ntypes
        if dgl_graph.num_nodes(ntype) > 0
    }

    with torch.no_grad():
        logits = model(dgl_graph, inputs)

    inv_mapping = {v: k for k, v in node_mapping.items()}
    task = model.task
    records = []

    for ntype in dgl_graph.ntypes:
        if ntype not in logits or dgl_graph.num_nodes(ntype) == 0:
            continue

        pred = logits[ntype]

        if task == 'shell':
            probs = torch.sigmoid(pred).squeeze(-1)
            if probs.dim() == 0:
                probs = probs.unsqueeze(0)
            for local_idx in range(dgl_graph.num_nodes(ntype)):
                node_id = inv_mapping.get((ntype, local_idx))
                if node_id is not None:
                    prob = probs[local_idx].item()
                    records.append({
                        'client_uk': node_id,
                        'predicted_label': 1 if prob >= 0.5 else 0,
                        'probability': prob,
                    })
        else:
            probs = F.softmax(pred, dim=1)
            for local_idx in range(dgl_graph.num_nodes(ntype)):
                node_id = inv_mapping.get((ntype, local_idx))
                if node_id is not None:
                    class_probs = probs[local_idx]
                    predicted_class = class_probs.argmax().item()
                    records.append({
                        'client_uk': node_id,
                        'predicted_label': ROLE_LABELS_INV.get(predicted_class, 'regular'),
                        'probability': class_probs.max().item(),
                    })

    return pd.DataFrame(records)


# =============================================================================
# Model Persistence
# =============================================================================

def save_gnn_model(model, node_mapping, scaler, metadata, path):
    """
    Save trained GNN model and associated artifacts.

    Args:
        model: trained RGCNModel
        node_mapping: {nx_node_id: (node_type, local_index)}
        scaler: fitted StandardScaler
        metadata: dict with training hyperparameters, metrics, date
        path: file path for the saved model (.pt)
    """
    _check_deps()

    save_dict = {
        'model_state_dict': model.state_dict(),
        'node_mapping': node_mapping,
        'scaler': scaler,
        'label_encoder': ROLE_LABELS,
        'metadata': metadata,
        'task': model.task,
        'model_config': {
            'in_feats': len(FEATURE_NAMES),
            'hidden_dim': model.hidden_dim,
            'n_classes': model.classifier.out_features,
            'n_layers': model.n_layers,
            'task': model.task,
        },
        'rel_names': None,  # populated from model conv layers
    }

    # Extract rel_names from the first conv layer
    first_conv = model.convs[0]
    if hasattr(first_conv, 'mods'):
        save_dict['rel_names'] = list(first_conv.mods.keys())

    dir_name = os.path.dirname(path)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)
    torch.save(save_dict, path)
    logger.info(f"GNN model saved to {path}")


def load_gnn_model(path):
    """
    Load trained GNN model and associated artifacts.

    Args:
        path: file path to saved model (.pt)

    Returns:
        (model, node_mapping, scaler, metadata)
    """
    _check_deps()

    checkpoint = torch.load(path, weights_only=False)

    cfg = checkpoint['model_config']
    rel_names = checkpoint.get('rel_names', [])

    model = RGCNModel(
        in_feats=cfg['in_feats'],
        hidden_dim=cfg['hidden_dim'],
        n_classes=cfg['n_classes'],
        rel_names=rel_names,
        n_layers=cfg['n_layers'],
        task=cfg['task'],
    )
    model.load_state_dict(checkpoint['model_state_dict'])
    model.eval()

    return (
        model,
        checkpoint['node_mapping'],
        checkpoint['scaler'],
        checkpoint.get('metadata', {}),
    )
