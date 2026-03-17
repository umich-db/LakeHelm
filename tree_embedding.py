#!/usr/bin/env python3
"""Tree convolution-based query embedding module.

Provides BatchTreeConvCBAM and utilities to replace learnable nn.Embedding
with structural query plan embeddings.
"""

import os
import re
import sys
import random
import torch
import torch.nn as nn
import torch.nn.functional as F
from typing import List, Dict, Any, Tuple, Optional
from collections import defaultdict

# Add workspace root so bao_server is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bao_server.TreeConvolution.spark_feature_embedding import (
    process_general_plans, parse_files, extract_all_table_names,
    mapping_table_job, mapping_table_tpcds, mapping_table_ssb, mapping_table_tpch,
    columns_num_job, columns_num_tpcds, columns_num_ssb, columns_num_tpch,
    columns_num_job_short, columns_num_tpcds_short, columns_num_ssb_short, columns_num_tpch_short,
    query_data,
)

# ===================== Paths =====================

PLAN_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        'orig_train', 'train', 'data', 'experiment_3',
                        'unify_plans', 'plans', 'all')

HIST_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        'orig_train', 'train', 'data')

# ===================== MLP for BatchTreeConvCBAM =====================

def create_tree_mlp(layer_sizes, dropout_prob=0.0):
    """MLP with LayerNorm + Sigmoid at the end (matches original all_ratio.py)."""
    layers = []
    for i in range(len(layer_sizes) - 1):
        layers.append(nn.Linear(layer_sizes[i], layer_sizes[i + 1]))
        if dropout_prob > 0:
            layers.append(nn.Dropout(dropout_prob))
    final_dim = layer_sizes[-1]
    layers.append(nn.LayerNorm(final_dim))
    layers.append(nn.Sigmoid())
    return nn.Sequential(*layers)


class ChannelAttention1D(nn.Module):
    def __init__(self, channels, reduction=16):
        super().__init__()
        self.fc = nn.Sequential(
            nn.Linear(channels, channels // reduction, bias=False),
            nn.ReLU(),
            nn.Linear(channels // reduction, channels, bias=False),
        )

    def forward(self, x):
        avg = x.mean(dim=2)
        mx = x.max(dim=2)[0]
        w = self.fc(avg) + self.fc(mx)
        w = torch.sigmoid(w).unsqueeze(2)
        return x * w


class SpatialAttention1D(nn.Module):
    def __init__(self, kernel_size=7):
        super().__init__()
        self.conv = nn.Conv1d(2, 1, kernel_size, padding=(kernel_size - 1) // 2, bias=False)

    def forward(self, x):
        avg = x.mean(dim=1, keepdim=True)
        mx, _ = x.max(dim=1, keepdim=True)
        y = torch.cat([avg, mx], dim=1)
        w = torch.sigmoid(self.conv(y))
        return x * w


class BatchTreeConvCBAM(nn.Module):
    """Tree convolution with optional CBAM attention. From all_ratio.py."""

    def __init__(self, feat_dim: int, hidden_dims: List[int] = None, num_kernels: int = 4,
                 reduction: int = 16, kernel_size: int = 7, use_cbam: bool = False,
                 dropout_prob: float = 0.0, device: str = 'cpu'):
        super().__init__()
        self.device = device
        self.D = feat_dim
        self.K = num_kernels
        self.use_cbam = use_cbam

        total_in = 3 * self.D
        total_out = self.K * self.D
        if hidden_dims:
            mlp_sizes = [total_in] + hidden_dims + [total_out]
            self.kernel_mlp = create_tree_mlp(mlp_sizes, dropout_prob=dropout_prob)
        else:
            self.kernel_mlp = nn.Linear(total_in, total_out)

        if self.use_cbam:
            self.cbam = nn.Sequential(
                ChannelAttention1D(self.K, reduction=reduction),
                SpatialAttention1D(kernel_size=kernel_size)
            )

    def forward(self, node_features, children, root_ids):
        N, D, K = node_features.size(0), self.D, self.K
        dev = node_features.device

        if N == 0:
            empty = torch.zeros(0, K * D, device=dev)
            return empty, empty

        # Build parent info and compute bottom-up depth (leaves=0)
        parents = [[] for _ in range(N)]
        unvis = [len(ch) for ch in children]
        for p_idx, ch_list in enumerate(children):
            for c in ch_list:
                parents[c].append(p_idx)

        depth = [0] * N
        queue = [i for i in range(N) if not children[i]]
        order = []
        while queue:
            nid = queue.pop(0)
            order.append(nid)
            for p in parents[nid]:
                depth[p] = max(depth[p], depth[nid] + 1)
                unvis[p] -= 1
                if unvis[p] == 0:
                    queue.append(p)

        # Group nodes by depth level
        max_depth = max(depth)
        levels = [[] for _ in range(max_depth + 1)]
        for nid in range(N):
            levels[depth[nid]].append(nid)

        # Per-node embeddings stored in list (autograd-safe, no in-place tensor ops)
        node_embs: List[Optional[torch.Tensor]] = [None] * N

        # Level 0: leaves — expand self_feat to K copies, no MLP
        if levels[0]:
            leaf_ids = levels[0]
            leaf_feats = node_features[leaf_ids]  # (n_leaves, D)
            leaf_expanded = leaf_feats.unsqueeze(1).expand(-1, K, -1).reshape(-1, K * D)
            for i, nid in enumerate(leaf_ids):
                node_embs[nid] = leaf_expanded[i]

        # Precompute kernel diagonal index
        k_idx = torch.arange(K, device=dev)
        zero_kd = torch.zeros(K, D, device=dev)

        # Process levels bottom-up (batched per level)
        for d in range(1, max_depth + 1):
            nodes = levels[d]
            if not nodes:
                continue
            n = len(nodes)

            # Gather self features: (n, K, D)
            self_mat = node_features[nodes].unsqueeze(1).expand(-1, K, -1)

            # Gather left/right child embeddings
            left_list = []
            right_list = []
            for nid in nodes:
                ch = children[nid]
                left_list.append(node_embs[ch[0]].view(K, D))
                right_list.append(
                    node_embs[ch[1]].view(K, D) if len(ch) > 1 else zero_kd
                )

            left_batch = torch.stack(left_list)    # (n, K, D)
            right_batch = torch.stack(right_list)   # (n, K, D)

            # Concatenate [self, left, right]: (n, K, 3D) -> MLP: (n*K, 3D) -> (n*K, K*D)
            cat = torch.cat([self_mat, left_batch, right_batch], dim=2)
            flat = self.kernel_mlp(cat.reshape(n * K, 3 * D))

            # Extract diagonal: for kernel k, take output slice [k*D:(k+1)*D]
            # Reshape (n*K, K*D) -> (n, K, K, D), take [:, k, k, :] for each k
            diag = flat.view(n, K, K, D)[:, k_idx, k_idx, :]  # (n, K, D)

            # Optional CBAM attention
            if self.use_cbam:
                diag = self.cbam(diag)

            # Store per-node embeddings
            diag_flat = diag.reshape(n, K * D)
            for i, nid in enumerate(nodes):
                node_embs[nid] = diag_flat[i]

        # Collect results
        node_embeddings = torch.stack(node_embs, dim=0)
        if isinstance(root_ids, (list, tuple)):
            root_embs = node_embeddings[root_ids]
            return root_embs, node_embeddings
        else:
            return node_embeddings[root_ids], node_embeddings


# ===================== Tree Processing Helpers (from all_ratio.py) =====================

def create_column_mapping(data_dict):
    """Create column-to-number mapping for feature construction."""
    column_to_number = {}
    for key, table_columns in data_dict.items():
        benchmark_sfix = key.split('_')[0]
        table_num = columns_num_job
        if benchmark_sfix == "tpcds":
            table_num = columns_num_tpcds
        elif benchmark_sfix == "ssb":
            table_num = columns_num_ssb
        elif benchmark_sfix == "tpch":
            table_num = columns_num_tpch

        for col in table_columns:
            table_key = key.replace(key.split('_')[0] + '_', '')
            if table_key in table_num:
                table_volume = table_num[table_key]
            else:
                table_volume = 1
            column_to_number[key + '.' + col] = max(len(str(table_volume)), 1)

    def map_columns_to_numbers(columns):
        return [column_to_number.get(col, col) for col in columns]

    return column_to_number, map_columns_to_numbers


def construct_tree(nodes, adjacency_matrix):
    """Build nested tuple tree from nodes and adjacency matrix."""
    def build_subtree(node_index):
        node = tuple(nodes[node_index])
        children = [i for i, is_child in enumerate(adjacency_matrix[node_index]) if is_child]
        if not children:
            return (node,)
        return (node,) + tuple(build_subtree(child) for child in children)

    root = next(i for i, row in enumerate(zip(*adjacency_matrix)) if sum(row) == 0)
    return build_subtree(root)


def build_batch_representation(nested_tree):
    """Convert nested tuple tree to flat batch representation."""
    node_features = []
    children = []

    def dfs(tree):
        idx = len(node_features)
        clean_vals = [
            0.0 if isinstance(v, str) else float(v)
            for v in tree[0]
        ]
        parent_feat = torch.tensor(clean_vals, dtype=torch.float)
        node_features.append(parent_feat)
        children.append([])
        for child_subtree in tree[1:]:
            child_idx = dfs(child_subtree)
            children[idx].append(child_idx)
        return idx

    root_id = dfs(nested_tree)
    node_features_tensor = torch.stack(node_features, dim=0)
    return node_features_tensor, children, root_id


def convert_to_flat_list(data, hists, benchmark_sfix):
    """Convert tree vector data to flat feature list."""
    flat_list = []
    flat_list.append(data['operator'])
    flat_list.append(len(str(data.get('limit', 0))))

    mtable = mapping_table_job
    table_num = columns_num_job_short
    if benchmark_sfix == "tpcds":
        mtable = mapping_table_tpcds
        table_num = columns_num_tpcds_short
    elif benchmark_sfix == "ssb":
        mtable = mapping_table_ssb
        table_num = columns_num_ssb_short
    elif benchmark_sfix == "tpch":
        mtable = mapping_table_tpch
        table_num = columns_num_tpch_short

    # Process predicate
    if isinstance(data['predicate'], tuple):
        if data['predicate'][0] is not None and data['predicate'][0] != '':
            table = data['predicate'][0].split('_')[0]
            column = data['predicate'][0]
            prefixes = ["StartsWith", "EndsWith", "Contains", "LIKE"]
            if data['predicate'][1] in prefixes:
                num = table_num.get(table, 1) * 0.1
            elif '#' in str(data['predicate'][2]):
                num = table_num.get(table, 1)
            else:
                num = query_data(hists, column, data['predicate'][2],
                                 data['predicate'][1] if data['predicate'][1] != "IN" else '=', table_num)
            flat_list.extend([num, 0])
        else:
            flat_list.extend([0, 0])
    else:
        if data['predicate'] == 'AND':
            flat_list.extend([0, 1])
        elif data['predicate'] == 'OR':
            flat_list.extend([0, 2])
        else:
            if isinstance(data['predicate'], str) and data['predicate'].split('_')[0] in mtable:
                num = table_num.get(data['predicate'].split('_')[0], 0)
                flat_list.extend([num, 0])
            else:
                if isinstance(data['predicate'], str) and data['predicate'] != '':
                    table_col = data['predicate'].split()[-1]
                    if '_' in table_col and table_col.split('_')[0] in table_num:
                        num = table_num[table_col.split('_')[0]]
                    else:
                        num = 0
                    flat_list.extend([num, 0])
                else:
                    flat_list.extend([0, 0])

    # Add table columns
    for item in data.get('scan_table_column', []):
        flat_list.append(item if item is not None else 0)
    for item in data.get('join_table_column', []):
        if item is not None:
            flat_list.append(item[0] if isinstance(item, (list, tuple)) and len(item) > 0 else 0)
            flat_list.append(item[2] if isinstance(item, (list, tuple)) and len(item) > 2 else 0)
        else:
            flat_list.append(0)
            flat_list.append(0)
    for item in data.get('project_table_column', []):
        flat_list.append(0 if item is None else item)
    for item in data.get('aggregate_table_column', []):
        flat_list.append(0 if item is None else item)
    for item in data.get('sort_table_column', []):
        flat_list.append(0 if item is None else item)

    return flat_list


def augment_tree_subtree(nested_tree, swap_prob=0.3):
    """Augment tree by randomly swapping subtrees."""
    def _recurse(tree):
        node = tree[0]
        children = list(tree[1:])
        if len(children) == 2 and random.random() < swap_prob:
            children[0], children[1] = children[1], children[0]
        new_children = [_recurse(child) for child in children]
        return (node, *new_children)
    return _recurse(nested_tree)


def get_plan_tree(folder_path, benchmark, schema, num_augment=2, swap_prob=0.3,
                  hists_dir=None):
    """Process plan files and build tree representations.

    Args:
        folder_path: Directory containing *_plan.txt files
        benchmark: Benchmark name (e.g., 'tpcds', 'tpch', 'ssb', 'job')
        schema: 'cross' to load all benchmarks
        num_augment: Number of augmented copies per tree
        swap_prob: Probability of swapping children during augmentation
        hists_dir: Directory containing histogram .txt files

    Returns:
        dict mapping query_key -> (node_feats, children, root_id)
    """
    if hists_dir is None:
        hists_dir = HIST_DIR

    print(f"Processing plan files from: {folder_path}")
    print(f"Benchmark: {benchmark}, Schema: {schema}")

    sql_matrix, sql_tree, sql_vectors, sql_rel, tree_height = process_general_plans(
        folder_path, benchmark, schema
    )

    hists = parse_files(hists_dir)

    for key, value in sql_vectors.items():
        mapping_list = []
        for vector in value['vectors']:
            mapping_list.append(
                convert_to_flat_list(vector, hists, key.split('_')[0])
            )
        sql_vectors[key]['vectors'] = mapping_list

    orig_plans_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   'orig_train', 'train', 'data', 'experiment_3',
                                   'unify_plans', 'plans', 'original')
    mapping_tables = extract_all_table_names(orig_plans_dir)
    column_to_number, map_columns_to_numbers = create_column_mapping(mapping_tables)

    mapped_tree = {}
    for qname, vec_info in sql_vectors.items():
        if qname not in tree_height:
            continue
        mapped_vector = [map_columns_to_numbers(col) for col in vec_info['vectors']]
        nested = construct_tree(mapped_vector, tree_height[qname])

        # Original tree
        node_feats, children, root_id = build_batch_representation(nested)
        mapped_tree[qname.replace('.sql', '')] = (node_feats, children, root_id)

        # Augmented trees
        for i in range(num_augment):
            aug_nested = augment_tree_subtree(nested, swap_prob=swap_prob)
            node_feats_a, children_a, root_id_a = build_batch_representation(aug_nested)
            aug_name = f"{qname}_aug{i + 1}"
            mapped_tree[aug_name.replace('.sql', '')] = (node_feats_a, children_a, root_id_a)

    if not mapped_tree:
        print("[WARN] mapped_tree is empty!")
    else:
        print(f"Built {len(mapped_tree)} tree representations "
              f"({len(mapped_tree) // (num_augment + 1)} base + augmented)")

    return mapped_tree


# ===================== Batch Forest Construction =====================

def batch_construct_forest(mapped_tree):
    """Combine multiple trees into a single batch for BatchTreeConvCBAM."""
    all_feats = []
    all_children = []
    root_ids = []
    offset = 0

    for qname, (node_feats, children, root_id) in mapped_tree.items():
        N = node_feats.size(0)
        all_feats.append(node_feats)
        for ch in children:
            all_children.append([c + offset for c in ch])
        root_ids.append(root_id + offset)
        offset += N

    all_feats = torch.cat(all_feats, dim=0)
    return all_feats, all_children, root_ids


def compute_all_query_root_embeddings(mapped_tree, tree_model, device, detach_emb=False):
    """Compute root embeddings for all queries in mapped_tree."""
    all_feats, all_children, root_ids = batch_construct_forest(mapped_tree)
    all_feats = all_feats.to(device)

    root_embs, _ = tree_model(all_feats, all_children, root_ids)

    if detach_emb:
        root_embs = root_embs.detach()

    return list(mapped_tree.keys()), root_embs


# ===================== Query Name to Plan Key Mapping =====================

def build_query_to_plan_mapping(q2idx, mapped_tree_keys):
    """Map normalized query names (sf{N}_{base}) to plan tree keys.

    Args:
        q2idx: dict {normalized_query_name: int_index}
        mapped_tree_keys: set of plan tree keys (excluding augmented)

    Returns:
        dict {int_index: plan_tree_key} for queries that have matching plans
    """
    base_keys = {k for k in mapped_tree_keys if '_aug' not in k}

    mapping = {}  # idx -> tree_key
    unmapped = []

    for qname, idx in q2idx.items():
        # Strip sf{N}_ prefix to get base query name
        m = re.match(r'^sf\d+_(.+)$', qname)
        base = m.group(1) if m else qname

        found = None

        # Strategy 1: tpcds query{N} → tpcds_q{N}
        if base.startswith('query'):
            suffix = base[5:]  # e.g., "1", "14_a", "14_b"
            key = 'tpcds_q' + suffix.replace('_', '')
            if key in base_keys:
                found = key

        # Strategy 2: tpcds tpcds_q{N} → tpcds_q{N}
        if not found and base.startswith('tpcds_q'):
            if base in base_keys:
                found = base
            else:
                # Try without underscore: tpcds_q_01 → tpcds_q1
                cleaned = re.sub(r'tpcds_q_0*(\d+)', r'tpcds_q\1', base)
                if cleaned in base_keys:
                    found = cleaned

        # Strategy 3: tpch db{N} → tpch_1_db{N} or tpch_0_q{N}
        if not found:
            m2 = re.match(r'^db(\d+)$', base)
            if m2:
                num = m2.group(1)
                for candidate in [f'tpch_1_db{num}', f'tpch_0_q{num}']:
                    if candidate in base_keys:
                        found = candidate
                        break

        # Strategy 4: ssb q{M}_{N} → ssb_Q{M}.{N} or ssb_q{M}{N}
        if not found:
            m3 = re.match(r'^q(\d+)_(\d+)$', base)
            if m3:
                for candidate in [f'ssb_Q{m3.group(1)}.{m3.group(2)}',
                                  f'ssb_q{m3.group(1)}{m3.group(2)}']:
                    if candidate in base_keys:
                        found = candidate
                        break

        # Strategy 5: ssb ssb_q{N} → ssb_q{N}
        if not found and base.startswith('ssb_'):
            if base in base_keys:
                found = base

        # Strategy 6: job job_{X} → job_{X}
        if not found and base.startswith('job_'):
            if base in base_keys:
                found = base

        # Strategy 7: tpch q{N} → tpch_0_q{N}
        if not found:
            m4 = re.match(r'^q(\d+)$', base)
            if m4:
                num = m4.group(1)
                for candidate in [f'tpch_0_q{num}', f'tpcds_q{num}']:
                    if candidate in base_keys:
                        found = candidate
                        break

        # Strategy 8: gpt query variants
        if not found and 'gpt' in base:
            # tpcds_gpt_q_1, etc.
            if base in base_keys:
                found = base

        if found:
            mapping[idx] = found
        else:
            unmapped.append(qname)

    if unmapped:
        print(f"  Queries without plan mapping ({len(unmapped)}/{len(q2idx)}):")
        # Print first few
        for q in unmapped[:10]:
            print(f"    {q}")
        if len(unmapped) > 10:
            print(f"    ... and {len(unmapped) - 10} more")

    print(f"  Plan mapping: {len(mapping)}/{len(q2idx)} queries mapped to plans")
    return mapping


# ===================== Tree Key Fix (from all_ratio.py line 1740-1743) =====================

def fix_tree_keys(mapped_tree):
    """Apply tree key normalization from original code."""
    tmp_tree = {}
    for key, value in mapped_tree.items():
        new_key = key.replace('query', 'q').replace('aug', '_aug').replace('_a', 'a').replace('_b', 'b')
        tmp_tree[new_key] = value
    return tmp_tree


# ===================== TreeQueryEncoder =====================

class TreeQueryEncoder(nn.Module):
    """Query encoder using tree convolution for structural embeddings.

    For queries with plan files: uses BatchTreeConvCBAM to compute embeddings.
    For queries without plans (e.g., ssb_flat): uses fallback learnable embedding.
    """

    def __init__(self, mapped_tree, idx_to_tree_key, num_queries, feat_dim,
                 hidden_dims=None, num_kernels=4, dropout_prob=0.5, device='cpu',
                 proj_dim=0):
        super().__init__()
        self.num_queries = num_queries
        self.feat_dim = feat_dim
        self.num_kernels = num_kernels
        self.raw_emb_dim = feat_dim * num_kernels  # 288
        self.use_projection = (proj_dim > 0)
        self.emb_dim = proj_dim if self.use_projection else self.raw_emb_dim
        self._device = device

        # Tree convolution model
        self.tree_model = BatchTreeConvCBAM(
            feat_dim=feat_dim,
            hidden_dims=hidden_dims if hidden_dims else [256, 128],
            num_kernels=num_kernels,
            dropout_prob=dropout_prob,
            device=device,
        )

        # Optional projection: raw_emb_dim (288) -> proj_dim
        if self.use_projection:
            self.projection = nn.Sequential(
                nn.Linear(self.raw_emb_dim, proj_dim),
                nn.LayerNorm(proj_dim),
                nn.GELU(),
            )

        # Fallback embedding for queries without plans
        self.fallback_embedding = nn.Embedding(num_queries, self.emb_dim)
        nn.init.xavier_uniform_(self.fallback_embedding.weight)

        # Build a minimal mapped_tree containing only the trees actually used
        needed_keys = set(idx_to_tree_key.values())
        self.needed_tree = {k: v for k, v in mapped_tree.items() if k in needed_keys}
        self.idx_to_tree_key = idx_to_tree_key  # {int: str}

        # Reverse mapping: tree_key -> list of query indices using it
        self._tree_key_to_indices = {}
        for idx, key in idx_to_tree_key.items():
            if key not in self._tree_key_to_indices:
                self._tree_key_to_indices[key] = []
            self._tree_key_to_indices[key].append(idx)

        self._raw_table = None
        self._has_tree = None

        print(f"  TreeQueryEncoder: {len(self.needed_tree)} needed trees "
              f"(from {len(mapped_tree)} total), {len(needed_keys)} unique plan keys")

    def recompute_tree_embeddings(self, device):
        """Precompute tree embeddings into a detached lookup table."""
        if not self.needed_tree:
            self._emb_table = self.fallback_embedding.weight.detach().clone()
            return

        with torch.no_grad():
            keys, raw_embs = compute_all_query_root_embeddings(
                self.needed_tree, self.tree_model, device, detach_emb=True
            )
            if self.use_projection:
                embs = self.projection(raw_embs)
            else:
                embs = raw_embs
        tree_emb_dict = dict(zip(keys, embs))

        table = self.fallback_embedding.weight.detach().clone()
        for idx, tree_key in self.idx_to_tree_key.items():
            if tree_key in tree_emb_dict:
                table[idx] = tree_emb_dict[tree_key]
        self._emb_table = table

    def precompute_training_table(self, device):
        """Precompute ALL tree embeddings WITH gradients into a lookup table.

        Call once per training phase. The returned table has grad_fn so that
        loss.backward() propagates gradients through tree model + projection.
        Subsequent forward_train() calls are pure index lookups (fast).
        """
        if not self.needed_tree:
            self._train_table = self.fallback_embedding.weight
            return

        # One forward pass through ALL needed trees (with gradients)
        keys, embs = compute_all_query_root_embeddings(
            self.needed_tree, self.tree_model, device, detach_emb=False
        )
        if self.use_projection:
            embs = self.projection(embs)
        tree_emb_dict = dict(zip(keys, embs))

        # Build table: fallback for unmapped queries, projected tree emb for mapped ones
        # Use torch.stack (no in-place ops) to keep autograd clean
        rows = []
        for idx in range(self.num_queries):
            tree_key = self.idx_to_tree_key.get(idx)
            if tree_key and tree_key in tree_emb_dict:
                rows.append(tree_emb_dict[tree_key])
            else:
                rows.append(self.fallback_embedding.weight[idx])
        self._train_table = torch.stack(rows)

    def forward_train(self, query_ids):
        """Fast lookup from precomputed training table (WITH gradients).

        Must call precompute_training_table() first. Gradients flow back
        through the tree model via the shared computation graph.
        """
        return self._train_table[query_ids]

    def forward_with_grad(self, query_ids):
        """Compute embeddings with gradient flow through tree model.

        Per-workload tree conv forward. Use forward_train() instead for
        batch-precomputed lookups (much faster).
        """
        query_ids_list = query_ids.tolist() if isinstance(query_ids, torch.Tensor) else query_ids

        # Collect unique tree keys needed for this batch
        unique_keys = set()
        for idx in query_ids_list:
            key = self.idx_to_tree_key.get(idx)
            if key:
                unique_keys.add(key)

        # Compute tree embeddings for needed keys
        if unique_keys:
            batch_tree = {k: self.needed_tree[k] for k in unique_keys if k in self.needed_tree}
            if batch_tree:
                all_feats, all_children, root_ids = batch_construct_forest(batch_tree)
                all_feats = all_feats.to(query_ids.device if isinstance(query_ids, torch.Tensor)
                                          else self.fallback_embedding.weight.device)
                root_embs, _ = self.tree_model(all_feats, all_children, root_ids)
                if self.use_projection:
                    root_embs = self.projection(root_embs)
                tree_emb_dict = dict(zip(batch_tree.keys(), root_embs))
            else:
                tree_emb_dict = {}
        else:
            tree_emb_dict = {}

        # Build output embedding tensor
        rows = []
        for idx in query_ids_list:
            tree_key = self.idx_to_tree_key.get(idx)
            if tree_key and tree_key in tree_emb_dict:
                rows.append(tree_emb_dict[tree_key])
            else:
                rows.append(self.fallback_embedding.weight[idx])

        return torch.stack(rows)

    def forward(self, query_ids):
        """Fast lookup from precomputed detached table."""
        if hasattr(self, '_emb_table') and self._emb_table is not None:
            return self._emb_table[query_ids]
        return self.fallback_embedding(query_ids)


# ===================== Convenience: Load and Prepare Everything =====================

def _pad_tree_features(mapped_tree, target_dim):
    """Pad node features in all trees to target_dim (zero-pad on right)."""
    padded = {}
    for key, (node_feats, children, root_id) in mapped_tree.items():
        if node_feats.size(1) < target_dim:
            padding = torch.zeros(node_feats.size(0), target_dim - node_feats.size(1))
            node_feats = torch.cat([node_feats, padding], dim=1)
        padded[key] = (node_feats, children, root_id)
    return padded


CACHE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.tree_cache.pt')


def load_all_plan_trees(plan_dir=None, num_augment=5, swap_prob=0.3, use_cache=True):
    """Load all plan trees from disk and return mapped_tree + feat_dim.

    Processes each benchmark separately then pads to a common feature dimension.
    Results are cached to disk for fast subsequent loads.

    Returns:
        mapped_tree: dict {tree_key: (node_feats, children, root_id)}
        feat_dim: int, feature dimension of tree nodes
    """
    if plan_dir is None:
        plan_dir = PLAN_DIR

    if not os.path.isdir(plan_dir):
        print(f"[WARN] Plan directory not found: {plan_dir}")
        return {}, 0

    # Try loading from cache
    if use_cache and os.path.exists(CACHE_PATH):
        try:
            cache = torch.load(CACHE_PATH, map_location='cpu')
            if (cache.get('num_augment') == num_augment and
                    cache.get('plan_dir') == plan_dir):
                mapped_tree = cache['mapped_tree']
                feat_dim = cache['feat_dim']
                print(f"Loaded {len(mapped_tree)} trees from cache (feat_dim={feat_dim})")
                return mapped_tree, feat_dim
        except Exception as e:
            print(f"Cache load failed: {e}")

    # Process each benchmark separately
    mapped_tree_all = {}
    feat_dims = []
    benchmarks = ['tpch', 'tpcds', 'ssb', 'job']

    for bm in benchmarks:
        import time as _time
        t0 = _time.time()
        print(f"Processing {bm} plans...")
        mt = get_plan_tree(plan_dir, bm, bm,
                           num_augment=num_augment, swap_prob=swap_prob)
        mt = fix_tree_keys(mt)
        if mt:
            any_key = next(iter(mt))
            fd = mt[any_key][0].size(1)
            feat_dims.append(fd)
            mapped_tree_all[bm] = mt
            print(f"  {bm}: {len(mt)} trees, feat_dim={fd}, {_time.time()-t0:.1f}s")

    if not mapped_tree_all:
        print("[WARN] No plan trees loaded!")
        return {}, 0

    # Pad all trees to max feature dimension
    max_feat_dim = max(feat_dims)
    print(f"\nPadding all tree features to dim={max_feat_dim}")

    mapped_tree = {}
    for bm, mt in mapped_tree_all.items():
        padded = _pad_tree_features(mt, max_feat_dim)
        mapped_tree.update(padded)

    feat_dim = max_feat_dim
    base_count = sum(1 for k in mapped_tree if '_aug' not in k)
    print(f"Total: {len(mapped_tree)} trees ({base_count} base), feat_dim={feat_dim}")

    # Cache to disk
    if use_cache:
        try:
            torch.save({
                'mapped_tree': mapped_tree,
                'feat_dim': feat_dim,
                'num_augment': num_augment,
                'plan_dir': plan_dir,
            }, CACHE_PATH)
            print(f"Cached to {CACHE_PATH}")
        except Exception as e:
            print(f"Cache save failed: {e}")

    return mapped_tree, feat_dim
