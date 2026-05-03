#!/usr/bin/env -S python3 -u
"""
LKHelm Training Script - TwoGateMoE with Tree Embeddings.

Trains a Mixture of Experts model to predict optimal (engine, datalake, config)
combinations for database query workloads. Uses leave-one-out cross-validation
across 5 benchmarks (tpcds, tpch, ssb, ssb_flat, job) at scale factors 1/10/100.

Usage:
  python3 train_local.py --benchmark tpcds --sf 100
  python3 train_local.py --benchmark job --sf 10 --epochs 60
"""

import csv
import os
import time
import math
import random
import numpy as np

# Workaround: torch 2.3.0dev lazily imports transformers via onnx, which breaks
# due to huggingface_hub version mismatch. Block that import chain.
import sys
import types
_fake_transformers = types.ModuleType('transformers')
_fake_transformers.__version__ = '0.0.0'
sys.modules.setdefault('transformers', _fake_transformers)

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.optim import SGD, Adam
from torch.optim.lr_scheduler import CosineAnnealingLR
from typing import List, Dict, Any, Tuple, Optional
from collections import defaultdict
from pathlib import Path

from tree_embedding import (
    load_all_plan_trees, build_query_to_plan_mapping, TreeQueryEncoder
)

SEED = 42
random.seed(SEED)
np.random.seed(SEED)
torch.manual_seed(SEED)

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Using device: {device}")

DATA_DIR = Path(os.environ.get("LKHELM_DATA_DIR", os.path.join(os.path.dirname(__file__), "data", "output")))

combos = [
    (0, 0), (0, 1), (0, 2),  # spark+delta, spark+iceberg, spark+hudi
    (1, 0), (1, 1), (1, 2),  # presto+delta, presto+iceberg, presto+hudi
    (2, 0), (2, 1), (2, 2),  # trino+delta, trino+iceberg, trino+hudi
]

COMBO_NAMES = {
    (0,0): "spark/delta", (0,1): "spark/iceberg", (0,2): "spark/hudi",
    (1,0): "presto/delta", (1,1): "presto/iceberg", (1,2): "presto/hudi",
    (2,0): "trino/delta", (2,1): "trino/iceberg", (2,2): "trino/hudi",
}

ENGINE_NAME2ID = {'spark': 0, 'presto': 1, 'trino': 2}
LAKE_NAME2ID = {'delta': 0, 'iceberg': 1, 'hudi': 2}

import re as _re

def normalize_query_name(qname, sf):
    """Normalize query name: strip datalake prefix, add sf prefix.
    tpch_0_q1 -> sf10_q1,  db1 -> sf1_db1,  job_10a -> sf10_job_10a
    """
    # Strip benchmark+datalake prefix: tpch_0_q1 -> q1, ssb_2_q11 -> q11
    m = _re.match(r'^[a-z][\w-]*_\d+_(q\w+)$', qname)
    if m:
        qname = m.group(1)
    return f"sf{sf}_{qname}"


def parse_conf(conf_str):
    """Parse conf string into (engine_config, lake_config) float lists.
    Formats:
      - 'engine;vals|lake;vals'  (pipe-separated)
      - 'key=val;key=val'       (semicolon key=value pairs from normalized data)
      - 'val;val;val'           (plain semicolons)
    """
    if '|' in conf_str:
        parts = conf_str.split('|', 1)
        try:
            engine_config = [float(x) for x in parts[0].split(';') if x.strip()]
        except ValueError:
            engine_config = [0.0]
        try:
            lake_config = [float(x) for x in parts[1].split(';') if x.strip()]
        except ValueError:
            lake_config = [0.0]
    else:
        # Try to parse as plain semicolons of numbers
        vals = []
        for x in conf_str.split(';'):
            x = x.strip()
            # Strip units like MB, GB etc
            x = _re.sub(r'[a-zA-Z]+$', '', x)
            if '=' in x:
                x = x.split('=', 1)[1]
            try:
                vals.append(float(x))
            except ValueError:
                vals.append(0.0)
        engine_config = vals
        lake_config = [0.0]
    return engine_config, lake_config

# ===================== Neural Network Components =====================

def create_mlp(layer_sizes, dropout_prob=0.0):
    layers = []
    for i in range(len(layer_sizes) - 1):
        layers.append(nn.Linear(layer_sizes[i], layer_sizes[i + 1]))
        if i < len(layer_sizes) - 2:
            layers.append(nn.LayerNorm(layer_sizes[i + 1]))
            layers.append(nn.ReLU())
            if dropout_prob > 0:
                layers.append(nn.Dropout(dropout_prob))
    return nn.Sequential(*layers)


def build_mlp_moe(input_dim, hidden_dims, output_dim, dropout_prob=0.5):
    if isinstance(output_dim, (list, tuple)):
        output_dim = output_dim[0]
    layers = []
    prev_dim = input_dim
    for h in hidden_dims:
        layers.append(nn.Linear(prev_dim, h))
        layers.append(nn.LayerNorm(h))
        layers.append(nn.ReLU())
        layers.append(nn.Dropout(dropout_prob))
        prev_dim = h
    layers.append(nn.Linear(prev_dim, output_dim))
    return nn.Sequential(*layers)


class ConfEncoder(nn.Module):
    """Encode raw config vector into a dense representation."""
    def __init__(self, conf_dim, hidden_dim=64, out_dim=64, dropout_prob=0.1):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(conf_dim, hidden_dim),
            nn.LayerNorm(hidden_dim),
            nn.ReLU(),
            nn.Dropout(dropout_prob),
            nn.Linear(hidden_dim, hidden_dim),
            nn.LayerNorm(hidden_dim),
            nn.ReLU(),
            nn.Dropout(dropout_prob),
            nn.Linear(hidden_dim, out_dim),
        )

    def forward(self, conf):
        return self.net(conf)


class TwoGateMoE(nn.Module):
    ENGINE_CLASSES = 3
    LAKE_CLASSES = 3
    EXP_VEC_DIM = 128
    CONF_ENC_DIM = 64

    def __init__(self, emb_dim, conf_dim, gate_hidden_dims=None,
                 expert_hidden_dims=None, dropout_prob=0.05, gate_emb_dim=None):
        super().__init__()
        # Gate can use richer input (e.g., mean+std+max of query embeddings)
        gate_in_dim = gate_emb_dim if gate_emb_dim else emb_dim
        self.conf_encoder = ConfEncoder(conf_dim, hidden_dim=64, out_dim=self.CONF_ENC_DIM,
                                        dropout_prob=dropout_prob)
        self.engine_gate = (
            build_mlp_moe(gate_in_dim, gate_hidden_dims, self.ENGINE_CLASSES, dropout_prob)
            if gate_hidden_dims else nn.Linear(gate_in_dim, self.ENGINE_CLASSES)
        )
        self.lake_gate = (
            build_mlp_moe(gate_in_dim, gate_hidden_dims, self.LAKE_CLASSES, dropout_prob)
            if gate_hidden_dims else nn.Linear(gate_in_dim, self.LAKE_CLASSES)
        )
        inp_dim_eng = emb_dim + self.CONF_ENC_DIM + self.LAKE_CLASSES
        self.engine_experts = nn.ModuleList([
            build_mlp_moe(inp_dim_eng, expert_hidden_dims, self.EXP_VEC_DIM, dropout_prob)
            if expert_hidden_dims else nn.Linear(inp_dim_eng, self.EXP_VEC_DIM)
            for _ in range(self.ENGINE_CLASSES)
        ])
        inp_dim_lake = emb_dim + self.CONF_ENC_DIM + self.ENGINE_CLASSES
        self.lake_experts = nn.ModuleList([
            build_mlp_moe(inp_dim_lake, expert_hidden_dims, self.EXP_VEC_DIM, dropout_prob)
            if expert_hidden_dims else nn.Linear(inp_dim_lake, self.EXP_VEC_DIM)
            for _ in range(self.LAKE_CLASSES)
        ])
        self.post_mlp = build_mlp_moe(
            2 * self.EXP_VEC_DIM, [128, 256, 256, 128, 64], 1, dropout_prob
        )

    def forward(self, w_emb, conf, use_gumbel=False, tau=1.0):
        """Paper-style forward.

        Returns: pred, eng_probs, lak_probs, eng_logits, lak_logits.
        Routing is differentiable: experts' contributions are weighted by
        Gumbel/softmax probabilities (no hard arg-max in the forward path).
        The expert input from the *other* gate is its soft probabilities
        (instead of one-hot) so gradients flow through both gates.
        """
        conf_enc = self.conf_encoder(conf)
        eng_logits = self.engine_gate(w_emb)
        lak_logits = self.lake_gate(w_emb)

        if use_gumbel:
            eng_probs = F.gumbel_softmax(eng_logits, tau=tau, hard=False)
            lak_probs = F.gumbel_softmax(lak_logits, tau=tau, hard=False)
        else:
            eng_probs = F.softmax(eng_logits, dim=-1)
            lak_probs = F.softmax(lak_logits, dim=-1)

        # Engine experts conditioned on lake soft probs (lets gradients flow to lake gate too)
        eng_inp = torch.cat([w_emb, conf_enc, lak_probs], dim=1)
        eng_vecs = torch.stack([exp(eng_inp) for exp in self.engine_experts], dim=1)
        eng_feat = (eng_probs.unsqueeze(-1) * eng_vecs).sum(1)

        # Lake experts conditioned on engine soft probs
        lak_inp = torch.cat([w_emb, conf_enc, eng_probs], dim=1)
        lak_vecs = torch.stack([exp(lak_inp) for exp in self.lake_experts], dim=1)
        lak_feat = (lak_probs.unsqueeze(-1) * lak_vecs).sum(1)

        fused = torch.cat([eng_feat, lak_feat], dim=1)
        pred = self.post_mlp(fused)
        return pred, eng_probs, lak_probs, eng_logits, lak_logits

    def forward_for_eng_lak(self, w_emb, conf, eng_id, lak_id):
        """Inference helper: score given (engine, lake) combo + config.

        Used at inference time after gates have selected eng_id, lak_id.
        Routes to that specific engine/lake expert pair.
        """
        conf_enc = self.conf_encoder(conf)
        B = w_emb.size(0)
        eng_onehot = F.one_hot(torch.full((B,), eng_id, device=w_emb.device, dtype=torch.long),
                                self.ENGINE_CLASSES).float()
        lak_onehot = F.one_hot(torch.full((B,), lak_id, device=w_emb.device, dtype=torch.long),
                                self.LAKE_CLASSES).float()
        eng_inp = torch.cat([w_emb, conf_enc, lak_onehot], dim=1)
        eng_feat = self.engine_experts[eng_id](eng_inp)
        lak_inp = torch.cat([w_emb, conf_enc, eng_onehot], dim=1)
        lak_feat = self.lake_experts[lak_id](lak_inp)
        fused = torch.cat([eng_feat, lak_feat], dim=1)
        return self.post_mlp(fused)

    def forward_oracle(self, w_emb, conf, combo_ids):
        """Forward with oracle routing: use actual combo to route to correct expert.
        combo_ids: tensor of shape (B, 2) with [engine_id, lake_id] per record.
        """
        conf_enc = self.conf_encoder(conf)
        B = w_emb.size(0)

        # Create one-hot from actual combo IDs (not gate predictions)
        eng_ids = combo_ids[:, 0].long()
        lak_ids = combo_ids[:, 1].long()

        eng_onehot = F.one_hot(eng_ids, self.ENGINE_CLASSES).float()
        lak_onehot = F.one_hot(lak_ids, self.LAKE_CLASSES).float()

        # Engine experts conditioned on actual lake
        eng_inp = torch.cat([w_emb, conf_enc, lak_onehot], dim=1)
        eng_vecs = torch.stack([exp(eng_inp) for exp in self.engine_experts], dim=1)
        # Route to the correct engine expert only
        eng_feat = (eng_onehot.unsqueeze(-1) * eng_vecs).sum(1)

        # Lake experts conditioned on actual engine
        lak_inp = torch.cat([w_emb, conf_enc, eng_onehot], dim=1)
        lak_vecs = torch.stack([exp(lak_inp) for exp in self.lake_experts], dim=1)
        lak_feat = (lak_onehot.unsqueeze(-1) * lak_vecs).sum(1)

        fused = torch.cat([eng_feat, lak_feat], dim=1)
        pred = self.post_mlp(fused)
        return pred


class QueryEncoder(nn.Module):
    """Legacy learnable query encoder. Kept for checkpoint loading compatibility."""
    def __init__(self, num_queries, emb_dim=128):
        super().__init__()
        self.embedding = nn.Embedding(num_queries, emb_dim)
        nn.init.xavier_uniform_(self.embedding.weight)

    def forward(self, query_ids):
        return self.embedding(query_ids)


# ===================== Data Loading =====================

def load_csv_data(benchmark, supply=False):
    """Load all CSV data from data/output/{benchmark}/sf{1,10,100}/*.csv"""
    if supply:
        benchmark_name = f"{benchmark}_supply"
    else:
        benchmark_name = benchmark

    # Map benchmark name for directory: ssb_flat -> ssb-flat
    dir_name = benchmark_name.replace('_', '-')
    benchmark_dir = DATA_DIR / dir_name
    if not benchmark_dir.exists():
        # Try original name
        benchmark_dir = DATA_DIR / benchmark_name
    if not benchmark_dir.exists():
        print(f"  Warning: {benchmark_dir} does not exist")
        return {}, 0

    grouped_data = {}
    max_dim = 0

    for file_path in sorted(benchmark_dir.rglob("*.csv")):
        filename = file_path.name
        try:
            with open(file_path) as f:
                reader = csv.DictReader(f)
                row_count = 0
                for row in reader:
                    engine_name = row.get('engine', '').strip()
                    lake_name = row.get('datalake', '').strip()
                    qname = row.get('query name', '').strip()
                    conf_str = row.get('conf', '').strip()
                    sf = row.get('sf', '').strip()
                    try:
                        latency = float(row.get('latency', 0))
                    except (ValueError, TypeError):
                        continue

                    if not all([engine_name, lake_name, qname, sf]) or latency < 1500:
                        continue

                    engine_id = ENGINE_NAME2ID.get(engine_name)
                    lake_id = LAKE_NAME2ID.get(lake_name)
                    if engine_id is None or lake_id is None:
                        continue

                    query_id = normalize_query_name(qname, sf)
                    engine_config, lake_config = parse_conf(conf_str)

                    combined = engine_config + lake_config + [float(engine_id), float(lake_id)]
                    max_dim = max(max_dim, len(combined))

                    if query_id not in grouped_data:
                        grouped_data[query_id] = {}
                    combo = (engine_id, lake_id)
                    if combo not in grouped_data[query_id]:
                        grouped_data[query_id][combo] = []
                    grouped_data[query_id][combo].append(
                        (torch.tensor(combined, dtype=torch.float), latency)
                    )
                    row_count += 1
                if row_count > 0:
                    print(f"  Loaded {row_count} rows from {filename}")
        except Exception as e:
            print(f"  Error reading {filename}: {e}")

    return grouped_data, max_dim


def fix_floor_latencies(all_data, floor_val=1500.0):
    """Replace latency=floor_val records with interpolated latencies.

    For each combo: if some records are at floor_val, replace them with
    samples from the non-floor distribution. If ALL records are at floor_val,
    use the global distribution for that combo across all queries.
    """
    rng = np.random.RandomState(42)
    fixed_count = 0

    # Step 1: Collect per-combo non-floor latencies across ALL queries
    combo_real_lats = {}
    for qid, combo_data in all_data.items():
        for cc, recs in combo_data.items():
            if cc not in combo_real_lats:
                combo_real_lats[cc] = []
            for conf, lat in recs:
                if lat != floor_val:
                    combo_real_lats[cc].append(lat)

    # Step 2: For each query+combo, replace floor records
    for qid, combo_data in all_data.items():
        for cc, recs in combo_data.items():
            floor_indices = [i for i, (_, lat) in enumerate(recs) if lat == floor_val]
            if not floor_indices:
                continue

            # Get non-floor lats for this query+combo
            local_real = [lat for _, lat in recs if lat != floor_val]

            if local_real:
                mu = np.mean([math.log(l + 1) for l in local_real])
                sigma = max(np.std([math.log(l + 1) for l in local_real]), 0.1)
            elif combo_real_lats.get(cc):
                all_log = [math.log(l + 1) for l in combo_real_lats[cc]]
                mu = np.mean(all_log)
                sigma = max(np.std(all_log), 0.1)
            else:
                mu = math.log(10000)
                sigma = 0.5

            for idx in floor_indices:
                conf, _ = recs[idx]
                new_log = rng.normal(mu, sigma * 0.5)
                new_lat = max(floor_val + 1, math.exp(new_log) - 1)
                recs[idx] = (conf, new_lat)
                fixed_count += 1

    if fixed_count > 0:
        print(f"  Fixed {fixed_count} floor-latency records")
    return all_data


def get_all_data(test_benchmark=None, test_sf=None, use_supply=False,
                 train_frac=0.70, valid_frac=0.15, test_frac=0.15, split_seed=42):
    """Load benchmark CSVs and split queries RANDOMLY into train/valid/test.

    No leave-one-out splitting. If `test_benchmark` is set, only that benchmark's
    data is loaded; otherwise all benchmarks are pooled. The split is then a
    purely random query-level partition (default 70/15/15). When `test_sf` is
    given, only queries with sf prefix matching `sf{test_sf}_` are kept.

    Returns (all_data, max_dim, train_qnames, valid_qnames, test_qnames).
    """
    all_benchmarks = ['tpcds', 'ssb', 'ssb_flat', 'job', 'tpch']
    benchmarks_to_load = [test_benchmark] if test_benchmark else all_benchmarks
    print(f"\nLoading benchmarks: {benchmarks_to_load}")
    if test_sf is not None:
        print(f"Filter: only queries at sf={test_sf}")
    print(f"Random split: train={train_frac:.0%} valid={valid_frac:.0%} test={test_frac:.0%} (seed={split_seed})")

    all_data = {}
    max_dim = 0
    for bm in benchmarks_to_load:
        print(f"\nLoading ({bm}):")
        bm_data, bm_max_dim = load_csv_data(bm)
        max_dim = max(max_dim, bm_max_dim)
        for qid, qdata in bm_data.items():
            if qid in all_data:
                for cc, recs in qdata.items():
                    if cc not in all_data[qid]:
                        all_data[qid][cc] = recs
                    else:
                        all_data[qid][cc].extend(recs)
            else:
                all_data[qid] = qdata
        if use_supply:
            print(f"  Loading supply ({bm}):")
            supply_data, s_max_dim = load_csv_data(bm, supply=True)
            max_dim = max(max_dim, s_max_dim)
            for qid, qdata in supply_data.items():
                if qid in all_data:
                    for cc, recs in qdata.items():
                        if cc not in all_data[qid]:
                            all_data[qid][cc] = recs
                        else:
                            all_data[qid][cc].extend(recs)
                else:
                    all_data[qid] = qdata

    # Optional sf filter.
    if test_sf is not None:
        sf_prefix = f"sf{test_sf}_"
        all_data = {q: v for q, v in all_data.items() if q.startswith(sf_prefix)}

    # Random query-level split.
    qnames = sorted(all_data.keys())
    rng = random.Random(split_seed)
    rng.shuffle(qnames)
    n = len(qnames)
    n_train = int(n * train_frac)
    n_valid = int(n * valid_frac)
    train_qnames = qnames[:n_train]
    valid_qnames = qnames[n_train:n_train + n_valid]
    test_qnames = qnames[n_train + n_valid:]

    # Repair floor-latency records (1500ms artifacts)
    print("\nFixing floor-latency artifacts...")
    all_data = fix_floor_latencies(all_data, floor_val=1500.0)

    print(f"\nData summary:")
    print(f"  Total queries: {len(all_data)}")
    print(f"  Train: {len(train_qnames)}, Valid: {len(valid_qnames)}, Test: {len(test_qnames)}")
    print(f"  Max config dim: {max_dim}")

    return all_data, max_dim, train_qnames, valid_qnames, test_qnames


# ===================== Workload Generation =====================

def precompute_query_combo_stats(grouped_data):
    """Precompute per-query per-combo: best latency, best config, all (config, latency) pairs."""
    stats = {}
    for qid, combo_data in grouped_data.items():
        stats[qid] = {}
        for cc, recs in combo_data.items():
            best_lat = float('inf')
            best_conf = None
            for conf, lat in recs:
                if lat < best_lat:
                    best_lat = lat
                    best_conf = conf
            stats[qid][cc] = {
                'best_lat': best_lat,
                'best_conf': best_conf,
                'all': recs,
            }
    return stats


def generate_workloads(query_ids, grouped_data, num_workloads, min_q=3, max_q=15,
                       min_combo_coverage=3, seed=42):
    rng = random.Random(seed)
    available = [q for q in query_ids if q in grouped_data]
    if not available:
        return []

    max_q = min(max_q, len(available))
    min_q = min(min_q, len(available))

    # Precompute stats for fast workload generation
    stats = precompute_query_combo_stats(grouped_data)

    workloads = []
    attempts = 0
    while len(workloads) < num_workloads and attempts < num_workloads * 20:
        attempts += 1
        n = rng.randint(min_q, max_q)
        sampled = rng.sample(available, n)

        # Check all sampled queries have enough combo coverage
        # Build per-query data: {qid: {combo: [(conf, lat), ...]}}
        per_query = {}
        valid_combos = set(combos)
        for qid in sampled:
            if qid not in stats:
                valid_combos = set()
                break
            per_query[qid] = {}
            for cc in combos:
                if cc in stats[qid]:
                    per_query[qid][cc] = stats[qid][cc]['all']
                # (query might not have all combos, that's ok)
            # Track which combos ALL queries have
            qid_combos = set(per_query[qid].keys())
            valid_combos &= qid_combos

        # Also build aggregated + combo_totals for training (gate/expert)
        aggregated = {}
        combo_totals = {}
        for cc in valid_combos:
            total_lat = 0.0
            cc_recs = []
            for qid in sampled:
                s = stats[qid][cc]
                total_lat += s['best_lat']
                cc_recs.extend(s['all'])
            aggregated[cc] = cc_recs
            combo_totals[cc] = total_lat

        if len(valid_combos) >= min_combo_coverage:
            workloads.append({
                'query_ids': sampled,
                'aggregated_data': aggregated,
                'combo_totals': combo_totals,
                'per_query_data': per_query,  # for per-query evaluation
            })

    return workloads


# ===================== Workload aggregation =====================

class AttentionPool(nn.Module):
    """Multi-head attention pool over per-query embeddings.

    Each head learns a different "what to look at" projection of input → scalar score.
    Pool = sum_q (softmax_q(score_q) * qembs_q). Concat across heads.
    Result: (1, num_heads * D). Adds (mean, max) raw stats too as residual.
    """
    def __init__(self, dim, num_heads=4):
        super().__init__()
        self.num_heads = num_heads
        self.dim = dim
        # One linear per head: D -> 1 score
        self.score_heads = nn.ModuleList([
            nn.Linear(dim, 1, bias=True) for _ in range(num_heads)
        ])
        for h in self.score_heads:
            nn.init.orthogonal_(h.weight, gain=1.0)
            nn.init.zeros_(h.bias)
        self.out_dim = num_heads * dim + 2 * dim   # heads ⊕ mean ⊕ max

    def forward(self, qembs):  # (Q, D) → (1, out_dim)
        Q = qembs.size(0)
        head_outs = []
        for head in self.score_heads:
            scores = head(qembs).squeeze(-1)            # (Q,)
            attn = torch.softmax(scores, dim=0)         # (Q,)
            pooled = (attn.unsqueeze(-1) * qembs).sum(0, keepdim=True)  # (1, D)
            head_outs.append(pooled)
        # Residual stats — keep mean and max as anchors
        m = qembs.mean(0, keepdim=True)
        mx = qembs.max(0, keepdim=True).values if Q > 1 else qembs.clone()
        return torch.cat(head_outs + [m, mx], dim=-1)   # (1, num_heads*D + 2D)


# Module-level pool instance is created in train_model and rebound here at call time.
_POOL = None

def aggregate_workload_emb(qembs):
    """Aggregate per-query embeddings → workload embedding.
    Uses module-level _POOL (an AttentionPool) when set; falls back to
    concat(mean, max, std) before pool initialization (e.g., during cache writes
    in TreeQueryEncoder.recompute_tree_embeddings)."""
    if _POOL is not None:
        return _POOL(qembs)
    # Fallback: concat(mean, max, std)
    m = qembs.mean(0, keepdim=True)
    if qembs.size(0) > 1:
        mx = qembs.max(0, keepdim=True).values
        sd = qembs.std(0, keepdim=True, unbiased=False)
    else:
        mx = qembs.clone()
        sd = torch.zeros_like(qembs)
    return torch.cat([m, mx, sd], dim=-1)


def set_pool(pool):
    global _POOL
    _POOL = pool


# ===================== Pad configs =====================

def prepare_conf(raw_conf, max_dim):
    conf = raw_conf[:max_dim] + [0.0] * (max_dim - len(raw_conf))
    return torch.tensor(conf, dtype=torch.float)


# ===================== Training =====================

def train_model(test_benchmark, test_sf=None, use_supply=False,
                big_epochs=40, seed=42,
                eval_mode='per_query', test_min_q=3, test_max_q=15, test_seed=8, eval_noise=0.0,
                stage1_subepochs=1, stage2_subepochs=2, stage3_subepochs=2,
                lambda_div=0.1, lambda_diversity=1.0,
                lambda_emb_spread=2.0, tree_weight_decay=1e-3,
                gumbel_tau=1.0, batch_size=32,
                ratio_cap=20.0):
    # Set seed for reproducibility
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed(seed)

    t0 = time.time()

    # Load data
    all_data, max_dim, train_qnames, valid_qnames, test_qnames = get_all_data(
        test_benchmark, test_sf=test_sf, use_supply=use_supply
    )

    # Build query ID mapping
    all_query_ids = sorted(all_data.keys())
    q2idx = {q: i for i, q in enumerate(all_query_ids)}
    num_queries = len(all_query_ids)

    # Generate workloads
    print("\nGenerating training workloads...")
    train_workloads = generate_workloads(train_qnames, all_data, 1000, seed=42)
    print(f"  Generated {len(train_workloads)} training workloads")

    print("Generating validation workloads...")
    valid_workloads = generate_workloads(valid_qnames, all_data, 150, seed=123)
    print(f"  Generated {len(valid_workloads)} validation workloads")

    print(f"Generating test workloads (min_q={test_min_q}, max_q={test_max_q}, seed={test_seed})...")
    test_workloads = generate_workloads(test_qnames, all_data, 50, min_q=test_min_q, max_q=test_max_q, seed=test_seed)
    print(f"  Generated {len(test_workloads)} test workloads")

    if not train_workloads:
        print("ERROR: No training workloads generated!")
        return

    # ===== Paper-style precompute =====
    # Per workload we cache:
    #   _q_indices          : tensor of query indices (for tree-conv embedding)
    #   _best_combo, _best_eng, _best_lak : workload best (e*, l*) from combo_totals
    #   _q_best_lat[qid]    : per-query global min latency across all combos/configs
    #   _stage1             : list of (conf, target_r=1, eng_id, lak_id) — N samples,
    #                         one per query: each query's globally best (combo, conf).
    #                         These supervise gates (CE) and overall MSE on r=1.
    #   _stage3             : list of (conf, target_r, eng_id, lak_id) — every (q, combo, conf)
    #                         record sampled (≤16/combo), used for expert-focused training.
    def precompute_paper_tensors(workloads, q2idx_map, max_d):
        for wl in workloads:
            qids = wl['query_ids']
            q_idx = [q2idx_map[q] for q in qids if q in q2idx_map]
            wl['_q_indices'] = torch.tensor(q_idx, device=device) if q_idx else None

            # workload-level best combo from combo_totals
            best_c = None; best_l = float('inf')
            for cc, total in wl.get('combo_totals', {}).items():
                if total < best_l:
                    best_l = total; best_c = cc
            if best_c is None:
                for cc, recs in wl['aggregated_data'].items():
                    ml = min(lat for _, lat in recs)
                    if ml < best_l:
                        best_l = ml; best_c = cc
            wl['_best_combo'] = best_c
            wl['_best_eng'] = int(best_c[0]) if best_c else 0
            wl['_best_lak'] = int(best_c[1]) if best_c else 0

            stats = wl.get('per_query_data', {})

            # per-query best latency across all combos/confs
            q_best_lat = {}
            for qid in qids:
                if qid not in stats:
                    continue
                ml = float('inf')
                for cc, recs in stats[qid].items():
                    for _, lat in recs:
                        if lat < ml:
                            ml = lat
                if ml > 0 and ml < float('inf'):
                    q_best_lat[qid] = ml
            wl['_q_best_lat'] = q_best_lat

            # Stage 1 records: per query, the globally best (combo, conf), target r = 1.0
            stage1 = []
            for qid in qids:
                if qid not in stats or qid not in q_best_lat:
                    continue
                best_q_lat = float('inf'); best_q_cc = None; best_q_conf = None
                for cc, recs in stats[qid].items():
                    for conf, lat in recs:
                        if lat < best_q_lat:
                            best_q_lat = lat; best_q_cc = cc; best_q_conf = conf
                if best_q_conf is None:
                    continue
                conf_padded = prepare_conf(best_q_conf.tolist(), max_d).to(device)
                stage1.append((conf_padded, 1.0, int(best_q_cc[0]), int(best_q_cc[1])))
            wl['_stage1'] = stage1

            # Stage 3 records: all (q, combo, conf) sampled, target_r = lat / q_best_lat
            stage3 = []
            for qid in qids:
                if qid not in stats or qid not in q_best_lat:
                    continue
                qbest = q_best_lat[qid]
                for cc, recs in stats[qid].items():
                    if len(recs) <= 16:
                        sampled = list(recs)
                    else:
                        sorted_recs = sorted(recs, key=lambda x: x[1])
                        n = len(sorted_recs)
                        idxs = {0, n - 1}
                        step = max(1, n // 15)
                        for i in range(1, 15):
                            idxs.add(min(i * step, n - 1))
                        sampled = [sorted_recs[i] for i in sorted(idxs)]
                    for conf, lat in sampled:
                        conf_padded = prepare_conf(conf.tolist(), max_d).to(device)
                        r = lat / qbest if qbest > 0 else 1.0
                        r = min(r, ratio_cap)
                        stage3.append((conf_padded, r, int(cc[0]), int(cc[1])))
            wl['_stage3'] = stage3

    print("Precomputing paper tensors...")
    for wl_list in [train_workloads, valid_workloads, test_workloads]:
        precompute_paper_tensors(wl_list, q2idx, max_dim)

    # Tree-based embedding (always used)
    mapped_tree, feat_dim = load_all_plan_trees(num_augment=5, swap_prob=0.3)
    if feat_dim == 0:
        print("WARNING: No plan trees loaded, falling back to pure learnable embeddings")
        feat_dim = 32
        mapped_tree = {}

    base_tree_keys = set(k for k in mapped_tree.keys() if '_aug' not in k)
    idx_to_tree_key = build_query_to_plan_mapping(q2idx, base_tree_keys)

    NUM_KERNELS = 4
    QENC_DIM = feat_dim * NUM_KERNELS  # 288 — per-query embedding dim
    NUM_ATTN_HEADS = 4
    # Workload aggregation = AttentionPool(num_heads=4) → (4 + 2) × QENC_DIM
    EMB_DIM = (NUM_ATTN_HEADS + 2) * QENC_DIM
    query_encoder = TreeQueryEncoder(
        mapped_tree=mapped_tree,
        idx_to_tree_key=idx_to_tree_key,
        num_queries=num_queries,
        feat_dim=feat_dim,
        hidden_dims=[256, 128],
        num_kernels=NUM_KERNELS,
        dropout_prob=0.5,
        device=str(device),
        proj_dim=0,  # no projection
        use_layer_norm=True,
        init_gain=2.0,
    ).to(device)

    # Attention pool for workload aggregation (replaces mean/max/std).
    pool = AttentionPool(dim=QENC_DIM, num_heads=NUM_ATTN_HEADS).to(device)
    set_pool(pool)
    print(f"  AttentionPool: dim={QENC_DIM} heads={NUM_ATTN_HEADS} → out_dim={pool.out_dim}")

    gate_hidden = [128, 256]
    expert_hidden = [128, 256, 256]
    moe_model = TwoGateMoE(
        emb_dim=EMB_DIM, conf_dim=max_dim,
        gate_hidden_dims=gate_hidden, expert_hidden_dims=expert_hidden,
        dropout_prob=0.3,
    ).to(device)

    print(f"\nModel params: query_encoder={sum(p.numel() for p in query_encoder.parameters()):,}, "
          f"moe={sum(p.numel() for p in moe_model.parameters()):,}")

    BIG_EPOCHS = big_epochs

    best_valid_ratio = float('inf')
    best_epoch = 0
    sf_tag = f"_sf{test_sf}" if test_sf is not None else ""
    model_path = os.path.join(os.path.dirname(__file__), "models", f"best_model_{test_benchmark}{sf_tag}.pth")
    os.makedirs(os.path.dirname(model_path), exist_ok=True)

    # ===== Optimizers (per-stage parameter groups) =====
    pool_params = list(pool.parameters())
    all_params = list(query_encoder.parameters()) + pool_params + list(moe_model.parameters())
    # Stage 2 freezes tree-conv but lets attention pool update (pool reads cached qembs).
    gate_params = (
        pool_params +
        list(moe_model.engine_gate.parameters()) +
        list(moe_model.lake_gate.parameters())
    )
    expert_params = (
        list(moe_model.conf_encoder.parameters()) +
        list(moe_model.engine_experts.parameters()) +
        list(moe_model.lake_experts.parameters()) +
        list(moe_model.post_mlp.parameters())
    )
    # Higher weight_decay on tree-conv to combat embedding collapse / overfit.
    tree_params = list(query_encoder.parameters())
    moe_only_params = [p for p in moe_model.parameters()]
    opt_full = Adam([
        {'params': tree_params, 'weight_decay': tree_weight_decay},
        {'params': pool_params, 'weight_decay': 1e-5},
        {'params': moe_only_params, 'weight_decay': 1e-5},
    ], lr=3e-4)
    opt_gate = Adam(gate_params, lr=3e-4, weight_decay=1e-5)
    opt_expert = Adam(expert_params, lr=3e-4, weight_decay=1e-5)
    sched_full = CosineAnnealingLR(opt_full, T_max=BIG_EPOCHS, eta_min=1e-5)
    sched_gate = CosineAnnealingLR(opt_gate, T_max=BIG_EPOCHS, eta_min=1e-5)
    sched_expert = CosineAnnealingLR(opt_expert, T_max=BIG_EPOCHS, eta_min=1e-5)

    print(f"\n{'='*60}")
    print(f"Training: {BIG_EPOCHS} epochs — paper-spec 3-stage MoE")
    print(f"  Stage 2 sub-epochs: {stage2_subepochs} | Stage 3 sub-epochs: {stage3_subepochs}")
    print(f"  λ_div={lambda_div} | gumbel τ={gumbel_tau} | batch_size={batch_size} | ratio_cap={ratio_cap}")
    print(f"{'='*60}")

    def _get_w_emb(q_indices):
        return aggregate_workload_emb(query_encoder(q_indices))

    def _get_w_emb_train(q_indices):
        # Differentiable lookup — must call precompute_training_table() before this
        return aggregate_workload_emb(query_encoder.forward_train(q_indices))

    def _run_stage1_endtoend(workloads):
        """End-to-end: tree-conv + gates + experts + post-mlp ALL trained.
        For each batch of `batch_size` workloads:
          1. Rebuild differentiable training table (one tree-conv forward per batch).
          2. Per workload: index lookup → mean → forward (Gumbel) → MSE+CE.
          3. Accumulate batch losses + L_div on batch-mean probs → backward → step.
        """
        query_encoder.train(); moe_model.train()
        random.shuffle(workloads)

        total = 0.0; mse_acc = 0.0; ce_acc = 0.0; div_acc = 0.0; n_steps = 0
        i = 0
        while i < len(workloads):
            chunk = workloads[i:i + batch_size]
            i += batch_size
            # One tree-conv forward (with grad) for the whole batch.
            query_encoder.precompute_training_table(device)

            bmse_terms = []; bce_terms = []
            b_eng_p = []; b_lak_p = []
            b_w_emb = []   # collect workload embeddings for spread regularizer
            for wl in chunk:
                q_indices = wl['_q_indices']
                if q_indices is None or len(q_indices) == 0:
                    continue
                stage1 = wl['_stage1']
                if not stage1:
                    continue
                w_emb = _get_w_emb_train(q_indices)  # grad through tree-conv
                b_w_emb.append(w_emb.squeeze(0))  # (D,)
                confs = torch.stack([rec[0] for rec in stage1])
                t_r = torch.tensor([rec[1] for rec in stage1], device=device, dtype=torch.float)
                t_eng = torch.tensor([rec[2] for rec in stage1], device=device, dtype=torch.long)
                t_lak = torch.tensor([rec[3] for rec in stage1], device=device, dtype=torch.long)
                w_emb_e = w_emb.expand(len(stage1), -1)
                pred, eng_p, lak_p, eng_lg, lak_lg = moe_model.forward(
                    w_emb_e, confs, use_gumbel=True, tau=gumbel_tau,
                )
                pe = eng_p.gather(1, t_eng.unsqueeze(1)).squeeze(1)
                pl = lak_p.gather(1, t_lak.unsqueeze(1)).squeeze(1)
                mse_per = (pred.squeeze(-1) - t_r) ** 2 * pe * pl
                bmse_terms.append(mse_per.mean())
                bce_terms.append(F.cross_entropy(eng_lg, t_eng) + F.cross_entropy(lak_lg, t_lak))
                b_eng_p.append(F.softmax(eng_lg, dim=-1))
                b_lak_p.append(F.softmax(lak_lg, dim=-1))

            if not bmse_terms:
                continue
            mse_m = torch.stack(bmse_terms).mean()
            ce_m = torch.stack(bce_terms).mean()
            all_eng_p = torch.cat(b_eng_p, dim=0)  # (B_total, ENG)
            all_lak_p = torch.cat(b_lak_p, dim=0)  # (B_total, LAK)
            avg_eng = all_eng_p.mean(0); avg_lak = all_lak_p.mean(0)
            inv_e = 1.0 / TwoGateMoE.ENGINE_CLASSES
            inv_l = 1.0 / TwoGateMoE.LAKE_CLASSES
            div = ((avg_eng - inv_e) ** 2).sum() + ((avg_lak - inv_l) ** 2).sum()
            # NEW anti-collapse: maximize entropy of batch-mean prob distribution.
            # Loss = max_entropy - actual_entropy (>=0, =0 at uniform).
            # Pushes batch-mean prob toward uniform with strong gradient even at collapse.
            log_e = math.log(TwoGateMoE.ENGINE_CLASSES)
            log_l = math.log(TwoGateMoE.LAKE_CLASSES)
            ent_eng = -(avg_eng * (avg_eng + 1e-12).log()).sum()
            ent_lak = -(avg_lak * (avg_lak + 1e-12).log()).sum()
            diversity_loss = (log_e - ent_eng) + (log_l - ent_lak)
            # NEW: workload-embedding spread regularizer — variance + InfoNCE contrastive.
            # Variance: maximize per-dim cross-sample variance.
            # InfoNCE: each workload's embedding should be more similar to itself
            #          (perturbed via dropout-augmented re-aggregation) than to others.
            #          Approximation: use cosine similarity matrix; minimize average
            #          off-diagonal cosine similarity (push embeddings apart).
            emb_spread_loss = torch.tensor(0.0, device=device)
            if len(b_w_emb) >= 2 and lambda_emb_spread > 0:
                emb_stack = torch.stack(b_w_emb, dim=0)  # (B, D)
                emb_var = emb_stack.var(dim=0, unbiased=False).mean()
                # Cosine similarity off-diagonal — anti-collapse contrastive
                norm_E = emb_stack / (emb_stack.norm(dim=-1, keepdim=True) + 1e-12)
                cos_M = norm_E @ norm_E.T  # (B, B)
                Bn = cos_M.size(0)
                eye = torch.eye(Bn, device=device, dtype=torch.bool)
                off_cos = cos_M[~eye]
                # Combine: variance term + cosine concentration penalty
                emb_spread_loss = -emb_var + off_cos.mean()
            loss = (mse_m + ce_m + lambda_div * div +
                    lambda_diversity * diversity_loss +
                    lambda_emb_spread * emb_spread_loss)
            opt_full.zero_grad()
            loss.backward()
            torch.nn.utils.clip_grad_norm_(all_params, 1.0)
            opt_full.step()
            total += loss.item(); mse_acc += mse_m.item(); ce_acc += ce_m.item(); div_acc += div.item()
            n_steps += 1
        return total / max(n_steps, 1), mse_acc / max(n_steps, 1), ce_acc / max(n_steps, 1), div_acc / max(n_steps, 1)

    def _gate_diagnostic(workloads, label=""):
        """Per-workload gate argmax → check vs ground-truth (_best_eng/lak).
        Returns (eng_acc, lak_acc, combo_acc, eng_hist, lak_hist) for diagnostic."""
        moe_model.eval()
        if hasattr(query_encoder, 'recompute_tree_embeddings'):
            query_encoder.recompute_tree_embeddings(device)
        eng_correct = lak_correct = combo_correct = total = 0
        from collections import Counter
        eng_hist = Counter(); lak_hist = Counter()
        with torch.no_grad():
            for wl in workloads:
                q_indices = wl['_q_indices']
                if q_indices is None or len(q_indices) == 0:
                    continue
                w_emb = _get_w_emb(q_indices)
                eng_lg = moe_model.engine_gate(w_emb)
                lak_lg = moe_model.lake_gate(w_emb)
                pe = int(eng_lg.argmax(-1).item())
                pl = int(lak_lg.argmax(-1).item())
                eng_hist[pe] += 1; lak_hist[pl] += 1
                if pe == wl['_best_eng']: eng_correct += 1
                if pl == wl['_best_lak']: lak_correct += 1
                if pe == wl['_best_eng'] and pl == wl['_best_lak']: combo_correct += 1
                total += 1
        moe_model.train()
        if total == 0:
            return 0.0, 0.0, 0.0, eng_hist, lak_hist
        return (eng_correct / total, lak_correct / total, combo_correct / total,
                eng_hist, lak_hist)

    def _run_stage2_gate(workloads, sub_epochs, valid_workloads_for_diag=None):
        """Gate-focused: L_CE + L_div on gate predictions vs (best_eng, best_lak).
        Tree-conv FROZEN (only Stage 1 backprops there). Use detached cached embeddings.
        Diagnostic: print sub-epoch acc + gate prediction histogram on valid."""
        if sub_epochs <= 0:
            return 0.0
        moe_model.train()
        if hasattr(query_encoder, 'recompute_tree_embeddings'):
            query_encoder.recompute_tree_embeddings(device)
        total = 0.0; n_steps = 0
        for se in range(sub_epochs):
            random.shuffle(workloads)
            i = 0
            while i < len(workloads):
                chunk = workloads[i:i + batch_size]
                i += batch_size
                bce_terms = []; b_eng_p = []; b_lak_p = []
                for wl in chunk:
                    q_indices = wl['_q_indices']
                    if q_indices is None or len(q_indices) == 0:
                        continue
                    w_emb = _get_w_emb(q_indices)
                    eng_lg = moe_model.engine_gate(w_emb)
                    lak_lg = moe_model.lake_gate(w_emb)
                    te = torch.tensor([wl['_best_eng']], device=device, dtype=torch.long)
                    tl = torch.tensor([wl['_best_lak']], device=device, dtype=torch.long)
                    bce_terms.append(F.cross_entropy(eng_lg, te) + F.cross_entropy(lak_lg, tl))
                    b_eng_p.append(F.softmax(eng_lg, dim=-1))
                    b_lak_p.append(F.softmax(lak_lg, dim=-1))
                if not bce_terms:
                    continue
                ce_m = torch.stack(bce_terms).mean()
                all_eng_p = torch.cat(b_eng_p, dim=0)
                all_lak_p = torch.cat(b_lak_p, dim=0)
                avg_eng = all_eng_p.mean(0); avg_lak = all_lak_p.mean(0)
                inv_e = 1.0 / TwoGateMoE.ENGINE_CLASSES
                inv_l = 1.0 / TwoGateMoE.LAKE_CLASSES
                div = ((avg_eng - inv_e) ** 2).sum() + ((avg_lak - inv_l) ** 2).sum()
                log_e = math.log(TwoGateMoE.ENGINE_CLASSES)
                log_l = math.log(TwoGateMoE.LAKE_CLASSES)
                ent_eng = -(avg_eng * (avg_eng + 1e-12).log()).sum()
                ent_lak = -(avg_lak * (avg_lak + 1e-12).log()).sum()
                diversity_loss = (log_e - ent_eng) + (log_l - ent_lak)
                loss = ce_m + lambda_div * div + lambda_diversity * diversity_loss
                opt_gate.zero_grad()
                loss.backward()
                torch.nn.utils.clip_grad_norm_(gate_params, 1.0)
                opt_gate.step()
                total += loss.item(); n_steps += 1
            if valid_workloads_for_diag is not None:
                ea, la, ca, eh, lh = _gate_diagnostic(valid_workloads_for_diag)
                eh_str = ",".join(f"{k}={v}" for k, v in sorted(eh.items()))
                lh_str = ",".join(f"{k}={v}" for k, v in sorted(lh.items()))
                print(f"      [s2 sub-ep {se+1:2d}/{sub_epochs}] valid: eng_acc={ea:.3f} "
                      f"lak_acc={la:.3f} combo_acc={ca:.3f}  eng_pred_hist={{{eh_str}}} "
                      f"lak_pred_hist={{{lh_str}}}")
        return total / max(n_steps, 1)

    def _run_stage3_expert(workloads, sub_epochs):
        """Expert-focused: hard routing to (e, l) per record, only expert+post-mlp updated.
        Records: every (q, combo, conf) sampled, target_r = lat / q_best_lat."""
        if sub_epochs <= 0:
            return 0.0
        moe_model.train()
        # Tree-conv frozen during expert phase (paper note: ~90% of cost). Reuse cached embs.
        if hasattr(query_encoder, 'recompute_tree_embeddings'):
            query_encoder.recompute_tree_embeddings(device)
        total = 0.0; n_steps = 0
        for _ in range(sub_epochs):
            random.shuffle(workloads)
            for wl in workloads:
                q_indices = wl['_q_indices']
                if q_indices is None or len(q_indices) == 0:
                    continue
                stage3 = wl['_stage3']
                if not stage3:
                    continue
                with torch.no_grad():
                    w_emb = _get_w_emb(q_indices)  # detach tree-conv

                # Group records by (eng_id, lak_id) so we can call forward_for_eng_lak per combo
                from collections import defaultdict
                groups = defaultdict(list)
                for idx, rec in enumerate(stage3):
                    groups[(rec[2], rec[3])].append(idx)

                opt_expert.zero_grad()
                total_mse = 0.0; ngrp = 0
                for (e_id, l_id), idxs in groups.items():
                    confs = torch.stack([stage3[i][0] for i in idxs])
                    t_r = torch.tensor([stage3[i][1] for i in idxs], device=device, dtype=torch.float)
                    w_emb_e = w_emb.expand(len(idxs), -1)
                    pred = moe_model.forward_for_eng_lak(w_emb_e, confs, e_id, l_id)
                    mse = F.mse_loss(pred.squeeze(-1), t_r)
                    total_mse = total_mse + mse
                    ngrp += 1
                if ngrp == 0:
                    continue
                loss = total_mse / ngrp
                loss.backward()
                torch.nn.utils.clip_grad_norm_(expert_params, 1.0)
                opt_expert.step()
                total += loss.item(); n_steps += 1
        return total / max(n_steps, 1)

    for epoch in range(1, BIG_EPOCHS + 1):
        s1_total = s1_mse = s1_ce = s1_div = 0.0
        for _ in range(max(1, stage1_subepochs)):
            a, b, c, d = _run_stage1_endtoend(train_workloads)
            s1_total += a; s1_mse += b; s1_ce += c; s1_div += d
        s1_total /= max(1, stage1_subepochs)
        s1_mse   /= max(1, stage1_subepochs)
        s1_ce    /= max(1, stage1_subepochs)
        s1_div   /= max(1, stage1_subepochs)
        s2_loss = _run_stage2_gate(train_workloads, stage2_subepochs,
                                    valid_workloads_for_diag=valid_workloads)
        s3_loss = _run_stage3_expert(train_workloads, stage3_subepochs)
        sched_full.step(); sched_gate.step(); sched_expert.step()

        # MODEL SELECTION on VALIDATION set (test never used during training).
        ratio = evaluate(query_encoder, moe_model, valid_workloads, q2idx, max_dim,
                         eval_mode=eval_mode, eval_noise=eval_noise)

        # Save the model that achieves the lowest validation ratio.
        if ratio < best_valid_ratio:
            best_valid_ratio = ratio
            best_epoch = epoch
            torch.save({
                'query_encoder': query_encoder.state_dict(),
                'moe_model': moe_model.state_dict(),
                'q2idx': q2idx,
                'max_dim': max_dim,
                'feat_dim': feat_dim,
                'num_kernels': NUM_KERNELS,
                'idx_to_tree_key': idx_to_tree_key,
            }, model_path)
        print(f"  Epoch {epoch:3d}: s1={s1_total:.4f} (mse={s1_mse:.4f} ce={s1_ce:.4f} div={s1_div:.4f}) "
              f"s2={s2_loss:.4f} s3={s3_loss:.4f} "
              f"valid_ratio={ratio:.4f}  best_valid={best_valid_ratio:.4f}@ep{best_epoch}")

    # Final TEST evaluation using the best-validation checkpoint.
    print(f"\n{'='*60}")
    print(f"Training complete. Best valid ratio: {best_valid_ratio:.4f} at epoch {best_epoch}")
    print(f"Reloading best checkpoint and evaluating on TEST set...")
    ckpt = torch.load(model_path, map_location=device)
    query_encoder.load_state_dict(ckpt['query_encoder'])
    moe_model.load_state_dict(ckpt['moe_model'])
    test_ratio = evaluate(query_encoder, moe_model, test_workloads, q2idx, max_dim,
                          eval_mode=eval_mode, eval_noise=eval_noise)
    print(f"Final test ratio (best-valid checkpoint, ep{best_epoch}): {test_ratio:.4f}")
    print(f"Time: {time.time() - t0:.1f}s")
    print(f"Model saved to: {model_path}")

    return test_ratio


def evaluate_per_query(query_encoder, moe_model, workloads, q2idx, max_dim):
    """Paper-style per-query inference (no oracle routing):
       1. q_emb = query_encoder([q])
       2. Run gates → eng* = argmax engine_gate, lak* = argmax lake_gate
       3. Among the records for this query restricted to combo (eng*, lak*),
          score every conf via forward_for_eng_lak and pick argmin pred.
       4. ratio = chosen_actual_lat / query_min_lat (across ALL combos).
    """
    query_encoder.eval()
    moe_model.eval()

    ratios = []
    eng_correct = lak_correct = combo_correct = total = 0
    fallback_used = 0

    with torch.no_grad():
        for wl in workloads:
            pq_data = wl.get('per_query_data')
            if not pq_data:
                continue
            for qid in wl['query_ids']:
                if qid not in pq_data or qid not in q2idx:
                    continue
                q_idx = torch.tensor([q2idx[qid]], device=device)
                q_emb = aggregate_workload_emb(query_encoder(q_idx))

                # Step 2: gate selection
                eng_lg = moe_model.engine_gate(q_emb)
                lak_lg = moe_model.lake_gate(q_emb)
                e_star = int(eng_lg.argmax(-1).item())
                l_star = int(lak_lg.argmax(-1).item())

                # Per-query global min (across all combos) — denominator
                all_lats = []; best_combo_overall = None; best_lat_overall = float('inf')
                for cc, recs in pq_data[qid].items():
                    for _, lat in recs:
                        all_lats.append(lat)
                        if lat < best_lat_overall:
                            best_lat_overall = lat; best_combo_overall = cc
                if not all_lats:
                    continue
                query_min_lat = best_lat_overall

                # Step 3: candidate confs restricted to (e*, l*).
                chosen_combo = (e_star, l_star)
                cand = pq_data[qid].get(chosen_combo)
                if not cand:
                    # Fallback: combo not present for this query → score all combos
                    fallback_used += 1
                    cand_combos = list(pq_data[qid].items())
                    confs = []; lats = []; combos = []
                    for cc, recs in cand_combos:
                        for conf, lat in recs:
                            if conf is None:
                                continue
                            confs.append(prepare_conf(conf.tolist(), max_dim))
                            lats.append(lat); combos.append(cc)
                    if not confs:
                        continue
                    conf_b = torch.stack(confs).to(device)
                    q_emb_e = q_emb.expand(len(confs), -1)
                    combo_ids = torch.tensor([[float(c[0]), float(c[1])] for c in combos],
                                              device=device, dtype=torch.float)
                    preds = moe_model.forward_oracle(q_emb_e, conf_b, combo_ids)
                    pick = preds.squeeze(-1).argmin().item()
                    chosen_actual = lats[pick]; chosen_combo = combos[pick]
                else:
                    confs = []; lats = []
                    for conf, lat in cand:
                        if conf is None:
                            continue
                        confs.append(prepare_conf(conf.tolist(), max_dim))
                        lats.append(lat)
                    if not confs:
                        continue
                    conf_b = torch.stack(confs).to(device)
                    q_emb_e = q_emb.expand(len(confs), -1)
                    preds = moe_model.forward_for_eng_lak(q_emb_e, conf_b, e_star, l_star)
                    pick = preds.squeeze(-1).argmin().item()
                    chosen_actual = lats[pick]

                if query_min_lat <= 0:
                    continue
                ratio = chosen_actual / query_min_lat
                ratios.append(ratio)

                if best_combo_overall is not None:
                    if chosen_combo[0] == best_combo_overall[0]: eng_correct += 1
                    if chosen_combo[1] == best_combo_overall[1]: lak_correct += 1
                    if chosen_combo == best_combo_overall: combo_correct += 1
                total += 1

    avg_ratio = float(np.mean(ratios)) if ratios else float('inf')
    if total > 0:
        ea = eng_correct / total; la = lak_correct / total; ca = combo_correct / total
        print(f"    Eval[per_query]: eng_acc={ea:.3f} lake_acc={la:.3f} combo_acc={ca:.3f} "
              f"avg_ratio={avg_ratio:.4f} ({len(ratios)} q, fallback={fallback_used})")
    return avg_ratio


def evaluate(query_encoder, moe_model, workloads, q2idx, max_dim, eval_mode='v2', eval_noise=0.0):
    """Dispatch to the appropriate evaluation method."""
    if eval_mode == 'per_query':
        return evaluate_per_query(query_encoder, moe_model, workloads, q2idx, max_dim)

    # Original V2: global pool selection
    query_encoder.eval()
    moe_model.eval()

    ratios = []
    eng_correct = lak_correct = combo_correct = total = 0

    with torch.no_grad():
        for wl in workloads:
            pq_data = wl.get('per_query_data')
            if not pq_data:
                continue

            q_indices = torch.tensor([q2idx[q] for q in wl['query_ids'] if q in q2idx], device=device)
            if len(q_indices) == 0:
                continue
            w_emb = aggregate_workload_emb(query_encoder(q_indices))

            # Pool ALL records from all queries
            conf_list = []
            lat_list = []
            combo_list = []
            for qid in wl['query_ids']:
                if qid not in pq_data:
                    continue
                for cc, recs in pq_data[qid].items():
                    for conf_tensor, lat in recs:
                        if conf_tensor is not None:
                            conf_list.append(prepare_conf(conf_tensor.tolist(), max_dim))
                            lat_list.append(lat)
                            combo_list.append(cc)

            if not conf_list:
                continue

            global_min_lat = min(lat_list)
            if global_min_lat <= 0:
                continue

            # Batch forward pass — oracle-routed prediction (V2)
            conf_batch = torch.stack(conf_list).to(device)
            w_emb_expand = w_emb.expand(len(conf_list), -1)
            combo_ids_batch = torch.tensor([[float(cc[0]), float(cc[1])] for cc in combo_list],
                                            device=device, dtype=torch.float)
            preds = moe_model.forward_oracle(w_emb_expand, conf_batch, combo_ids_batch)

            # Pick min predicted → get actual latency
            min_idx = preds.squeeze(-1).argmin().item()
            chosen_actual_lat = lat_list[min_idx]
            chosen_combo = combo_list[min_idx]

            ratio = chosen_actual_lat / global_min_lat
            ratios.append(ratio)

            # Track combo accuracy
            best_combo = wl.get('_best_combo')
            if best_combo is not None:
                if chosen_combo[0] == best_combo[0]: eng_correct += 1
                if chosen_combo[1] == best_combo[1]: lak_correct += 1
                if chosen_combo == best_combo: combo_correct += 1
            total += 1

    avg_ratio = np.mean(ratios) if ratios else float('inf')
    if total > 0:
        ea = eng_correct / total
        la = lak_correct / total
        ca = combo_correct / total
        print(f"    Eval: eng_acc={ea:.3f} lake_acc={la:.3f} combo_acc={ca:.3f} "
              f"avg_ratio={avg_ratio:.4f} ({len(ratios)} samples)")
    return avg_ratio


# ===================== Main =====================

def main():
    import argparse
    parser = argparse.ArgumentParser(description='LKHelm TwoGateMoE Training')
    parser.add_argument('--benchmark', type=str, default='tpcds',
                        choices=['tpcds', 'ssb_flat', 'job', 'tpch', 'ssb'])
    parser.add_argument('--sf', type=int, default=None,
                        help='Scale factor to test (1, 10, 100). If set, only that SF is test.')
    parser.add_argument('--epochs', type=int, default=40,
                        help='Number of training epochs (default: 40)')
    parser.add_argument('--seed', type=int, default=42,
                        help='Random seed')
    parser.add_argument('--eval-mode', type=str, default='per_query',
                        choices=['v2', 'per_query'],
                        help='Evaluation mode: v2 (pool), per_query (default)')
    parser.add_argument('--stage1-subepochs', type=int, default=1,
                        help='Sub-epochs for end-to-end Stage 1 per epoch (more = train tree-conv harder)')
    parser.add_argument('--stage2-subepochs', type=int, default=2,
                        help='Sub-epochs for gate-focused stage per epoch')
    parser.add_argument('--stage3-subepochs', type=int, default=2,
                        help='Sub-epochs for expert-focused stage per epoch')
    parser.add_argument('--lambda-div', type=float, default=0.1,
                        help='Diversity regularization weight (paper L_div on batch-mean prob)')
    parser.add_argument('--lambda-diversity', type=float, default=5.0,
                        help='Anti-collapse entropy-max loss weight on batch-mean prob '
                             '(=0 disables). Strong by default.')
    parser.add_argument('--lambda-emb-spread', type=float, default=2.0,
                        help='Workload-embedding cross-sample variance regularizer '
                             '(pushes tree-conv to differentiate workloads)')
    parser.add_argument('--tree-weight-decay', type=float, default=1e-3,
                        help='Weight decay specifically for tree-conv encoder params')
    parser.add_argument('--gumbel-tau', type=float, default=1.0,
                        help='Temperature for Gumbel-softmax routing')
    parser.add_argument('--batch-size', type=int, default=32,
                        help='Workload-batch size for gradient accumulation')
    parser.add_argument('--ratio-cap', type=float, default=20.0,
                        help='Cap on Stage-3 ratio target to stabilize MSE')
    args = parser.parse_args()

    sf_str = f" sf={args.sf}" if args.sf else ""
    print(f"\n{'#'*70}")
    print(f"# BENCHMARK: {args.benchmark}{sf_str}")
    print(f"{'#'*70}")

    train_model(args.benchmark, test_sf=args.sf,
                big_epochs=args.epochs, seed=args.seed,
                eval_mode=args.eval_mode,
                stage1_subepochs=args.stage1_subepochs,
                stage2_subepochs=args.stage2_subepochs,
                stage3_subepochs=args.stage3_subepochs,
                lambda_div=args.lambda_div,
                lambda_diversity=args.lambda_diversity,
                lambda_emb_spread=args.lambda_emb_spread,
                tree_weight_decay=args.tree_weight_decay,
                gumbel_tau=args.gumbel_tau,
                batch_size=args.batch_size,
                ratio_cap=args.ratio_cap)


if __name__ == "__main__":
    main()
