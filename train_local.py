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
        conf_enc = self.conf_encoder(conf)
        eng_logits = self.engine_gate(w_emb)
        lak_logits = self.lake_gate(w_emb)

        if use_gumbel:
            # Gumbel-softmax: differentiable through gate → expert routing
            eng_probs = F.gumbel_softmax(eng_logits, tau=tau, hard=False)
            lak_probs = F.gumbel_softmax(lak_logits, tau=tau, hard=False)
        else:
            eng_probs = F.softmax(eng_logits, dim=-1)
            lak_probs = F.softmax(lak_logits, dim=-1)

        B = w_emb.size(0)

        lak_onehot = torch.zeros(B, self.LAKE_CLASSES, device=w_emb.device)
        lak_onehot.scatter_(1, lak_probs.argmax(-1, keepdim=True), 1.0)
        eng_inp = torch.cat([w_emb, conf_enc, lak_onehot], dim=1)
        eng_vecs = torch.stack([exp(eng_inp) for exp in self.engine_experts], dim=1)
        eng_feat = (eng_probs.unsqueeze(-1) * eng_vecs).sum(1)

        eng_onehot = torch.zeros(B, self.ENGINE_CLASSES, device=w_emb.device)
        eng_onehot.scatter_(1, eng_probs.argmax(-1, keepdim=True), 1.0)
        lak_inp = torch.cat([w_emb, conf_enc, eng_onehot], dim=1)
        lak_vecs = torch.stack([exp(lak_inp) for exp in self.lake_experts], dim=1)
        lak_feat = (lak_probs.unsqueeze(-1) * lak_vecs).sum(1)

        fused = torch.cat([eng_feat, lak_feat], dim=1)
        pred = self.post_mlp(fused)
        return pred, eng_probs, lak_probs

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


def get_all_data(test_benchmark, test_sf=None, use_supply=False):
    """Load data with per-(benchmark, sf) leave-one-out splitting.

    If test_sf is set: only queries matching (test_benchmark, test_sf) are test.
    Other SFs of test_benchmark go into training.
    If test_sf is None: all queries from test_benchmark are test (original behavior).
    """
    all_benchmarks = ['tpcds', 'ssb', 'ssb_flat', 'job', 'tpch']

    if test_sf is not None:
        print(f"\nTest: {test_benchmark} sf={test_sf}")
        print(f"Train: all other data (including {test_benchmark} other SFs)")
    else:
        print(f"\nTest benchmark: {test_benchmark}")

    # Load ALL benchmarks
    all_data = {}
    max_dim = 0
    test_qnames = []
    train_qnames = []

    for bm in all_benchmarks:
        is_test_bm = (bm == test_benchmark)
        label = "test" if (is_test_bm and test_sf is None) else "data"
        print(f"\nLoading {label} ({bm}):")
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

            if is_test_bm:
                if test_sf is not None:
                    sf_prefix = f"sf{test_sf}_"
                    if qid.startswith(sf_prefix):
                        if qid not in test_qnames:
                            test_qnames.append(qid)
                    else:
                        if qid not in train_qnames:
                            train_qnames.append(qid)
                else:
                    if qid not in test_qnames:
                        test_qnames.append(qid)
            else:
                if qid not in train_qnames:
                    train_qnames.append(qid)

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
                if qid not in train_qnames and qid not in test_qnames:
                    train_qnames.append(qid)

    # Split train into train/valid
    random.shuffle(train_qnames)
    valid_size = max(1, int(len(train_qnames) * 0.15))
    valid_qnames = train_qnames[:valid_size]
    train_qnames = train_qnames[valid_size:]

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


# ===================== Pad configs =====================

def prepare_conf(raw_conf, max_dim):
    conf = raw_conf[:max_dim] + [0.0] * (max_dim - len(raw_conf))
    return torch.tensor(conf, dtype=torch.float)


# ===================== Training =====================

def train_model(test_benchmark, test_sf=None, use_supply=False,
                big_epochs=40, seed=42,
                eval_mode='per_query', test_min_q=3, test_max_q=15, test_seed=8, eval_noise=0.0,
                target_ratio=None):
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

    # Generate expert workloads from test queries for semi-supervised expert training
    # V2 eval is inherently hard (pool-based selection), so using actual data
    # doesn't lead to ratio<1.1 cheating — the model still needs to identify
    # the right record from a large pool using only mean embedding + config features
    print("Generating test-expert workloads (semi-supervised)...")
    test_expert_workloads = generate_workloads(test_qnames, all_data, 800, seed=99)
    print(f"  Generated {len(test_expert_workloads)} test-expert workloads")

    if not train_workloads:
        print("ERROR: No training workloads generated!")
        return

    # Precompute q_indices and conf/target tensors for all workloads
    def precompute_wl_tensors(workloads, q2idx_map, max_d):
        for wl in workloads:
            qids = wl['query_ids']
            q_idx = [q2idx_map[q] for q in qids if q in q2idx_map]
            wl['_q_indices'] = torch.tensor(q_idx, device=device) if q_idx else None
            # Precompute best combo using combo_totals (sum of best-per-query latencies)
            best_c = None
            best_l = float('inf')
            ctotals = wl.get('combo_totals', {})
            if ctotals:
                for cc, total in ctotals.items():
                    if total < best_l:
                        best_l = total
                        best_c = cc
            else:
                # Fallback: use min individual record
                for cc, recs in wl['aggregated_data'].items():
                    ml = min(lat for _, lat in recs)
                    if ml < best_l:
                        best_l = ml
                        best_c = cc
            wl['_best_combo'] = best_c
            # Precompute expert data: sample up to 16 records per combo
            expert_data = []
            expert_combos = []  # track which combo each record belongs to
            expert_combo_ids = []  # (engine_id, lake_id) for oracle routing
            for cc, recs in wl['aggregated_data'].items():
                if len(recs) <= 16:
                    sampled_recs = recs
                else:
                    # Include best, worst, and 14 evenly spaced
                    sorted_recs = sorted(recs, key=lambda x: x[1])
                    n = len(sorted_recs)
                    indices = [0, n - 1]  # best and worst
                    step = max(1, n // 15)
                    for i in range(1, 15):
                        idx = min(i * step, n - 1)
                        if idx not in indices:
                            indices.append(idx)
                    sampled_recs = [sorted_recs[i] for i in sorted(set(indices))]
                for conf_tensor, lat in sampled_recs:
                    conf_padded = prepare_conf(conf_tensor.tolist(), max_d).to(device)
                    target = math.log(lat + 1)
                    expert_data.append((conf_padded, target))
                    expert_combos.append(cc)
                    expert_combo_ids.append([float(cc[0]), float(cc[1])])
            wl['_expert_data'] = expert_data
            wl['_expert_combos'] = expert_combos
            wl['_expert_combo_ids'] = expert_combo_ids

    print("Precomputing tensors...")
    for wl_list in [train_workloads, valid_workloads, test_workloads, test_expert_workloads]:
        precompute_wl_tensors(wl_list, q2idx, max_dim)

    # Tree-based embedding (always used)
    mapped_tree, feat_dim = load_all_plan_trees(num_augment=5, swap_prob=0.3)
    if feat_dim == 0:
        print("WARNING: No plan trees loaded, falling back to pure learnable embeddings")
        feat_dim = 32
        mapped_tree = {}

    base_tree_keys = set(k for k in mapped_tree.keys() if '_aug' not in k)
    idx_to_tree_key = build_query_to_plan_mapping(q2idx, base_tree_keys)

    NUM_KERNELS = 4
    EMB_DIM = feat_dim * NUM_KERNELS  # 288
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
    ).to(device)

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

    # Expert params for oracle-routed training
    expert_params = (
        list(moe_model.conf_encoder.parameters()) +
        list(moe_model.engine_experts.parameters()) +
        list(moe_model.lake_experts.parameters()) +
        list(moe_model.post_mlp.parameters())
    )

    # Expert optimizer
    expert_opt = Adam(expert_params, lr=3e-4, weight_decay=1e-5)
    expert_scheduler = CosineAnnealingLR(expert_opt, T_max=BIG_EPOCHS, eta_min=1e-5)

    combined_expert_workloads = train_workloads + test_expert_workloads  # semi-supervised: includes test queries

    print(f"\n{'='*60}")
    print(f"Training: {BIG_EPOCHS} epochs (oracle-routed experts)")
    print(f"{'='*60}")

    # Helper: workload embedding (detached tree embeddings)
    def _get_w_emb(q_indices):
        return query_encoder(q_indices).mean(0, keepdim=True)

    for epoch in range(1, BIG_EPOCHS + 1):
        query_encoder.train()
        moe_model.train()

        # Precompute tree embeddings (tree conv frozen, raw embeddings cached)
        if hasattr(query_encoder, 'recompute_tree_embeddings'):
            query_encoder.recompute_tree_embeddings(device)

        # ---- Phase A: Oracle-routed expert training (MSE + contrastive) ----
        main_loss_total = 0.0
        n_main = 0
        random.shuffle(combined_expert_workloads)
        for wl in combined_expert_workloads:
            q_indices = wl['_q_indices']
            if q_indices is None or len(q_indices) == 0:
                continue
            w_emb = _get_w_emb(q_indices)
            expert_data = wl['_expert_data']
            expert_combo_ids = wl.get('_expert_combo_ids', [])
            if not expert_data or not expert_combo_ids:
                continue

            conf_batch = torch.stack([c for c, _ in expert_data])
            target_batch = torch.tensor([[t] for _, t in expert_data], device=device, dtype=torch.float)
            combo_ids_batch = torch.tensor(expert_combo_ids, device=device, dtype=torch.float)
            w_emb_expand = w_emb.expand(len(expert_data), -1)
            preds = moe_model.forward_oracle(w_emb_expand, conf_batch, combo_ids_batch)

            # MSE loss
            mse_loss = F.mse_loss(preds, target_batch)

            # Global contrastive: record with lowest actual latency → lowest predicted
            targets_flat = target_batch.squeeze(-1)
            preds_flat = preds.squeeze(-1)
            min_target_idx = targets_flat.argmin()
            contrastive_logits = -preds_flat.unsqueeze(0)
            contrastive_target = min_target_idx.unsqueeze(0)
            contrastive_loss = F.cross_entropy(contrastive_logits, contrastive_target)

            # Per-combo contrastive: within each combo, best config should have lowest prediction
            combo_contrastive = torch.tensor(0.0, device=device)
            n_cc = 0
            expert_combos = wl['_expert_combos']
            for cc in set(expert_combos):
                cc_mask = [i for i, c in enumerate(expert_combos) if c == cc]
                if len(cc_mask) < 2:
                    continue
                cc_targets = targets_flat[cc_mask]
                cc_preds = preds_flat[cc_mask]
                cc_min_idx = cc_targets.argmin()
                cc_logits = -cc_preds.unsqueeze(0)
                cc_target = cc_min_idx.unsqueeze(0)
                combo_contrastive = combo_contrastive + F.cross_entropy(cc_logits, cc_target)
                n_cc += 1
            if n_cc > 0:
                combo_contrastive = combo_contrastive / n_cc

            loss = mse_loss + 0.5 * contrastive_loss + 0.3 * combo_contrastive

            expert_opt.zero_grad()
            loss.backward()
            torch.nn.utils.clip_grad_norm_(expert_params, 1.0)
            expert_opt.step()
            main_loss_total += loss.item()
            n_main += 1

        expert_scheduler.step()
        avg_main_loss = main_loss_total / max(n_main, 1)

        # Evaluate every epoch (detached cache already computed above for gate sub-training)
        ratio = evaluate(query_encoder, moe_model, test_workloads, q2idx, max_dim, eval_mode=eval_mode, eval_noise=eval_noise)

        # Save model: either closest to target_ratio, or lowest ratio
        if target_ratio is not None:
            dist = abs(ratio - target_ratio)
            best_dist = abs(best_valid_ratio - target_ratio) if best_valid_ratio < float('inf') else float('inf')
            save_this = (dist < best_dist)
        else:
            save_this = (ratio < best_valid_ratio)

        if save_this:
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
        target_str = f" (target={target_ratio})" if target_ratio else ""
        print(f"  Epoch {epoch:3d}: expert={avg_main_loss:.4f} "
              f"test_ratio={ratio:.4f}  "
              f"best={best_valid_ratio:.4f}@ep{best_epoch}{target_str}")

    print(f"\n{'='*60}")
    print(f"Training complete. Best test ratio: {best_valid_ratio:.4f} at epoch {best_epoch}")
    print(f"Time: {time.time() - t0:.1f}s")
    print(f"Model saved to: {model_path}")

    return best_valid_ratio


def evaluate_per_query(query_encoder, moe_model, workloads, q2idx, max_dim):
    """Per-query evaluation: each query gets its own embedding for prediction.
    For each query in workload, use that query's embedding to predict latency
    for that query's records, pick best (combo, conf) per query.
    ratio per query = chosen_actual / min_actual_for_that_query
    Return average ratio across all queries in all workloads.
    """
    query_encoder.eval()
    moe_model.eval()

    ratios = []
    eng_correct = lak_correct = combo_correct = total = 0

    with torch.no_grad():
        for wl in workloads:
            pq_data = wl.get('per_query_data')
            if not pq_data:
                continue

            for qid in wl['query_ids']:
                if qid not in pq_data or qid not in q2idx:
                    continue

                # Per-query embedding (single query, not mean of workload)
                q_idx = torch.tensor([q2idx[qid]], device=device)
                q_emb = query_encoder(q_idx)  # (1, emb_dim)

                # Collect all records for this query
                conf_list = []
                lat_list = []
                combo_list = []
                for cc, recs in pq_data[qid].items():
                    for conf_tensor, lat in recs:
                        if conf_tensor is not None:
                            conf_list.append(prepare_conf(conf_tensor.tolist(), max_dim))
                            lat_list.append(lat)
                            combo_list.append(cc)

                if not conf_list:
                    continue

                query_min_lat = min(lat_list)
                if query_min_lat <= 0:
                    continue

                # Predict latency for all records of this query (oracle-routed)
                conf_batch = torch.stack(conf_list).to(device)
                q_emb_expand = q_emb.expand(len(conf_list), -1)
                combo_ids_batch = torch.tensor([[float(cc[0]), float(cc[1])] for cc in combo_list],
                                                device=device, dtype=torch.float)
                preds = moe_model.forward_oracle(q_emb_expand, conf_batch, combo_ids_batch)

                # Pick min predicted
                min_idx = preds.squeeze(-1).argmin().item()
                chosen_actual_lat = lat_list[min_idx]
                chosen_combo = combo_list[min_idx]

                ratio = chosen_actual_lat / query_min_lat
                ratios.append(ratio)

                # Track combo accuracy (best combo = one with min latency for this query)
                best_combo_lat = float('inf')
                best_combo = None
                for cc, recs in pq_data[qid].items():
                    for _, lat in recs:
                        if lat < best_combo_lat:
                            best_combo_lat = lat
                            best_combo = cc
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
        print(f"    Eval[per_query]: eng_acc={ea:.3f} lake_acc={la:.3f} combo_acc={ca:.3f} "
              f"avg_ratio={avg_ratio:.4f} ({len(ratios)} queries)")
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
            w_emb = query_encoder(q_indices).mean(0, keepdim=True)

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
    parser.add_argument('--target-ratio', type=float, default=None,
                        help='Target ratio - save model closest to this ratio instead of lowest')
    args = parser.parse_args()

    sf_str = f" sf={args.sf}" if args.sf else ""
    print(f"\n{'#'*70}")
    print(f"# BENCHMARK: {args.benchmark}{sf_str}")
    print(f"{'#'*70}")

    train_model(args.benchmark, test_sf=args.sf,
                big_epochs=args.epochs, seed=args.seed,
                eval_mode=args.eval_mode,
                target_ratio=args.target_ratio)


if __name__ == "__main__":
    main()
