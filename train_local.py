#!/usr/bin/env -S python3 -u
"""
LKHelm Training Script - TwoGateMoE with Tree Embeddings.

Trains a Mixture of Experts model to predict optimal (engine, datalake, config)
combinations for database query workloads. Random query-level train/valid/test
split across 5 benchmarks (tpcds, tpch, ssb, ssb_flat, job) at scale factors 1/10/100.

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
    load_all_plan_trees, build_query_to_plan_mapping, TreeQueryEncoder,
    schema_agnostic_node_features
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

def normalize_query_name(qname, sf, benchmark=None, canonical_aliases=False):
    """Normalize query name: strip datalake prefix, add sf prefix.
    tpch_0_q1 -> sf10_q1,  db1 -> sf1_db1,  job_10a -> sf10_job_10a
    """
    # Strip benchmark+datalake prefix: tpch_0_q1 -> q1, ssb_2_q11 -> q11
    m = _re.match(r'^[a-z][\w-]*_\d+_(q\w+)$', qname)
    if m:
        qname = m.group(1)
    if canonical_aliases and benchmark == 'tpcds':
        # The collected TPC-DS CSVs mix three spellings for the same query
        # template: query10, tpcds_q10, and tpcds_q_10.  Keeping them separate
        # gives identical plan embeddings contradictory route labels and can
        # split one logical query across train/valid/test.
        m = _re.match(r'^(?:query|tpcds_q_?|q_?)0*(\d+)(?:_?([ab]))?$', qname)
        if m:
            suffix = m.group(2) or ''
            qname = f"tpcds_q{int(m.group(1))}{suffix}"
    elif canonical_aliases and benchmark == 'tpch':
        # Spark-side files use dbN while other collectors use qN for the same
        # TPC-H template.  Canonicalize only inside the TPC-H benchmark.
        m = _re.match(r'^(?:db|q)0*(\d+)$', qname)
        if m:
            qname = f"tpch_q{int(m.group(1))}"
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

# Canonical, name-aligned config encoding (--conf-canonical).  The CSVs mix three
# config formats: 'key=value; ...' (named), plain 'v;v;...' and 'engine|lake'
# positional lists whose positions mean different knobs in different collectors.
# parse_conf() reads them all positionally, so a feature slot means different
# knobs across benchmarks.  Here named knobs go to fixed slots; positional
# formats only fill a separate raw block, flagged by a format one-hot.
CONF_CANONICAL = False
_KNOBS = {
    'spark': ['spark.executor.memory', 'spark.executor.cores', 'spark.executor.instances',
              'spark.driver.memory', 'spark.sql.shuffle.partitions',
              'spark.sql.files.maxPartitionBytes'],
    'presto': ['memory.heap-headroom-per-node', 'node-scheduler.max-splits-per-node',
               'query.max-memory', 'query.max-memory-per-node',
               'query.max-total-memory-per-node', 'task.concurrency', 'task.max-worker-threads'],
}
_KNOBS['trino'] = _KNOBS['presto']
_N_KNOB, _N_RAW = 7, 9


def _num(v):
    v = v.strip()
    m = _re.match(r'^([-+]?[\d.]+(?:e[-+]?\d+)?)\s*([a-zA-Z]*)$', v)
    if not m:
        return None
    x = float(m.group(1)); u = m.group(2).lower()
    return x * {'g': 1024.0, 'gb': 1024.0, 'm': 1.0, 'mb': 1.0, 'k': 1 / 1024.0, 'kb': 1 / 1024.0}.get(u, 1.0)


def parse_conf_canonical(conf_str, engine):
    """[named knobs (7, log1p) | named mask | format one-hot kv/plain/pipe (3) | raw positional (9, log1p)]"""
    knobs = [0.0] * _N_KNOB; raw = [0.0] * _N_RAW
    slog = lambda x: math.copysign(math.log1p(abs(x)), x)
    if '=' in conf_str and '|' not in conf_str:
        names = _KNOBS.get(engine, [])
        for part in conf_str.split(';'):
            if '=' not in part:
                continue
            k, v = part.split('=', 1)
            k = k.strip()
            if k in names:
                x = _num(v)
                if x is not None:
                    knobs[names.index(k)] = slog(x)
        return knobs + [1.0] + [1.0, 0.0, 0.0] + raw
    vals = [_num(x) for x in _re.split(r'[;|]', conf_str) if x.strip()]
    vals = [slog(x) for x in vals if x is not None][:_N_RAW]
    raw[:len(vals)] = vals
    fmt = [0.0, 0.0, 1.0] if '|' in conf_str else [0.0, 1.0, 0.0]
    return knobs + [0.0] + fmt + raw


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

def load_csv_data(benchmark, supply=False, canonical_aliases=False, bm_prefix=False):
    """Load all CSV data from data/output/{benchmark}/sf{1,10,100}/*.csv

    bm_prefix: prefix query ids with ``{benchmark}|`` so that pooling several
    benchmarks cannot merge distinct queries that share a name (ssb and tpch
    both use db1..db22; ssb and ssb_flat share q1_1 ...).
    """
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

                    query_id = normalize_query_name(
                        qname, sf, benchmark=benchmark,
                        canonical_aliases=canonical_aliases,
                    )
                    if bm_prefix:
                        query_id = f"{benchmark}|{query_id}"
                    if CONF_CANONICAL:
                        engine_config, lake_config = parse_conf_canonical(conf_str, engine_name), []
                    else:
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


LATENCY_CAP_RATIO = 0.0
LATENCY_CAP_SCOPE = None   # None = all queries; else set of (benchmark, sf) strings


def _in_cap_scope(qid, default_bm):
    if LATENCY_CAP_SCOPE is None:
        return True
    bm, rest = qid.split('|', 1) if '|' in qid else (default_bm, qid)
    sf = rest.split('_', 1)[0][2:]
    return (bm, sf) in LATENCY_CAP_SCOPE


def cap_latencies(all_data, cap_ratio, default_bm=None):
    """Clip every record's latency to cap_ratio x that query's best latency
    (over all combos/configs).  Removes anomalous runs (e.g. a collector batch
    measuring 1000-2000 s where every other run of the same query/combo takes
    ~3 s) and bounds any single query's ratio at cap_ratio."""
    if cap_ratio <= 0:
        return all_data
    n_cap = n_all = 0
    for qid, combo_data in all_data.items():
        if not _in_cap_scope(qid, default_bm):
            continue
        best = min((lat for recs in combo_data.values() for _, lat in recs), default=0.0)
        if best <= 0:
            continue
        lim = cap_ratio * best
        for cc, recs in combo_data.items():
            n_all += len(recs)
            n_cap += sum(lat > lim for _, lat in recs)
            combo_data[cc] = [(c, min(lat, lim)) for c, lat in recs]
    print(f"  Latency cap: {n_cap}/{n_all} records clipped to {cap_ratio:g}x their query's best "
          f"(scope: {sorted(LATENCY_CAP_SCOPE) if LATENCY_CAP_SCOPE else 'all'})")
    return all_data


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


def filter_noisy_training_records(all_data, train_qnames, max_fraction=0.05,
                                  z_threshold=3.5, min_records=5):
    """Remove robust log-latency outliers from training queries only.

    Validation and test queries are untouched. Removal is capped independently
    for every query/combo group, so a noisy large group cannot consume the
    budget of a small group or erase an engine/lake candidate entirely.
    """
    max_fraction = min(max(float(max_fraction), 0.0), 0.20)
    if max_fraction == 0:
        return 0, 0
    removed = considered = 0
    for qid in train_qnames:
        for cc, recs in all_data.get(qid, {}).items():
            n = len(recs)
            considered += n
            max_remove = min(int(math.floor(n * max_fraction)), n - 1)
            if n < min_records or max_remove <= 0:
                continue
            logs = np.asarray([math.log(max(lat, 1e-12)) for _, lat in recs])
            med = float(np.median(logs))
            mad = float(np.median(np.abs(logs - med)))
            if mad <= 1e-12:
                continue
            robust_z = np.abs(logs - med) / (1.4826 * mad)
            candidates = np.flatnonzero(robust_z > z_threshold)
            if not len(candidates):
                continue
            ranked = sorted(candidates.tolist(), key=lambda i: robust_z[i], reverse=True)
            drop = set(ranked[:max_remove])
            all_data[qid][cc] = [rec for i, rec in enumerate(recs) if i not in drop]
            removed += len(drop)
    print(f"  Noise filter: removed {removed}/{considered} training records "
          f"({(100.0 * removed / considered) if considered else 0.0:.2f}%, "
          f"per-group cap={100 * max_fraction:.1f}%)")
    return removed, considered


def normalize_config_features(all_data, train_qnames, max_dim):
    """Z-normalize padded configuration vectors using training records only."""
    train_rows = []
    for qid in train_qnames:
        for recs in all_data.get(qid, {}).values():
            for conf, _ in recs:
                values = conf.tolist()[:max_dim]
                train_rows.append(values + [0.0] * (max_dim - len(values)))
    if not train_rows:
        return
    train_matrix = np.asarray(train_rows, dtype=np.float32)
    mean = train_matrix.mean(axis=0)
    std = train_matrix.std(axis=0)
    std[std < 1e-6] = 1.0

    for qdata in all_data.values():
        for cc, recs in qdata.items():
            normalized = []
            for conf, latency in recs:
                values = conf.tolist()[:max_dim]
                padded = np.asarray(
                    values + [0.0] * (max_dim - len(values)), dtype=np.float32
                )
                normalized.append((torch.from_numpy((padded - mean) / std), latency))
            qdata[cc] = normalized
    print(f"  Config normalization: fitted {len(train_rows)} training records "
          f"across {max_dim} padded features")


def get_all_data(test_benchmark=None, test_sf=None, use_supply=False,
                 train_frac=0.70, valid_frac=0.15, test_frac=0.15, split_seed=42,
                 canonical_query_aliases=False, holdout=False):
    """Load benchmark CSVs and split queries RANDOMLY into train/valid/test.

    holdout=True (cross-schema): every query of `test_benchmark` (only sf=`test_sf`
    when given) is test; ALL sfs of `test_benchmark` are excluded from training;
    the other benchmarks (all sfs) are split randomly into train/valid.

    No leave-one-out splitting. If `test_benchmark` is set, only that benchmark's
    data is loaded; otherwise all benchmarks are pooled. The split is then a
    purely random query-level partition (default 70/15/15). When `test_sf` is
    given, only queries with sf prefix matching `sf{test_sf}_` are kept.

    Returns (all_data, max_dim, train_qnames, valid_qnames, test_qnames).
    """
    all_benchmarks = ['tpcds', 'ssb', 'ssb_flat', 'job', 'tpch']
    if holdout:
        return _get_holdout_data(test_benchmark, test_sf, all_benchmarks,
                                 train_frac / (train_frac + valid_frac), split_seed,
                                 canonical_query_aliases)
    benchmarks_to_load = [test_benchmark] if test_benchmark else all_benchmarks
    print(f"\nLoading benchmarks: {benchmarks_to_load}")
    if test_sf is not None:
        print(f"Filter: only queries at sf={test_sf}")
    print(f"Random split: train={train_frac:.0%} valid={valid_frac:.0%} test={test_frac:.0%} (seed={split_seed})")

    all_data = {}
    max_dim = 0
    for bm in benchmarks_to_load:
        print(f"\nLoading ({bm}):")
        bm_data, bm_max_dim = load_csv_data(
            bm, canonical_aliases=canonical_query_aliases
        )
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
            supply_data, s_max_dim = load_csv_data(
                bm, supply=True, canonical_aliases=canonical_query_aliases
            )
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
    all_data = cap_latencies(all_data, LATENCY_CAP_RATIO, default_bm=test_benchmark)

    print(f"\nData summary:")
    print(f"  Total queries: {len(all_data)}")
    print(f"  Train: {len(train_qnames)}, Valid: {len(valid_qnames)}, Test: {len(test_qnames)}")
    print(f"  Max config dim: {max_dim}")

    return all_data, max_dim, train_qnames, valid_qnames, test_qnames


def _get_holdout_data(test_benchmark, test_sf, all_benchmarks, train_share, split_seed,
                      canonical_query_aliases):
    """Cross-schema split: test = held-out benchmark, train/valid = all others."""
    print(f"\nCross-schema holdout: test={test_benchmark}"
          f"{f' sf={test_sf}' if test_sf is not None else ' (all sf)'}; "
          f"train/valid = {[b for b in all_benchmarks if b != test_benchmark]} (all sf), "
          f"train share={train_share:.0%} (seed={split_seed})")
    all_data = {}
    max_dim = 0
    for bm in all_benchmarks:
        print(f"\nLoading ({bm}):")
        bm_data, bm_max_dim = load_csv_data(
            bm, canonical_aliases=canonical_query_aliases, bm_prefix=True
        )
        max_dim = max(max_dim, bm_max_dim)
        all_data.update(bm_data)

    test_prefix = f"{test_benchmark}|" + (f"sf{test_sf}_" if test_sf is not None else "")
    test_qnames = sorted(q for q in all_data if q.startswith(test_prefix))
    # Other sfs of the held-out benchmark are dropped, never trained on.
    all_data = {q: v for q, v in all_data.items()
                if not q.startswith(f"{test_benchmark}|") or q in set(test_qnames)}
    pool = sorted(q for q in all_data if not q.startswith(f"{test_benchmark}|"))
    random.Random(split_seed).shuffle(pool)
    n_train = int(len(pool) * train_share)
    train_qnames, valid_qnames = pool[:n_train], pool[n_train:]

    print("\nFixing floor-latency artifacts...")
    all_data = fix_floor_latencies(all_data, floor_val=1500.0)
    all_data = cap_latencies(all_data, LATENCY_CAP_RATIO, default_bm=test_benchmark)
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


def generate_single_query_workloads(query_ids, grouped_data, repeats=1, seed=42):
    """Build per-query workloads without changing the train/eval input shape.

    The original per-query evaluation trains gates on multi-query workloads but
    calls them with a single query at inference time.  Keeping every workload at
    one query also removes conflicting per-query gate labels for a shared
    workload embedding.  Validation and test use ``repeats=1`` so every metric
    observation is an independent query rather than a duplicated sample.
    """
    workloads = []
    for repeat in range(repeats):
        ordered = list(query_ids)
        random.Random(seed + repeat).shuffle(ordered)
        for offset, qid in enumerate(ordered):
            generated = generate_workloads(
                [qid], grouped_data, 1, min_q=1, max_q=1,
                min_combo_coverage=1, seed=seed + repeat * 100003 + offset,
            )
            workloads.extend(generated)
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
                ratio_cap=20.0, consistent_per_query=False,
                per_query_train_repeats=16,
                log_ratio_target=False, noise_filter_fraction=0.05,
                router_aux_weight=0.0, router_margin_scale=0.25,
                router_class_balance=False, router_stage1_aux_weight=0.0,
                router_regret_weight=0.0, num_train_workloads=1000,
                canonical_query_aliases=False, normalize_configs=False,
                neutral_unseen_fallback=False, per_query_expert_weight=0.0,
                expert_rank_weight=0.0, workload_expert_weight=1.0,
                workload_gate_ce_weight=1.0, early_stop_patience=0,
                stage3_median_configs=False, gate_prior_reg='uniform',
                gate_soft_label_temp=0.0, gate_lr=3e-4, gate_dropout=None,
                joint_gate_inference=False, select_metric='mean', holdout=False,
                stage2_train_encoder=False, tree_feat_norm='raw', tree_readout='root',
                export_gate_data=None, sf_embedding=False, expert_residual_prior=False):
    # Set seed for reproducibility
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed(seed)

    t0 = time.time()

    # Load data
    all_data, max_dim, train_qnames, valid_qnames, test_qnames = get_all_data(
        test_benchmark, test_sf=test_sf, use_supply=use_supply,
        canonical_query_aliases=canonical_query_aliases, holdout=holdout,
    )
    filter_noisy_training_records(
        all_data, train_qnames, max_fraction=noise_filter_fraction
    )
    if normalize_configs:
        normalize_config_features(all_data, train_qnames, max_dim)

    # Build query ID mapping
    all_query_ids = sorted(all_data.keys())
    q2idx = {q: i for i, q in enumerate(all_query_ids)}
    num_queries = len(all_query_ids)

    # Generate workloads
    print("\nGenerating training workloads...")
    if eval_mode == 'per_query' and consistent_per_query:
        train_workloads = generate_single_query_workloads(
            train_qnames, all_data, repeats=per_query_train_repeats, seed=42
        )
    else:
        train_workloads = generate_workloads(
            train_qnames, all_data, num_train_workloads, seed=42
        )
    print(f"  Generated {len(train_workloads)} training workloads")

    print("Generating validation workloads...")
    valid_workloads = generate_workloads(valid_qnames, all_data, 150, seed=123)
    print(f"  Generated {len(valid_workloads)} validation workloads")

    print(f"Generating test workloads (min_q={test_min_q}, max_q={test_max_q}, seed={test_seed})...")
    if holdout:
        # Every held-out query evaluated exactly once, grouped per sf.
        test_groups = defaultdict(list)
        for q in test_qnames:
            test_groups[q.split('|', 1)[1].split('_', 1)[0]].append(q)
        test_workloads_by_sf = {
            sf: generate_single_query_workloads(qs, all_data, repeats=1, seed=test_seed)
            for sf, qs in sorted(test_groups.items())
        }
        test_workloads = [wl for wls in test_workloads_by_sf.values() for wl in wls]
    else:
        test_workloads = generate_workloads(
            test_qnames, all_data, 50,
            min_q=test_min_q, max_q=test_max_q, seed=test_seed,
        )
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
    median_config_cache = {}

    # Train-split prior of each (sf, combo, config): mean log(lat / query best).
    # With --expert-residual-prior the experts regress the residual against it,
    # so an unsure expert defaults to configs that are good across training
    # schemas.  Built from train_qnames ONLY (never valid/test).
    def _conf_key(conf):
        return tuple(round(float(x), 4) for x in conf.tolist())

    def _sf_of(qid):
        return _re.search(r'sf(\d+)_', qid).group(1)

    _pr_acc = defaultdict(list); _pr_acc_c = defaultdict(list)
    for _q in train_qnames:
        _recs = all_data.get(_q, {})
        _best = min((l for rs in _recs.values() for _, l in rs), default=0.0)
        if _best <= 0:
            continue
        for _cc, _rs in _recs.items():
            for _c, _l in _rs:
                if _c is None:
                    continue
                v = math.log(max(min(_l / _best, ratio_cap), 1.0))
                _pr_acc[(_sf_of(_q), _cc, _conf_key(_c))].append(v)
                _pr_acc_c[(_sf_of(_q), _cc)].append(v)
    _pr = {k: float(np.mean(v)) for k, v in _pr_acc.items()}
    _pr_c = {k: float(np.mean(v)) for k, v in _pr_acc_c.items()}
    _pr_g = float(np.mean([v for vs in _pr_acc.values() for v in vs])) if _pr_acc else 0.0

    def config_prior(qid, cc, conf):
        if not expert_residual_prior:
            return 0.0
        sf_ = _sf_of(qid)
        return _pr.get((sf_, cc, _conf_key(conf)), _pr_c.get((sf_, cc), _pr_g))

    if expert_residual_prior:
        assert log_ratio_target, '--expert-residual-prior needs --log-ratio-target'
        print(f"  Expert residual prior: {len(_pr)} (sf, combo, config) keys from "
              f"{len(train_qnames)} train queries")
    _EVAL_OPTS['config_prior'] = config_prior

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
            # Log-slowdown of each combo for the whole workload (soft gate labels).
            wl_cost = np.full(
                (TwoGateMoE.ENGINE_CLASSES, TwoGateMoE.LAKE_CLASSES),
                math.log(max(ratio_cap, 1.0)), dtype=np.float32,
            )
            if best_c is not None and best_l > 0:
                totals = wl.get('combo_totals') or {
                    cc: min(lat for _, lat in recs)
                    for cc, recs in wl['aggregated_data'].items() if recs
                }
                for cc, tot in totals.items():
                    wl_cost[int(cc[0]), int(cc[1])] = math.log(
                        max(1.0, min(tot / best_l, ratio_cap))
                    )
            wl['_route_cost'] = torch.tensor(wl_cost, device=device)
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

            # Auxiliary labels for the existing gates at the exact single-query
            # shape used by per-query inference. This changes supervision only;
            # the TreeEncoder -> AttentionPool -> two-gate architecture is
            # unchanged. Ambiguous routes receive less weight.
            per_query_routes = []
            for qid in qids:
                if qid not in stats or qid not in q2idx_map:
                    continue
                combo_best = []
                for cc, recs in stats[qid].items():
                    if recs:
                        combo_best.append((min(lat for _, lat in recs), cc))
                combo_best.sort(key=lambda item: item[0])
                if not combo_best:
                    continue
                best_latency, best_cc = combo_best[0]
                if len(combo_best) > 1 and best_latency > 0:
                    margin = (combo_best[1][0] - best_latency) / best_latency
                else:
                    margin = router_margin_scale
                confidence = min(1.0, max(0.05, margin / max(router_margin_scale, 1e-6)))

                # Cost-sensitive routing target.  Cross-entropy treats a 1%
                # miss and a 20x miss as equally wrong; log-regret preserves
                # that distinction while keeping the same two hard gates.
                route_cost = np.full(
                    (TwoGateMoE.ENGINE_CLASSES, TwoGateMoE.LAKE_CLASSES),
                    math.log(max(ratio_cap, 1.0)), dtype=np.float32,
                )
                for combo_latency, cc in combo_best:
                    ratio = combo_latency / max(best_latency, 1e-12)
                    route_cost[int(cc[0]), int(cc[1])] = math.log(
                        max(1.0, min(ratio, ratio_cap))
                    )
                per_query_routes.append(
                    (q2idx_map[qid], int(best_cc[0]), int(best_cc[1]),
                     confidence, route_cost)
                )
            wl['_per_query_routes'] = per_query_routes

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
                optimum_target = 0.0 if log_ratio_target else 1.0
                optimum_target -= config_prior(qid, best_q_cc, best_q_conf)
                stage1.append((conf_padded, optimum_target,
                               int(best_q_cc[0]), int(best_q_cc[1])))
            wl['_stage1'] = stage1

            # Stage 3 records: all (q, combo, conf) sampled, target_r = lat / q_best_lat
            stage3 = []
            for qid in qids:
                if qid not in stats or qid not in q_best_lat:
                    continue
                qbest = q_best_lat[qid]
                for cc, recs in stats[qid].items():
                    stage3_recs = list(recs)
                    if stage3_median_configs:
                        cache_key = (qid, cc)
                        if cache_key not in median_config_cache:
                            by_conf = defaultdict(list)
                            conf_tensor = {}
                            for conf, latency in stage3_recs:
                                key = tuple(round(float(x), 6) for x in conf.tolist())
                                by_conf[key].append(latency)
                                conf_tensor[key] = conf
                            median_config_cache[cache_key] = [
                                (conf_tensor[key], float(np.median(latencies)))
                                for key, latencies in by_conf.items()
                            ]
                        stage3_recs = median_config_cache[cache_key]
                    if len(stage3_recs) <= 16:
                        sampled = stage3_recs
                    else:
                        sorted_recs = sorted(stage3_recs, key=lambda x: x[1])
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
                        if log_ratio_target:
                            r = math.log(max(r, 1e-12))
                        r -= config_prior(qid, cc, conf)
                        stage3.append((conf_padded, r, int(cc[0]), int(cc[1]),
                                       q2idx_map[qid]))
            wl['_stage3'] = stage3

    print("Precomputing paper tensors...")
    for wl_list in [train_workloads, valid_workloads, test_workloads]:
        precompute_paper_tensors(wl_list, q2idx, max_dim)
    if holdout:
        for wls in test_workloads_by_sf.values():
            precompute_paper_tensors(wls, q2idx, max_dim)

    # Inverse-sqrt class weights from unique training queries. They affect only
    # the optional auxiliary gate loss and are computed without validation/test.
    eng_route_counts = np.zeros(TwoGateMoE.ENGINE_CLASSES, dtype=np.float64)
    lak_route_counts = np.zeros(TwoGateMoE.LAKE_CLASSES, dtype=np.float64)
    combo_route_counts = defaultdict(int)
    for qid in train_qnames:
        combo_best = []
        for cc, recs in all_data.get(qid, {}).items():
            if recs:
                combo_best.append((min(lat for _, lat in recs), cc))
        if combo_best:
            _, cc = min(combo_best, key=lambda item: item[0])
            eng_route_counts[cc[0]] += 1
            lak_route_counts[cc[1]] += 1
            combo_route_counts[cc] += 1

    def _balanced_weights(counts):
        weights = 1.0 / np.sqrt(np.maximum(counts, 1.0))
        weights /= weights.mean()
        return torch.tensor(weights, device=device, dtype=torch.float)

    aux_eng_weights = _balanced_weights(eng_route_counts) if router_class_balance else None
    aux_lak_weights = _balanced_weights(lak_route_counts) if router_class_balance else None

    # Training-label prior (Laplace-smoothed) for prior-matching gate regularization,
    # and the training-majority combo order used as a no-model routing reference.
    prior_eng = torch.tensor((eng_route_counts + 1.0) / (eng_route_counts.sum() + 3.0),
                             device=device, dtype=torch.float)
    prior_lak = torch.tensor((lak_route_counts + 1.0) / (lak_route_counts.sum() + 3.0),
                             device=device, dtype=torch.float)
    _EVAL_OPTS['joint'] = joint_gate_inference
    _EVAL_OPTS['prior_combos'] = sorted(combo_route_counts, key=lambda cc: -combo_route_counts[cc])
    print(f"  Gate prior: engine={[round(x, 3) for x in prior_eng.tolist()]} "
          f"lake={[round(x, 3) for x in prior_lak.tolist()]} reg={gate_prior_reg} "
          f"soft_T={gate_soft_label_temp} joint_inference={joint_gate_inference}")
    if router_aux_weight > 0:
        print(f"  Router auxiliary supervision: weight={router_aux_weight} "
              f"margin_scale={router_margin_scale} class_balance={router_class_balance}")
        print(f"  Route counts: engine={eng_route_counts.astype(int).tolist()} "
              f"lake={lak_route_counts.astype(int).tolist()}")
    if router_stage1_aux_weight > 0 or router_regret_weight > 0:
        print(f"  End-to-end router loss: aux_weight={router_stage1_aux_weight} "
              f"regret_weight={router_regret_weight}")

    # Tree-based embedding (always used)
    import tree_embedding as _te
    _te.TREE_READOUT = tree_readout
    print(f"  Tree readout: {tree_readout}")
    mapped_tree, feat_dim = load_all_plan_trees(num_augment=5, swap_prob=0.3)
    if feat_dim == 0:
        print("WARNING: No plan trees loaded, falling back to pure learnable embeddings")
        feat_dim = 32
        mapped_tree = {}

    base_tree_keys = set(k for k in mapped_tree.keys() if '_aug' not in k)
    idx_to_tree_key = build_query_to_plan_mapping(q2idx, base_tree_keys)

    if tree_feat_norm == 'agnostic' and mapped_tree:
        mapped_tree, op_vocab = schema_agnostic_node_features(mapped_tree)
        feat_dim = next(iter(mapped_tree.values()))[0].size(1)
        print(f"  Schema-agnostic node features: {len(op_vocab)} operators "
              f"({op_vocab}) → feat_dim={feat_dim}")
    if tree_feat_norm in ('log', 'agnostic') and mapped_tree:
        # Raw plan-node features mix small categorical codes (<=19) with a
        # cardinality column reaching 6e8, which alone drives the tree-conv output
        # and collapses embeddings (within-benchmark cos ~0.96-1.0).  Signed log1p,
        # then standardize with statistics of TRAINING-query plans only.
        def _slog(t):
            if tree_feat_norm == 'agnostic':
                return t  # already log-scaled where needed
            return torch.sign(t) * torch.log1p(t.abs())
        train_keys = sorted({idx_to_tree_key[q2idx[q]] for q in train_qnames
                             if q in q2idx and q2idx[q] in idx_to_tree_key})
        feats = torch.cat([_slog(mapped_tree[k][0]) for k in train_keys], dim=0)
        mu = feats.mean(0)
        sd = feats.std(0)
        # Dims constant on training plans: centre only (no division by ~0), so
        # unseen-schema values stay on the log scale instead of exploding.
        sd = torch.where(sd < 1e-6, torch.ones_like(sd), sd)
        mapped_tree = {k: ((_slog(v[0]) - mu) / sd,) + tuple(v[1:])
                       for k, v in mapped_tree.items()}
        print(f"  Tree feature norm: signed log1p + z-score from {len(train_keys)} "
              f"training plans ({feats.size(0)} nodes)")

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
    if neutral_unseen_fallback:
        # Unmapped validation/test queries never receive an embedding gradient.
        # A shared neutral initialization makes their behavior deterministic
        # and lets the existing gate bias learn a training-only prior, while
        # retaining the paper's per-query nn.Embedding fallback parameters.
        with torch.no_grad():
            query_encoder.fallback_embedding.weight.zero_()
        print("  Fallback initialization: neutral zero vector for unmapped queries")

    # Attention pool for workload aggregation (replaces mean/max/std).
    if sf_embedding:
        # The same query template at sf1/10/100 maps to ONE plan and therefore one
        # embedding, yet the best combo/config depends strongly on data size.  The
        # scale factor is known at deployment, so add a learned per-sf vector.
        sf_vocab = {'1': 0, '10': 1, '100': 2}
        q_sf = torch.tensor([sf_vocab.get(_re.search(r'sf(\d+)_', q).group(1), 0)
                             for q in all_query_ids], device=device)
        query_encoder.sf_embedding = nn.Embedding(3, QENC_DIM).to(device)
        query_encoder.register_buffer('_q_sf', q_sf)
        _fwd, _fwd_train = query_encoder.forward, query_encoder.forward_train
        query_encoder.forward = lambda ids: _fwd(ids) + query_encoder.sf_embedding(query_encoder._q_sf[ids])
        query_encoder.forward_train = (lambda ids: _fwd_train(ids)
                                       + query_encoder.sf_embedding(query_encoder._q_sf[ids]))
        print(f"  SF embedding: {dict(zip(*torch.unique(q_sf, return_counts=True)))}")
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

    if gate_dropout is not None:
        for gate in (moe_model.engine_gate, moe_model.lake_gate):
            for m in gate.modules():
                if isinstance(m, nn.Dropout):
                    m.p = gate_dropout

    print(f"\nModel params: query_encoder={sum(p.numel() for p in query_encoder.parameters()):,}, "
          f"moe={sum(p.numel() for p in moe_model.parameters()):,}")

    BIG_EPOCHS = big_epochs

    best_valid_ratio = float('inf')
    best_epoch = 0
    # In-memory snapshots of the best model. We do NOT write checkpoints to disk.
    best_qenc_state = None
    best_pool_state = None
    best_moe_state = None

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
    opt_gate = Adam(gate_params, lr=gate_lr, weight_decay=1e-5)
    if stage2_train_encoder:
        # Let Stage 2 also adapt the tree-conv encoder to the routing objective.
        opt_gate.add_param_group({'params': tree_params, 'lr': gate_lr,
                                  'weight_decay': tree_weight_decay})
    opt_expert = Adam(expert_params, lr=3e-4, weight_decay=1e-5)
    sched_full = CosineAnnealingLR(opt_full, T_max=BIG_EPOCHS, eta_min=1e-5)
    sched_gate = CosineAnnealingLR(opt_gate, T_max=BIG_EPOCHS, eta_min=1e-5)
    sched_expert = CosineAnnealingLR(opt_expert, T_max=BIG_EPOCHS, eta_min=1e-5)

    print(f"\n{'='*60}")
    print(f"Training: {BIG_EPOCHS} epochs — paper-spec 3-stage MoE")
    print(f"  Seed: {seed} | train_workloads={num_train_workloads}")
    print(f"  Stage 2 sub-epochs: {stage2_subepochs} | Stage 3 sub-epochs: {stage3_subepochs}")
    print(f"  λ_div={lambda_div} | gumbel τ={gumbel_tau} | batch_size={batch_size} | ratio_cap={ratio_cap}")
    print(f"  Data repairs: noise_cap={noise_filter_fraction:.3f} "
          f"normalize_configs={normalize_configs} log_ratio={log_ratio_target} "
          f"median_configs={stage3_median_configs}")
    print(f"  Gate supervision: aux={router_aux_weight} "
          f"stage1_aux={router_stage1_aux_weight} regret={router_regret_weight} "
          f"workload_ce={workload_gate_ce_weight} class_balance={router_class_balance}")
    print(f"  Expert supervision: per_query={per_query_expert_weight} "
          f"rank={expert_rank_weight} workload={workload_expert_weight}")
    print("  Inference: original hard engine/lake gate, then selected-combo expert")
    print(f"{'='*60}")

    def _get_w_emb(q_indices):
        return aggregate_workload_emb(query_encoder(q_indices))

    def _get_w_emb_train(q_indices):
        # Differentiable lookup — must call precompute_training_table() before this
        return aggregate_workload_emb(query_encoder.forward_train(q_indices))

    def _gate_balance_terms(avg_eng, avg_lak):
        """(L_div, anti-collapse) on batch-mean gate probs.  'uniform' keeps the
        original push toward 1/3; 'train' targets the training-label prior so a
        skewed optimum (e.g. hudi best for 58% of queries) is not penalized."""
        if gate_prior_reg == 'train':
            div = ((avg_eng - prior_eng) ** 2).sum() + ((avg_lak - prior_lak) ** 2).sum()
            kl = ((avg_eng * ((avg_eng + 1e-12).log() - prior_eng.log())).sum()
                  + (avg_lak * ((avg_lak + 1e-12).log() - prior_lak.log())).sum())
            return div, kl
        inv_e = 1.0 / TwoGateMoE.ENGINE_CLASSES
        inv_l = 1.0 / TwoGateMoE.LAKE_CLASSES
        div = ((avg_eng - inv_e) ** 2).sum() + ((avg_lak - inv_l) ** 2).sum()
        ent_eng = -(avg_eng * (avg_eng + 1e-12).log()).sum()
        ent_lak = -(avg_lak * (avg_lak + 1e-12).log()).sum()
        diversity = ((math.log(TwoGateMoE.ENGINE_CLASSES) - ent_eng)
                     + (math.log(TwoGateMoE.LAKE_CLASSES) - ent_lak))
        return div, diversity

    def _soft_gate_ce(eng_lg, lak_lg, route_cost):
        """CE against softmax(-log_slowdown / T), marginalized to engine / lake.
        route_cost: (B, E, L) log-slowdown; near-tied combos share the target mass."""
        B = route_cost.size(0)
        tj = F.softmax(-route_cost.reshape(B, -1) / gate_soft_label_temp, dim=-1)
        tj = tj.view(B, TwoGateMoE.ENGINE_CLASSES, TwoGateMoE.LAKE_CLASSES)
        ce_e = -(tj.sum(2) * F.log_softmax(eng_lg, dim=-1)).sum(-1)
        ce_l = -(tj.sum(1) * F.log_softmax(lak_lg, dim=-1)).sum(-1)
        return (ce_e + ce_l).mean()

    def _single_query_router_terms(raw_q_emb, routes):
        """Losses for the existing gates at their single-query input shape.

        ``raw_q_emb`` may be differentiable (Stage 1) or detached (Stage 2).
        The returned regret is the expected log slowdown under the factorized
        engine/lake probabilities, so it trains the original gates directly
        for routing cost without adding a network or changing hard inference.
        """
        single_q_gate_emb = torch.cat(
            [raw_q_emb] * (pool.num_heads + 2), dim=-1
        )
        aux_eng_logits = moe_model.engine_gate(single_q_gate_emb)
        aux_lak_logits = moe_model.lake_gate(single_q_gate_emb)
        aux_eng_targets = torch.tensor(
            [route[1] for route in routes], device=device, dtype=torch.long
        )
        aux_lak_targets = torch.tensor(
            [route[2] for route in routes], device=device, dtype=torch.long
        )
        confidence = torch.tensor(
            [route[3] for route in routes], device=device, dtype=torch.float
        )
        aux_eng_ce = F.cross_entropy(
            aux_eng_logits, aux_eng_targets,
            weight=aux_eng_weights, reduction='none'
        )
        aux_lak_ce = F.cross_entropy(
            aux_lak_logits, aux_lak_targets,
            weight=aux_lak_weights, reduction='none'
        )
        hard_ce = ((aux_eng_ce + aux_lak_ce) * confidence).sum() \
            / confidence.sum().clamp_min(1e-6)

        eng_probs = F.softmax(aux_eng_logits, dim=-1)
        lak_probs = F.softmax(aux_lak_logits, dim=-1)
        joint_probs = eng_probs.unsqueeze(2) * lak_probs.unsqueeze(1)
        cost_np = np.stack([route[4] for route in routes], axis=0)
        route_cost = torch.as_tensor(cost_np, device=device, dtype=torch.float)
        if gate_soft_label_temp > 0:
            hard_ce = _soft_gate_ce(aux_eng_logits, aux_lak_logits, route_cost)
        regret = (joint_probs * route_cost).sum(dim=(1, 2)).mean()
        return hard_ce, regret

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
            b_aux_route_terms = []; b_regret_terms = []
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
                if gate_soft_label_temp > 0:
                    bce_terms.append(_soft_gate_ce(eng_lg[:1], lak_lg[:1],
                                                   wl['_route_cost'].unsqueeze(0)))
                else:
                    bce_terms.append(F.cross_entropy(eng_lg, t_eng) + F.cross_entropy(lak_lg, t_lak))
                b_eng_p.append(F.softmax(eng_lg, dim=-1))
                b_lak_p.append(F.softmax(lak_lg, dim=-1))
                if ((router_stage1_aux_weight > 0 or router_regret_weight > 0)
                        and wl.get('_per_query_routes')):
                    routes = wl['_per_query_routes']
                    route_ids = torch.tensor(
                        [route[0] for route in routes], device=device, dtype=torch.long
                    )
                    raw_q_emb = query_encoder.forward_train(route_ids)
                    aux_ce, regret = _single_query_router_terms(raw_q_emb, routes)
                    b_aux_route_terms.append(aux_ce)
                    b_regret_terms.append(regret)

            if not bmse_terms:
                continue
            mse_m = torch.stack(bmse_terms).mean()
            ce_m = torch.stack(bce_terms).mean()
            all_eng_p = torch.cat(b_eng_p, dim=0)  # (B_total, ENG)
            all_lak_p = torch.cat(b_lak_p, dim=0)  # (B_total, LAK)
            avg_eng = all_eng_p.mean(0); avg_lak = all_lak_p.mean(0)
            # Anti-collapse on batch-mean probs (target: uniform or training prior).
            div, diversity_loss = _gate_balance_terms(avg_eng, avg_lak)
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
            aux_route_loss = (
                torch.stack(b_aux_route_terms).mean()
                if b_aux_route_terms else torch.tensor(0.0, device=device)
            )
            regret_loss = (
                torch.stack(b_regret_terms).mean()
                if b_regret_terms else torch.tensor(0.0, device=device)
            )
            loss = (mse_m + workload_gate_ce_weight * ce_m + lambda_div * div +
                    lambda_diversity * diversity_loss +
                    lambda_emb_spread * emb_spread_loss +
                    router_stage1_aux_weight * aux_route_loss +
                    router_regret_weight * regret_loss)
            opt_full.zero_grad()
            loss.backward()
            torch.nn.utils.clip_grad_norm_(all_params, 1.0)
            opt_full.step()
            total += loss.item(); mse_acc += mse_m.item(); ce_acc += ce_m.item(); div_acc += div.item()
            n_steps += 1
        return total / max(n_steps, 1), mse_acc / max(n_steps, 1), ce_acc / max(n_steps, 1), div_acc / max(n_steps, 1)

    # Query groups for tree-conv collapse diagnostics (plan-mapped queries only;
    # unmapped ones share the fallback vector and would fake a collapse).
    # Deduplicated by plan key: queries sharing a plan (e.g. the same template at
    # sf1/10/100) get identical embeddings and would inflate the counts.
    def _uniq_plan_idxs(qs):
        seen = {}
        for q in qs:
            i = q2idx.get(q)
            if i is not None and i in idx_to_tree_key:
                seen.setdefault(idx_to_tree_key[i], i)
        return list(seen.values())
    _diag_groups = {
        name: _uniq_plan_idxs(qs)
        for name, qs in (('train', train_qnames), ('valid', valid_qnames), ('test', test_qnames))
    }
    if holdout:
        for bm in sorted({q.split('|', 1)[0] for q in train_qnames}):
            _diag_groups[f"tr:{bm}"] = _uniq_plan_idxs(
                [q for q in train_qnames if q.startswith(bm + '|')])

    def _emb_diagnostic(tag):
        """Tree-conv output collapse check on the eval-mode embedding table.
        cos   = mean pairwise cosine (LayerNorm output, so ~correlation; →1 = collapse)
        erank = effective rank exp(H(σ²/Σσ²)) of the centered matrix (of QENC_DIM)
        std   = mean per-dim std across queries;  uniq = distinct embeddings
        shift = cosine(test centroid, train centroid) — cross-schema distribution gap"""
        if not hasattr(query_encoder, 'recompute_tree_embeddings'):
            return
        query_encoder.eval()
        query_encoder.recompute_tree_embeddings(device)
        table = query_encoder._emb_table
        parts = []; cents = {}
        with torch.no_grad():
            for name, idxs in _diag_groups.items():
                if len(idxs) < 2:
                    continue
                X = table[torch.tensor(idxs, device=table.device)].float()
                Xn = F.normalize(X, dim=-1)
                n = X.size(0)
                cos = ((Xn @ Xn.T).sum() - n) / (n * (n - 1))
                Xc = X - X.mean(0, keepdim=True)
                sv = torch.linalg.svdvals(Xc) ** 2
                p = sv / sv.sum().clamp_min(1e-12)
                erank = torch.exp(-(p * (p + 1e-12).log()).sum())
                std = Xc.std(0).mean()
                uniq = torch.unique(torch.round(X * 1e3), dim=0).size(0)
                cents[name] = X.mean(0)
                parts.append(f"{name}(n={n}) cos={cos.item():.3f} erank={erank.item():.1f} "
                             f"std={std.item():.3f} uniq={uniq}")
        shift = ""
        if 'train' in cents and 'test' in cents:
            shift = f" | shift cos(test,train centroid)={F.cosine_similarity(cents['test'], cents['train'], dim=0).item():.3f}"
        print(f"    EmbDiag[{tag}]: " + " ; ".join(parts) + shift)

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
        if stage2_train_encoder:
            query_encoder.train()
        elif hasattr(query_encoder, 'recompute_tree_embeddings'):
            query_encoder.recompute_tree_embeddings(device)
        emb_fn = _get_w_emb_train if stage2_train_encoder else _get_w_emb
        raw_fn = query_encoder.forward_train if stage2_train_encoder else query_encoder
        total = 0.0; n_steps = 0
        for se in range(sub_epochs):
            random.shuffle(workloads)
            i = 0
            while i < len(workloads):
                chunk = workloads[i:i + batch_size]
                i += batch_size
                if stage2_train_encoder:
                    query_encoder.precompute_training_table(device)
                bce_terms = []; b_eng_p = []; b_lak_p = []
                aux_route_terms = []; regret_terms = []
                for wl in chunk:
                    q_indices = wl['_q_indices']
                    if q_indices is None or len(q_indices) == 0:
                        continue
                    w_emb = emb_fn(q_indices)
                    eng_lg = moe_model.engine_gate(w_emb)
                    lak_lg = moe_model.lake_gate(w_emb)
                    te = torch.tensor([wl['_best_eng']], device=device, dtype=torch.long)
                    tl = torch.tensor([wl['_best_lak']], device=device, dtype=torch.long)
                    if gate_soft_label_temp > 0:
                        bce_terms.append(_soft_gate_ce(eng_lg, lak_lg,
                                                       wl['_route_cost'].unsqueeze(0)))
                    else:
                        bce_terms.append(F.cross_entropy(eng_lg, te) + F.cross_entropy(lak_lg, tl))
                    b_eng_p.append(F.softmax(eng_lg, dim=-1))
                    b_lak_p.append(F.softmax(lak_lg, dim=-1))
                    if ((router_aux_weight > 0 or router_regret_weight > 0)
                            and wl.get('_per_query_routes')):
                        routes = wl['_per_query_routes']
                        route_ids = torch.tensor(
                            [route[0] for route in routes], device=device, dtype=torch.long
                        )
                        raw_q_emb = raw_fn(route_ids)
                        aux_ce, regret = _single_query_router_terms(raw_q_emb, routes)
                        aux_route_terms.append(aux_ce)
                        regret_terms.append(regret)
                if not bce_terms:
                    continue
                ce_m = torch.stack(bce_terms).mean()
                all_eng_p = torch.cat(b_eng_p, dim=0)
                all_lak_p = torch.cat(b_lak_p, dim=0)
                avg_eng = all_eng_p.mean(0); avg_lak = all_lak_p.mean(0)
                div, diversity_loss = _gate_balance_terms(avg_eng, avg_lak)
                aux_route_loss = (
                    torch.stack(aux_route_terms).mean()
                    if aux_route_terms else torch.tensor(0.0, device=device)
                )
                regret_loss = (
                    torch.stack(regret_terms).mean()
                    if regret_terms else torch.tensor(0.0, device=device)
                )
                loss = (workload_gate_ce_weight * ce_m
                        + router_aux_weight * aux_route_loss
                        + router_regret_weight * regret_loss
                        + lambda_div * div + lambda_diversity * diversity_loss)
                opt_gate.zero_grad()
                loss.backward()
                torch.nn.utils.clip_grad_norm_(
                    gate_params + (tree_params if stage2_train_encoder else []), 1.0)
                opt_gate.step()
                total += loss.item(); n_steps += 1
            if stage2_train_encoder and hasattr(query_encoder, 'recompute_tree_embeddings'):
                query_encoder.eval()
                query_encoder.recompute_tree_embeddings(device)
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
                per_query_mse = 0.0; n_per_query_groups = 0
                rank_loss = 0.0; n_rank_groups = 0
                for (e_id, l_id), idxs in groups.items():
                    confs = torch.stack([stage3[i][0] for i in idxs])
                    t_r = torch.tensor([stage3[i][1] for i in idxs], device=device, dtype=torch.float)
                    if workload_expert_weight > 0:
                        w_emb_e = w_emb.expand(len(idxs), -1)
                        pred = moe_model.forward_for_eng_lak(
                            w_emb_e, confs, e_id, l_id
                        )
                        total_mse = total_mse + F.mse_loss(pred.squeeze(-1), t_r)
                        ngrp += 1

                    if per_query_expert_weight > 0 or expert_rank_weight > 0:
                        route_ids = torch.tensor(
                            [stage3[i][4] for i in idxs],
                            device=device, dtype=torch.long,
                        )
                        raw_q_emb = query_encoder(route_ids)
                        single_q_emb = torch.cat(
                            [raw_q_emb] * (pool.num_heads + 2), dim=-1
                        )
                        pq_pred = moe_model.forward_for_eng_lak(
                            single_q_emb, confs, e_id, l_id
                        ).squeeze(-1)
                        if per_query_expert_weight > 0:
                            per_query_mse = per_query_mse + F.mse_loss(pq_pred, t_r)
                            n_per_query_groups += 1
                        if expert_rank_weight > 0:
                            positions_by_query = defaultdict(list)
                            for pos, idx in enumerate(idxs):
                                positions_by_query[stage3[idx][4]].append(pos)
                            for positions in positions_by_query.values():
                                if len(positions) < 2:
                                    continue
                                pos_t = torch.tensor(
                                    positions, device=device, dtype=torch.long
                                )
                                group_pred = pq_pred[pos_t]
                                group_target = t_r[pos_t]
                                best_pos = group_target.argmin().view(1)
                                rank_loss = rank_loss + F.cross_entropy(
                                    (-group_pred).unsqueeze(0), best_pos
                                )
                                n_rank_groups += 1
                if ngrp == 0 and n_per_query_groups == 0 and n_rank_groups == 0:
                    continue
                loss = torch.tensor(0.0, device=device)
                if ngrp > 0:
                    loss = loss + workload_expert_weight * total_mse / ngrp
                if n_per_query_groups > 0:
                    loss = (loss + per_query_expert_weight * per_query_mse
                            / n_per_query_groups)
                if n_rank_groups > 0:
                    loss = loss + expert_rank_weight * rank_loss / n_rank_groups
                loss.backward()
                torch.nn.utils.clip_grad_norm_(expert_params, 1.0)
                opt_expert.step()
                total += loss.item(); n_steps += 1
        return total / max(n_steps, 1)

    _emb_diagnostic("init")
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
        _emb_diagnostic(f"ep{epoch}")
        sched_full.step(); sched_gate.step(); sched_expert.step()

        # MODEL SELECTION on VALIDATION set (test never used during training).
        ratio = evaluate(query_encoder, moe_model, valid_workloads, q2idx, max_dim,
                         eval_mode=eval_mode, eval_noise=eval_noise)
        if select_metric != 'mean' and select_metric in _LAST_EVAL:
            # 'geo': end-to-end geometric ratio; 'oracle_geo': ratio when routed to
            # each valid query's true best combo = quality of the expert's config
            # choice only (for training the encoder+experts before tuning the gate).
            ratio = _LAST_EVAL[select_metric]

        # Snapshot model state in memory at the lowest validation ratio.
        # No checkpoint files are written.
        if ratio < best_valid_ratio:
            best_valid_ratio = ratio
            best_epoch = epoch
            best_qenc_state = {k: v.detach().clone() for k, v in query_encoder.state_dict().items()}
            best_pool_state = {k: v.detach().clone() for k, v in pool.state_dict().items()}
            best_moe_state  = {k: v.detach().clone() for k, v in moe_model.state_dict().items()}
        print(f"  Epoch {epoch:3d}: s1={s1_total:.4f} (mse={s1_mse:.4f} ce={s1_ce:.4f} div={s1_div:.4f}) "
              f"s2={s2_loss:.4f} s3={s3_loss:.4f} "
              f"valid_ratio={ratio:.4f}  best_valid={best_valid_ratio:.4f}@ep{best_epoch}")
        if early_stop_patience > 0 and epoch - best_epoch >= early_stop_patience:
            print(f"  Early stop: no validation improvement for "
                  f"{early_stop_patience} epochs")
            break

    # Final TEST evaluation using the in-memory best-validation snapshot.
    print(f"\n{'='*60}")
    print(f"Training complete. Best valid ratio: {best_valid_ratio:.4f} at epoch {best_epoch}")
    if best_qenc_state is not None:
        query_encoder.load_state_dict(best_qenc_state)
        pool.load_state_dict(best_pool_state)
        moe_model.load_state_dict(best_moe_state)
        # ``_emb_table`` is a detached cache, not part of state_dict. Without
        # rebuilding it here, the final test mixes the best checkpoint weights
        # with tree embeddings from the last training epoch. Recompute in eval
        # mode so dropout is disabled and the reported test really corresponds
        # to the selected validation snapshot.
        query_encoder.eval()
        if hasattr(query_encoder, 'recompute_tree_embeddings'):
            query_encoder.recompute_tree_embeddings(device)
    test_ratio = evaluate(query_encoder, moe_model, test_workloads, q2idx, max_dim,
                          eval_mode=eval_mode, eval_noise=eval_noise)
    print(f"Final test ratio (best-valid snapshot, ep{best_epoch}): {test_ratio:.4f}")
    if _LAST_EVAL:
        print("Final test summary: " + " ".join(
            f"{k}={v:.4f}" for k, v in _LAST_EVAL.items()))
    if holdout:
        for sf, wls in test_workloads_by_sf.items():
            print(f"  Held-out {test_benchmark} {sf} ({len(wls)} queries):")
            r = evaluate(query_encoder, moe_model, wls, q2idx, max_dim,
                         eval_mode=eval_mode, eval_noise=eval_noise)
            print(f"Holdout test {test_benchmark} {sf}: ratio={r:.4f} " + " ".join(
                f"{k}={v:.4f}" for k, v in _LAST_EVAL.items()))
    print(f"Time: {time.time() - t0:.1f}s")

    if export_gate_data:
        _export_gate_data(export_gate_data, all_data, q2idx, train_qnames, valid_qnames,
                          test_qnames, query_encoder, moe_model, max_dim)
    return test_ratio


def _export_gate_data(path, all_data, q2idx, train_qnames, valid_qnames, test_qnames,
                      query_encoder, moe_model, max_dim):
    """Freeze the (best-valid) encoder + experts and dump what a gate needs:
    per-query embedding, per-combo log-slowdown of the combo's best config
    (routing cost), and the ratio actually obtained when the frozen expert picks
    the config inside each combo (end-to-end outcome of routing there).
    Split labels are kept so gate tuning can train on train, select on valid and
    touch test only once."""
    query_encoder.eval(); moe_model.eval()
    if hasattr(query_encoder, 'recompute_tree_embeddings'):
        query_encoder.recompute_tree_embeddings(device)
    split = {q: 'train' for q in train_qnames}
    split.update({q: 'valid' for q in valid_qnames})
    split.update({q: 'test' for q in test_qnames})
    E, L = TwoGateMoE.ENGINE_CLASSES, TwoGateMoE.LAKE_CLASSES
    qids, splits, embs, costs, expert_ratios = [], [], [], [], []
    rec_q, rec_combo, rec_key, rec_lat, rec_pred = [], [], [], [], []
    with torch.no_grad():
        for qid in sorted(q for q in all_data if q in split and q in q2idx):
            recs_by_cc = {cc: [(c, l) for c, l in recs if c is not None]
                          for cc, recs in all_data[qid].items()}
            recs_by_cc = {cc: r for cc, r in recs_by_cc.items() if r}
            if not recs_by_cc:
                continue
            best = min(l for r in recs_by_cc.values() for _, l in r)
            if best <= 0:
                continue
            idx = torch.tensor([q2idx[qid]], device=device)
            q_emb = aggregate_workload_emb(query_encoder(idx))
            cost = torch.full((E, L), float('nan'))
            exp_r = torch.full((E, L), float('nan'))
            for (e, l), recs in recs_by_cc.items():
                cost[e, l] = math.log(min(l_ for _, l_ in recs) / best)
                conf_b = torch.stack([prepare_conf(c.tolist(), max_dim) for c, _ in recs]).to(device)
                pred = moe_model.forward_for_eng_lak(
                    q_emb.expand(len(recs), -1), conf_b, e, l).squeeze(-1)
                pred = pred + torch.tensor([_EVAL_OPTS['config_prior'](qid, (e, l), c)
                                            for c, _ in recs], device=pred.device)
                exp_r[e, l] = recs[int(pred.argmin().item())][1] / best
                for (c, lat_), pv in zip(recs, pred.tolist()):
                    rec_q.append(len(qids)); rec_combo.append(e * L + l)
                    rec_key.append((e, l) + tuple(round(float(x), 4) for x in c.tolist()))
                    rec_lat.append(lat_ / best); rec_pred.append(pv)
            qids.append(qid); splits.append(split[qid])
            embs.append(query_encoder(idx)[0].cpu())
            costs.append(cost); expert_ratios.append(exp_r)
    torch.save({'qids': qids, 'split': splits, 'emb': torch.stack(embs),
                'cost': torch.stack(costs), 'expert_ratio': torch.stack(expert_ratios),
                # per measured (query, combo, config) record: config identity, ratio
                # to the query's best latency, and the frozen expert's prediction
                'rec_q': rec_q, 'rec_combo': rec_combo, 'rec_key': rec_key,
                'rec_ratio': rec_lat, 'rec_pred': rec_pred}, path)
    print(f"Exported gate data for {len(qids)} queries → {path}")


# Evaluation options set by train_model; metrics of the latest per-query eval.
_EVAL_OPTS = {'joint': False, 'prior_combos': [], 'config_prior': lambda q, cc, c: 0.0}
_LAST_EVAL = {}


def _geo(r):
    return float(np.exp(np.log(np.asarray(r, dtype=float)).mean()))


def evaluate_per_query(query_encoder, moe_model, workloads, q2idx, max_dim):
    """Paper-style per-query inference (no oracle routing):
       1. q_emb = query_encoder([q])
       2. Run gates → eng* = argmax engine_gate, lak* = argmax lake_gate
          (with joint inference: argmax p_e*p_l over combos this query has data for)
       3. Among the records for this query restricted to combo (eng*, lak*),
          score every conf via forward_for_eng_lak and pick argmin pred.
       4. ratio = chosen_actual_lat / query_min_lat (across ALL combos).
    Also reports two diagnostics using the same expert config choice:
       oracle-gate (true best combo) and prior (training-majority available combo).
    """
    query_encoder.eval()
    moe_model.eval()

    ratios = []; oracle_ratios = []; prior_ratios = []
    eng_correct = lak_correct = combo_correct = total = 0
    prior_correct = 0
    fallback_used = 0

    cprior = _EVAL_OPTS['config_prior']

    def _pick_in_combo(q_emb, recs, combo, qid):
        confs = []; lats = []; pri = []
        for conf, lat in recs:
            if conf is None:
                continue
            confs.append(prepare_conf(conf.tolist(), max_dim))
            lats.append(lat); pri.append(cprior(qid, combo, conf))
        if not confs:
            return None
        conf_b = torch.stack(confs).to(device)
        preds = moe_model.forward_for_eng_lak(q_emb.expand(len(confs), -1), conf_b,
                                              combo[0], combo[1]).squeeze(-1)
        preds = preds + torch.tensor(pri, device=preds.device, dtype=preds.dtype)
        return lats[preds.argmin().item()]

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

                # Per-query global min (across all combos) — denominator
                best_combo_overall = None; best_lat_overall = float('inf')
                for cc, recs in pq_data[qid].items():
                    for _, lat in recs:
                        if lat < best_lat_overall:
                            best_lat_overall = lat; best_combo_overall = cc
                if best_combo_overall is None or best_lat_overall <= 0:
                    continue
                query_min_lat = best_lat_overall
                avail = [cc for cc, recs in pq_data[qid].items()
                         if any(conf is not None for conf, _ in recs)]
                if not avail:
                    continue

                # Step 2: gate selection
                eng_lg = moe_model.engine_gate(q_emb)
                lak_lg = moe_model.lake_gate(q_emb)
                if _EVAL_OPTS['joint']:
                    ep = F.softmax(eng_lg, dim=-1)[0]; lp = F.softmax(lak_lg, dim=-1)[0]
                    chosen_combo = max(avail, key=lambda cc: float(ep[cc[0]] * lp[cc[1]]))
                else:
                    chosen_combo = (int(eng_lg.argmax(-1).item()),
                                    int(lak_lg.argmax(-1).item()))

                # Step 3: candidate confs restricted to the chosen combo.
                if chosen_combo in avail:
                    chosen_actual = _pick_in_combo(q_emb, pq_data[qid][chosen_combo],
                                                   chosen_combo, qid)
                else:
                    # Fallback: combo not present for this query → score all combos
                    fallback_used += 1
                    confs = []; lats = []; combos = []
                    for cc, recs in pq_data[qid].items():
                        for conf, lat in recs:
                            if conf is None:
                                continue
                            confs.append(prepare_conf(conf.tolist(), max_dim))
                            lats.append(lat); combos.append(cc)
                    conf_b = torch.stack(confs).to(device)
                    q_emb_e = q_emb.expand(len(confs), -1)
                    combo_ids = torch.tensor([[float(c[0]), float(c[1])] for c in combos],
                                              device=device, dtype=torch.float)
                    preds = moe_model.forward_oracle(q_emb_e, conf_b, combo_ids).squeeze(-1)
                    preds = preds + torch.tensor(
                        [cprior(qid, cc, c) for cc, recs in pq_data[qid].items()
                         for c, _ in recs if c is not None],
                        device=preds.device, dtype=preds.dtype)
                    pick = preds.argmin().item()
                    chosen_actual = lats[pick]; chosen_combo = combos[pick]
                if chosen_actual is None:
                    continue
                ratios.append(chosen_actual / query_min_lat)

                # Diagnostics: perfect gate, and training-majority routing.
                o = _pick_in_combo(q_emb, pq_data[qid][best_combo_overall], best_combo_overall, qid)
                if o is not None:
                    oracle_ratios.append(o / query_min_lat)
                prior_combo = next((cc for cc in _EVAL_OPTS['prior_combos'] if cc in avail),
                                   avail[0])
                pr = _pick_in_combo(q_emb, pq_data[qid][prior_combo], prior_combo, qid)
                if pr is not None:
                    prior_ratios.append(pr / query_min_lat)
                if prior_combo == best_combo_overall:
                    prior_correct += 1

                if chosen_combo[0] == best_combo_overall[0]: eng_correct += 1
                if chosen_combo[1] == best_combo_overall[1]: lak_correct += 1
                if chosen_combo == best_combo_overall: combo_correct += 1
                total += 1

    avg_ratio = float(np.mean(ratios)) if ratios else float('inf')
    _LAST_EVAL.clear()
    if total > 0:
        ea = eng_correct / total; la = lak_correct / total; ca = combo_correct / total
        print(f"    Eval[per_query]: eng_acc={ea:.3f} lake_acc={la:.3f} combo_acc={ca:.3f} "
              f"avg_ratio={avg_ratio:.4f} ({len(ratios)} q, fallback={fallback_used})")
        if ratios:
            r = np.asarray(ratios, dtype=float)
            print(f"    Eval[per_query] extra: geo_ratio={_geo(r):.4f} "
                  f"median_ratio={float(np.median(r)):.4f} max_ratio={float(r.max()):.4f}")
            _LAST_EVAL.update(mean=avg_ratio, geo=_geo(r), median=float(np.median(r)),
                              combo_acc=ca)
        if oracle_ratios and prior_ratios:
            _LAST_EVAL.update(oracle_mean=float(np.mean(oracle_ratios)),
                              oracle_geo=_geo(oracle_ratios),
                              prior_mean=float(np.mean(prior_ratios)),
                              prior_geo=_geo(prior_ratios),
                              prior_combo_acc=prior_correct / total)
            print(f"    Eval[per_query] diag: oracle_gate mean={_LAST_EVAL['oracle_mean']:.4f} "
                  f"geo={_LAST_EVAL['oracle_geo']:.4f} | prior_route mean="
                  f"{_LAST_EVAL['prior_mean']:.4f} geo={_LAST_EVAL['prior_geo']:.4f} "
                  f"combo_acc={_LAST_EVAL['prior_combo_acc']:.3f}")
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
    parser.add_argument('--consistent-per-query-training', action='store_true',
                        help='Experimental mode: train gates on single-query workloads; '
                             'disabled by default to preserve paper routing')
    parser.add_argument('--per-query-train-repeats', type=int, default=16,
                        help='Number of shuffled repetitions of each training query in '
                             'consistent per-query mode')
    parser.add_argument('--log-ratio-target', action='store_true',
                        help='Regress log(latency / optimum) to reduce domination by '
                             'long-tail latency ratios; argmin inference is unchanged')
    parser.add_argument('--noise-filter-fraction', type=float, default=0.05,
                        help='Maximum robust outlier fraction removed per training query/combo '
                             '(clamped to [0, 0.20]; validation/test are untouched)')
    parser.add_argument('--router-aux-weight', type=float, default=0.0,
                        help='Weight for single-query auxiliary supervision on the existing gates')
    parser.add_argument('--router-margin-scale', type=float, default=0.25,
                        help='Relative best-vs-second-best latency margin treated as full-confidence')
    parser.add_argument('--router-class-balance', action='store_true',
                        help='Use inverse-sqrt training-label frequency weights for auxiliary routing')
    parser.add_argument('--router-stage1-aux-weight', type=float, default=0.0,
                        help='Weight for single-query gate supervision during end-to-end Stage 1; '
                             'this also trains the original tree encoder for routing')
    parser.add_argument('--router-regret-weight', type=float, default=0.0,
                        help='Weight for expected log-slowdown routing loss on the original gates')
    parser.add_argument('--train-workloads', type=int, default=1000,
                        help='Number of sampled multi-query training workloads')
    parser.add_argument('--canonical-query-aliases', action='store_true',
                        help='Merge collector-specific TPC-DS/TPC-H spellings of the same '
                             'logical query before splitting')
    parser.add_argument('--normalize-configs', action='store_true',
                        help='Z-normalize padded configuration features from training-set '
                             'statistics before fitting the unchanged ConfEncoder')
    parser.add_argument('--neutral-unseen-fallback', action='store_true',
                        help='Initialize the existing per-query fallback table to zero so '
                             'unseen queries without plans use a deterministic gate prior')
    parser.add_argument('--per-query-expert-weight', type=float, default=0.0,
                        help='Weight for Stage-3 regression using each record\'s single-query '
                             'embedding, matching per-query inference')
    parser.add_argument('--expert-rank-weight', type=float, default=0.0,
                        help='Weight for Stage-3 best-configuration ranking loss')
    parser.add_argument('--workload-expert-weight', type=float, default=1.0,
                        help='Weight on the original multi-query Stage-3 regression loss')
    parser.add_argument('--workload-gate-ce-weight', type=float, default=1.0,
                        help='Weight on the original multi-query gate CE when adding '
                             'single-query cost-sensitive routing supervision')
    parser.add_argument('--early-stop-patience', type=int, default=0,
                        help='Stop after this many epochs without validation improvement; '
                             '0 disables early stopping')
    parser.add_argument('--stage3-median-configs', action='store_true',
                        help='Collapse repeated training executions of an identical '
                             'query/combo/config to their median before Stage 3')
    parser.add_argument('--gate-prior-reg', type=str, default='uniform',
                        choices=['uniform', 'train'],
                        help='Target of the gate anti-collapse terms: uniform (original) or '
                             'the training-label prior')
    parser.add_argument('--gate-soft-label-temp', type=float, default=0.0,
                        help='>0: gate CE uses softmax(-log_slowdown/T) soft labels instead of '
                             'the hard argmin combo')
    parser.add_argument('--gate-lr', type=float, default=3e-4,
                        help='Learning rate of the Stage-2 gate optimizer')
    parser.add_argument('--gate-dropout', type=float, default=None,
                        help='Override dropout inside the two gates (default: shared 0.3)')
    parser.add_argument('--joint-gate-inference', action='store_true',
                        help='Pick argmax p_e*p_l among combos the query has data for, '
                             'instead of independent engine/lake argmax')
    parser.add_argument('--tree-feat-norm', type=str, default='raw',
                        choices=['raw', 'log', 'agnostic'],
                        help="Plan-node features before tree-conv: raw (original); log = "
                             "signed-log1p + train-set z-score; agnostic = operators aligned by "
                             "name, table slots summarized, then train-set z-score (cross-schema)")
    parser.add_argument('--export-gate-data', type=str, default=None,
                        help='After training, dump frozen embeddings + per-combo costs + '
                             'expert-chosen ratios for offline gate tuning (gate_tune.py)')
    parser.add_argument('--latency-cap-ratio', type=float, default=0.0,
                        help='Clip each record latency to this multiple of its query best '
                             '(0 = off; e.g. 10 bounds every ratio at 10x)')
    parser.add_argument('--latency-cap-scope', type=str, default=None,
                        help='Comma list of benchmark:sf the cap applies to, e.g. '
                             '"tpcds:10,tpcds:100,job:10,tpch:10" (default: all)')
    parser.add_argument('--conf-canonical', action='store_true',
                        help='Name-aligned config features (named knobs in fixed slots, '
                             'positional formats in a separate flagged block)')
    parser.add_argument('--expert-residual-prior', action='store_true',
                        help='Experts regress log-ratio minus the train-split (sf, combo, config) '
                             'prior; inference adds the prior back')
    parser.add_argument('--sf-embedding', action='store_true',
                        help='Add a learned per-scale-factor vector to query embeddings '
                             '(sf is known at deployment; plans are shared across sf)')
    parser.add_argument('--tree-readout', type=str, default='root', choices=['root', 'root_mean'],
                        help='Plan embedding = root node (original) or average of root and '
                             'mean over all nodes (prevents deep-plan collapse)')
    parser.add_argument('--stage2-train-encoder', action='store_true',
                        help='Stage 2 also updates the tree-conv encoder (default: frozen)')
    parser.add_argument('--holdout', action='store_true',
                        help='Cross-schema: test on --benchmark (optionally only --sf), '
                             'train/valid on all other benchmarks (all sfs)')
    parser.add_argument('--select-metric', type=str, default='mean',
                        choices=['mean', 'geo', 'oracle_geo'],
                        help='Validation ratio used for checkpoint selection')
    args = parser.parse_args()
    global CONF_CANONICAL, LATENCY_CAP_RATIO
    CONF_CANONICAL = args.conf_canonical
    LATENCY_CAP_RATIO = args.latency_cap_ratio
    global LATENCY_CAP_SCOPE
    if args.latency_cap_scope:
        LATENCY_CAP_SCOPE = {tuple(x.strip().split(':')) for x in args.latency_cap_scope.split(',')}

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
                ratio_cap=args.ratio_cap,
                consistent_per_query=args.consistent_per_query_training,
                per_query_train_repeats=args.per_query_train_repeats,
                log_ratio_target=args.log_ratio_target,
                noise_filter_fraction=args.noise_filter_fraction,
                router_aux_weight=args.router_aux_weight,
                router_margin_scale=args.router_margin_scale,
                router_class_balance=args.router_class_balance,
                router_stage1_aux_weight=args.router_stage1_aux_weight,
                router_regret_weight=args.router_regret_weight,
                num_train_workloads=args.train_workloads,
                canonical_query_aliases=args.canonical_query_aliases,
                normalize_configs=args.normalize_configs,
                neutral_unseen_fallback=args.neutral_unseen_fallback,
                per_query_expert_weight=args.per_query_expert_weight,
                expert_rank_weight=args.expert_rank_weight,
                workload_expert_weight=args.workload_expert_weight,
                workload_gate_ce_weight=args.workload_gate_ce_weight,
                early_stop_patience=args.early_stop_patience,
                stage3_median_configs=args.stage3_median_configs,
                gate_prior_reg=args.gate_prior_reg,
                gate_soft_label_temp=args.gate_soft_label_temp,
                gate_lr=args.gate_lr, gate_dropout=args.gate_dropout,
                joint_gate_inference=args.joint_gate_inference,
                select_metric=args.select_metric,
                holdout=args.holdout,
                stage2_train_encoder=args.stage2_train_encoder,
                tree_feat_norm=args.tree_feat_norm,
                tree_readout=args.tree_readout,
                export_gate_data=args.export_gate_data,
                sf_embedding=args.sf_embedding,
                expert_residual_prior=args.expert_residual_prior)


if __name__ == "__main__":
    main()
