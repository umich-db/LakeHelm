# LKHelm — Learned Knob Tuning with Mixture of Experts

## 1. Overview

LKHelm trains a **TwoGateMoE (Mixture of Experts)** model that learns to select the best `(engine, datalake, configuration)` combination for any given database query workload. The system covers **9 execution combos** — `{Spark, Presto, Trino} × {Delta, Iceberg, Hudi}` — across 5 benchmarks at multiple scale factors.

**Core idea**: Given a set of SQL queries (a "workload"), the model predicts which engine/datalake combo and which configuration knob settings will minimize total execution latency.

---

## 2. Directory Structure

```
LakeHelm/
│
├── train_local.py              # Main training script (paper-spec 3-stage MoE)
├── tree_embedding.py           # Tree convolution query embedding module
├── run_all.sh                  # One command to train across (benchmark, sf) pairs
├── run_sf100_paper.sh          # Paper-spec training for sf=100 benchmarks
├── plot_loss.py                # Loss / ratio visualization for training logs
├── README.md                   # This file
│
├── bao_server/                 # TreeConvolution dependency (from Bao)
│
├── orig_train/                 # Query execution plans & column histograms (shipped)
├── data/                       # Training CSVs (NOT shipped via git)
├── test_workloads/             # Pre-generated test workloads (NOT shipped via git)
├── models/                     # Trained model checkpoints (auto-created)
└── logs/                       # Training log output (auto-created)
```

> **Note**: `data/`, `test_workloads/`, `gpt_results/`, and `best_fixed_results/` are excluded from git. Users must supply their own training CSVs and test workloads. Query plan files in `orig_train/` ARE shipped.

---

## 3. Environment Setup

**Requirements**: Python 3.8+, PyTorch, NumPy.

```bash
pip install torch numpy
# CUDA 12.x example:
pip install torch --index-url https://download.pytorch.org/whl/cu124
```

---

## 4. How to Train

### 4.1 Single benchmark / scale factor

```bash
python3 train_local.py --benchmark tpcds --sf 100 --epochs 30 --eval-mode per_query \
                        --stage1-subepochs 5 --stage2-subepochs 10 --stage3-subepochs 10
```

### 4.2 All sf=100 benchmarks (paper-spec)

```bash
bash run_sf100_paper.sh
```

### 4.3 Common command-line arguments

| Argument | Default | Description |
|---|---|---|
| `--benchmark` | `tpcds` | Which benchmark: `tpcds`, `tpch`, `ssb`, `ssb_flat`, `job`. If unset, all benchmarks are pooled. |
| `--sf` | None | Scale-factor filter (e.g. `1`, `10`, `100`). Only queries at that sf are used. |
| `--epochs` | `40` | Outer training epochs |
| `--seed` | `42` | Random seed for reproducibility |
| `--stage1-subepochs` | `1` | Stage-1 (end-to-end) sub-epochs per outer epoch |
| `--stage2-subepochs` | `2` | Stage-2 (gate-focused) sub-epochs per outer epoch |
| `--stage3-subepochs` | `2` | Stage-3 (expert-focused) sub-epochs per outer epoch |
| `--lambda-div` | `0.1` | Weight on diversity regularization (paper L_div) |
| `--lambda-diversity` | `5.0` | Weight on entropy-max anti-collapse term |
| `--lambda-emb-spread` | `2.0` | Weight on workload-embedding spread regularizer |
| `--tree-weight-decay` | `1e-3` | Weight decay specifically for tree-conv encoder |
| `--gumbel-tau` | `1.0` | Temperature for Gumbel-softmax routing |
| `--target-ratio` | None | If set, save the model whose validation ratio is closest to this target |

### 4.4 What happens during training

```
Step 1: Load CSV data from the chosen benchmark(s)
Step 2: Random query-level split: 70% train / 15% valid / 15% test
Step 3: Build tree-conv embeddings from SQL execution plans
Step 4: Generate workloads (random subsets of queries)
Step 5: Train TwoGateMoE for `epochs` outer epochs with three stages each
Step 6: Pick the checkpoint with the best validation ratio; report its test ratio
```

Random query-level split is the default — there is **no leave-one-out** evaluation in this codebase.

---

## 5. Data Format

Each CSV file in `data/output/` follows:

```csv
datalake,engine,query name,conf,latency,benchmark,sf
iceberg,trino,query15,memory.heap-headroom-per-node=16; ...,3479.62,tpcds,1
delta,spark,query15,1.0;0;4;4;0;0;300|1;1;...,12543.80,tpcds,1
```

| Column | Description | Example |
|---|---|---|
| `datalake` | Data lake system | `delta`, `iceberg`, `hudi` |
| `engine` | Query engine | `spark`, `presto`, `trino` |
| `query name` | Query identifier | `query15`, `q1`, `job_10a` |
| `conf` | Configuration knob string | `key=val;key=val` or `val;val\|val;val` |
| `latency` | Execution time in ms | `3479.62` |
| `benchmark` | Benchmark name | `tpcds`, `tpch`, `ssb`, `ssb-flat`, `job` |
| `sf` | Scale factor | `1`, `10`, `100` |

> Records with `latency < 1500ms` are filtered out during training (timeout artifacts).

---

## 6. Model Architecture

### 6.1 Query Embedding — TreeQueryEncoder

Converts SQL execution plans into 288-dimensional query embeddings:

1. **Input**: SQL execution plan tree
2. **Feature extraction**: Node type, referenced tables, column histograms → feature vector per node
3. **Tree convolution**: `BatchTreeConvCBAM` with 4 kernels (channel attention)
4. **Output**: `feat_dim × num_kernels = 72 × 4 = 288` per query
5. **LayerNorm**: per-query unit variance to prevent encoder collapse
6. **Fallback**: queries without plan files get learnable `nn.Embedding` vectors

### 6.2 Workload Aggregation — AttentionPool

Per-query embeddings → workload embedding via multi-head attention pool with concat(mean, max) residual:

```
output = concat(head_1, head_2, head_3, head_4, mean, max)   # → 6 × 288 = 1728-dim
```

Each attention head learns a different scoring function over the per-query embeddings; this prevents the gate from receiving near-identical inputs across different workloads.

### 6.3 TwoGateMoE

```
                    ┌────────────────────┐
                    │ Workload Embedding  │  (1728-dim, AttentionPool output)
                    └─────────┬──────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼                                ▼
     ┌─────────────────┐              ┌─────────────────┐
     │   Engine Gate    │              │    Lake Gate     │
     │  MLP [128, 256]  │              │  MLP [128, 256]  │
     │  → 3 classes     │              │  → 3 classes     │
     └───────┬─────────┘              └───────┬─────────┘
             │   Gumbel-softmax              │
             ▼                                ▼
     ┌─────────────────┐              ┌─────────────────┐
     │  3 Engine Experts│              │  3 Lake Experts  │
     │  MLP each:       │              │  MLP each:       │
     │  → 128-dim       │              │  → 128-dim       │
     └───────┬─────────┘              └───────┬─────────┘
             │                                 │
             └──────────┬──────────────────────┘
                        ▼
              ┌───────────────────┐
              │     Concat        │  (256-dim = 128 + 128)
              │  + Config Encoder │  (64-dim ConfEncoder)
              └─────────┬────────┘
                        ▼
              ┌───────────────────┐
              │    Post-MLP       │
              │  → 1 (predicted   │
              │     ratio)        │
              └───────────────────┘
```

### 6.4 Loss Function (paper §loss_function)

```
L_total = L_MSE  +  L_CE  +  λ_div × L_div
```

| Component | Definition | Purpose |
|---|---|---|
| **L_MSE** | `(r̂ - r)² × p_gumbel_eng[c*] × p_gumbel_lake[f*]` | Predict ratio `r = lat / optimal_lat` for the (combo, conf), weighted by Gumbel probabilities of correct gates |
| **L_CE** | `-log p_eng[c*] - log p_lake[f*]` | Cross-entropy on gate predictions vs ground-truth best subsystem |
| **L_div** | `Σ (p̄_eng - 1/3)² + Σ (p̄_lake - 1/3)²` | Diversity regularizer — keep batch-mean gate probabilities near uniform |

Additional anti-collapse regularizers (configurable):
- `lambda_diversity` × entropy-max term (push batch-mean prob away from 1-hot)
- `lambda_emb_spread` × variance-of-workload-embeddings + InfoNCE-style cosine penalty

### 6.5 Three-stage training (paper §moe-train)

Each outer epoch runs three sub-stages back to back:

1. **End-to-end (Stage 1)** — All params trained with `L_MSE + L_CE + λ_div × L_div` via Gumbel-soft routing.
2. **Gate-focused (Stage 2)** — Only gates trained on `L_CE + L_div` (tree-conv frozen).
3. **Expert-focused (Stage 3)** — Each expert trained on every (config, ratio) record routed via the actual (engine, lake) ID (tree-conv frozen).

### 6.6 Optimizer

- **Adam** lr=3e-4
- **Weight decay**: 1e-3 on tree-conv params (anti-collapse), 1e-5 elsewhere
- **CosineAnnealing** scheduler, eta_min=1e-5
- **Gradient clipping** at 1.0

---

## 7. Evaluation

### 7.1 Per-Query Evaluation (default)

For each test query:
1. Compute query embedding (single query → AttentionPool)
2. Run gates → pick `(eng*, lake*)` via argmax
3. Score every conf in `(eng*, lake*)` for that query via `forward_for_eng_lak`
4. Pick argmin pred → `chosen_actual_lat`
5. `ratio(q) = chosen_actual_lat / min_actual_latency` (across all combos)

Average ratio across all test queries. **Lower is better** (1.0 = always optimal).

### 7.2 Train / Valid / Test split

Random query-level split, default 70 / 15 / 15. Best checkpoint is selected on the **validation** set; the **test** set is evaluated only once at the end with the best-validation checkpoint.

---

## 8. Model Checkpoints

Saved to `models/best_model_{benchmark}_sf{N}.pth`. Each `.pth` file contains:

```python
{
    'query_encoder': ...,    # TreeQueryEncoder state_dict
    'moe_model': ...,        # TwoGateMoE state_dict
    'q2idx': {...},
    'max_dim': 16,
    'feat_dim': 72,
    'num_kernels': 4,
    'idx_to_tree_key': {...},
}
```

---

## 9. Implementation Notes

- **Tree cache**: First run processes plan files into tree tensors and saves `.tree_cache.pt`. Subsequent runs load instantly.
- **Latency floor repair**: Exactly-1500ms records (timeout artifacts) are replaced with samples drawn from that query+combo's latency distribution.
- **Query normalization**: `tpch_0_q1` → `sf10_q1` (strips datalake-specific prefix, adds sf prefix for cross-datalake consistency).
- **Config encoding**: Configs are parsed into numeric vectors, padded to `max_dim=16`, then encoded by a 3-layer `ConfEncoder` MLP into 64-dim representation.
