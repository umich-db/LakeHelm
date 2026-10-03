# LKHelm — Learned Knob Tuning with Mixture of Experts

## 1. Overview

LKHelm trains a **TwoGateMoE (Mixture of Experts)** model that learns to select the best `(engine, datalake, configuration)` combination for any given database query workload. The system covers **9 execution combos** — `{Spark, Presto, Trino} × {Delta, Iceberg, Hudi}` — across 5 benchmarks at multiple scale factors.

**Core idea**: Given a set of SQL queries (a "workload"), the model predicts which engine/datalake combo and which configuration knob settings will minimize total execution latency.

---

## 2. Environment Setup

**Requirements**: Python 3.8+, PyTorch, NumPy.

```bash
pip install torch numpy
# CUDA 12.x example:
pip install torch --index-url https://download.pytorch.org/whl/cu124
```

---

## 3. How to Train

### 3.1 Cross-schema evaluation (main protocol)

The held-out benchmark (all of its scale factors) is the test set; the other four
benchmarks (all scale factors) are split 85/15 into train/valid. One model per
held-out benchmark reports every sf of that benchmark, so 5 models cover the 13
(benchmark, sf) tasks.

```bash
# one held-out benchmark
python3 train_local.py --benchmark tpch --holdout --seed 0 --epochs 30 --eval-mode per_query \
    --early-stop-patience 10 --canonical-query-aliases \
    --tree-feat-norm agnostic --tree-readout root_mean --sf-embedding \
    --expert-residual-prior --select-metric oracle_geo \
    --consistent-per-query-training --per-query-train-repeats 4 \
    --router-aux-weight 1.0 --router-class-balance --router-regret-weight 1.0 \
    --log-ratio-target --per-query-expert-weight 1.0 --expert-rank-weight 0.5 \
    --normalize-configs --stage3-median-configs --neutral-unseen-fallback
```

### 3.2 Reproduce all 13 tasks

```bash
GPUS="0 1 2 3" SEEDS="0 1" PY=python3 bash run_all.sh   # 5 benchmarks x 2 seeds
```

Logs go to `logs/holdout_<benchmark>_s<seed>.log`. Each ends with one line per scale
factor of the held-out benchmark:

```
Holdout test tpch sf10: ratio=... geo=... combo_acc=... oracle_geo=... prior_geo=...
```

`geo` is the end-to-end result with the learned gate; `prior_geo` routes every query to
the training-majority combo (config still chosen by the model); `oracle_geo` assumes a
perfect gate. Data: run `git lfs pull` first (latency CSVs are stored with Git LFS); the
plan-tree cache `.tree_cache.pt` is built automatically on the first run. No checkpoint
is needed — every run trains from scratch.

### 3.3 Common command-line arguments

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
| `--holdout` | off | Cross-schema: test = `--benchmark` (all sf, or only `--sf`); train/valid = the other benchmarks |
| `--canonical-query-aliases` | off | Merge spellings of the same query (`db3`/`q3`, `query10`/`tpcds_q_10`) before splitting |
| `--tree-feat-norm` | `raw` | `agnostic`: operators aligned by name, per-schema table slots summarized, train-set z-score |
| `--tree-readout` | `root` | `root_mean`: average root and mean-over-nodes (prevents deep-plan collapse) |
| `--sf-embedding` | off | Learned per-scale-factor vector added to query embeddings |
| `--expert-residual-prior` | off | Experts regress log-ratio minus a train-split (sf, combo, config) prior |
| `--select-metric` | `mean` | Checkpoint selection on valid: `mean`, `geo`, or `oracle_geo` (expert quality only) |
| `--export-gate-data` | none | Dump frozen embeddings / costs / expert predictions for `gate_tune.py`, `route_tune.py` |

### 3.4 What happens during training

```
Step 1: Load CSVs of all benchmarks (query ids prefixed with the benchmark)
Step 2: --holdout: held-out benchmark = test; other benchmarks → 85% train / 15% valid
Step 3: Build schema-agnostic plan-node features (stats from training plans only) → tree-conv embeddings
Step 4: Build the train-split (sf, combo, config) prior used by the residual experts
Step 5: Train TwoGateMoE for `epochs` outer epochs with three stages each
Step 6: Pick the checkpoint with the best valid oracle-gate geo ratio; report test per sf once
```

Without `--holdout` the original within-benchmark random 70/15/15 query split is used.
Nothing computed from test queries feeds training, feature statistics, priors or model selection.

---

## 4. Model Architecture

### 4.1 Query Embedding — TreeQueryEncoder

Converts SQL execution plans into 288-dimensional query embeddings:

1. **Input**: SQL execution plan tree
2. **Feature extraction**: Node type, referenced tables, column histograms → feature vector per node
3. **Tree convolution**: `BatchTreeConvCBAM` with 4 kernels (channel attention)
4. **Output**: `feat_dim × num_kernels = 72 × 4 = 288` per query
5. **LayerNorm**: per-query unit variance to prevent encoder collapse
6. **Fallback**: queries without plan files get learnable `nn.Embedding` vectors

### 4.2 Workload Aggregation — AttentionPool

Per-query embeddings → workload embedding via multi-head attention pool with concat(mean, max) residual:

```
output = concat(head_1, head_2, head_3, head_4, mean, max)   # → 6 × 288 = 1728-dim
```

Each attention head learns a different scoring function over the per-query embeddings; this prevents the gate from receiving near-identical inputs across different workloads.

### 4.3 TwoGateMoE

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

### 4.4 Loss Function (paper §loss_function)

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

### 4.5 Three-stage training (paper §moe-train)

Each outer epoch runs three sub-stages back to back:

1. **End-to-end (Stage 1)** — All params trained with `L_MSE + L_CE + λ_div × L_div` via Gumbel-soft routing.
2. **Gate-focused (Stage 2)** — Only gates trained on `L_CE + L_div` (tree-conv frozen).
3. **Expert-focused (Stage 3)** — Each expert trained on every (config, ratio) record routed via the actual (engine, lake) ID (tree-conv frozen).

### 4.6 Optimizer

- **Adam** lr=3e-4
- **Weight decay**: 1e-3 on tree-conv params (anti-collapse), 1e-5 elsewhere
- **CosineAnnealing** scheduler, eta_min=1e-5
- **Gradient clipping** at 1.0

---

## 5. Evaluation

### 5.1 Per-Query Evaluation (default)

For each test query:
1. Compute query embedding (single query → AttentionPool)
2. Run gates → pick `(eng*, lake*)` via argmax
3. Score every conf in `(eng*, lake*)` for that query via `forward_for_eng_lak`
4. Pick argmin pred → `chosen_actual_lat`
5. `ratio(q) = chosen_actual_lat / min_actual_latency` (across all combos)

Average ratio across all test queries. **Lower is better** (1.0 = always optimal).

### 5.2 Train / Valid / Test split

Random query-level split, default 70 / 15 / 15. Best checkpoint is selected on the **validation** set; the **test** set is evaluated only once at the end with the best-validation checkpoint.

---

## 6. Implementation Notes

- **Tree cache**: First run processes plan files into tree tensors and saves `.tree_cache.pt`. Subsequent runs load instantly.
- **Latency floor repair**: Exactly-1500ms records (timeout artifacts) are replaced with samples drawn from that query+combo's latency distribution.
- **Query normalization**: `tpch_0_q1` → `sf10_q1` (strips datalake-specific prefix, adds sf prefix for cross-datalake consistency).
- **Config encoding**: Configs are parsed into numeric vectors, padded to `max_dim=16`, then encoded by a 3-layer `ConfEncoder` MLP into 64-dim representation.

---

## 7. Add-on: Calcite plans for your own queries

`lakehelm_calcite.py` turns raw SQL into optimized logical plans with Apache Calcite
(`calcite_planner/`, a small Maven project built automatically on first use; needs Java 11+
and Maven) and into plan-tree features with the same 30-dim layout as
`--tree-feat-norm agnostic`. It is an add-on: `run_all.sh` does not use it.

Inputs: a DDL file (`CREATE TABLE ...`), a JSON of table row counts (used by Calcite's
cost-based join ordering and the cardinality features), and SQL files — see
`examples/calcite/`.

```bash
# print the optimized plans
python3 lakehelm_calcite.py plan --ddl examples/calcite/schema.sql --rows examples/calcite/rows.json \
    --sql examples/calcite/queries/*.sql --text

# measured workload (query, config, latency) -> plans + features + latency CSVs
python3 lakehelm_calcite.py prepare --ddl examples/calcite/schema.sql --rows examples/calcite/rows.json \
    --workload examples/calcite/workload.csv --benchmark mybench --out prepared/
```

Workload CSV columns: `query_name, sql_file, engine, datalake, conf, latency, sf`
(`engine` ∈ spark/presto/trino, `datalake` ∈ delta/iceberg/hudi, latency in ms).
`prepare` writes `prepared/<benchmark>/sf<N>/<benchmark>_<engine>_<datalake>_sf<N>.csv`
(the `data/output` format), `prepared/plans.json` and `prepared/trees.pt`
(`{query: (node_feats, children, root)}` plus `feature_names`).
