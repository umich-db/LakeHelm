#!/bin/bash
# Reproduce the cross-schema results: for each benchmark, train on the OTHER four
# benchmarks (all sf) and test on every sf of the held-out one.  5 benchmarks x
# SEEDS runs; each log ends with one "Holdout test <bm> sf<N>" line per scale factor.
#
#   GPUS="0 1 2 3" SEEDS="0 1" bash run_all.sh
set -e
cd "$(dirname "$0")"
mkdir -p logs

GPUS=(${GPUS:-0})
SEEDS=(${SEEDS:-0 1})
PY=${PY:-python3}

run_one() {
    local bm=$1 seed=$2 gpu=$3
    CUDA_VISIBLE_DEVICES=$gpu OMP_NUM_THREADS=4 $PY -u train_local.py --benchmark "$bm" --holdout \
        --epochs 30 --eval-mode per_query --early-stop-patience 10 --seed "$seed" \
        --canonical-query-aliases \
        --tree-feat-norm agnostic --tree-readout root_mean --sf-embedding \
        --expert-residual-prior --select-metric oracle_geo \
        --consistent-per-query-training --per-query-train-repeats 4 \
        --router-aux-weight 1.0 --router-class-balance --router-regret-weight 1.0 \
        --log-ratio-target --per-query-expert-weight 1.0 --expert-rank-weight 0.5 \
        --normalize-configs --stage3-median-configs --neutral-unseen-fallback \
        --stage3-subepochs 2 \
        > "logs/holdout_${bm}_s${seed}.log" 2>&1
    echo "$(date '+%H:%M:%S') done ${bm} seed=${seed}"
}

i=0
for seed in "${SEEDS[@]}"; do
    for bm in tpcds job tpch ssb ssb_flat; do
        run_one "$bm" "$seed" "${GPUS[$(( i % ${#GPUS[@]} ))]}" &
        i=$((i + 1))
    done
done
wait
grep -h "Holdout test" logs/holdout_*_s*.log
