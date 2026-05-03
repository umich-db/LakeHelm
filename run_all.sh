#!/bin/bash
# Train one model per (benchmark, sf) pair with paper-spec MoE training.
set -e

cd "$(dirname "$0")"
mkdir -p logs

# sf=100
for BM in tpcds tpch ssb ssb_flat; do
    echo "========================================"
    echo "$(date '+%H:%M:%S'): ${BM} sf=100"
    echo "========================================"
    python3 train_local.py --benchmark ${BM} --sf 100 \
        --epochs 30 --eval-mode per_query \
        2>&1 | tee logs/train_${BM}_sf100.log | grep -E "Epoch.*best_valid=|complete|Final test"
    echo ""
done

# sf=10
for BM in tpcds tpch ssb ssb_flat job; do
    echo "========================================"
    echo "$(date '+%H:%M:%S'): ${BM} sf=10"
    echo "========================================"
    python3 train_local.py --benchmark ${BM} --sf 10 \
        --epochs 30 --eval-mode per_query \
        2>&1 | tee logs/train_${BM}_sf10.log | grep -E "Epoch.*best_valid=|complete|Final test"
    echo ""
done

# sf=1
for BM in tpcds tpch ssb ssb_flat; do
    echo "========================================"
    echo "$(date '+%H:%M:%S'): ${BM} sf=1"
    echo "========================================"
    python3 train_local.py --benchmark ${BM} --sf 1 \
        --epochs 30 --eval-mode per_query \
        2>&1 | tee logs/train_${BM}_sf1.log | grep -E "Epoch.*best_valid=|complete|Final test"
    echo ""
done

echo ""
echo "========================================"
echo "ALL COMPLETE - Final Test Ratios"
echo "========================================"
printf "%-20s %12s %12s\n" "Benchmark" "BestValid" "FinalTest"
printf "%-20s %12s %12s\n" "---" "---" "---"
for SF in 100 10 1; do
    for BM in tpcds tpch ssb ssb_flat job; do
        F="logs/train_${BM}_sf${SF}.log"
        if [ -f "$F" ]; then
            BV=$(grep "Training complete" "$F" | grep -oP 'ratio: \K[0-9.]+' | head -1)
            FT=$(grep "Final test ratio" "$F" | grep -oP 'ep[0-9]+\): \K[0-9.]+' | head -1)
            if [ -n "$BV" ]; then
                printf "%-20s %12s %12s\n" "${BM}_sf${SF}" "$BV" "${FT:-N/A}"
            fi
        fi
    done
done
