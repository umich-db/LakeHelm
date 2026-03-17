#!/bin/bash
# Train all 13 (benchmark, sf) models with tree embeddings, per_query eval
set -e

echo "Starting all benchmarks with per_query eval..."
echo ""

declare -A TARGETS
TARGETS[tpcds_sf1]=1.23; TARGETS[tpch_sf1]=1.10; TARGETS[ssb_sf1]=1.13; TARGETS[ssb_flat_sf1]=1.29
TARGETS[tpcds_sf10]=1.88; TARGETS[tpch_sf10]=1.68; TARGETS[ssb_sf10]=1.46; TARGETS[ssb_flat_sf10]=1.53; TARGETS[job_sf10]=1.51
TARGETS[tpcds_sf100]=1.79; TARGETS[tpch_sf100]=1.42; TARGETS[ssb_sf100]=1.62; TARGETS[ssb_flat_sf100]=1.21

cd "$(dirname "$0")"

# sf=100
for BM in tpcds tpch ssb ssb_flat; do
    KEY="${BM}_sf100"
    T=${TARGETS[$KEY]}
    echo "========================================"
    echo "$(date '+%H:%M:%S'): ${BM} sf=100 (target=$T)"
    echo "========================================"
    python3 train_local.py --benchmark ${BM} --sf 100 \
        --epochs 30 --eval-mode per_query --target-ratio $T \
        2>&1 | tee logs/train_${BM}_sf100.log | grep -E "Epoch.*best=|complete"
    echo ""
done

# sf=10
for BM in tpcds tpch ssb ssb_flat job; do
    KEY="${BM}_sf10"
    T=${TARGETS[$KEY]}
    echo "========================================"
    echo "$(date '+%H:%M:%S'): ${BM} sf=10 (target=$T)"
    echo "========================================"
    python3 train_local.py --benchmark ${BM} --sf 10 \
        --epochs 30 --eval-mode per_query --target-ratio $T \
        2>&1 | tee logs/train_${BM}_sf10.log | grep -E "Epoch.*best=|complete"
    echo ""
done

# sf=1
for BM in tpcds tpch ssb ssb_flat; do
    KEY="${BM}_sf1"
    T=${TARGETS[$KEY]}
    echo "========================================"
    echo "$(date '+%H:%M:%S'): ${BM} sf=1 (target=$T)"
    echo "========================================"
    python3 train_local.py --benchmark ${BM} --sf 1 \
        --epochs 30 --eval-mode per_query --target-ratio $T \
        2>&1 | tee logs/train_${BM}_sf1.log | grep -E "Epoch.*best=|complete"
    echo ""
done

echo ""
echo "========================================"
echo "ALL COMPLETE - Results Summary"
echo "========================================"
printf "%-20s %8s %8s %8s\n" "Benchmark" "Result" "Target" "Status"
printf "%-20s %8s %8s %8s\n" "---" "---" "---" "---"

for SF in 100 10 1; do
    for BM in tpcds tpch ssb ssb_flat job; do
        KEY="${BM}_sf${SF}"
        F="logs/train_${BM}_sf${SF}.log"
        if [ -f "$F" ]; then
            R=$(grep "Training complete" "$F" | grep -oP 'ratio: \K[0-9.]+' | head -1)
            T=${TARGETS[$KEY]:-"N/A"}
            if [ -n "$R" ]; then
                STATUS="OK"
                if (( $(echo "$R < 1.1" | bc -l) )); then STATUS="SUSPECT"; fi
                if (( $(echo "$R > 2.2" | bc -l) )); then STATUS="HIGH"; fi
                printf "%-20s %8s %8s %8s\n" "$KEY" "$R" "$T" "$STATUS"
            fi
        fi
    done
done
