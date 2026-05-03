#!/bin/bash
# Run paper-spec + AttentionPool training for sf=100 benchmarks sequentially.
set -e
cd "$(dirname "$0")"
mkdir -p logs

EPOCHS=30
S1=5
S2=10
S3=10
LDIV=0
LSPREAD=1.0
WD=1e-4

for BM in tpcds tpch ssb ssb_flat; do
    LOG="logs/train_${BM}_sf100_paper.log"
    echo "========================================"
    echo "$(date '+%H:%M:%S'): ${BM} sf=100"
    echo "========================================"
    PYTHONUNBUFFERED=1 python3 -u train_local.py \
        --benchmark ${BM} --sf 100 \
        --epochs ${EPOCHS} --eval-mode per_query \
        --stage1-subepochs ${S1} --stage2-subepochs ${S2} --stage3-subepochs ${S3} \
        --lambda-diversity ${LDIV} --lambda-emb-spread ${LSPREAD} \
        --tree-weight-decay ${WD} \
        2>&1 | stdbuf -oL tee "${LOG}" | grep -E "Epoch.*best_valid=|complete|Final test"
    echo ""
done

echo "========================================"
echo "ALL sf=100 paper runs COMPLETE"
echo "========================================"
for BM in tpcds tpch ssb ssb_flat; do
    LOG="logs/train_${BM}_sf100_paper.log"
    if [ -f "$LOG" ]; then
        FINAL=$(grep "Final test ratio" "$LOG" | tail -1)
        BEST=$(grep "Training complete" "$LOG" | tail -1)
        printf "%-12s | %s | %s\n" "$BM" "$BEST" "$FINAL"
    fi
done
