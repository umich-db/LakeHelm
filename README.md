LakeHelm
========

This package provides a CSV-based pipeline to run the ratio training flow without re-parsing raw logs.

Structure:
- lakehelm/csv_loader.py: Load grouped data structures directly from CSVs produced by ratio_csv_pipeline.
- scripts/train_from_csv.py: Orchestrates workload generation and splits using the existing utilities from train/all_ratio.py, but sources data from CSVs.
- data/, experiments/: Optional folders for outputs and experiment configs.

Prerequisites:
- CSVs generated at `/home/jovyan/train/ratio_csv_pipeline/csv/` (e.g., job.csv, tpcds.csv, and *_supply.csv variants).

Usage:
- Run training orchestration from CSVs:
  python /home/jovyan/LakeHelm/scripts/train_from_csv.py

Notes:
- The orchestrator imports workload utilities from `train/all_ratio.py` to keep behavior aligned.
- Toggle `use_supply = True/False` in `train_from_csv.py` to pick supply vs normal CSVs.


