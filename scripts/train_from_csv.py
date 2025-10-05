#!/usr/bin/env python3
import os
import sys
from pathlib import Path
from typing import Dict, Tuple, List, Any

# Ensure we can import the existing training utilities
sys.path.insert(0, "/home/jovyan/train")

from all_ratio import (
	generate_balanced_workloads,
	generate_workloads_from_queries_test,
	split_workloads_randomly,
	compute_valid_combo_ratio,
)

from lakehelm.csv_loader import load_grouped_from_csvs


COMBOS = [
	(0, 0), (0, 1), (0, 2),
	(1, 0), (1, 1), (1, 2),
	(2, 0), (2, 1), (2, 2),
]


def workload_list_to_dicts(workload_list: List[Dict[str, Any]]):
	valid_workloads = {}
	workload_grouped_data = {}
	for idx, wl in enumerate(workload_list):
		wid = f"w_{idx}"
		valid_workloads[wid] = wl['query_ids']
		workload_grouped_data[wid] = wl['aggregated_data']
	# Optional: print distribution
	_ = compute_valid_combo_ratio(workload_grouped_data, COMBOS)
	return valid_workloads, workload_grouped_data


def main():
	base_csv = Path('/home/jovyan/train/ratio_csv_pipeline/csv')
	use_supply = True
	all_grouped = load_grouped_from_csvs(base_csv, use_supply=use_supply)
	print("Loaded CSV benchmarks:", list(all_grouped.keys()))

	# Choose a benchmark to run (example: tpcds)
	benchmark = 'tpcds'
	grouped_data = all_grouped.get(benchmark, {})
	if not grouped_data:
		print(f"No data for benchmark {benchmark}")
		return

	# Prepare query lists
	all_qnames = list(grouped_data.keys())
	# Simple split: last 20% as test, first 80% as train
	cut = max(1, int(len(all_qnames) * 0.8))
	train_qnames = all_qnames[:cut]
	test_qnames = all_qnames[cut:]

	print(f"Queries total={len(all_qnames)} train={len(train_qnames)} test={len(test_qnames)}")

	# Generate workloads
	train_workloads_list = generate_balanced_workloads(
		query_ids=train_qnames,
		workload_num=min(1000, max(50, len(train_qnames))),
		min_queries=5,
		max_queries=min(20, len(train_qnames)),
		query_grouped_data=grouped_data,
		combos=COMBOS,
		min_combo_coverage=3,
		balance_ratio=0.8,
		seed=42,
	)

	test_workloads_list = generate_workloads_from_queries_test(
		query_ids=test_qnames,
		workload_num=min(200, max(20, len(test_qnames))),
		min_queries=3,
		max_queries=min(20, len(test_qnames)) if len(test_qnames) > 0 else 3,
		query_grouped_data=grouped_data,
		combos=COMBOS,
		seed=7,
	)

	# Split train into train/valid
	train_final, valid_final, _ = split_workloads_randomly(
		train_workloads_list, train_ratio=0.85, valid_ratio=0.15, test_ratio=0.0, seed=42
	)

	train_workloads, train_grouped = workload_list_to_dicts(train_final)
	valid_workloads, valid_grouped = workload_list_to_dicts(valid_final)
	test_workloads, test_grouped = workload_list_to_dicts(test_workloads_list)

	print(f"Workloads -> train={len(train_workloads)} valid={len(valid_workloads)} test={len(test_workloads)}")
	# At this point, you can plug in your model training logic that expects the same structures
	# as in all_ratio.py's training pipeline, but sourced from CSVs.


if __name__ == '__main__':
	main()
