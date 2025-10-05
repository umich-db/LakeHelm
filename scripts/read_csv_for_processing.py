#!/usr/bin/env python3
"""
Read per-benchmark CSVs and reconstruct the grouped data structure
used by train/ratio.py (query_id -> (engine_id,lake_id) -> [(conf_tensor, latency), ...]).
"""

import csv
from pathlib import Path
from typing import Dict, Tuple, List
import torch


def parse_conf(conf_str: str) -> List[float]:
    if not conf_str:
        return []
    return [float(x) for x in conf_str.split(';') if x != '']


def read_benchmark_csv(csv_path: Path) -> Dict[str, Dict[Tuple[int, int], List[Tuple[torch.Tensor, float]]]]:
    grouped: Dict[str, Dict[Tuple[int, int], List[Tuple[torch.Tensor, float]]]] = {}
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            qid = row['query_id']
            e_id = int(row['engine_id'])
            l_id = int(row['lake_id'])
            conf = parse_conf(row['config'])
            lat = float(row['latency'])
            grouped.setdefault(qid, {}).setdefault((e_id, l_id), []).append(
                (torch.tensor(conf, dtype=torch.float), lat)
            )
    return grouped


def read_all_benchmarks(csv_dir: Path) -> Dict[str, Dict]:
    results: Dict[str, Dict] = {}
    for name in [
        'job_supply', 'ssb_flat_supply', 'ssb_supply', 'tpcds_supply', 'tpch_supply']:
        p = csv_dir / f"{name}.csv"
        if p.exists():
            results[name] = read_benchmark_csv(p)
    return results


def main():
    base = Path('/home/jovyan/train/ratio_csv_pipeline/csv')
    data = read_all_benchmarks(base)
    print("Loaded benchmarks:", list(data.keys()))
    # Example: print counts per benchmark
    for name, grouped in data.items():
        n_q = len(grouped)
        n_rec = sum(len(v) for q,v in grouped.items())
        print(f"{name}: {n_q} queries, {n_rec} combos with records")


if __name__ == '__main__':
    main()


