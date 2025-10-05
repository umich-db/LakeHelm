from pathlib import Path
import csv
from typing import Dict, Tuple, List


def parse_conf(conf_str: str) -> List[float]:
	if not conf_str:
		return []
	return [float(x) for x in conf_str.split(';') if x != '']


def read_benchmark_csv(csv_path: Path) -> Dict[str, Dict[Tuple[int, int], List[Tuple[List[float], float]]]]:
	grouped: Dict[str, Dict[Tuple[int, int], List[Tuple[List[float], float]]]] = {}
	with open(csv_path, 'r', encoding='utf-8') as f:
		reader = csv.DictReader(f)
		for row in reader:
			qid = row['query_id']
			e_id = int(row['engine_id'])
			l_id = int(row['lake_id'])
			conf = parse_conf(row['config'])
			lat = float(row['latency'])
			grouped.setdefault(qid, {}).setdefault((e_id, l_id), []).append((conf, lat))
	return grouped


def load_grouped_from_csvs(csv_dir: Path, use_supply: bool = False) -> Dict[str, Dict]:
	"""Load multiple benchmarks grouped_data from CSV directory.
	If use_supply is True, prefer *_supply.csv when present.
	"""
	bench_names = ['job', 'ssb_flat', 'ssb', 'tpcds', 'tpch']
	result: Dict[str, Dict] = {}
	for name in bench_names:
		file_name = f"{name}_supply.csv" if use_supply else f"{name}.csv"
		p = csv_dir / file_name
		if not p.exists() and use_supply:
			# fallback to normal if supply missing
			p = csv_dir / f"{name}.csv"
		if p.exists():
			result[name] = read_benchmark_csv(p)
	return result
