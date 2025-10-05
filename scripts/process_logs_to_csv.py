#!/usr/bin/env python3
"""
Parse logs/raw data for five benchmarks and write unified CSVs per split.

Benchmarks: job_supply, ssb_flat_supply, ssb_supply, tpcds_supply, tpch_supply
This script extracts the logic from train/ratio.py for CSV saving.
"""

import os
import sys
from pathlib import Path
from typing import Dict, Tuple, List

# Torch is not required for CSV generation. Keep data as Python lists.

# Ensure package path is available when running as a script
sys.path.insert(0, "/home/jovyan/train")

# Import processors copied to processors/
from ratio_csv_pipeline.processors.spark_feature_embedding import (
    process_spark_experiment,
    process_general_plans,
    columns_num_job,
    query_data,
    mapping_table_job,
    mapping_table_tpcds,
    columns_num_tpcds,
    mapping_table_ssb,
    columns_num_ssb,
    mapping_table_tpch,
    columns_num_tpch,
    columns_num_job_short,
    columns_num_tpcds_short,
    columns_num_ssb_short,
    columns_num_tpch_short,
    parse_files,
    extract_all_table_names,
)
from ratio_csv_pipeline.processors.presto_feature_embedding import process_presto_experiment
from ratio_csv_pipeline.processors.trino_feature_embedding import process_trino_experiment


def zero_pad_conf(conf: List[float], max_dim: int) -> List[float]:
    if len(conf) < max_dim:
        return conf + [0.0] * (max_dim - len(conf))
    return conf[:max_dim]


def build_9combo_data_by_query(
    lat_spark_del, conf_spark_del, lat_spark_ice, conf_spark_ice,
    lat_spark_hudi, conf_spark_hudi, lat_presto_del, conf_presto_del,
    lat_presto_ice, conf_presto_ice, lat_presto_hudi, conf_presto_hudi,
    lat_trino_del, conf_trino_del, lat_trino_ice, conf_trino_ice,
    lat_trino_hudi, conf_trino_hudi,
) -> Tuple[Dict, int]:
    combos = [
        (0, 0), (0, 1), (0, 2),
        (1, 0), (1, 1), (1, 2),
        (2, 0), (2, 1), (2, 2),
    ]

    def unify_conf_dimensions(*all_conf_lists) -> int:
        max_dim_local = 0
        for conf_list in all_conf_lists:
            for c in conf_list:
                max_dim_local = max(max_dim_local, len(c))
        return max_dim_local

    max_dim = unify_conf_dimensions(
        conf_spark_del, conf_spark_ice, conf_spark_hudi,
        conf_presto_del, conf_presto_ice, conf_presto_hudi,
        conf_trino_del, conf_trino_ice, conf_trino_hudi,
    )

    grouped_data = {}
    combo_sources = [
        (lat_spark_del, conf_spark_del, 0, 0),
        (lat_spark_ice, conf_spark_ice, 0, 1),
        (lat_spark_hudi, conf_spark_hudi, 0, 2),
        (lat_presto_del, conf_presto_del, 1, 0),
        (lat_presto_ice, conf_presto_ice, 1, 1),
        (lat_presto_hudi, conf_presto_hudi, 1, 2),
        (lat_trino_del, conf_trino_del, 2, 0),
        (lat_trino_ice, conf_trino_ice, 2, 1),
        (lat_trino_hudi, conf_trino_hudi, 2, 2),
    ]

    for (lat_dict, conf_list, e_id, l_id) in combo_sources:
        for qname, lat_arr in lat_dict.items():
            if qname not in grouped_data:
                grouped_data[qname] = {}
            if (e_id, l_id) not in grouped_data[qname]:
                grouped_data[qname][(e_id, l_id)] = []

            for i, lat_val in enumerate(lat_arr):
                if i >= len(conf_list):
                    break
                conf_pad = zero_pad_conf(conf_list[i], max_dim)
                final_conf = conf_pad + [float(e_id), float(l_id)]
                if lat_val > 100:
                    # store plain list for config to avoid torch dependency
                    grouped_data[qname][(e_id, l_id)].append((final_conf, float(lat_val)))

    return grouped_data, max_dim


def fix_qnames(lat_dict, prefix):
    tmp = {}
    for key, val in lat_dict.items():
        if prefix == "tpcds_" or prefix == "tpch_":
            tmp[key.replace('query', 'q').replace('.sql', '')] = val
        else:
            tmp[key] = val
    return tmp


def fix_group_data(grouped_data_fix, bench_sfix):
    tmp_dict = {}
    for key, value in grouped_data_fix.items():
        if not key.startswith(bench_sfix):
            tmp_dict[bench_sfix + key.replace('query', 'q')] = value
        else:
            tmp_dict[key] = value
    return tmp_dict


def main():
    # Paths identical to ratio.py logic
    presto_base_tpcds = "/home/jovyan/log/presto/results/lake"
    presto_base_tpch = "/home/jovyan/log/presto/results/lake/ssb-flat"
    presto_base_job = "/home/jovyan/log/presto/results/lake"
    presto_base_ssb = "/home/jovyan/log/presto/results/lake"
    presto_base_ssb_flat = "/home/jovyan/log/presto/results/lake/ssb-flat"

    trino_base_tpcds = "/home/jovyan/log/trino/results/lake"
    trino_base_tpch = "/home/jovyan/log/trino/results/lake/ssb-flat"
    trino_base_ssb = "/home/jovyan/log/trino/results/lake/ssb-flat"

    spark_log_iceberg_tpcds = '/home/jovyan/log/spark/tpcds/tpcds_ice.log'
    spark_log_delta_tpcds = '/home/jovyan/log/spark/tpcds/tpcds_del.log'
    spark_log_hudi_tpcds = '/home/jovyan/log/spark/tpcds/tpcds_hudi.log'

    spark_log_iceberg_tpch = '/home/jovyan/log/spark/tpch/tpch_lake_ice.log'
    spark_log_delta_tpch = '/home/jovyan/log/spark/tpch/tpch_lake_del.log'
    spark_log_hudi_tpch = '/home/jovyan/log/spark/tpch/tpch_lake_hudi.log'

    spark_log_iceberg_job = '/home/jovyan/log/spark/job/job_ice_lake.log'
    spark_log_delta_job = '/home/jovyan/log/spark/job/delta_job_lake.log'
    spark_log_hudi_job = '/home/jovyan/log/spark/job/job_hudi.log'

    spark_log_iceberg_ssb = '/home/jovyan/log/spark/ssb/ssb_ice.log'
    spark_log_delta_ssb = '/home/jovyan/log/spark/ssb/ssb_del.log'
    spark_log_hudi_ssb = '/home/jovyan/log/spark/ssb/ssb_hudi.log'

    spark_log_iceberg_ssb_flat = '/home/jovyan/log/spark/ssb-flat/ssb_flat_ice.log'
    spark_log_delta_ssb_flat = '/home/jovyan/log/spark/ssb-flat/ssb_flat_del.log'
    spark_log_hudi_ssb_flat = '/home/jovyan/log/spark/ssb-flat/ssb_flat_hudi.log'

    presto_log_iceberg_tpcds = '/home/jovyan/log/presto/tpcds/lake_tpcds_c.log'
    presto_log_delta_tpcds = '/home/jovyan/log/presto/tpcds/tpcds_delta_fix.log'
    presto_log_hudi_tpcds = '/home/jovyan/log/presto/tpcds/tpcds_hudi_fix.log'

    presto_log_iceberg_tpch = '/home/jovyan/log/presto/tpch/lake_tpch_c.log'
    presto_log_delta_tpch = '/home/jovyan/log/presto/tpch/lake_tpch.log'
    presto_log_hudi_tpch = '/home/jovyan/log/presto/tpch/tpch_hudi.log'

    presto_log_iceberg_job = '/home/jovyan/log/presto/job/job.log'
    presto_log_delta_job = '/home/jovyan/log/presto/job/job_del.log'
    presto_log_hudi_job = '/home/jovyan/log/presto/job/job_hudi.log'

    presto_log_iceberg_ssb = '/home/jovyan/log/presto/ssb/lake_ssb.log'
    presto_log_delta_ssb = '/home/jovyan/log/presto/ssb/lake_ssb.log'
    presto_log_hudi_ssb = '/home/jovyan/log/presto/ssb/ssb_hudi_fix.log'

    presto_log_iceberg_ssb_flat = '/home/jovyan/log/presto/ssb-flat/ssb_flat_lake.log'
    presto_log_delta_ssb_flat = '/home/jovyan/log/presto/ssb-flat/ssb_flat_lake.log'
    presto_log_hudi_ssb_flat = '/home/jovyan/log/presto/ssb-flat/ssb_flat_lake.log'

    trino_log_iceberg_tpcds = '/home/jovyan/log/trino/tpcds/lake_tpcds_c.log'
    trino_log_delta_tpcds = '/home/jovyan/log/trino/tpcds/tpcds_delta.log.fix'
    trino_log_hudi_tpcds = '/home/jovyan/log/trino/tpcds/tpcds_hudi_fix.log'

    trino_log_iceberg_tpch = '/home/jovyan/log/trino/tpch/lake_tpch_c.log'
    trino_log_delta_tpch = '/home/jovyan/log/trino/tpch/lake_tpch.log'
    trino_log_hudi_tpch = '/home/jovyan/log/trino/tpch/tpch_hudi_fix.log'

    trino_log_iceberg_job = '/home/jovyan/log/trino/job/lake.log'
    trino_log_delta_job = '/home/jovyan/log/trino/job/job_delta.log'
    trino_log_hudi_job = '/home/jovyan/log/trino/job/lake_c.log'

    trino_log_iceberg_ssb = '/home/jovyan/log/trino/ssb/lake_ssb.log'
    trino_log_delta_ssb = '/home/jovyan/log/trino/ssb/lake_ssb.log'
    trino_log_hudi_ssb = '/home/jovyan/log/trino/ssb/lake_ssb.log'

    trino_log_iceberg_ssb_flat = '/home/jovyan/log/trino/ssb-flat/ssb_flat.log.fix'
    trino_log_delta_ssb_flat = '/home/jovyan/log/trino/ssb-flat/ssb_flat.log.fix'
    trino_log_hudi_ssb_flat = '/home/jovyan/log/trino/ssb-flat/ssb_flat.log.fix'

    print("Processing five benchmarks to CSV...")

    # TPC-DS
    conf_spark_del, lat_spark_del = process_spark_experiment(spark_log_delta_tpcds)
    conf_spark_ice, lat_spark_ice = process_spark_experiment(spark_log_iceberg_tpcds)
    conf_spark_hudi, lat_spark_hudi = process_spark_experiment(spark_log_hudi_tpcds)
    lat_spark_ice = fix_qnames(lat_spark_ice, "tpcds_")
    lat_spark_del = fix_qnames(lat_spark_del, "tpcds_")
    lat_spark_hudi = fix_qnames(lat_spark_hudi, "tpcds_")
    conf_presto_del, lat_presto_del = process_presto_experiment(presto_log_delta_tpcds, "delta", presto_base_tpcds)
    conf_presto_ice, lat_presto_ice = process_presto_experiment(presto_log_iceberg_tpcds, "iceberg", presto_base_tpcds)
    conf_presto_hudi, lat_presto_hudi = process_presto_experiment(presto_log_hudi_tpcds, "hudi", "/home/jovyan/log/presto/results/lake/ssb-flat")
    conf_trino_del, lat_trino_del = process_trino_experiment(trino_log_delta_tpcds, "delta", "/home/jovyan/log/log_sup/ssb-flat")
    conf_trino_ice, lat_trino_ice = process_trino_experiment(trino_log_iceberg_tpcds, "iceberg", trino_base_tpcds)
    conf_trino_hudi, lat_trino_hudi = process_trino_experiment(trino_log_hudi_tpcds, "hudi", "/home/jovyan/log/trino/results/lake/ssb-flat")
    grouped_data_tpcds, max_dim_tpcds = build_9combo_data_by_query(
        lat_spark_del, conf_spark_del, lat_spark_ice, conf_spark_ice, lat_spark_hudi, conf_spark_hudi,
        lat_presto_del, conf_presto_del, lat_presto_ice, conf_presto_ice, lat_presto_hudi, conf_presto_hudi,
        lat_trino_del, conf_trino_del, lat_trino_ice, conf_trino_ice, lat_trino_hudi, conf_trino_hudi,
    )

    # JOB
    conf_spark_del, lat_spark_del = process_spark_experiment(spark_log_delta_job)
    conf_spark_ice, lat_spark_ice = process_spark_experiment(spark_log_iceberg_job)
    conf_spark_hudi, lat_spark_hudi = process_spark_experiment(spark_log_hudi_job)
    conf_presto_del, lat_presto_del = process_presto_experiment(presto_log_delta_job, "delta", presto_base_job)
    conf_presto_ice, lat_presto_ice = process_presto_experiment(presto_log_iceberg_job, "iceberg", presto_base_job)
    conf_presto_hudi, lat_presto_hudi = process_presto_experiment(presto_log_hudi_job, "hudi", presto_base_job)
    conf_trino_del, lat_trino_del = process_trino_experiment(trino_log_delta_job, "delta", trino_base_tpcds)
    conf_trino_ice, lat_trino_ice = process_trino_experiment(trino_log_iceberg_job, "iceberg", trino_base_tpcds)
    conf_trino_hudi, lat_trino_hudi = process_trino_experiment(trino_log_hudi_job, "hudi", trino_base_tpcds)
    grouped_data_job, _ = build_9combo_data_by_query(
        lat_spark_del, conf_spark_del, lat_spark_ice, conf_spark_ice, lat_spark_hudi, conf_spark_hudi,
        lat_presto_del, conf_presto_del, lat_presto_ice, conf_presto_ice, lat_presto_hudi, conf_presto_hudi,
        lat_trino_del, conf_trino_del, lat_trino_ice, conf_trino_ice, lat_trino_hudi, conf_trino_hudi,
    )

    # TPCH
    conf_spark_del, lat_spark_del = process_spark_experiment(spark_log_delta_tpch)
    conf_spark_ice, lat_spark_ice = process_spark_experiment(spark_log_iceberg_tpch)
    conf_spark_hudi, lat_spark_hudi = process_spark_experiment(spark_log_hudi_tpch)
    conf_presto_del, lat_presto_del = process_presto_experiment(presto_log_delta_tpch, "delta", presto_base_tpcds)
    conf_presto_ice, lat_presto_ice = process_presto_experiment(presto_log_iceberg_tpch, "iceberg", presto_base_tpcds)
    conf_presto_hudi, lat_presto_hudi = process_presto_experiment(presto_log_hudi_tpch, "hudi", presto_base_tpch)
    conf_trino_del, lat_trino_del = process_trino_experiment(trino_log_delta_tpch, "delta", trino_base_tpcds)
    conf_trino_ice, lat_trino_ice = process_trino_experiment(trino_log_iceberg_tpch, "iceberg", trino_base_tpcds)
    conf_trino_hudi, lat_trino_hudi = process_trino_experiment(trino_log_hudi_tpch, "hudi", "/home/jovyan/log/trino/results/lake/ssb-flat")
    grouped_data_tpch, _ = build_9combo_data_by_query(
        lat_spark_del, conf_spark_del, lat_spark_ice, conf_spark_ice, lat_spark_hudi, conf_spark_hudi,
        lat_presto_del, conf_presto_del, lat_presto_ice, conf_presto_ice, lat_presto_hudi, conf_presto_hudi,
        lat_trino_del, conf_trino_del, lat_trino_ice, conf_trino_ice, lat_trino_hudi, conf_trino_hudi,
    )

    # SSB
    conf_spark_del, lat_spark_del = process_spark_experiment(spark_log_delta_ssb)
    conf_spark_ice, lat_spark_ice = process_spark_experiment(spark_log_iceberg_ssb)
    conf_spark_hudi, lat_spark_hudi = process_spark_experiment(spark_log_hudi_ssb)
    conf_presto_del, lat_presto_del = process_presto_experiment(presto_log_delta_ssb, "delta", presto_base_ssb)
    conf_presto_ice, lat_presto_ice = process_presto_experiment(presto_log_iceberg_ssb, "iceberg", presto_base_ssb)
    conf_presto_hudi, lat_presto_hudi = process_presto_experiment(presto_log_hudi_ssb, "hudi", "/home/jovyan/log/presto/results/lake/ssb-flat")
    conf_trino_del, lat_trino_del = process_trino_experiment(trino_log_delta_ssb, "delta", trino_base_tpcds)
    conf_trino_ice, lat_trino_ice = process_trino_experiment(trino_log_iceberg_ssb, "iceberg", trino_base_tpcds)
    conf_trino_hudi, lat_trino_hudi = process_trino_experiment(trino_log_hudi_ssb, "hudi", trino_base_tpcds)
    grouped_data_ssb, _ = build_9combo_data_by_query(
        lat_spark_del, conf_spark_del, lat_spark_ice, conf_spark_ice, lat_spark_hudi, conf_spark_hudi,
        lat_presto_del, conf_presto_del, lat_presto_ice, conf_presto_ice, lat_presto_hudi, conf_presto_hudi,
        lat_trino_del, conf_trino_del, lat_trino_ice, conf_trino_ice, lat_trino_hudi, conf_trino_hudi,
    )

    # SSB-FLAT
    conf_spark_del, lat_spark_del = process_spark_experiment(spark_log_delta_ssb_flat)
    conf_spark_ice, lat_spark_ice = process_spark_experiment(spark_log_iceberg_ssb_flat)
    conf_spark_hudi, lat_spark_hudi = process_spark_experiment(spark_log_hudi_ssb_flat)
    conf_presto_del, lat_presto_del = process_presto_experiment(presto_log_delta_ssb_flat, "delta", presto_base_ssb_flat)
    conf_presto_ice, lat_presto_ice = process_presto_experiment(presto_log_iceberg_ssb_flat, "iceberg", presto_base_ssb_flat)
    conf_presto_hudi, lat_presto_hudi = process_presto_experiment(presto_log_hudi_ssb_flat, "hudi", presto_base_ssb_flat)
    conf_trino_del, lat_trino_del = process_trino_experiment(trino_log_delta_ssb_flat, "delta", trino_base_ssb)
    conf_trino_ice, lat_trino_ice = process_trino_experiment(trino_log_iceberg_ssb_flat, "iceberg", trino_base_ssb)
    conf_trino_hudi, lat_trino_hudi = process_trino_experiment(trino_log_hudi_ssb_flat, "hudi", trino_base_ssb)
    tmp = {}
    for key, val in lat_spark_ice.items():
        tmp[key.replace('query', 'q').replace('.sql', '')] = val
    lat_spark_ice = tmp
    tmp = {}
    for key, val in lat_spark_del.items():
        tmp[key.replace('query', 'q').replace('.sql', '')] = val
    lat_spark_del = tmp
    tmp = {}
    for key, val in lat_spark_hudi.items():
        tmp[key.replace('query', 'q').replace('.sql', '')] = val
    lat_spark_hudi = tmp
    grouped_data_ssb_flat, max_dim = build_9combo_data_by_query(
        lat_spark_del, conf_spark_del, lat_spark_ice, conf_spark_ice, lat_spark_hudi, conf_spark_hudi,
        lat_presto_del, conf_presto_del, lat_presto_ice, conf_presto_ice, lat_presto_hudi, conf_presto_hudi,
        lat_trino_del, conf_trino_del, lat_trino_ice, conf_trino_ice, lat_trino_hudi, conf_trino_hudi,
    )

    # Normalize keys per benchmark prefix
    grouped_data_tpcds = fix_group_data(grouped_data_tpcds, "tpcds_")
    grouped_data_job = fix_group_data(grouped_data_job, "job_")
    grouped_data_tpch = fix_group_data(grouped_data_tpch, "tpch_")
    grouped_data_ssb = fix_group_data(grouped_data_ssb, "ssb_")
    grouped_data_ssb_flat = fix_group_data(grouped_data_ssb_flat, "ssb_flat_")

    # -------- Supply datasets (from all_ratio.py supply paths) --------
    trino_base = "/home/jovyan/train/supply_data/result"
    presto_base = "/home/jovyan/train/supply_data/result"

    spark_log_iceberg_tpcds_supply = '/home/jovyan/train/supply_data/spark/tpcds/tpcds_ice_sup.log'
    spark_log_delta_tpcds_supply = '/home/jovyan/train/supply_data/spark/tpcds/delta_tpcds_sup.log'
    spark_log_hudi_tpcds_supply = '/home/jovyan/train/supply_data/spark/tpcds/hudi_tpcds_sup2.log'

    spark_log_iceberg_tpch_supply = '/home/jovyan/train/supply_data/spark/tpch/tpch_ice_sup.log'
    spark_log_delta_tpch_supply = '/home/jovyan/train/supply_data/spark/tpch/tpch_sup_del.log'
    spark_log_hudi_tpch_supply = '/home/jovyan/train/supply_data/spark/tpch/tpch_hudi_sup.log'

    spark_log_iceberg_job_supply = '/home/jovyan/train/supply_data/spark/job/job_ice_sup.log'
    spark_log_delta_job_supply = '/home/jovyan/train/supply_data/spark/job/job_delta_10g_sup.log'
    spark_log_hudi_job_supply = '/home/jovyan/train/supply_data/spark/job/job_hudi_sup2.log'

    spark_log_iceberg_ssb_flat_supply = '/home/jovyan/train/supply_data/spark/ssb-flat/ssb_flat_ice_sup.log'
    spark_log_delta_ssb_flat_supply = '/home/jovyan/train/supply_data/spark/ssb-flat/del_ssb_flat_sup.log'
    spark_log_hudi_ssb_flat_supply = '/home/jovyan/train/supply_data/spark/ssb-flat/hudi_ssb_flat_sup.log'

    presto_log_iceberg_tpcds_supply = '/home/jovyan/train/supply_data/presto/tpcds/tpcds_ice.log'
    presto_log_delta_tpcds_supply = '/home/jovyan/train/supply_data/presto/tpcds/tpcds_sup.log'
    presto_log_hudi_tpcds_supply = '/home/jovyan/train/supply_data/presto/tpcds/tpcds_sup.log'

    presto_log_iceberg_tpch_supply = '/home/jovyan/train/supply_data/presto/tpch/tpch_sup_ice_hudi.log'
    presto_log_delta_tpch_supply = '/home/jovyan/train/supply_data/presto/tpch/tpch_sup_ice_hudi.log'
    presto_log_hudi_tpch_supply = '/home/jovyan/train/supply_data/presto/tpch/tpch_sup_ice_hudi.log'

    presto_log_iceberg_job_supply = '/home/jovyan/train/supply_data/presto/job/job_sup_c.log'
    presto_log_delta_job_supply = '/home/jovyan/train/supply_data/presto/job/job_sup.log'
    presto_log_hudi_job_supply = '/home/jovyan/train/supply_data/presto/job/job_hudi_10g_sup.log'

    presto_log_iceberg_ssb_flat_supply = '/home/jovyan/train/supply_data/presto/ssb_flat/ssb_flat_ice_sup.log'
    presto_log_delta_ssb_flat_supply = '/home/jovyan/train/supply_data/presto/ssb_flat/ssb_flat_ice_sup.log'
    presto_log_hudi_ssb_flat_supply = '/home/jovyan/train/supply_data/presto/ssb_flat/ssb_flat_ice_sup.log'

    trino_log_iceberg_tpcds_supply = '/home/jovyan/train/supply_data/trino/tpcds/tpcds_sup.log'
    trino_log_delta_tpcds_supply = '/home/jovyan/train/supply_data/trino/tpcds/tpcds_sup.log'
    trino_log_hudi_tpcds_supply = '/home/jovyan/train/supply_data/trino/tpcds/tpcds_sup.log'

    trino_log_iceberg_tpch_supply = '/home/jovyan/train/supply_data/trino/tpch/tpch_supply.log'
    trino_log_delta_tpch_supply = '/home/jovyan/train/supply_data/trino/tpch/tpch_supply.log'
    trino_log_hudi_tpch_supply = '/home/jovyan/train/supply_data/trino/tpch/tpch_supply_hudi.log'

    trino_log_iceberg_job_supply = '/home/jovyan/train/supply_data/trino/job/job_sup_c.log'
    trino_log_delta_job_supply = '/home/jovyan/train/supply_data/trino/job/job_sup.log'
    trino_log_hudi_job_supply = '/home/jovyan/train/supply_data/trino/job/job_hudi_10g_sup.log'

    trino_log_iceberg_ssb_flat_supply = '/home/jovyan/train/supply_data/trino/ssb-flat/ssb_flat_ice_sup.log'
    trino_log_delta_ssb_flat_supply = '/home/jovyan/train/supply_data/trino/ssb-flat/ssb_flat_ice_sup.log'
    trino_log_hudi_ssb_flat_supply = '/home/jovyan/train/supply_data/trino/ssb-flat/ssb_flat_ice_sup.log'

    # TPC-DS supply
    grouped_data_tpcds_supply = {}
    try:
        conf_spark_del, lat_spark_del = process_spark_experiment(spark_log_delta_tpcds_supply)
        conf_spark_ice, lat_spark_ice = process_spark_experiment(spark_log_iceberg_tpcds_supply)
        conf_spark_hudi, lat_spark_hudi = process_spark_experiment(spark_log_hudi_tpcds_supply)
        lat_spark_ice = fix_qnames(lat_spark_ice, "tpcds_")
        lat_spark_del = fix_qnames(lat_spark_del, "tpcds_")
        lat_spark_hudi = fix_qnames(lat_spark_hudi, "tpcds_")
        conf_presto_del, lat_presto_del = process_presto_experiment(presto_log_delta_tpcds_supply, "delta", presto_base)
        conf_presto_ice, lat_presto_ice = process_presto_experiment(presto_log_iceberg_tpcds_supply, "iceberg", presto_base)
        conf_presto_hudi, lat_presto_hudi = process_presto_experiment(presto_log_hudi_tpcds_supply, "hudi", presto_base)
        conf_trino_del, lat_trino_del = process_trino_experiment(trino_log_delta_tpcds_supply, "delta", trino_base)
        conf_trino_ice, lat_trino_ice = process_trino_experiment(trino_log_iceberg_tpcds_supply, "iceberg", trino_base)
        conf_trino_hudi, lat_trino_hudi = process_trino_experiment(trino_log_hudi_tpcds_supply, "hudi", trino_base)
        grouped_data_tpcds_supply, _ = build_9combo_data_by_query(
            lat_spark_del, conf_spark_del, lat_spark_ice, conf_spark_ice, lat_spark_hudi, conf_spark_hudi,
            lat_presto_del, conf_presto_del, lat_presto_ice, conf_presto_ice, lat_presto_hudi, conf_presto_hudi,
            lat_trino_del, conf_trino_del, lat_trino_ice, conf_trino_ice, lat_trino_hudi, conf_trino_hudi,
        )
        grouped_data_tpcds_supply = fix_group_data(grouped_data_tpcds_supply, "tpcds_")
    except Exception as e:
        print(f"[WARN] TPC-DS supply processing failed: {e}")

    # JOB supply
    grouped_data_job_supply = {}
    try:
        conf_spark_del, lat_spark_del = process_spark_experiment(spark_log_delta_job_supply)
        conf_spark_ice, lat_spark_ice = process_spark_experiment(spark_log_iceberg_job_supply)
        conf_spark_hudi, lat_spark_hudi = process_spark_experiment(spark_log_hudi_job_supply)
        conf_presto_del, lat_presto_del = process_presto_experiment(presto_log_delta_job_supply, "delta", presto_base)
        conf_presto_ice, lat_presto_ice = process_presto_experiment(presto_log_iceberg_job_supply, "iceberg", presto_base)
        conf_presto_hudi, lat_presto_hudi = process_presto_experiment(presto_log_hudi_job_supply, "hudi", presto_base)
        conf_trino_del, lat_trino_del = process_trino_experiment(trino_log_delta_job_supply, "delta", trino_base)
        conf_trino_ice, lat_trino_ice = process_trino_experiment(trino_log_iceberg_job_supply, "iceberg", trino_base)
        conf_trino_hudi, lat_trino_hudi = process_trino_experiment(trino_log_hudi_job_supply, "hudi", trino_base)
        grouped_data_job_supply, _ = build_9combo_data_by_query(
            lat_spark_del, conf_spark_del, lat_spark_ice, conf_spark_ice, lat_spark_hudi, conf_spark_hudi,
            lat_presto_del, conf_presto_del, lat_presto_ice, conf_presto_ice, lat_presto_hudi, conf_presto_hudi,
            lat_trino_del, conf_trino_del, lat_trino_ice, conf_trino_ice, lat_trino_hudi, conf_trino_hudi,
        )
        grouped_data_job_supply = fix_group_data(grouped_data_job_supply, "job_")
    except Exception as e:
        print(f"[WARN] JOB supply processing failed: {e}")

    # TPCH supply
    grouped_data_tpch_supply = {}
    try:
        conf_spark_del, lat_spark_del = process_spark_experiment(spark_log_delta_tpch_supply)
        conf_spark_ice, lat_spark_ice = process_spark_experiment(spark_log_iceberg_tpch_supply)
        conf_spark_hudi, lat_spark_hudi = process_spark_experiment(spark_log_hudi_tpch_supply)
        conf_presto_del, lat_presto_del = process_presto_experiment(presto_log_delta_tpch_supply, "delta", presto_base)
        conf_presto_ice, lat_presto_ice = process_presto_experiment(presto_log_iceberg_tpch_supply, "iceberg", presto_base)
        conf_presto_hudi, lat_presto_hudi = process_presto_experiment(presto_log_hudi_tpch_supply, "hudi", presto_base)
        conf_trino_del, lat_trino_del = process_trino_experiment(trino_log_delta_tpch_supply, "delta", trino_base)
        conf_trino_ice, lat_trino_ice = process_trino_experiment(trino_log_iceberg_tpch_supply, "iceberg", trino_base)
        conf_trino_hudi, lat_trino_hudi = process_trino_experiment(trino_log_hudi_tpch_supply, "hudi", trino_base)
        grouped_data_tpch_supply, _ = build_9combo_data_by_query(
            lat_spark_del, conf_spark_del, lat_spark_ice, conf_spark_ice, lat_spark_hudi, conf_spark_hudi,
            lat_presto_del, conf_presto_del, lat_presto_ice, conf_presto_ice, lat_presto_hudi, conf_presto_hudi,
            lat_trino_del, conf_trino_del, lat_trino_ice, conf_trino_ice, lat_trino_hudi, conf_trino_hudi,
        )
        grouped_data_tpch_supply = fix_group_data(grouped_data_tpch_supply, "tpch_")
    except Exception as e:
        print(f"[WARN] TPCH supply processing failed: {e}")

    # SSB-FLAT supply
    grouped_data_ssb_flat_supply = {}
    try:
        conf_spark_del, lat_spark_del = process_spark_experiment(spark_log_delta_ssb_flat_supply)
        conf_spark_ice, lat_spark_ice = process_spark_experiment(spark_log_iceberg_ssb_flat_supply)
        conf_spark_hudi, lat_spark_hudi = process_spark_experiment(spark_log_hudi_ssb_flat_supply)
        conf_presto_del, lat_presto_del = process_presto_experiment(presto_log_delta_ssb_flat_supply, "delta", presto_base)
        conf_presto_ice, lat_presto_ice = process_presto_experiment(presto_log_iceberg_ssb_flat_supply, "iceberg", presto_base)
        conf_presto_hudi, lat_presto_hudi = process_presto_experiment(presto_log_hudi_ssb_flat_supply, "hudi", presto_base)
        conf_trino_del, lat_trino_del = process_trino_experiment(trino_log_delta_ssb_flat_supply, "delta", trino_base)
        conf_trino_ice, lat_trino_ice = process_trino_experiment(trino_log_iceberg_ssb_flat_supply, "iceberg", trino_base)
        conf_trino_hudi, lat_trino_hudi = process_trino_experiment(trino_log_hudi_ssb_flat_supply, "hudi", trino_base)
        grouped_data_ssb_flat_supply, _ = build_9combo_data_by_query(
            lat_spark_del, conf_spark_del, lat_spark_ice, conf_spark_ice, lat_spark_hudi, conf_spark_hudi,
            lat_presto_del, conf_presto_del, lat_presto_ice, conf_presto_ice, lat_presto_hudi, conf_presto_hudi,
            lat_trino_del, conf_trino_del, lat_trino_ice, conf_trino_ice, lat_trino_hudi, conf_trino_hudi,
        )
        grouped_data_ssb_flat_supply = fix_group_data(grouped_data_ssb_flat_supply, "ssb_flat_")
    except Exception as e:
        print(f"[WARN] SSB-FLAT supply processing failed: {e}")

    # Combine
    all_grouped = {}
    for d in [grouped_data_job, grouped_data_ssb_flat, grouped_data_ssb, grouped_data_tpcds, grouped_data_tpch]:
        all_grouped.update(d)

    # Prepare CSV save directory
    out_dir = Path('/home/jovyan/train/ratio_csv_pipeline/csv')
    out_dir.mkdir(parents=True, exist_ok=True)

    # Save a single CSV per benchmark with all records
    import csv
    # Write normal datasets (no _supply suffix)
    benchmarks = [
        ("job", grouped_data_job),
        ("ssb_flat", grouped_data_ssb_flat),
        ("ssb", grouped_data_ssb),
        ("tpcds", grouped_data_tpcds),
        ("tpch", grouped_data_tpch),
    ]
    combos = [(0,0),(0,1),(0,2),(1,0),(1,1),(1,2),(2,0),(2,1),(2,2)]

    def write_benchmark_csv(name: str, grouped: Dict):
        filename = out_dir / f"{name}.csv"
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            # Minimal fields required for later reading
            # We store per-query records with engine/lake ids and latency
            writer = csv.writer(f)
            header = [
                'query_id','engine_id','lake_id','config','latency'
            ]
            writer.writerow(header)
            for qid, data in grouped.items():
                for (e_id,l_id) in combos:
                    recs = data.get((e_id,l_id), [])
                    for conf, lat in recs:
                        writer.writerow([
                            qid,
                            e_id,
                            l_id,
                            ';'.join(map(lambda x: str(float(x)), conf)),
                            float(lat),
                        ])
        print(f"Saved {filename}")

    for name, grouped in benchmarks:
        write_benchmark_csv(name, grouped)

    # Write supply datasets
    supply_benchmarks = [
        ("job_supply", grouped_data_job_supply),
        ("ssb_flat_supply", grouped_data_ssb_flat_supply),
        ("tpcds_supply", grouped_data_tpcds_supply),
        ("tpch_supply", grouped_data_tpch_supply),
    ]
    for name, grouped in supply_benchmarks:
        if grouped:
            write_benchmark_csv(name, grouped)

    print("Done writing benchmark CSVs.")


if __name__ == '__main__':
    main()


