Ratio CSV Pipeline
===================

Scripts:
- scripts/process_logs_to_csv.py: Parse original logs for five benchmarks and write CSVs under csv/.
- scripts/read_csv_for_processing.py: Read those CSVs and reconstruct grouped data structures.

Processors are copied into processors/ (spark/presto/trino) and used by the scripts.

Run:
  python /home/jovyan/train/ratio_csv_pipeline/scripts/process_logs_to_csv.py
  python /home/jovyan/train/ratio_csv_pipeline/scripts/read_csv_for_processing.py


