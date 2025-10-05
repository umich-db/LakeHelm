# import os
# import re
# import csv
# from collections import OrderedDict
# from typing import List, Dict, Tuple
#
#
#
# def extract_db_info(line: str, datalake:str) -> Tuple[str, str]:
#     pattern = ''
#     # if datalake == 'job':
#     #     pattern = r'results/trino/tpch/job/(\d+)/runs/(db\d+)\.sql\.run'
#     #     pattern = r'results/trino/tpch/job/8core64g/(\d+)/plans/(\d+_db\d+)\.sql\.plan'.format(datalake)
#     # else:
#         # pattern = r'results/trino/tpch/{}/8core64g/timestamp=(\d+)/runs/(db\d+)\.sql\.run'.format(datalake)
#     # results / presto / tpcds / job / tpcds_iceberg_10g_mor_roc / timestamp = 1730793289
#     pattern = r'results/trino/tpch/{}/8core64g/timestamp=(\d+)/plans/(\d+_db\d+)\.sql\.plan'.format(datalake)
#     # pattern_6table = r'results/presto/tpcds/{}/tpcds_iceberg_10g_mor_roc/timestamp=(\d+)/plans/(query\d+)\.sql\.plan'.format(datalake)
#     # pattern_6table = r'results/presto/tpcds/job/tpcds_iceberg_10g_mor_roc/timestamp=(\d+)/plans/(query\d+).sql.plan'
#     match = re.search(pattern, line)
#     if match:
#         return match.group(1), match.group(2)
#     return None, None
#
# def group_db_orders(log_file: str,datalake: str) -> Tuple[List[str], List[List[str]]]:
#     db_orders = [[]]
#     timestamps = []
#     with open(log_file, 'r') as file:
#         for line in file:
#             timestamp, db = extract_db_info(line, datalake)
#             if timestamp and db:
#                 if len(db_orders[-1]) == 220:
#                     timestamps.append(timestamp)
#                     if len(db_orders[-1]) == 220:
#                         db_orders.append([])
#                 db_orders[-1].append(db)
#     db_orders = db_orders[:len(timestamps)]
#     return timestamps, db_orders
#
# def read_csv_file(csv_path: str, db_order: List[str]) -> OrderedDict:
#     db_data = OrderedDict((db, []) for db in db_order)
#     if not os.path.exists(csv_path):
#         print(f"CSV file not found: {csv_path}")
#         return db_data
#
#     with open(csv_path, 'r') as csvfile:
#         reader = csv.reader(csvfile)
#         for row in reader:
#             if len(row) < 2:
#                 continue
#             for db in db_order:
#                 if not db_data[db]:
#                     db_data[db] = row[1:]
#                     break
#     return db_data
#
# def read_all_csv_files(timestamps: List[str], db_orders: List[List[str]], datalake) -> List[OrderedDict]:
#     base_path = ''
#     latency_list = []
#     csv_path = ''
#     if datalake == 'iceberg2':
#         base_path = f'datasets/trino/tpch/{datalake}'
#         for timestamp, db_order in zip(timestamps, db_orders):
#             latency_list.append(read_csv_file(f"{base_path}/{csv_path}{timestamp}/presto_runtimes_raw.csv", db_order))
#         return latency_list
#     else:
#         csv_path = '8core64g/timestamp='
#         # csv_path_6table = 'timestamp='
#         base_path = f'datasets/trino/tpch/{datalake}'
#         # base_path_6table = 'datasets/trino/tpcds_iceberg_10g_mor_roc'
#         for timestamp, db_order in zip(timestamps, db_orders):
#             latency = read_csv_file(f"{base_path}/{csv_path}{timestamp}/trino_runtimes_raw.csv", db_order)
#             if len(list(latency.values())[0]) > 0:
#                 latency_list.append(latency)
#
#         return latency_list
#
#
# def combine_latencies(results: List[OrderedDict]) -> Tuple[Dict[str, List[str]], List[int]]:
#     combine_latency = {}
#     error_list = [1000]
#     for i, result in enumerate(results):
#         for db, data in result.items():
#             if db not in combine_latency:
#                 combine_latency[db] = [data[0]] if data else []
#             else:
#                 if data:
#                     combine_latency[db].append(data[0])
#                 elif i != error_list[-1]:
#                     error_list.append(i)
#     return combine_latency, error_list[1:]
#
# def remove_elements(lst: List, indices: List[int]) -> List:
#     for index in sorted(indices, reverse=True):
#         if 0 <= index < len(lst):
#             del lst[index]
#     return lst
#
# def extract_parameters(text: str) -> List[List[int]]:
#     patterns = [
#         r'query\.max-memory=(\d+)GB',
#         r'query\.max-memory-per-node=(\d+)GB',
#         r'memory\.heap-headroom-per-node=(\d+)GB',
#         r'task\.concurrency=(\d+)',
#         r'task\.max-worker-threads=(\d+)',
#         r'node-scheduler\.max-splits-per-node=(\d+)'
#     ]
#     results = []
#     current_group = []
#     for line in text.split('\n'):
#         for pattern in patterns:
#             match = re.search(pattern, line)
#             if match:
#                 current_group.append(int(match.group(1)))
#                 break
#         if len(current_group) == 6:
#             results.append(current_group)
#             current_group = []
#     return [results[i] for i in range(0, len(results), 4)]
#
# def extract_parameters_from_file(filename: str) -> List[List[int]]:
#     try:
#         with open(filename, 'r') as file:
#             return extract_parameters(file.read())
#     except FileNotFoundError:
#         print(f"Error: File '{filename}' not found.")
#     except IOError:
#         print(f"Error: Unable to read file '{filename}'.")
#     return []
#
# def get_config_latency(log_file: str, datalake:str):
#     timestamps, db_orders = group_db_orders(log_file, datalake)
#     results = read_all_csv_files(timestamps, db_orders,datalake)
#     combine_latency, error_list = combine_latencies(results)
#
#     # print("Error list:", error_list)
#     # print("Combined latencies:", combine_latency)
#
#     parameters = extract_parameters_from_file(log_file)
#     parameters = parameters[:-1]
#     parameters = remove_elements(parameters, error_list)
#
#     # print("Extracted parameters:", parameters)
#     # print(len(parameters))
#     return parameters, combine_latency
#
#
#
#
# #==========================================================================
# def parse_fragments(text):
#     fragment_pattern = r'Fragment (\d+) \[.*?\]\s*'
#     fragment_splits = re.split(fragment_pattern, text)
#
#     fragments = {}
#     for i in range(1, len(fragment_splits), 2):
#         fragment_number = int(fragment_splits[i])
#         fragment_content = fragment_splits[i + 1]
#         fragments[fragment_number] = fragment_content
#
#     return fragments
#
#
# def process_fragment_content(fragment_content):
#     lines = fragment_content.split('\n')
#     tree_structure = []
#
#     for line in lines:
#         matches = re.finditer(r'(\b\w+)\[(.*?)\]', line)
#         for match in matches:
#             word, contents = match.groups()
#             level = int((match.start(1) - 4) / 3)
#             tree_structure.append((level, word, contents))
#
#     return tree_structure
#
#
# def replace_remote_sources(tree, all_trees):
#     new_tree = []
#     has_remote_source = False
#     for level, word, contents in tree:
#         if word == 'RemoteSource':
#             has_remote_source = True
#             source_fragment_id = int(re.search('sourceFragmentIds = \[(\d+)', contents).group(1))
#             if source_fragment_id in all_trees:
#                 subtree = all_trees[source_fragment_id]
#                 for sub_level, sub_word, sub_contents in subtree:
#                     new_tree.append((level + sub_level, sub_word, sub_contents))
#             else:
#                 new_tree.append((level, word, contents))
#         elif word == 'RemoteMerge' and len(tree) - 1 == level:
#             has_remote_source = True
#             source_fragment_id = int(re.search('sourceFragmentIds = \[(\d+)', contents).group(1))
#             new_tree.append((level, word, contents))
#             if source_fragment_id in all_trees:
#                 subtree = all_trees[source_fragment_id]
#                 for sub_level, sub_word, sub_contents in subtree:
#                     new_tree.append((level + sub_level + 1, sub_word, sub_contents))
#         else:
#             new_tree.append((level, word, contents))
#     return new_tree, has_remote_source
#
#
# def extract_table_name(contents):
#     match = re.search(r'table = (\w+:[\w.]+)', contents)
#     if match:
#         return match.group(1)
#     return None
#
#
# def collect_operators_and_tables(directory_path):
#     all_operators = set()
#     all_tables = set()
#
#     for filename in os.listdir(directory_path):
#         if filename.endswith('.plan'):
#             file_path = os.path.join(directory_path, filename)
#             with open(file_path, 'r') as file:
#                 text = file.read()
#
#             fragments = parse_fragments(text)
#             all_trees = {}
#             for fragment_id, fragment_content in fragments.items():
#                 tree = process_fragment_content(fragment_content)
#                 all_trees[fragment_id] = tree
#
#             current_tree = all_trees[0]
#             while True:
#                 new_tree, has_remote_source = replace_remote_sources(current_tree, all_trees)
#                 if not has_remote_source:
#                     break
#                 current_tree = new_tree
#
#             for _, word, contents in current_tree:
#                 all_operators.add(word)
#                 table = extract_table_name(contents)
#                 if table:
#                     all_tables.add(table)
#
#     return sorted(list(all_operators)), sorted(list(all_tables))
#
#
# def create_mappings(operators, tables):
#     operator_mapping = {operator: index for index, operator in enumerate(operators)}
#     table_mapping = {table: index for index, table in enumerate(tables)}
#     return operator_mapping, table_mapping
#
#
# def create_vector(operator_index, table_index, vector_length):
#     vector = [0] * vector_length
#     vector[operator_index] = 1
#     vector[-2] = table_index
#     return vector
#
#
# def convert_to_nested_tuple(tree, operator_mapping, table_mapping):
#     vector_length = len(operator_mapping) + 1
#
#     def convert_node(node_index):
#         level, word, contents = tree[node_index]
#         operator_index = operator_mapping[word]
#         table = extract_table_name(contents)
#         table_index = table_mapping.get(table, -1)
#         node_vector = create_vector(operator_index, table_index, vector_length)
#
#         children = []
#         for i in range(node_index + 1, len(tree)):
#             child_level, child_word, child_contents = tree[i]
#             if child_level == level + 1:
#                 children.append(i)
#             elif child_level <= level:
#                 break
#
#         if len(children) == 0:
#             return tuple(node_vector), None, None
#         elif len(children) == 1:
#             return tuple(node_vector), convert_node(children[0]), None
#         else:
#             return tuple(node_vector), convert_node(children[0]), convert_node(children[1])
#
#     return convert_node(0)
#
#
# def convert_to_binary_tuple(tuple_tree):
#     if isinstance(tuple_tree, tuple):  # This is a vector node
#         vector = tuple_tree[0]
#         if tuple_tree[1] is None:  # Leaf node
#             zero_vector = [0] * (len(vector) - 1) + [1]  # All zeros with last element as 1
#             return (vector, (tuple(zero_vector),), (tuple(zero_vector),))
#         elif tuple_tree[2] is None:  # Node with one child
#             zero_vector = [0] * (len(vector) - 1) + [1]  # All zeros with last element as 1
#             return (vector, convert_to_binary_tuple(tuple_tree[1]), (tuple(zero_vector),))
#         else:  # Node with two or more children
#             return (vector, convert_to_binary_tuple(tuple_tree[1]), convert_to_binary_tuple(tuple_tree[2]))
#
# def process_file(file_path, operator_mapping, table_mapping):
#     with open(file_path, 'r') as file:
#         text = file.read()
#
#     fragments = parse_fragments(text)
#
#     all_trees = {}
#     for fragment_id, fragment_content in fragments.items():
#         tree = process_fragment_content(fragment_content)
#         all_trees[fragment_id] = tree
#
#     current_tree = all_trees[0]
#     while True:
#         new_tree, has_remote_source = replace_remote_sources(current_tree, all_trees)
#         if not has_remote_source:
#             break
#         current_tree = new_tree
#
#     nested_tree = convert_to_nested_tuple(current_tree, operator_mapping, table_mapping)
#     binary_tree = convert_to_binary_tuple(nested_tree)
#
#     return binary_tree
#
#
# def print_binary_tree(tree, indent=""):
#     vector, children = tree
#     # print(f"{indent}{vector}")
#     if children:
#         for child in children:
#             print_binary_tree(child, indent + "  ")
#
#
# def process_trino_plans(directory_path, datalake):
#     # First, collect all_old unique operators and tables
#     all_operators, all_tables = collect_operators_and_tables(directory_path)
#
#     # Create unified mappings
#     operator_mapping, table_mapping = create_mappings(all_operators, all_tables)
#
#     # Now process each file
#     tree_dict = {}
#     for filename in os.listdir(directory_path):
#         if filename.endswith('.sql.plan'):
#             file_path = os.path.join(directory_path, filename)
#             binary_tree = process_file(file_path, operator_mapping, table_mapping)
#             tree_dict[filename.split('.')[0]] = binary_tree
#     return tree_dict, None, None
#
#     # results = {}
#     # input_tree = []
#     # for key,tree in tree_dict.items():
#     #     input_tree.append(tree)
#
#     # prepared_tree = prepare_trees(input_tree, transformer, left_child, right_child)
#     # results = net(prepared_tree).tolist()
#     # result_dict = {}
#     # for filename, result in zip(tree_dict.keys(), results):
#     #     result_dict[filename.split('.')[0]] = result
#     # results = result_dict
#     # print(results)
#     # return input_tree
# #!/usr/bin/env python3
# import os
# import re
# import csv
#
#
# def deduplicate_in_groups(data_list, group_size=4):
#     """
#     对 data_list 中的元素每 group_size 个为一组做去重：
#       - 如果一组内所有元素都相同，则只保留一个；
#       - 否则，该组内的元素保持不变。
#     对于不足 group_size 个的剩余部分，直接保留原样。
#     """
#     deduped = []
#     n = len(data_list)
#     for i in range(0, n, group_size):
#         group = data_list[i:i + group_size]
#         if len(group) == group_size and group.count(group[0]) == group_size:
#             deduped.append(group[0])
#         else:
#             deduped.extend(group)
#     return deduped
#
#
# def extract_config(log_file_path):
#     """
#     扫描 log 文件中所有形如：
#        query.max-memory=20GB
#        query.max-memory-per-node=18GB
#        memory.heap-headroom-per-node=18GB
#        task.concurrency=16
#        task.max-worker-threads=16
#        node-scheduler.max-splits-per-node=256
#     的行。每当收集到一组包含以上6个配置项的记录后，
#     按预定顺序构造成一个配置组（列表形式，如 [20,18,18,16,16,256]）。
#     最后对配置组列表按连续 4 个为一组做去重处理：
#        如果一组内的 4 个配置组完全相同，则只保留一个，
#        否则保持原状。
#     返回最终的配置组列表（列表中每个元素为列表）。
#     """
#     desired_keys = [
#         "query.max-memory",
#         "query.max-memory-per-node",
#         "memory.heap-headroom-per-node",
#         "task.concurrency",
#         "task.max-worker-threads",
#         "node-scheduler.max-splits-per-node"
#     ]
#     # 每行格式形如 key=value
#     config_pattern = re.compile(
#         r"^(query\.max-memory|query\.max-memory-per-node|memory\.heap-headroom-per-node|task\.concurrency|task\.max-worker-threads|node-scheduler\.max-splits-per-node)\s*=\s*(\S+)"
#     )
#
#     config_groups = []  # 保存所有配置组（每组为列表）
#     current_config = {}  # 临时存放当前组数据
#
#     with open(log_file_path, 'r', encoding='utf-8') as f:
#         for line in f:
#             line = line.strip()
#             m = config_pattern.match(line)
#             if m:
#                 key, value = m.group(1), m.group(2)
#                 # 内存相关配置去掉 GB 后缀（假设为大写 GB）
#                 if key in ["query.max-memory", "query.max-memory-per-node", "memory.heap-headroom-per-node"]:
#                     if value.endswith("GB"):
#                         value = value[:-2]
#                 try:
#                     num = int(float(value))
#                 except Exception as e:
#                     print(f"无法将配置 {key} 的值 {value} 转换为数字: {e}")
#                     continue
#                 current_config[key] = num
#
#                 # 当收集到所有需要的配置后，构成一个配置组
#                 if all(k in current_config for k in desired_keys):
#                     group = [current_config[k] for k in desired_keys]
#                     config_groups.append(group)
#                     current_config = {}  # 重置，准备收集下一个组
#
#     # 对配置组列表按每4个为一组做去重（仅针对连续的4个配置组）
#     config_groups_deduped = deduplicate_in_groups(config_groups, group_size=4)
#     return config_groups_deduped
#
#
# import re
#
#
# def extract_timestamps_from_log(log_file_path):
#     """
#     从 log 文件中搜索所有形如：
#          tpcds_iceberg_10g/timestamp=1738739
#          tpcds_delta_10g/timestamp=1738739
#          ssb_iceberg_10g/timestamp=1738739
#          ssb_delta_10g/timestamp=1738739
#          job_iceberg_10g/timestamp=1738739
#          job_delta_10g/timestamp=1738739
#     的匹配，返回两个列表：
#       - iceberg_ts：每个元素为 (full_match, ts_value) ，full_match 中包含 "job"
#       - delta_ts：每个元素为 (full_match, ts_value) ，full_match 中包含 "iceberg"
#     最后按照 ts_value 数值大小排序后返回。
#     """
#     # 匹配所有需要的模式
#     # ts_pattern = re.compile(
#     #     r"((?:tpcds_iceberg_10g|tpcds_delta_10g|ssb_iceberg_10g|ssb_delta_10g|job_iceberg_10g|job_delta_10g)/timestamp=(\d+))"
#     # )
#     ts_pattern = re.compile(
#         r"((?:tpch_iceberg_10g|tpch_delta_10g|tpcds_iceberg_10g|tpcds_delta_10g|tpcds_delta_100g|tpcds_iceberg_100g|ssb_iceberg_10g|ssb_delta_10g|ssb_delta_10g_presto|ssb_delta_10g_trino|job_iceberg_10g|job_iceberg_10g_presto|job_iceberg_10g_trino|job_delta_10g|job_delta_10g_presto|job_delta_10g_trino|ssb_iceberg_10g_presto|ssb_iceberg_10g_trino)/timestamp=(\d+))"
#     )
#     iceberg_ts = set()
#     delta_ts = set()
#
#     with open(log_file_path, 'r', encoding='utf-8') as f:
#         for line in f:
#             line = line.strip()
#             m = ts_pattern.search(line)
#             if m:
#                 full_str = m.group(1)  # 完整匹配的字符串，如 "tpcds_iceberg_10g/timestamp=1738739"
#                 ts_value = m.group(2)  # 时间戳的值，如 "1738739"
#                 # 根据匹配字符串中包含的关键字归类
#                 if "iceberg" in full_str:
#                     iceberg_ts.add((full_str, ts_value))
#                 elif "delta" in full_str:
#                     delta_ts.add((full_str, ts_value))
#
#     # 按 ts_value 数值大小排序
#     iceberg_ts_list = sorted(iceberg_ts, key=lambda x: int(x[1]))
#     delta_ts_list = sorted(delta_ts, key=lambda x: int(x[1]))
#
#     return iceberg_ts_list, delta_ts_list
#
#
# def extract_latency_for_type(ts_list, examples_base):
#     """
#     针对给定的 ts_list（每个元素为 (full_match, ts_value)），
#     从对应目录 examples_base/{full_match}/trino_runtimes_raw.csv 中读取 CSV 文件，
#     CSV 文件中每行格式为：
#          queryX.sql,explain,"","419","2905","FINISHED"
#          queryX.sql,run,"","5697","93","FINISHED"
#     仅处理第二列为 "run" 且第6列为 "FINISHED" 的记录，
#     对于符合条件的行，将第4列和第5列转换为整数后相加得到 latency，
#     并按 query（第一列，不含扩展名）归类到字典中。
#     返回字典，格式如：
#          {"query1": [5790, ...], "query10": [5103, ...], ...}
#     """
#     latency_dict = {}  # key 为 query 文件名前缀，value 为 latency 数值列表
#     valid_ts_count = 0  # 记录有效的时间戳数量（对应 CSV 文件中有有效数据的）
#
#     for full_str, ts in ts_list:
#         csv_path = os.path.join(examples_base, full_str, "trino_runtimes_raw.csv")
#         if not os.path.isfile(csv_path):
#             print(f"CSV 文件不存在: {csv_path}")
#             continue
#         if os.path.getsize(csv_path) == 0:
#             print(f"CSV 文件为空: {csv_path}")
#             continue
#
#         valid_records = []  # 收集当前 CSV 中符合条件的 latency
#         with open(csv_path, 'r', encoding='utf-8') as csvfile:
#             reader = csv.reader(csvfile)
#             for row in reader:
#                 # 预期行格式例如：
#                 # query1.sql,run,"","5697","93","FINISHED"
#                 if len(row) < 6:
#                     continue
#                 # 去掉文件扩展名，如 query1.sql --> query1
#                 query_name = row[0].strip().replace('.sql','').replace('query','q').replace('_a','a').replace('_b','b')
#                 record_type = row[1].strip().lower()  # "run" 或 "explain"
#                 status = row[5].strip().upper()         # "FINISHED" 或 "FAILED"
#                 if record_type != "run" or status != "FINISHED":
#                     continue
#                 try:
#                     runtime = int(row[3].strip().replace('"', ''))
#                     analysis = int(row[4].strip().replace('"', ''))
#                     latency = runtime + analysis
#                 except Exception as e:
#                     print(f"解析 CSV 行失败: {row}, 错误：{e}")
#                     continue
#                 valid_records.append((query_name, latency))
#
#         if not valid_records:
#             # print(f"CSV 文件中无有效记录: {csv_path}")
#             continue
#
#         valid_ts_count += 1
#         for query_name, latency in valid_records:
#             if query_name not in latency_dict:
#                 latency_dict[query_name] = []
#             latency_dict[query_name].append(latency)
#
#     return latency_dict
#
#
# def process_new_trino_experiment(log_file_path,examples_base):
#     # 根据实际情况修改 examples_base 路径
#
#     conf_groups = extract_config(log_file_path)
#     # conf_groups 为列表，每个配置组为一个列表
#     iceberg_ts_list, delta_ts_list = extract_timestamps_from_log(log_file_path)
#     print(iceberg_ts_list)
#     print(delta_ts_list)
#     latency_dict_iceberg = extract_latency_for_type(iceberg_ts_list, examples_base)
#     latency_dict_delta = extract_latency_for_type(delta_ts_list, examples_base)
#
#     # 以 job 为例，假设各 query 的 latency 数量一致，则取其中任一 query作为样本
#     if latency_dict_iceberg:
#         sample_key = sorted(latency_dict_iceberg.keys())[0]
#         valid_count_iceberg = len(latency_dict_iceberg[sample_key])
#         if len(conf_groups) > valid_count_iceberg:
#             conf_groups_iceberg = conf_groups[:valid_count_iceberg]
#         else:
#             conf_groups_iceberg = conf_groups
#     else:
#         conf_groups_iceberg = conf_groups
#
#     if latency_dict_delta:
#         sample_key = sorted(latency_dict_delta.keys())[0]
#         valid_count_delta = len(latency_dict_delta[sample_key])
#         if len(conf_groups) > valid_count_delta:
#             conf_groups_delta = conf_groups[:valid_count_delta]
#         else:
#             conf_groups_delta = conf_groups
#     else:
#         conf_groups_delta = conf_groups
#
#     # 返回两个组的数据，格式如下：
#     #   1. 配置组列表（列表中每个元素为配置组列表）
#     #   2. latency 字典（键为 query，值为 latency 列表）
#     #
#     # 分别为 job 和 iceberg 两组
#     result = {
#         "iceberg": {
#             "configs": conf_groups_iceberg,
#             "latency": latency_dict_iceberg
#         },
#         "delta": {
#             "configs": conf_groups_delta,
#             "latency": latency_dict_delta
#         }
#     }
#     return result
#
#
# def process_trino_experiment(log_file, datalake, example_base):
#     results = process_new_trino_experiment(log_file, example_base)
#     #
#     # # 以下输出结果示例
#     # print(len(results["iceberg"]["configs"]))
#     # print(results["iceberg"]["configs"])
#     # print(results["iceberg"]["latency"])
#     # print(len(results["iceberg"]["configs"]))
#     # print(results["iceberg"]["configs"])
#     # print(results["iceberg"]["latency"])
#     return results[datalake]["configs"], results[datalake]["latency"]
#
#

# =======================new code=======================

import os
import re
import csv
from collections import OrderedDict
from typing import List, Dict, Tuple


def extract_db_info(line: str, datalake: str) -> Tuple[str, str]:
    pattern = ''
    # if datalake == 'hudi':
    pattern = r'results/trino/tpch/{}/8core64g/timestamp=(\d+)/runs/(\d+_db\d+)\.sql\.run'.format(datalake)
    # else:
    #     pattern = r'results/trino/tpcds/{}/8core64g/timestamp=(\d+)/runs/(\d+_db\d+)\.sql\.run'.format(datalake)
    match = re.search(pattern, line)
    if match:
        return match.group(1), match.group(2)
    return None, None


def group_db_orders(log_file: str, datalake: str) -> Tuple[List[str], List[List[str]]]:
    db_orders = [[]]
    timestamps = []
    with open(log_file, 'r') as file:
        for line in file:
            timestamp, db = extract_db_info(line, datalake)
            if timestamp and db:
                if len(db_orders[-1]) == 220:
                    timestamps.append(timestamp)
                    if len(db_orders[-1]) == 220:
                        db_orders.append([])
                db_orders[-1].append(db)
    db_orders = db_orders[:len(timestamps)]
    return timestamps, db_orders


def read_csv_file(csv_path: str, db_order: List[str]) -> OrderedDict:
    db_data = OrderedDict((db, []) for db in db_order)
    if not os.path.exists(csv_path):
        print(f"CSV file not found: {csv_path}")
        return db_data

    with open(csv_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row) < 2:
                continue
            for db in db_order:
                if not db_data[db]:
                    db_data[db] = row[1:]
                    break
    return db_data


def read_all_csv_files(timestamps: List[str], db_orders: List[List[str]], datalake: str) -> List[OrderedDict]:
    base_path = f'datasets/trino/tpch/{datalake}/8core64g'
    return [read_csv_file(f"{base_path}/timestamp={timestamp}/trino_runtimes_raw.csv", db_order)
            for timestamp, db_order in zip(timestamps, db_orders)]


def combine_latencies(results: List[OrderedDict]) -> Tuple[Dict[str, List[str]], List[int]]:
    combine_latency = {}
    error_list = [1000]
    for i, result in enumerate(results):
        for db, data in result.items():
            if db not in combine_latency:
                combine_latency[db] = [data[0]] if data else []
            else:
                if data:
                    combine_latency[db].append(data[0])
                elif i != error_list[-1]:
                    error_list.append(i)
    return combine_latency, error_list[1:]


def remove_elements(lst: List, indices: List[int]) -> List:
    for index in sorted(indices, reverse=True):
        if 0 <= index < len(lst):
            del lst[index]
    return lst


def extract_parameters(text: str) -> List[List[int]]:
    patterns = [
        r'query\.max-memory=(\d+)GB',
        r'query\.max-memory-per-node=(\d+)GB',
        r'memory\.heap-headroom-per-node=(\d+)GB',
        r'query\.max-total-memory-per-node=(\d+)GB',
        r'task\.concurrency=(\d+)',
        r'task\.max-worker-threads=(\d+)',
        r'node-scheduler\.max-splits-per-node=(\d+)'
    ]
    results = []
    current_group = []
    for line in text.split('\n'):
        for pattern in patterns:
            match = re.search(pattern, line)
            if match:
                current_group.append(int(match.group(1)))
                break
        if len(current_group) == 7:
            results.append(current_group)
            current_group = []
    return [results[i] for i in range(0, len(results), 4)]


def extract_parameters_from_file(filename: str) -> List[List[int]]:
    try:
        with open(filename, 'r') as file:
            return extract_parameters(file.read())
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
    except IOError:
        print(f"Error: Unable to read file '{filename}'.")
    return []


def get_config_latency(log_file, datalake):
    timestamps, db_orders = group_db_orders(log_file, datalake)
    results = read_all_csv_files(timestamps, db_orders, datalake)
    combine_latency, error_list = combine_latencies(results)

    parameters = extract_parameters_from_file(log_file)
    parameters = parameters[:-1]
    parameters = parameters[:42]
    parameters = remove_elements(parameters, error_list)

    return parameters, combine_latency


import numpy as np
from torch import nn
from collections import Counter, defaultdict


# ========================================TreeConvolution========================
def parse_fragments(text):
    fragment_pattern = r'Fragment (\d+) \[.*?\]\s*'
    fragment_splits = re.split(fragment_pattern, text)

    fragments = {}
    for i in range(1, len(fragment_splits), 2):
        fragment_number = int(fragment_splits[i])
        fragment_content = fragment_splits[i + 1]
        fragments[fragment_number] = fragment_content

    return fragments


def process_fragment_content(fragment_content):
    lines = fragment_content.split('\n')
    tree_structure = []

    for line in lines:
        matches = re.finditer(r'(\b\w+)\[(.*?)\]', line)
        for match in matches:
            word, contents = match.groups()
            level = int((match.start(1) - 4) / 3)
            tree_structure.append((level, word, contents))

    return tree_structure


def replace_remote_sources(tree, all_trees):
    new_tree = []
    has_remote_source = False
    for level, word, contents in tree:
        if word == 'RemoteSource':
            has_remote_source = True
            source_fragment_id = int(re.search('sourceFragmentIds = \[(\d+)', contents).group(1))
            if source_fragment_id in all_trees:
                subtree = all_trees[source_fragment_id]
                for sub_level, sub_word, sub_contents in subtree:
                    new_tree.append((level + sub_level, sub_word, sub_contents))
            else:
                new_tree.append((level, word, contents))
        elif word == 'RemoteMerge' and len(tree) - 1 == level:
            has_remote_source = True
            source_fragment_id = int(re.search('sourceFragmentIds = \[(\d+)', contents).group(1))
            new_tree.append((level, word, contents))
            if source_fragment_id in all_trees:
                subtree = all_trees[source_fragment_id]
                for sub_level, sub_word, sub_contents in subtree:
                    new_tree.append((level + sub_level + 1, sub_word, sub_contents))
        else:
            new_tree.append((level, word, contents))
    return new_tree, has_remote_source


def extract_table_name(contents):
    match = re.search(r'table = (\w+:[\w.]+)', contents)
    if match:
        return match.group(1)
    return None


def collect_operators_and_tables(directory_path):
    all_operators = set()
    all_tables = set()

    for filename in os.listdir(directory_path):
        if filename.endswith('.plan'):
            file_path = os.path.join(directory_path, filename)
            with open(file_path, 'r') as file:
                text = file.read()

            fragments = parse_fragments(text)
            all_trees = {}
            for fragment_id, fragment_content in fragments.items():
                tree = process_fragment_content(fragment_content)
                all_trees[fragment_id] = tree

            current_tree = all_trees[0]
            while True:
                new_tree, has_remote_source = replace_remote_sources(current_tree, all_trees)
                if not has_remote_source:
                    break
                current_tree = new_tree

            for _, word, contents in current_tree:
                all_operators.add(word)
                table = extract_table_name(contents)
                if table:
                    all_tables.add(table)

    return sorted(list(all_operators)), sorted(list(all_tables))


def create_mappings(operators, tables):
    operator_mapping = {operator: index for index, operator in enumerate(operators)}
    table_mapping = {table: index for index, table in enumerate(tables)}
    return operator_mapping, table_mapping


def create_vector(operator_index, table_index, vector_length):
    vector = [0] * vector_length
    vector[operator_index] = 1
    vector[-2] = table_index
    return vector


def convert_to_nested_tuple(tree, operator_mapping, table_mapping):
    vector_length = len(operator_mapping) + 1

    def convert_node(node_index):
        level, word, contents = tree[node_index]
        operator_index = operator_mapping[word]
        table = extract_table_name(contents)
        table_index = table_mapping.get(table, -1)
        node_vector = create_vector(operator_index, table_index, vector_length)

        children = []
        for i in range(node_index + 1, len(tree)):
            child_level, child_word, child_contents = tree[i]
            if child_level == level + 1:
                children.append(i)
            elif child_level <= level:
                break

        if len(children) == 0:
            return tuple(node_vector), None, None
        elif len(children) == 1:
            return tuple(node_vector), convert_node(children[0]), None
        else:
            return tuple(node_vector), convert_node(children[0]), convert_node(children[1])

    return convert_node(0)


def convert_to_binary_tuple(tuple_tree):
    if isinstance(tuple_tree, tuple):  # This is a vector node
        vector = tuple_tree[0]
        if tuple_tree[1] is None:  # Leaf node
            zero_vector = [0] * (len(vector) - 1) + [1]  # All zeros with last element as 1
            return (vector, (tuple(zero_vector),), (tuple(zero_vector),))
        elif tuple_tree[2] is None:  # Node with one child
            zero_vector = [0] * (len(vector) - 1) + [1]  # All zeros with last element as 1
            return (vector, convert_to_binary_tuple(tuple_tree[1]), (tuple(zero_vector),))
        else:  # Node with two or more children
            return (vector, convert_to_binary_tuple(tuple_tree[1]), convert_to_binary_tuple(tuple_tree[2]))


def process_file(file_path, operator_mapping, table_mapping):
    with open(file_path, 'r') as file:
        text = file.read()

    fragments = parse_fragments(text)

    all_trees = {}
    for fragment_id, fragment_content in fragments.items():
        tree = process_fragment_content(fragment_content)
        all_trees[fragment_id] = tree

    current_tree = all_trees[0]
    while True:
        new_tree, has_remote_source = replace_remote_sources(current_tree, all_trees)
        if not has_remote_source:
            break
        current_tree = new_tree

    nested_tree = convert_to_nested_tuple(current_tree, operator_mapping, table_mapping)
    binary_tree = convert_to_binary_tuple(nested_tree)

    return binary_tree


def print_binary_tree(tree, indent=""):
    vector, children = tree
    if children:
        for child in children:
            print_binary_tree(child, indent + "  ")


def process_trino_plans(directory_path, datalake):
    # First, collect all unique operators and tables
    all_operators, all_tables = collect_operators_and_tables(directory_path)

    # Create unified mappings
    operator_mapping, table_mapping = create_mappings(all_operators, all_tables)

    # Now process each file
    tree_dict = {}
    for filename in os.listdir(directory_path):
        if filename.endswith('.sql.plan'):
            file_path = os.path.join(directory_path, filename)
            binary_tree = process_file(file_path, operator_mapping, table_mapping)
            tree_dict[filename.split('.')[0]] = binary_tree
    return tree_dict, None, None


# !/usr/bin/env python3
import os
import re
import csv


def deduplicate_in_groups(data_list, group_size=4):
    """
    对 data_list 中的元素每 group_size 个为一组做去重：
      - 如果一组内所有元素都相同，则只保留一个；
      - 否则，该组内的元素保持不变。
    对于不足 group_size 个的剩余部分，直接保留原样。
    """
    deduped = []
    n = len(data_list)
    for i in range(0, n, group_size):
        group = data_list[i:i + group_size]
        if len(group) == group_size and group.count(group[0]) == group_size:
            deduped.append(group[0])
        else:
            deduped.extend(group)
    return deduped


def extract_engine_config(log_file_path):
    """
    扫描 log 文件中所有形如：
       query.max-memory=20GB
       query.max-memory-per-node=18GB
       memory.heap-headroom-per-node=18GB
       query.max-total-memory-per-node=18GB
       task.concurrency=16
       task.max-worker-threads=16
       node-scheduler.max-splits-per-node=256
    的行，提取引擎配置参数。
    """
    desired_keys = [
        "query.max-memory",
        "query.max-memory-per-node",
        "memory.heap-headroom-per-node",
        "query.max-total-memory-per-node",
        "task.concurrency",
        "task.max-worker-threads",
        "node-scheduler.max-splits-per-node"
    ]

    config_pattern = re.compile(
        r"^(query\.max-memory|query\.max-memory-per-node|memory\.heap-headroom-per-node|query\.max-total-memory-per-node|task\.concurrency|task\.max-worker-threads|node-scheduler\.max-splits-per-node)\s*=\s*(\S+)"
    )

    config_groups = []
    current_config = {}

    with open(log_file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            m = config_pattern.match(line)
            if m:
                key, value = m.group(1), m.group(2)
                # 内存相关配置去掉 GB 后缀
                if key in ["query.max-memory", "query.max-memory-per-node", "memory.heap-headroom-per-node",
                           "query.max-total-memory-per-node"]:
                    if value.endswith("GB"):
                        value = value[:-2]
                try:
                    num = int(float(value))
                except Exception as e:
                    print(f"无法将配置 {key} 的值 {value} 转换为数字: {e}")
                    continue
                current_config[key] = num

                # 当收集到所有需要的配置后，构成一个配置组
                if all(k in current_config for k in desired_keys):
                    group = [current_config[k] for k in desired_keys]
                    config_groups.append(group)
                    current_config = {}

    config_groups_deduped = deduplicate_in_groups(config_groups, group_size=4)
    return config_groups_deduped


def extract_datalake_config(log_file_path):
    """
    扫描 log 文件中所有形如 'set session xxx=yyy' 的行，提取数据湖配置参数。
    类似引擎配置的处理方式，每当遇到数据湖相关配置时进行收集。
    数据湖配置的识别标准：set session 后的内容包含 delta/iceberg/hudi 关键字。
    将True/False映射为1/0，并进行去重处理。
    返回配置组列表和配置键名列表。
    """
    session_pattern = re.compile(r'set\s+session\s+([^=\s]+)\s*=\s*([^;\s]+)', re.IGNORECASE)

    datalake_config_groups = []
    all_session_keys = set()
    current_session_configs = {}

    # 第一遍扫描：确定所有可能的数据湖配置键
    with open(log_file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    for line in content.split('\n'):
        line = line.strip()
        m = session_pattern.search(line)
        if m:
            key, value = m.group(1).strip(), m.group(2).strip()

            # 检查是否为数据湖相关的配置（key或value包含delta/iceberg/hudi关键字）
            key_lower = key.lower()
            value_lower = value.lower()
            if any(keyword in key_lower or keyword in value_lower
                   for keyword in ['delta', 'iceberg', 'hudi']):
                all_session_keys.add(key)

    # 如果没有找到数据湖相关配置，直接返回空结果
    if not all_session_keys:
        return [], []

    sorted_keys = sorted(all_session_keys)  # 固定顺序

    # 第二遍扫描：提取配置组
    with open(log_file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            m = session_pattern.search(line)
            if m:
                key, value = m.group(1).strip(), m.group(2).strip()

                # 只处理数据湖相关配置
                if key not in all_session_keys:
                    continue

                # 去掉可能的引号
                if value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                elif value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]

                # 转换值：True/False -> 1/0，数字保持数字，其他转为字符串hash值
                if value.lower() == 'true':
                    numeric_value = 1
                elif value.lower() == 'false':
                    numeric_value = 0
                else:
                    try:
                        if '.' in value:
                            numeric_value = float(value)
                        else:
                            numeric_value = int(value)
                    except ValueError:
                        # 对于其他字符串值，使用简单的hash映射
                        numeric_value = hash(value) % 1000  # 限制在0-999范围内

                current_session_configs[key] = numeric_value

                # 当收集到所有需要的配置键时，构成一个配置组
                if all(k in current_session_configs for k in sorted_keys):
                    config_group = [current_session_configs[k] for k in sorted_keys]
                    datalake_config_groups.append(config_group)
                    current_session_configs = {}  # 重置，准备收集下一个组

    # 对数据湖配置进行去重处理
    datalake_config_groups_deduped = deduplicate_in_groups(datalake_config_groups, group_size=4)

    return datalake_config_groups_deduped, sorted_keys


def combine_configs(engine_configs, datalake_configs):
    """
    将引擎配置和数据湖配置拼接成完整的配置。

    Args:
        engine_configs: 引擎配置列表，每个元素是一个配置组列表
        datalake_configs: 数据湖配置列表，每个元素是一个配置组列表

    Returns:
        完整配置列表，每个元素是引擎配置+数据湖配置的拼接
    """
    if not engine_configs and not datalake_configs:
        return []

    # 确保两个配置列表长度一致
    max_len = max(len(engine_configs) if engine_configs else 0,
                  len(datalake_configs) if datalake_configs else 0)

    combined_configs = []
    for i in range(max_len):
        # 获取当前索引的引擎配置，如果超出范围则使用最后一个或空列表
        if engine_configs:
            if i < len(engine_configs):
                engine_config = engine_configs[i]
            else:
                engine_config = engine_configs[-1]  # 使用最后一个配置
        else:
            engine_config = []

        # 获取当前索引的数据湖配置，如果超出范围则使用最后一个或空列表
        if datalake_configs:
            if i < len(datalake_configs):
                datalake_config = datalake_configs[i]
            else:
                datalake_config = datalake_configs[-1]  # 使用最后一个配置
        else:
            datalake_config = []

        # 拼接配置
        combined_config = list(engine_config) + list(datalake_config)
        combined_configs.append(combined_config)

    return combined_configs


def extract_timestamps_from_log_new(log_file_path):
    """
    从 log 文件中搜索所有基准测试和数据湖类型的组合，支持：
    基准测试：tpcds, tpch, job, ssb, ssb_flat
    数据湖类型：delta, hudi, iceberg
    规模：10g, 100g 等
    引擎：presto, trino

    返回按数据湖类型分组的时间戳列表。
    """
    # 构建更全面的正则表达式，支持所有基准测试和数据湖类型组合
    benchmark_types = ['tpcds', 'tpch', 'job', 'ssb', 'ssb_flat']
    datalake_types = ['delta', 'hudi', 'iceberg']
    scales = ['10g', '100g', '1g', '30g']  # 支持不同规模
    engines = ['presto', 'trino', '']  # 可能有或没有引擎后缀

    # 构建所有可能的模式组合
    patterns = []
    for benchmark in benchmark_types:
        for datalake in datalake_types:
            for scale in scales:
                for engine in engines:
                    if engine:
                        pattern = f"{benchmark}_{datalake}_{scale}_{engine}"
                    else:
                        pattern = f"{benchmark}_{datalake}_{scale}"
                    patterns.append(pattern)

    # 创建正则表达式
    pattern_str = '|'.join(patterns)
    ts_pattern = re.compile(f"((?:{pattern_str})/timestamp=(\\d+))")

    # 初始化各数据湖类型的时间戳集合
    delta_ts = set()
    hudi_ts = set()
    iceberg_ts = set()

    with open(log_file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            m = ts_pattern.search(line)
            if m:
                full_str = m.group(1)  # 完整匹配的字符串
                ts_value = m.group(2)  # 时间戳值

                # 根据数据湖类型分类
                if '_delta_' in full_str:
                    delta_ts.add((full_str, ts_value))
                elif '_hudi_' in full_str:
                    hudi_ts.add((full_str, ts_value))
                elif '_iceberg_' in full_str:
                    iceberg_ts.add((full_str, ts_value))

    # 按时间戳值排序
    delta_ts_list = sorted(delta_ts, key=lambda x: int(x[1]))
    hudi_ts_list = sorted(hudi_ts, key=lambda x: int(x[1]))
    iceberg_ts_list = sorted(iceberg_ts, key=lambda x: int(x[1]))

    return delta_ts_list, hudi_ts_list, iceberg_ts_list


def extract_latency_for_type_new(ts_list, examples_base):
    """
    针对给定的 ts_list（每个元素为 (full_match, ts_value)），
    从对应目录 examples_base/{full_match}/trino_runtimes_raw.csv 中读取 CSV 文件。
    """
    latency_dict = {}
    valid_ts_count = 0

    for full_str, ts in ts_list:
        # 尝试不同的CSV文件名
        possible_csv_names = ["trino_runtimes_raw.csv"]
        csv_path = None

        for csv_name in possible_csv_names:
            potential_path = os.path.join(examples_base, full_str, csv_name)
            if os.path.isfile(potential_path) and os.path.getsize(potential_path) > 0:
                csv_path = potential_path
                break

        if not csv_path:
            continue

        valid_records = []
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if len(row) < 6:
                    continue

                query_name = row[0].strip().replace('.sql', '').replace('query', 'q').replace('_a', 'a').replace('_b',
                                                                                               'b')
                record_type = row[1].strip().lower()
                status = row[5].strip().upper()

                if record_type != "run" or status != "FINISHED":
                    continue

                try:
                    runtime = int(row[3].strip().replace('"', ''))
                    analysis = int(row[4].strip().replace('"', ''))
                    latency = runtime + analysis
                except Exception as e:
                    print(f"解析 CSV 行失败: {row}, 错误：{e}")
                    continue

                valid_records.append((query_name, latency))

        if not valid_records:
            continue

        valid_ts_count += 1
        for query_name, latency in valid_records:
            if query_name not in latency_dict:
                latency_dict[query_name] = []
            latency_dict[query_name].append(latency)

    return latency_dict


def process_new_trino_experiment(log_file_path, examples_base, datalake):
    """
    处理新的Trino实验，支持所有基准测试和数据湖类型组合，
    提取引擎配置和数据湖配置，并将它们合并成完整配置。
    """
    # 提取引擎配置
    engine_config_groups = extract_engine_config(log_file_path)

    # 提取数据湖配置
    datalake_config_groups, datalake_keys = extract_datalake_config(log_file_path)

    # 提取时间戳
    delta_ts_list, hudi_ts_list, iceberg_ts_list = extract_timestamps_from_log_new(log_file_path)

    # print("Delta timestamps:", delta_ts_list)
    # print("Hudi timestamps:", hudi_ts_list)
    # print("Iceberg timestamps:", iceberg_ts_list)
    # print("Datalake config keys:", datalake_keys)

    # 提取各数据湖类型的延迟数据
    latency_dict_delta = extract_latency_for_type_new(delta_ts_list, examples_base + '/' + datalake + '/')
    latency_dict_hudi = extract_latency_for_type_new(hudi_ts_list, examples_base + '/' + datalake + '/')
    latency_dict_iceberg = extract_latency_for_type_new(iceberg_ts_list, examples_base + '/' + datalake + '/')

    # 根据延迟数据调整配置组
    def adjust_config_groups(latency_dict, config_groups):
        if latency_dict:
            sample_key = sorted(latency_dict.keys())[0]
            valid_count = len(latency_dict[sample_key])
            if len(config_groups) > valid_count:
                return config_groups[:valid_count]
            else:
                return config_groups
        else:
            return config_groups

    engine_config_delta = adjust_config_groups(latency_dict_delta, engine_config_groups)
    engine_config_hudi = adjust_config_groups(latency_dict_hudi, engine_config_groups)
    engine_config_iceberg = adjust_config_groups(latency_dict_iceberg, engine_config_groups)

    # 对数据湖配置也进行调整
    datalake_config_delta = adjust_config_groups(latency_dict_delta, datalake_config_groups)
    datalake_config_hudi = adjust_config_groups(latency_dict_hudi, datalake_config_groups)
    datalake_config_iceberg = adjust_config_groups(latency_dict_iceberg, datalake_config_groups)

    # 合并引擎配置和数据湖配置
    complete_config_delta = combine_configs(engine_config_delta, datalake_config_delta)
    complete_config_hudi = combine_configs(engine_config_hudi, datalake_config_hudi)
    complete_config_iceberg = combine_configs(engine_config_iceberg, datalake_config_iceberg)

    # 返回所有数据湖类型的完整数据
    result = {
        "delta": {
            "complete_configs": complete_config_delta,
            "engine_configs": engine_config_delta,  # 保留原始的引擎配置供参考
            "datalake_configs": datalake_config_delta,  # 保留原始的数据湖配置供参考
            "datalake_keys": datalake_keys,  # 数据湖配置的键名列表
            "latency": latency_dict_delta
        },
        "hudi": {
            "complete_configs": complete_config_hudi,
            "engine_configs": engine_config_hudi,
            "datalake_configs": datalake_config_hudi,
            "datalake_keys": datalake_keys,
            "latency": latency_dict_hudi
        },
        "iceberg": {
            "complete_configs": complete_config_iceberg,
            "engine_configs": engine_config_iceberg,
            "datalake_configs": datalake_config_iceberg,
            "datalake_keys": datalake_keys,
            "latency": latency_dict_iceberg
        }
    }
    return result


def process_trino_experiment(log_file, datalake, examples_base):
    """
    处理Trino实验，支持指定的数据湖类型。

    Args:
        log_file: 日志文件路径
        datalake: 数据湖类型 ('delta', 'hudi', 'iceberg')
        examples_base: 示例基础路径

    Returns:
        Tuple of (complete_configs, latency) 为指定的数据湖类型
        complete_configs: 引擎配置和数据湖配置的拼接结果
        latency: 延迟数据字典
    """
    results = process_new_trino_experiment(log_file, examples_base, datalake)

    if datalake not in results:
        raise ValueError(f"Unsupported datalake type: {datalake}. Supported types: {list(results.keys())}")

    return (results[datalake]["complete_configs"],
            results[datalake]["latency"])


# 保持与旧版本兼容的函数，但使用新的处理方式
def extract_config(log_file_path):
    """
    为了保持向后兼容，保留这个函数，但内部调用新的引擎配置提取函数。
    """
    return extract_engine_config(log_file_path)


def extract_timestamps_from_log(log_file_path):
    """
    为了保持向后兼容，保留这个函数，但内部调用新的时间戳提取函数。
    返回格式调整为与原版本兼容，现在返回三种数据湖类型。
    """
    delta_ts_list, hudi_ts_list, iceberg_ts_list = extract_timestamps_from_log_new(log_file_path)

    # 为了兼容性，返回iceberg, delta, hudi的列表
    return iceberg_ts_list, delta_ts_list, hudi_ts_list


def extract_latency_for_type(ts_list, examples_base):
    """
    为了保持向后兼容，保留这个函数，但内部调用新的延迟提取函数。
    """
    return extract_latency_for_type_new(ts_list, examples_base)


def process_new_trino_experiment_old(log_file_path, examples_base):
    """
    为了保持向后兼容的旧接口函数。
    """
    results = process_new_trino_experiment(log_file_path, examples_base, "trino")

    # 返回格式调整为与原版本兼容，包含所有三种数据湖类型
    return {
        "iceberg": {
            "configs": results["iceberg"]["complete_configs"],
            "latency": results["iceberg"]["latency"]
        },
        "delta": {
            "configs": results["delta"]["complete_configs"],
            "latency": results["delta"]["latency"]
        },
        "hudi": {
            "configs": results["hudi"]["complete_configs"],
            "latency": results["hudi"]["latency"]
        }
    }