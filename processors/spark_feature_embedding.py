from datetime import datetime
from typing import Dict, List, Any

import numpy as np
from torch import nn
from collections import defaultdict, deque


#=================================TreeConvolution==================
class TreeNode:
    def __init__(self, operator, index, symbol):
        self.operator = operator
        self.index = index
        self.height = 0
        self.symbol = symbol
        self.left = None
        self.val = None
        self.right = None
        self.table_name = None
        self.join_table_column = []
        self.project_table_column = []
        self.aggregate_table_column = []
        self.scan_table_column = []
        self.sort_table_column = []
        self.encoding = None
        self.limit = 0

def build_tree(operators_with_index, all_operators, all_tables):
    if not operators_with_index:
        return None

    root = operators_with_index[0]

    for i in range(1, len(operators_with_index)):
        node = operators_with_index[i]
        # print(node.val, node.index)
        # 查找可能的父节点
        for j in range(i - 1, -1, -1):
            potential_parent = operators_with_index[j]
            if potential_parent.index == node.index - 3:
                if not potential_parent.left:
                    potential_parent.left = node
                else:
                    potential_parent.right = node
                break

    # 为每个节点添加编码
    for node in operators_with_index:
        node.encoding = create_encoding(node, all_operators, all_tables)
    # 确保每个节点都有两个孩子
    ensure_two_children(root, len(all_operators))

    return root

def create_encoding(node, all_operators, all_tables):
    encoding = [0] * (len(all_operators) + 2)
    encoding[list(all_operators.keys()).index(node.operator)] = 1
    if node.operator == "RelationV2" and node.table_name:
        encoding[-2] = list(all_tables).index(node.table_name) + 1  # +1 to avoid 0
    return encoding
def ensure_two_children(node, num_operators):
    if node is None:
        return
    if node.left is not None and node.right is None:
        dummy_encoding = [0] * (num_operators + 2)
        dummy_encoding[-1] = 1  # 设置最后一位为1
        node.right = TreeNode("Dummy", -1, "")
        node.right.encoding = dummy_encoding

    ensure_two_children(node.left, num_operators)
    ensure_two_children(node.right, num_operators)
def tree_to_tuple(node):
    if node is None:
        return None
    return (
        tuple(node.encoding),
        tree_to_tuple(node.left),
        tree_to_tuple(node.right)
    )

def print_tree(node, level=0):
    if node:
        if "Aggregate" in node.operator:
            print("  " * level + f"{node.operator}||{node.val}||{node.aggregate_table_column}")
        elif "Join" in node.operator:
            print("  " * level + f"{node.operator}||{node.val}||{node.join_table_column}")
        elif "Project" in node.operator:
            print("  " * level + f"{node.operator}||{node.val}||{node.project_table_column}")
        # elif "Scan" in node.operator:
        #     print("  " * level + f"{node.operator}||{node.val}||{node.scan_table_column}")
        elif "Sort" in node.operator:
            print("  " * level + f"{node.operator}||{node.val}||{node.sort_table_column}")
        else:
            print("  " * level + f"{node.operator}||{node.val}")
        print_tree(node.left, level + 1)
        print_tree(node.right, level + 1)


def process_physical_plan_index(plan,table_columns):
    operators_with_index = []
    lines = plan.split('\n')
    lines_num = len(lines)
    for i, line in enumerate(lines):
        if line.startswith("Sort "):
            node = TreeNode("Sort", -3, "")
            columns = [col.split("#")[0].replace('[','') for col in line.split() if "#" in col]
            node.sort_table_column = columns
            operators_with_index.append(node)
        elif line.startswith("Aggregate "):
            node = TreeNode("Aggregate", -3, "")
            operators_with_index.append(node)
        elif line.startswith("GlobalLimit "):
            node = TreeNode("GlobalLimit", -3, "")
            node.limit = int(line.split('Limit')[1].strip())
            operators_with_index.append(node)
        elif line.startswith("Filter "):
            node = TreeNode("Predicate", -3, "")
            columns = [col.split("#")[0].replace('[', '') for col in line.split() if "#" in col]
            node.scan_table_column = columns
            operators_with_index.append(node)
        elif line.startswith("RelationV2"):
            node = TreeNode("Scan", -3, "")
            columns = [col.split("#")[0].replace('RelationV2[', '') for col in line.split() if "#" in col]
            node.scan_table_column = columns
            operators_with_index.append(node)

        plus_minus_index = line.find("+-")
        colon_minus_index = line.find(":-")

        if plus_minus_index != -1 or colon_minus_index != -1:
            if plus_minus_index != -1 and (colon_minus_index == -1 or plus_minus_index < colon_minus_index):
                symbol_index = plus_minus_index
                symbol = "+-"
            else:
                symbol_index = colon_minus_index
                symbol = ":-"

            # operator_match = re.search(r'\b(\w+)', line[symbol_index + 2:])
            operator_match = re.search(r'\b(\w+)(?:\s+(\w+))?', line[symbol_index + 2:])
            if operator_match:
                operator = operator_match.group(1)
                node = TreeNode(operator, symbol_index, symbol)
                # 如果是BatchScan，寻找表名
                if operator == "RelationV2":
                    node.operator = "Scan"
                    target_string = "spark_catalog."
                    if target_string in lines[i]:

                        def extract_list(text):
                            # 使用正则匹配 RelationV2[...] 内的内容
                            match = re.search(r'RelationV2\s*\[(.*?)\]', text)
                            if match:
                                bracket_content = match.group(1)
                                # 按逗号分割后去除前后空格
                                data_cols = [col.strip() for col in bracket_content.split(',')]
                                # 如果希望只保留列名，去掉 '#...' 后缀：
                                data_cols = [col.split('#')[0] for col in data_cols]
                                return data_cols
                            else:
                                return []

                        # 将提取到的列列表保存到节点属性中
                        node.scan_table_column = extract_list(lines[i])
                        node.operator = "Scan"
                        # 提取目标字符串后面的部分，例如 "name"，作为 table 的名称
                        table_part = lines[i].split(target_string)[-1]
                        # 取第一个单词作为 table 名称（如果后面还有其他内容则忽略）
                        node.table_name = table_part.strip().split()[0]
                elif operator == "Filter":

                    def process_predicate_tree(predicate, node_index):
                        op = "Predicate"

                        def remove_between_then_end(tokens):
                            result = []
                            skip_mode = False
                            i = 0

                            while i < len(tokens):
                                token = tokens[i]

                                # 处理 CASE WHEN THEN 等关键词
                                if token.lower() == 'case':
                                    i += 1
                                    continue
                                if token.lower() == 'when':
                                    i += 1
                                    continue
                                if token.lower() == 'then':
                                    skip_mode = True
                                    i += 1
                                    continue
                                if token.lower() == 'end':
                                    skip_mode = False
                                    i += 1
                                    continue

                                # 正常处理 token
                                if not skip_mode:
                                    result.append(token)
                                i += 1
                            return result

                        def insert_token(tokens):
                            i = 0
                            while i < len(tokens):
                                token = tokens[i]
                                if (token.lower() == 'or' and
                                        i > 0 and i + 1 < len(tokens) and
                                        (tokens[i - 1] != ')' or tokens[i + 1] != '(')):

                                    # 向前查找并添加左括号
                                    count = 0
                                    for j in range(i - 1, -1, -1):
                                        if tokens[j] == ')':
                                            count += 1
                                        if tokens[j] == '(':
                                            if count == 0:
                                                tokens.insert(j + 1, '(')
                                                i += 1  # 更新当前位置
                                                break
                                            count -= 1
                                    # 在OR前添加右括号
                                    tokens.insert(i, ')')
                                    i += 1

                                    # 在OR后添加左括号
                                    tokens.insert(i + 1, '(')

                                    # 向后查找并添加右括号
                                    count = 0
                                    j = i
                                    while j < len(tokens):
                                        if tokens[j] == '(':
                                            count += 1
                                        if tokens[j] == ')':
                                            if count == 0:
                                                tokens.insert(j + 1, ')')
                                                break
                                            count -= 1
                                        j += 1

                                i += 1

                            return tokens
                        def build_tree(tokens, tree_index):
                            # 先处理tokens
                            tokens = remove_between_then_end(tokens)
                            root = TreeNode(op, tree_index, "")
                            stack = [root]
                            current = root

                            i = 0
                            parent = None
                            while i < len(tokens):
                                token = tokens[i]
                                if token == '(':
                                    new_node = TreeNode(op, current.index + 3, "")
                                    if current.left is None:
                                        current.left = new_node
                                    else:
                                        current.right = new_node
                                    parent = current
                                    current = new_node
                                    stack.append(current)
                                    i += 1

                                elif token == ')':
                                    stack.pop()
                                    if stack:
                                        parent = current
                                        current = stack[-1]
                                    i += 1

                                elif token in ['>', '<', '>=', '<=', '='] and tokens[i+1] == 'CheckOverflow':
                                    # 存储当前的比较运算符
                                    comp_op = token

                                    # 跳过当前token后的所有括号,直到找到第一个形如 word#number 的token
                                    j = i + 1
                                    paren_count = 0

                                    while j < len(tokens):
                                        if tokens[j] == '(':
                                            paren_count += 1
                                        elif tokens[j] == ')':
                                            paren_count -= 1
                                        elif '#' in tokens[j] and paren_count <= 0:
                                            # 找到目标token,更新current.val并退出循环
                                            if current.val is None:
                                                current.val = comp_op + ' ' + tokens[j]
                                            else:
                                                current.val += ' ' + comp_op + ' ' + tokens[j]
                                            break
                                        j += 1
                                    # 直接跳到找到的位置之后
                                    i = j + 1
                                else:
                                    if current.val is None:
                                        current.val = token
                                    else:
                                        current.val += ' ' + token
                                    i += 1

                            return root
                        
                        def process_tree(node):
                            if node is None:
                                return
                            # --- inside process_tree(node) ---

                            if node.val is not None:
                                val_name = "AND" if "AND" in node.val else "OR"
                                op_name  = "AND" if "AND" in node.val else "OR"
                                if ("AND" in node.val or "OR" in node.val) and "=" not in node.val:
                                    node.operator = val_name

                                    prefixes = {
                                        " IN": "IN",
                                        "StartsWith": "StartsWith,",
                                        "EndsWith": "EndsWith,",
                                        "Contains": "Contains,",
                                        "LIKE": ""
                                    }

                                    for keyword, prefix in prefixes.items():
                                        if keyword in node.val:

                                            if keyword == " IN" and "AND" in node.val:
                                                pattern = r'(\w+#\d+L? IN)(?:\s*AND)?|(?:AND\s*)?(\w+#\d+L? IN)'
                                                match = re.search(pattern, node.val)
                                                prefix = (match.group(1) or match.group(2) + " ") if match else prefix
                                                prefix = prefix + " "

                                            elif keyword == " IN" and "OR" in node.val:
                                                pattern = r'(\w+#\d+L? IN)(?:\s*OR)?|(?:OR\s*)?(\w+#\d+L? IN)'
                                                match = re.search(pattern, node.val)
                                                prefix = (match.group(1) or match.group(2) + " ") if match else prefix
                                                prefix = prefix + " "

                                            elif keyword == "LIKE":
                                                # Robustly extract column and detect NOT LIKE
                                                # Examples this should handle:
                                                #   "AND o_comment#83 LIKE %abc%"
                                                #   "OR NOT o_comment#83 LIKE %xyz%"
                                                #   "o_comment#83 LIKE %p%"
                                                like_is_not = bool(re.search(r'\bNOT\s+\w+#\d+L?\s+LIKE\b', node.val)) or " NOT " in node.val
                                                m = re.search(r'(\w+#\d+L?)\s+LIKE\b', node.val)
                                                col = m.group(1) if m else None
                                                logic = "AND" if "AND" in node.val else "OR"
                                                if col:
                                                    like_op = "NOT_LIKE" if like_is_not else "LIKE"
                                                    # We store as "col,LOGIC,LIKE_OR_NOT_LIKE" and fix it below in the comma branch
                                                    new_node_val = f"{col},{logic},{like_op}"
                                                    new_node = TreeNode(op_name, node.index + 3, "")
                                                    new_node.val = new_node_val
                                                    if node.left is None:
                                                        node.left = new_node
                                                    else:
                                                        node.right = new_node
                                                # If we can't parse, just skip creating the child (avoid crashing)

                                            # Finalize current node as a logical op
                                            node.val = val_name
                                            node.operator = op_name

                                            # Prefix adjustments for children (unchanged from your logic, with guard)
                                            if node.left and isinstance(node.left.val, str) and 'AND' not in node.left.val:
                                                node.left.val = prefix + node.left.val if ',' in node.left.val else node.left.val
                                            if node.right and isinstance(node.right.val, str) and 'AND' not in node.right.val:
                                                node.right.val = node.right.val if ',' not in node.right.val else prefix + node.right.val
                                            break

                                if "exists" in node.val:
                                    node.operator = "exist"
                                    node.val = "exist"

                                prefixs = ''
                                if 'spark_catalog.' in lines[i + 1]:
                                    if 'spark_catalog.job_iceberg_10g.' in lines[i + 1]:
                                        table = lines[i + 1].split('spark_catalog.job_iceberg_10g.')[1]
                                        for p in table.split('_'):
                                            prefixs += 'ch' if p == 'char' else p[0]
                                    elif 'spark_catalog.ssb_iceberg_10g.' in lines[i + 1]:
                                        table = lines[i + 1].split('spark_catalog.ssb_iceberg_10g.')[1]
                                        for p in table.split('_'):
                                            prefixs += 'ch' if p == 'char' else p[0]

                                if prefixs != '':
                                    prefixs = prefixs + '_'

                                if ">=" in node.val:
                                    node.val = (node.val.split('>=')[0].split('#')[0], '>=', node.val.split('>=')[1].strip())
                                elif "<=" in node.val:
                                    node.val = (node.val.split('<=')[0].split('#')[0], '<=', node.val.split('<=')[1].strip())
                                elif ">" in node.val:
                                    node.val = (node.val.split('>')[0].split('#')[0], '>', node.val.split('>')[1].strip())
                                elif "<" in node.val:
                                    node.val = (node.val.split('<')[0].split('#')[0], '<', node.val.split('<')[1].strip())
                                elif "=" in node.val:
                                    node.val = (node.val.split('=')[0].split('#')[0], '=', node.val.split('=')[1].strip())
                                elif " IN" in node.val:
                                    node.val = (node.val.split('IN')[0].split('#')[0], 'IN', node.val.split('IN')[1].split(','))
                                elif "," in node.val:
                                    # ✅ Fix: our LIKE helper produced "col,LOGIC,LIKE|NOT_LIKE"
                                    parts = [p.strip() for p in node.val.split(',')]
                                    if len(parts) >= 3:
                                        col_part = prefixs + parts[0].split('#')[0]
                                        logic    = parts[1]
                                        like_op  = parts[2]   # "LIKE" or "NOT_LIKE"
                                        node.val = (col_part, logic, like_op)
                                    # else: leave it as-is (defensive)
                                elif "#" in node.val:
                                    node.val = prefixs + node.val.split('#')[0]

                                operators_with_index.append(node)

                            process_tree(node.left)
                            process_tree(node.right)

                        #     if node.val is not None:
                        #         val_name = "AND" if "AND" in node.val else "OR"
                        #         op_name = "AND" if "AND" in node.val else "OR"
                        #         if ("AND" in node.val or "OR" in node.val) and "=" not in node.val:
                        #             node.operator = node.val
                        #             prefixes = {
                        #                 " IN": "IN",
                        #                 "StartsWith": "StartsWith,",
                        #                 "EndsWith": "EndsWith,",
                        #                 "Contains": "Contains,",
                        #                 "LIKE": ""
                        #             }
                        #             for keyword, prefix in prefixes.items():
                        #                 if keyword in node.val:

                        #                     if keyword == " IN" and "AND" in node.val:
                        #                         pattern = r'(\w+#\d+L? IN)(?:\s*AND)?|(?:AND\s*)?(\w+#\d+L? IN)'
                        #                         match = re.search(pattern, node.val)
                        #                         prefix = (match.group(1) or match.group(2) + " ") if match else prefix
                        #                         prefix = prefix + " "
                        #                     elif keyword == " IN" and "OR" in node.val:
                        #                         pattern = r'(\w+#\d+L? IN)(?:\s*OR)?|(?:OR\s*)?(\w+#\d+L? IN)'
                        #                         match = re.search(pattern, node.val)
                        #                         prefix = (match.group(1) or match.group(2) + " ") if match else prefix
                        #                         prefix = prefix + " "
                        #                     elif keyword == "LIKE":
                        #                         string_predicate = node.val.split()[:3]
                        #                         print(string_predicate)
                        #                         temp = string_predicate[1]
                        #                         string_predicate[1] = string_predicate[0]
                        #                         string_predicate[0] = temp
                        #                         new_node_val = ','.join(string_predicate)
                        #                         new_node = TreeNode(op, node.index + 3, "")
                        #                         new_node.val = new_node_val
                        #                         if node.left is None:
                        #                             node.left = new_node
                        #                         else:
                        #                             node.right = new_node

                        #                     node.val = val_name
                        #                     node.operator = op_name

                        #                     if node.left:
                        #                         if 'AND' not in node.left.val:
                        #                             node.left.val = prefix + node.left.val if ',' in node.left.val else node.left.val
                        #                     if node.right:
                        #                         if 'AND' not in node.right.val:
                        #                             node.right.val = node.right.val if ',' not in node.right.val else prefix + node.right.val
                        #                     break

                        #         if "exists" in node.val:
                        #             node.operator = "exist"
                        #             node.val = "exist"

                        #         prefixs = ''
                        #         if 'spark_catalog.' in lines[i + 1]:
                        #             if 'spark_catalog.job_iceberg_10g.' in lines[i + 1]:
                        #                 # print(lines[i+1])
                        #                 table = lines[i + 1].split('spark_catalog.job_iceberg_10g.')[1]
                        #                 for prefix in table.split('_'):
                        #                     if prefix == 'char':
                        #                         prefixs = prefixs + 'ch'
                        #                     else:
                        #                         prefixs = prefixs + prefix[0]
                        #             elif 'spark_catalog.ssb_iceberg_10g.' in lines[i + 1]:
                        #                 table = lines[i + 1].split('spark_catalog.ssb_iceberg_10g.')[1]
                        #                 for prefix in table.split('_'):
                        #                     if prefix == 'char':
                        #                         prefixs = prefixs + 'ch'
                        #                     else:
                        #                         prefixs = prefixs + prefix[0]

                        #         if prefixs != '':
                        #             prefixs = prefixs + '_'
                        #         if ">=" in node.val:
                        #             node.val = (node.val.split('>=')[0].split('#')[0], '>=', node.val.split('>=')[1].strip())
                        #         elif "<=" in node.val:
                        #             node.val = (node.val.split('<=')[0].split('#')[0], '<=', node.val.split('<=')[1].strip())
                        #         elif ">" in node.val:
                        #             node.val = (node.val.split('>')[0].split('#')[0], '>', node.val.split('>')[1].strip())
                        #         elif "<" in node.val:
                        #             node.val = (node.val.split('<')[0].split('#')[0], '<', node.val.split('<')[1].strip())
                        #         elif "=" in node.val:
                        #             node.val = (node.val.split('=')[0].split('#')[0], '=', node.val.split('=')[1].strip())
                        #         elif " IN" in node.val:
                        #             node.val = (node.val.split('IN')[0].split('#')[0], 'IN', node.val.split('IN')[1].split(','))
                        #         elif "," in node.val:
                        #             node.val = (prefixs + node.val.split(',')[1].split('#')[0].strip(), node.val.split(',')[0], node.val.split(',')[2])
                        #         elif "#" in node.val:
                        #             node.val = prefixs + node.val.split('#')[0]

                        #         operators_with_index.append(node)
                        #     process_tree(node.left)
                        #     process_tree(node.right)
                        predicate = re.sub(
                            r'(avg|sum|min)\((.*?)\)|cast\((.*?) as (double|date|decimal\(.*?\))\)|isnotnull|NOT|isnull|\(.*?AND dynamicpruning#\d+ \[.*?\].*?\)|substring\((\w+#\d+),\s*\d+,\s*\d+\)|coalesce\((\w+#\d+L?),\s*[0-9]+\)',
                            lambda m: (m.group(2) if m.group(2)
                                       else m.group(3) if m.group(3)
                            else m.group(5) if 'substring' in m.group(0)
                            else m.group(6) if 'coalesce' in m.group(0)
                            else ''),
                            predicate
                        )
                        tokens = []
                        word = ''
                        for ch in predicate:
                            if ch == '(' or ch == ')' or ch == ' ':
                                if word:
                                    tokens.append(word)
                                    word = ''
                                if ch != ' ':
                                    tokens.append(ch)
                            else:
                                word += ch
                        if word:
                            tokens.append(word)
                        root = build_tree(tokens, node_index)

                        process_tree(root)
                        return root
                    next_line = lines[i + 1]
                    target_string = "spark_catalog."
                    if target_string in next_line:
                        start_index = next_line.index(target_string)
                        remaining_part = next_line[start_index:]
                        table_name = remaining_part.split('.')[1].split(',')[0].replace('`','')
                    predicate = lines[i].split(f'{symbol} Filter ')[1]
                    node = process_predicate_tree(predicate, node.index - 3)

                elif operator == "Join":
                    next_word = operator_match.group(2)
                    node.operator = node.operator + " " + next_word

                    def extract_key_pairs(input_string):
                        import re
                        # 更新后的正则，允许处理 cast(...) 包裹的情况
                        pattern = r'\(\s*(?:cast\()?\s*(\w+#\d+L?)\s*(?:\s+as\s+\w+\))?\s*=\s*(?:cast\()?\s*(\w+#\d+L?)\s*(?:\s+as\s+\w+\))?\s*\)'
                        matches = re.findall(pattern, input_string)
                        if not matches:
                            return []
                        key_pairs = [[m1.split("#")[0], '=', m2.split("#")[0]] for m1, m2 in matches]
                        # 这里只处理第一个键对
                        first_pair = key_pairs[0]
                        m1, m2 = matches[0]
                        first_table = None
                        second_table = None
                        # 从 start_index 开始遍历行（i 和 lines_num 需在外部定义）
                        prefix = ""
                        for idx in range(i, lines_num):
                            line = lines[idx]

                            # Iceberg processing
                            if 'spark_catalog.job_iceberg_10g.' in line:
                                prefix = "job"
                                # 如果该行中包含左侧列名且未提取到 first_table，则提取
                                if m1 in line and first_table is None:
                                    m = re.search(r'spark_catalog\.job_iceberg_10g\.([\w\.]+)', line)
                                    if m:
                                        first_table = m.group(1)
                                        continue
                                # 如果 first_table 已提取且行中包含右侧列名，则提取 second_table
                                elif second_table is None and m2 in line:
                                    m = re.search(r'spark_catalog\.job_iceberg_10g\.([\w\.]+)', line)
                                    if m:
                                        second_table = m.group(1)
                                if first_table and second_table:
                                    break
                            elif 'spark_catalog.ssb_iceberg_10g.' in line:
                                prefix = "ssb"
                                if first_table is None and m1 in line:
                                    m = re.search(r'spark_catalog\.ssb_iceberg_10g\.([\w\.]+)', line)
                                    if m:
                                        first_table = m.group(1)
                                elif second_table is None and m2 in line:
                                    m = re.search(r'spark_catalog\.ssb_iceberg_10g\.([\w\.]+)', line)
                                    if m:
                                        second_table = m.group(1)
                                if first_table and second_table:
                                    break
                            elif 'spark_catalog.tpcds_iceberg_10g.' in line:
                                prefix = "tpcds"
                                if first_table is None and m1 in line:
                                    m = re.search(r'spark_catalog\.tpcds_iceberg_10g\.([\w\.]+)', line)
                                    if m:
                                        first_table = m.group(1)
                                elif second_table is None and m2 in line:
                                    m = re.search(r'spark_catalog\.tpcds_iceberg_10g\.([\w\.]+)', line)
                                    if m:
                                        second_table = m.group(1)
                                if first_table and second_table:
                                    break
                            elif 'spark_catalog.tpch_iceberg_10g.' in line:
                                prefix = "tpch"
                                if first_table is None and m1 in line:
                                    m = re.search(r'spark_catalog\.tpch_iceberg_10g\.([\w\.]+)', line)
                                    if m:
                                        first_table = m.group(1)
                                elif second_table is None and m2 in line:
                                    m = re.search(r'spark_catalog\.tpch_iceberg_10g\.([\w\.]+)', line)
                                    if m:
                                        second_table = m.group(1)
                                if first_table and second_table:
                                    break

                            # Hudi processing
                            elif 'spark_catalog.job_hudi_10g.' in line:
                                prefix = "job"
                                if m1 in line and first_table is None:
                                    m = re.search(r'spark_catalog\.job_hudi_10g\.([\w\.]+)', line)
                                    if m:
                                        first_table = m.group(1)
                                        continue
                                elif second_table is None and m2 in line:
                                    m = re.search(r'spark_catalog\.job_hudi_10g\.([\w\.]+)', line)
                                    if m:
                                        second_table = m.group(1)
                                if first_table and second_table:
                                    break
                            elif 'spark_catalog.ssb_hudi_10g.' in line:
                                prefix = "ssb"
                                if first_table is None and m1 in line:
                                    m = re.search(r'spark_catalog\.ssb_hudi_10g\.([\w\.]+)', line)
                                    if m:
                                        first_table = m.group(1)
                                elif second_table is None and m2 in line:
                                    m = re.search(r'spark_catalog\.ssb_hudi_10g\.([\w\.]+)', line)
                                    if m:
                                        second_table = m.group(1)
                                if first_table and second_table:
                                    break
                            elif 'spark_catalog.tpcds_hudi_10g.' in line:
                                prefix = "tpcds"
                                if first_table is None and m1 in line:
                                    m = re.search(r'spark_catalog\.tpcds_hudi_10g\.([\w\.]+)', line)
                                    if m:
                                        first_table = m.group(1)
                                elif second_table is None and m2 in line:
                                    m = re.search(r'spark_catalog\.tpcds_hudi_10g\.([\w\.]+)', line)
                                    if m:
                                        second_table = m.group(1)
                                if first_table and second_table:
                                    break
                            elif 'spark_catalog.tpch_hudi_10g.' in line:
                                prefix = "tpch"
                                if first_table is None and m1 in line:
                                    m = re.search(r'spark_catalog\.tpch_hudi_10g\.([\w\.]+)', line)
                                    if m:
                                        first_table = m.group(1)
                                elif second_table is None and m2 in line:
                                    m = re.search(r'spark_catalog\.tpch_hudi_10g\.([\w\.]+)', line)
                                    if m:
                                        second_table = m.group(1)
                                if first_table and second_table:
                                    break

                        # 从 start_index 开始遍历行（i 和 lines_num 需在外部定义）
                        # prefix = ""
                        # for idx in range(i, lines_num):
                        #     line = lines[idx]
                        #
                        #     if 'spark_catalog.job_iceberg_10g.' in line:
                        #         prefix = "job"
                        #         # 如果该行中包含左侧列名且未提取到 first_table，则提取
                        #         if m1 in line and first_table is None:
                        #             m = re.search(r'spark_catalog\.job_iceberg_10g\.([\w\.]+)', line)
                        #             if m:
                        #                 first_table = m.group(1)
                        #                 continue
                        #         # 如果 first_table 已提取且行中包含右侧列名，则提取 second_table
                        #         elif second_table is None and m2 in line:
                        #
                        #             m = re.search(r'spark_catalog\.job_iceberg_10g\.([\w\.]+)', line)
                        #             if m:
                        #                 second_table = m.group(1)
                        #         if first_table and second_table:
                        #             break
                        #     elif 'spark_catalog.ssb_iceberg_10g.' in line:
                        #         prefix = "ssb"
                        #         if first_table is None and m1 in line:
                        #             m = re.search(r'spark_catalog\.ssb_iceberg_10g\.([\w\.]+)', line)
                        #             if m:
                        #                 first_table = m.group(1)
                        #         elif second_table is None and m2 in line:
                        #             m = re.search(r'spark_catalog\.ssb_iceberg_10g\.([\w\.]+)', line)
                        #             if m:
                        #                 second_table = m.group(1)
                        #         if first_table and second_table:
                        #             break
                        #     elif 'spark_catalog.tpcds_iceberg_10g.' in line:
                        #         prefix = "tpcds"
                        #         if first_table is None and m1 in line:
                        #             m = re.search(r'spark_catalog\.tpcds_iceberg_10g\.([\w\.]+)', line)
                        #             if m:
                        #                 first_table = m.group(1)
                        #         elif second_table is None and m2 in line:
                        #             m = re.search(r'spark_catalog\.tpcds_iceberg_10g\.([\w\.]+)', line)
                        #             if m:
                        #                 second_table = m.group(1)
                        #         if first_table and second_table:
                        #             break
                        #     elif 'spark_catalog.tpch_iceberg_10g.' in line:
                        #         prefix = "tpch"
                        #         if first_table is None and m1 in line:
                        #             m = re.search(r'spark_catalog\.tpch_iceberg_10g\.([\w\.]+)', line)
                        #             if m:
                        #                 first_table = m.group(1)
                        #         elif second_table is None and m2 in line:
                        #             m = re.search(r'spark_catalog\.tpch_iceberg_10g\.([\w\.]+)', line)
                        #             if m:
                        #                 second_table = m.group(1)
                        #         if first_table and second_table:
                        #             break

                        final_result = []
                        if first_table and second_table:
                            final_result.append(
                                [f"{prefix}_{first_table}.{first_pair[0]}", first_pair[1], f"{prefix}_{second_table}.{first_pair[2]}"])
                        else:
                            final_result = key_pairs

                        return final_result

                    node.join_table_column = extract_key_pairs(line)

                elif operator == "Project":
                    def extract_project_keys(input_string):
                        # 使用正则表达式匹配方括号内的内容
                        pattern = r'\[(.*?)\]'
                        match = re.search(pattern, input_string)
                        if match:
                            # 提取方括号内的内容
                            bracket_content = match.group(1)

                            # 将内容分割成列表
                            keys = [key.strip().split('#')[0] for key in bracket_content.split(',')]

                            return keys
                        else:
                            return []
                    node.project_table_column= extract_project_keys(line)
                elif operator == "Aggregate":
                    def extract_aggregate_keys(input_string):
                        # 使用正则表达式匹配所有方括号内的内容
                        pattern = r'\[(.*?)\]'
                        matches = re.findall(pattern, input_string)
                        result = []
                        for match in matches:
                            # 将内容分割成列表
                            keys = [key.strip().split('#')[0] for key in match.split(',')]
                            result.append(keys)
                        if not match:
                            return []
                        return result[0]
                    node.aggregate_table_column= extract_aggregate_keys(line)
                if "Limit" in operator:
                    node.limit = int(line.split('Limit')[1].strip())

                if operator != "Filter":
                    operators_with_index.append(node)
    return operators_with_index


def extract_physical_plan(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
        plan_match = re.search(r'== Physical Plan ==\n([\s\S]*)', content)
        if plan_match:
            return plan_match.group(1).strip()
    return None
#
def extract_optimized_logical_plan(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
        # plan_match_tpch = re.search(r'== Optimized Logical Plan ==\n([\s\S]*?)\n== Physical Plan ==', content)


        plan_match = re.search(r'=== Optimized Logical Plan ===\n([\s\S]*?)\n=== Physical Plan ===', content)
        # plan_match = re.search(
        #     r'(?P<eq>==|===) Optimized Logical Plan (?P=eq)\n([\s\S]*?)\n(?P=eq) Physical Plan (?P=eq)',
        #     content
        # )

        if plan_match:
            return plan_match.group(1).strip()
        else:
            plan_match = re.search(r'== Optimized Logical Plan ==\n([\s\S]*?)\n== Physical Plan ==', content)
            if plan_match:
                return plan_match.group(1).strip()

    return None
# def extract_optimized_logical_plan(file_path):
#     with open(file_path, 'r') as file:
#         content = file.read()
#     return content

def process_spark_plans(folder_path):
    all_operators = defaultdict(lambda: defaultdict(list))
    all_tables = set()
    tree_dict = {}
    # First pass: collect all_old operators and tables
    for filename in os.listdir(folder_path):
        if filename.endswith('_plan.txt'):
            file_path = os.path.join(folder_path, filename)
            physical_plan = extract_physical_plan(file_path)
            # physical_plan = extract_optimized_logical_plan(file_path)
            if physical_plan:
                operators_with_index = process_physical_plan_index(physical_plan,extract_table_names(folder_path,filename))
                for node in operators_with_index:
                    all_operators[node.operator][filename].append((node.index, node.symbol))
                    if node.table_name:
                        all_tables.add(node.table_name)
    # Second pass: build trees and create encodings
    tree_roots = {}
    for filename in os.listdir(folder_path):
        if filename.endswith('_plan.txt'):
            file_path = os.path.join(folder_path, filename)
            # physical_plan = extract_optimized_logical_plan(file_path)
            physical_plan = extract_physical_plan(file_path)
            if physical_plan:

                operators_with_index = process_physical_plan_index(physical_plan,extract_table_names(folder_path,filename))
                tree_root = build_tree(operators_with_index, all_operators, all_tables)

                tree_roots[filename.split('_plan')[0]] = tree_root

                # print(f"\nTree for file: {filename}")

                # Convert tree to tuple
                tree_tuple = tree_to_tuple(tree_root)
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
                tree_dict[filename.split('_plan')[0]] = convert_to_binary_tuple(tree_tuple)
    return tree_dict, tree_roots, None
mapping_table_replace = {
    "c":"customer"
}
mapping_table_tpch ={
        "c":"customer",
        "l":"lineitem",
        "n":"nation",
        "o":"orders",
        "p":"part",
        "ps":"partsupp",
        "r":"region",
        "s":"supplier",
        "d":"date",
        }

mapping_table_ssb = {
    "s":"supplier",
    "lo":"lineorder",
    "c":"customer",
    "p":"part",
    "d":"date"
}

mapping_table_tpcds = {
    "ca": "customer_address",
    "cc": "call_center",
    "cd": "customer_demographics",
    "cp": "catalog_page",
    "cr": "catalog_returns",
    "cs": "catalog_sales",
    "c": "customer",
    "d": "date_dim",
    "hd": "household_demographics",
    "i": "item",
    "ib": "income_band",
    "inv": "inventory",
    "p": "promotion",
    "r": "reason",
    "sm": "ship_mode",
    "s": "store",
    "sr": "store_returns",
    "ss": "store_sales",
    "t": "time_dim",
    "w": "warehouse",
    "web": "web_site",
    "wp": "web_page",
    "wr": "web_returns",
    "ws": "web_sales"
}
columns_num_job = {'company_type': 4, 'supplier': 20000, 'movie_info_idx': 1380035, 'person_info': 2963664, 'lineorder': 59986052, 'char_name': 3140339, 'info_type': 113, 'title': 2528312, 'role_type': 12, 'company_name': 234997, 'movie_keyword': 4523930, 'comp_cast_type': 4, 'name': 4167491, 'aka_title': 361472, 'keyword': 134170, 'movie_link': 29997, 'part': 800000, 'cast_info': 36244344, 'aka_name': 901343, 'complete_cast': 135086, 'movie_companies': 2609129, 'kind_type': 7, 'customer': 300000, 'movie_info': 14835720, 'link_type': 18, 'date': 2556}
columns_num_tpcds = {'catalog_returns': 1439749, 'customer': 500000, 'customer_address': 250000, 'call_center': 1439749, 'customer_demographics': 24, 'catalog_page': 12000, 'catalog_sales': 14329288, 'date_dim': 73049, 'household_demographics': 7200, 'item': 102000, 'income_band': 20, 'inventory': 133110000, 'promotion': 500, 'reason': 45, 'store': 102, 'ship_mode': 20, 'store_returns': 2775158, 'store_sales': 27504814, 'time_dim': 86400, 'warehouse': 10, 'web_site': 42, 'web_page': 200, 'web_returns': 686665, 'web_sales': 7195778}
columns_num_tpch = {'customer': 1500000, 'lineitem': 59986052, 'nation': 25, 'orders': 15000000, 'partsupp': 8000000, 'part': 2000000, 'region': 5, 'supplier': 100000}
columns_num_ssb = {'customer': 3000000, 'date': 2556, 'lineorder': 600037902, 'part': 1400000, 'supplier': 200000}


columns_num_tpch_short = {
"c": 1500000,
"l": 59986052,
"n": 25,
"o": 15000000,
"ps": 8000000,
"p": 2000000,
"r": 5,
"s": 100000
}

columns_num_job_short = {
    "ct": 4,
    "s": 20000,
    "mii": 1380035,
    "pi": 2963664,
    "lo": 59986052,
    "chn": 3140339,
    "it": 113,
    "t": 2528312,
    "rt": 12,
    "cn": 234997,
    "mk": 4523930,
    "cct": 4,
    "n": 4167491,
    "at": 361472,
    "k": 134170,
    "ml": 29997,
    "p": 800000,
    "ci": 36244344,
    "an": 901343,
    "cc": 135086,
    "mc": 2609129,
    "kt": 7,
    "c": 300000,
    "mi": 14835720,
    "lt": 18,
    "d": 2556,
}

columns_num_ssb_short = {
"c": 3000000,
"d": 2556,
"l": 600037902,
"p": 1400000,
"s": 200000,
}


mapping_table_job = {
    "s":"supplier",
    "lo":"lineorder",
    "c":"customer",
    "p":"part",
    "d":"date",
    "an":    "aka_name",        # aka_name：a + n
    "at":    "aka_title",       # aka_title：a + t
    "ci":    "cast_info",       # cast_info：c + i
    "chn":   "char_name",       # 用“chn”表示字符名称，避免与company_name冲突
    "cn": "company_name",    # 用“cname”明确表示公司名称
    "ct":    "company_type",    # company_type：c + t
    "cct":   "comp_cast_type",  # comp_cast_type：c + c + t
    "cc":    "complete_cast",   # complete_cast：c + c
    "it":    "info_type",       # info_type：i + t
    "kw":    "keyword",         # keyword：k + w
    "kt":    "kind_type",       # kind_type：k + t
    "lt":    "link_type",       # link_type：l + t
    "mc":    "movie_companies", # movie_companies：m + c
    "mi":    "movie_info",      # movie_info：m + i
    "mii":   "movie_info_idx",  # movie_info_idx：m + i + i
    "mk":    "movie_keyword",   # movie_keyword：m + k
    "ml":    "movie_link",      # movie_link：m + l
    "nm":    "name",            # name：n + m（避免与“an”混淆）
    "pi":    "person_info",     # person_info：p + i
    "rt":    "role_type",       # role_type：r + t
    "tl":    "title"            # title：t + l（这里区分于aka_title）
}


def extract_table_names(folder_path, filename):
    columns = []
    # 读取文件内容
    file_path = os.path.join(folder_path, filename)
    with open(file_path, 'r') as file:
        content = file.read()

    # 使用正则表达式查找所有的 "#" 符号
    matches = re.finditer(r'#', content)

    # 遍历每个匹配的位置
    mapped_table = {}
    for match in matches:
        # 获取 "#" 符号的位置
        hash_pos = match.start()

        # 向前查找表名
        start_pos = hash_pos - 1
        while start_pos >= 0 and (content[start_pos].isalpha() or content[start_pos] == '_'):
            start_pos -= 1

        # 提取表名
        table_column = content[start_pos + 1:hash_pos]
        columns.append(table_column)
        for col in columns:
            if "_" in col:
                if col.split("_")[0] not in mapping_table_ssb:
                    continue
                table = mapping_table_ssb[col.split("_")[0]]
                column = col.split("_")[1]
                if table not in mapped_table:
                    mapped_table[table] = {table + '.' + column}
                else:
                    mapped_table[table].add(table + '.' + column)
    # print(file_path)
    return mapped_table


import os
import re


def extract_all_table_names(folder_path):
    mapped_table = {}
    # 正则说明：
    #   - RelationV2\[(.*?)\] 捕获中括号内的内容（非贪婪模式），即列信息
    #   - .*\.(\w+) 捕获最后一个点后面的连续字母、数字或下划线（即表名）
    pattern = re.compile(r"RelationV2\[(.*?)\].*\.(\w+)")

    for filename in os.listdir(folder_path):
        if filename.endswith('.txt'):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r') as file:
                for line in file:
                    # 仅处理包含指定关键字的行
                    if "RelationV2[" in line:
                        match = pattern.search(line)
                        if match:
                            columns_str = match.group(1)
                            table = match.group(2)
                            prefix = ""
                            if "job_iceberg_10g" in line:
                                prefix = "job"
                            elif "tpcds_iceberg_10g" in line:
                                prefix = "tpcds"
                            elif "ssb_iceberg_10g" in line:
                                prefix = "ssb"
                            elif "tpch_iceberg_10g" in line:
                                prefix = "tpch"



                            table = prefix + "_" + table
                            columns = [col.strip().split('#')[0] for col in columns_str.split(',')]
                            if table not in mapped_table:
                                mapped_table[table] = set()
                            mapped_table[table].update(columns)
    return mapped_table


def extract_all_table_names_tpcds(folder_path):
    mapped_table = {}

    for filename in os.listdir(folder_path):
        if filename.endswith('.txt'):
            file_path = os.path.join(folder_path, filename)

            columns = []
            # 读取文件内容
            with open(file_path, 'r') as file:
                content = file.read()

            # 使用正则表达式查找所有的 "#" 符号
            matches = re.finditer(r'#', content)

            # 遍历每个匹配的位置

            for match in matches:
                # 获取 "#" 符号的位置
                hash_pos = match.start()

                # 向前查找表名
                start_pos = hash_pos - 1
                while start_pos >= 0 and (content[start_pos].isalpha() or content[start_pos] == '_'):
                    start_pos -= 1

                # 提取表名
                table_column = content[start_pos + 1:hash_pos]
                columns.append(table_column)

            for col in columns:
                if "_" in col:
                    if col.split("_")[0] not in mapping_table_ssb:
                        continue
                    table = mapping_table_ssb[col.split("_")[0]]
                    column = col.split("_")[1]
                    if table not in mapped_table:
                        mapped_table[table] = {table + '.' + column}
                    else:
                        mapped_table[table].add(table + '.' + column)

            # 将结果添加到结果字典中

    return mapped_table

def process_general_plans(folder_path, benchmark,schema):
    all_operators = defaultdict(lambda: defaultdict(list))
    all_tables = set()
    tree_dict = {}
    tree_rel = {}
    tree_roots = {}
    tree_height = {}
    mapping_tables = extract_all_table_names(
        "/home/jovyan/train/data/experiment_3/unify_plans/plans/original")
    print(mapping_tables)
    # First pass: collect all_old operators and tables
    for filename in sorted(os.listdir(folder_path)):

        if filename.endswith('_plan.txt') and (filename.startswith(benchmark) or schema == "cross"):
            print(f"start parsing {filename}")
            file_path = os.path.join(folder_path, filename)
            # physical_plan = extract_physical_plan(file_path)
            physical_plan = extract_optimized_logical_plan(file_path)
            if physical_plan:
                operators_with_index = process_physical_plan_index(physical_plan,extract_table_names(folder_path,filename))
                for node in operators_with_index:
                    all_operators[node.operator][filename].append((node.index, node.symbol))
                    if node.table_name:
                        all_tables.add(node.table_name)

                tree_root = build_tree(operators_with_index, all_operators, all_tables)
                tree_roots[filename.split("_plan")[0]] = tree_root
                # print_tree(tree_root)
                def build_ancestor_matrix(root):
                    # 使用前序遍历获取节点列表和索引映射
                    def preorder_traverse(node):
                        nonlocal index
                        if not node:
                            return
                        node_list.append(node)
                        node_to_index[node] = index
                        index += 1
                        preorder_traverse(node.left)
                        preorder_traverse(node.right)

                    node_list = []
                    node_to_index = {}
                    index = 0
                    preorder_traverse(root)

                    n = len(node_list)

                    # 初始化n*n的矩阵,所有元素为0
                    matrix = [[0] * n for _ in range(n)]

                    # 填充矩阵
                    for i, node in enumerate(node_list):
                        if node.left:
                            left_index = node_to_index[node.left]
                            matrix[i][left_index] = 1
                        if node.right:
                            right_index = node_to_index[node.right]
                            matrix[i][right_index] = 1

                    return matrix

                def get_matrix(root):
                    import torch
                    import numpy as np

                    def tree_to_adjacency_list(root):
                        adjacency_list = []
                        features = []
                        node_map = {}

                        def dfs(node, parent_idx):
                            if not node:
                                return

                            current_idx = len(features)
                            features.append([1])  # 使用节点值作为特征
                            node_map[node] = current_idx

                            if parent_idx is not None:
                                adjacency_list.append([parent_idx, current_idx])
                                adjacency_list.append([current_idx, parent_idx])  # 添加双向边

                            dfs(node.left, current_idx)
                            dfs(node.right, current_idx)

                        dfs(root, None)
                        return features, adjacency_list

                    def pad_2d_unsqueeze(x, max_len):
                        if x.dim() == 1:
                            x = x.unsqueeze(0)
                        return torch.nn.functional.pad(x, (0, 0, 0, max_len - x.size(0)))

                    def floyd_warshall_rewrite(adj):
                        N = adj.shape[0]
                        dist = np.where(adj, 1, np.inf)
                        np.fill_diagonal(dist, 0)
                        for k in range(N):
                            for i in range(N):
                                for j in range(N):
                                    dist[i, j] = min(dist[i, j], dist[i, k] + dist[k, j])
                        return dist

                    def process_graph(features, adjacency_list, max_node, rel_pos_max):
                        N = len(features)
                        x = pad_2d_unsqueeze(torch.tensor(features, dtype=torch.float), max_node)

                        attn_bias = torch.zeros([N + 1, N + 1], dtype=torch.float)

                        edge_index = torch.tensor(adjacency_list).t()

                        if len(edge_index) == 0:
                            shortest_path_result = np.array([[0]])
                            adj = torch.tensor([[0]]).bool()
                        else:
                            adj = torch.zeros([N, N], dtype=torch.bool)
                            adj[edge_index[0, :], edge_index[1, :]] = True

                            shortest_path_result = floyd_warshall_rewrite(adj.numpy())

                        rel_pos = torch.from_numpy(shortest_path_result).long()

                        return x, attn_bias, rel_pos

                    features, adjacency_list = tree_to_adjacency_list(root)

                    max_node = len(operators_with_index)  # 最大节点数
                    rel_pos_max = 20  # 相对位置的最大值

                    x, attn_bias, rel_pos = process_graph(features, adjacency_list, max_node, rel_pos_max)

                    return x, attn_bias, rel_pos

                tree_height[filename.split("_plan")[0]] = build_ancestor_matrix(tree_root)
                x,tree_dict[filename.split("_plan")[0]], tree_rel[filename.split("_plan")[0]] = get_matrix(tree_root)
    print("operator mapping:", list(all_operators.keys()))
    # assert set(all_operators.keys()) == {'Sort', 'Aggregate', 'Project', 'Predicate', 'AND', 'OR'}, "wrong op type"
    def preorder_traversal(key, node, vectors, node_index_map, current_index, fix):

        if node is None:
            return current_index

        node.height = int((node.index + fix) / 3)

        # 创建一个vector
        vector = {
            'operator': None,
            'scan_table_column': [],
            'predicate': None,
            'join_table_column': [],
            'project_table_column': [],
            'aggregate_table_column': [],
            'sort_table_column': [],
            'height': 0
        }

        # 处理 operator
        if node.operator != "Dummy":
            vector['operator'] = list(all_operators.keys()).index(node.operator)
        else:
            vector['operator'] = len(all_operators)


        def convert_table_column_name(columns, benchmark_sfix ):
            columns_converted = []
            mtable = mapping_table_job
            if benchmark_sfix == "tpcds":
                mtable = mapping_table_tpcds
            elif benchmark_sfix == "ssb":
                mtable = mapping_table_ssb
            elif benchmark_sfix == "tpch":
                mtable = mapping_table_tpch


            for col in columns:
                if "_" in col:
                    col_key = col.split("_")[0]
                    if col_key not in mtable:
                        def find_keys(mapping, target):
                            for key, items in mapping.items():
                                for it in items:
                                    if target in it:
                                        return [key + '.' + it]
                            if benchmark_sfix == "tpch":
                                return ["tpch_customer.c_custkey"]
                            else:
                                return ["tpcds_customer.c_customer_id"]
                        tables = find_keys(mapping_tables, col)
                        for tbl in tables:
                            if tbl.startswith(benchmark_sfix):
                                table = tbl
                                columns_converted.append(table)
                                break
                    else:
                        table = mtable[col_key]
                        columns_converted.append(benchmark_sfix + "_" +table + '.' + col)
            if len(columns_converted) ==  0:
                return ["tpcds_customer.c_customer_id","tpcds_customer.c_customer_id"]
            return columns_converted
        # 处理其他属性
        benchmark_suffix = key.split('_')[0]
        vector['limit'] = node.limit
        vector['scan_table_column'] = convert_table_column_name(node.scan_table_column, benchmark_suffix) if node.scan_table_column is not None else []
        vector['join_table_column'] = node.join_table_column if node.join_table_column is not None else []

        if node.join_table_column is not None and node.join_table_column != []:
            if '.' not in node.join_table_column[0][0]:
                join_list = convert_table_column_name(node.join_table_column[0],benchmark_suffix)
                # print(key)
                # print(node.join_table_column)
                # print(join_list)
                vector['join_table_column'] = [[join_list[0],"=",join_list[1]]]

        vector['aggregate_table_column'] = convert_table_column_name(node.aggregate_table_column, benchmark_suffix) if node.aggregate_table_column is not None else []

        vector['sort_table_column'] = convert_table_column_name(node.sort_table_column, benchmark_suffix) if node.sort_table_column is not None else []
        vector['project_table_column'] = convert_table_column_name(node.project_table_column, benchmark_suffix) if node.project_table_column is not None else []
        # 不填充 predicate 和 height
        vector['predicate'] = node.val if node.val is not None else (None,None,None)
        vector['height'] = node.height
        vectors.append(vector)

        # 记录当前节点的索引
        node_index_map[node] = current_index
        current_index += 1

        # 递归遍历左子树
        current_index = preorder_traversal(key, node.left, vectors, node_index_map, current_index, fix)

        # 递归遍历右子树
        current_index = preorder_traversal(key, node.right, vectors, node_index_map, current_index,fix)

        return current_index

    # 主函数部分
    tree_vectors = {}
    max_predicate_length = 0
    max_scan_length = 0
    max_join_length = 0
    max_project_length = 0
    max_aggregate_length = 0
    max_sort_length = 0

    for key, tree in tree_roots.items():
        # print_tree(tree)
        vectors = []
        node_index_map = {}
        current_index = 0
        fix = 3
        if tree.index == -3:
            fix = 6
        preorder_traversal(key, tree, vectors, node_index_map, current_index, fix)
        max_predicate_length = max(max_predicate_length, max(
            len(vector['predicate']) if vector['predicate'] is not None else 0 for vector in vectors))

        max_scan_length = max(max_scan_length, max(len(vector['scan_table_column']) for vector in vectors))
        max_join_length = max(max_join_length, max(len(vector['join_table_column']) for vector in vectors))
        max_project_length = max(max_project_length, max(len(vector['project_table_column']) for vector in vectors))
        max_aggregate_length = max(max_aggregate_length,
                                   max(len(vector['aggregate_table_column']) for vector in vectors))
        max_sort_length = max(max_sort_length, max(len(vector['sort_table_column']) for vector in vectors))

        tree_vectors[key] = {
            'vectors': vectors,
        }

    # 填充处理
    for tree_data in tree_vectors.values():
        vectors = tree_data['vectors']
        for vector in vectors:
            # 不填充 predicate
            vector['scan_table_column'] += [None] * (max_scan_length - len(vector['scan_table_column']))
            vector['join_table_column'] += [None] * (max_join_length - len(vector['join_table_column']))
            vector['project_table_column'] += [None] * (max_project_length - len(vector['project_table_column']))
            vector['aggregate_table_column'] += [None] * (max_aggregate_length - len(vector['aggregate_table_column']))
            vector['sort_table_column'] += [None] * (max_sort_length - len(vector['sort_table_column']))

    print("max_predicate_length:", max_predicate_length)
    print("max_scan_length:", max_scan_length)
    print("max_join_length:", max_join_length)
    print("max_project_length:", max_project_length)
    print("max_aggregate_length:", max_aggregate_length)
    print("max_sort_length:", max_sort_length)
    return tree_dict, tree_roots, tree_vectors, tree_rel, tree_height

import re


def bytes_to_mb(value):
    """Convert bytes to MB. If not a number, return original value."""
    try:
        bytes_val = int(value)
        return f"{bytes_val / (1024 * 1024):.1f}MB"
    except ValueError:
        return value


def extract_lake_conf_values(log_content):
    """
    Extract lake configuration values from log file (both Iceberg and Hudi formats).
    Returns a list of property dictionaries with sizes converted to MB.
    Only processes the first table encountered for Iceberg format.
    """
    lines = log_content.split('\n')

    # Pattern for Iceberg: Altering table format
    iceberg_pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}\s+Altering table\s+([^\s]+)\s+with\s+\(([^)]+)\)'

    # Pattern for Hudi: Setting Hudi option format
    hudi_pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}\s+Setting Hudi option\s+([^\s]+)\s+=\s+(.+)'

    first_table = None
    conf_list = []
    current_hudi_config = {}

    for line in lines:
        line = line.strip()

        # Try Iceberg pattern first
        iceberg_match = re.match(iceberg_pattern, line)
        if iceberg_match:
            table_name = iceberg_match.group(1)
            props_string = iceberg_match.group(2)

            # Record first table
            if first_table is None:
                first_table = table_name

            # Only process first table
            if table_name == first_table:
                # Parse properties
                prop_matches = re.findall(r"'([^']+)'='([^']+)'", props_string)

                if prop_matches:
                    conf = {}
                    for key, value in prop_matches:
                        # Convert size values to MB
                        if 'size' in key.lower():
                            conf[key] = bytes_to_mb(value)
                        else:
                            conf[key] = value

                    conf_list.append(conf)

        # Try Hudi pattern
        else:
            hudi_match = re.match(hudi_pattern, line)
            if hudi_match:
                key = hudi_match.group(1)
                value = hudi_match.group(2).strip()

                # Convert size values to MB
                if 'size' in key.lower():
                    current_hudi_config[key] = bytes_to_mb(value)
                else:
                    current_hudi_config[key] = value

                # Check if we have a complete set of Hudi configs
                # (assuming the 4 properties you mentioned are a complete set)
                expected_keys = {'read.split.target-size', 'read.split.metadata-target-size',
                                 'read.split.planning-lookback', 'read.split.open-file-cost'}

                if set(current_hudi_config.keys()) >= expected_keys:
                    conf_list.append(current_hudi_config.copy())
                    current_hudi_config = {}

    # Add any remaining Hudi config if it has some properties
    if current_hudi_config:
        conf_list.append(current_hudi_config)

    return conf_list


def extract_engine_config_from_log(log_file):
    """Extract engine configuration from log file (original function with minor cleanup)"""
    result = []
    current_config = []

    with open(log_file, 'r') as file:
        for line in file:
            if line.startswith('spark.shuffle.memoryFraction') or line.startswith('spark.memory.fraction'):
                current_config.append(line.split()[1])
            elif line.startswith('spark.memory.storageFraction'):
                current_config.append(line.split()[1])
            elif line.startswith('spark.default.parallelism'):
                current_config.append(line.split()[1])
            elif line.startswith('spark.executor.cores'):
                current_config.append(line.split()[1])
            elif line.startswith('spark.executor.memory'):
                current_config.append(line.split()[1])
            elif 'The jars for the packages stored in: /home/xzw/.ivy2/jars' in line:
                result.append(current_config)
                current_config = []

    configuration = []
    for configs in result:
        processed_configs = []
        for config in configs:
            if config.startswith('read'):
                continue  # Skip configurations starting with 'read'
            elif config.endswith('g'):
                processed_configs.append(int(config.split('g')[0]))
            else:
                try:
                    processed_configs.append(float(config))
                except ValueError:
                    processed_configs.append(config)
        configuration.append(processed_configs)

    return configuration


def extract_combined_config(log_file):
    """
    Extract both engine and lake configurations from the same log file.
    Combines them into a single list where each element contains both engine and lake configs.

    Args:
        log_file (str): Path to the log file

    Returns:
        list: Each element is a list combining engine config + lake config values
    """
    # Read the log file content
    with open(log_file, 'r') as f:
        log_content = f.read()

    # Extract engine configurations
    engine_configs = extract_engine_config_from_log(log_file)

    # Extract lake configurations
    lake_configs = extract_lake_conf_values(log_content)

    # Convert lake configs to lists of values (in consistent order)
    lake_config_lists = []
    for lake_config in lake_configs:
        # Convert dict to list of values in a consistent order
        lake_values = []
        # Define the order of lake config keys
        key_order = ['read.split.target-size', 'read.split.metadata-target-size',
                     'read.split.planning-lookback', 'read.split.open-file-cost']

        for key in key_order:
            if key in lake_config:
                value = lake_config[key]
                # Convert MB values back to numeric for consistency
                if isinstance(value, str) and value.endswith('MB'):
                    lake_values.append(float(value.replace('MB', '')))
                else:
                    try:
                        lake_values.append(float(value))
                    except ValueError:
                        lake_values.append(value)
            # If key not present, add None or 0
        lake_config_lists.append(lake_values)

    # Combine engine and lake configs
    combined_configs = []
    max_len = max(len(engine_configs), len(lake_config_lists))

    for i in range(max_len):
        combined = []

        # Add engine config if available
        if i < len(engine_configs):
            combined.extend(engine_configs[i])

        # Add lake config if available
        if i < len(lake_config_lists):
            combined.extend(lake_config_lists[i])

        combined_configs.append(combined)

    return combined_configs
def extract_config_from_log(log_file):
    result = []
    current_config = []

    with open(log_file, 'r') as file:
        for line in file:
            if line.startswith('spark.shuffle.memoryFraction') or line.startswith('spark.memory.fraction'):
                current_config.append(line.split()[1])
            elif line.startswith('spark.memory.storageFraction'):
                current_config.append(line.split()[1])
            elif line.startswith('spark.default.parallelism'):
                current_config.append(line.split()[1])
            elif line.startswith('spark.executor.cores'):
                current_config.append(line.split()[1])
            elif line.startswith('spark.executor.memory'):
                current_config.append(line.split()[1])
            elif 'The jars for the packages stored in: /home/xzw/.ivy2/jars' in line:
                result.append(current_config)
                current_config = []
    configuration = []
    for configs in result:
        processed_configs = []
        for config in configs:
            if config.startswith('read'):
                continue  # 跳过以 'read' 开头的配置项
            elif config.endswith('g'):
                processed_configs.append(int(config.split('g')[0]))
            else:
                try:
                    processed_configs.append(float(config))
                except ValueError:
                    processed_configs.append(config)

        configuration.append(processed_configs)

    return configuration


def process_spark_experiment(log_file):
    config = extract_combined_config(log_file)
    import re

    def extract_query_times(file_path):
        query_times = {}
        # 匹配 physical time 行（忽略前面的时间戳）
        physical_pattern = r"physical time:\s+([\d\.]+)\s*ms"
        # 匹配 END took 行，并捕获执行时间和查询标识（例如 q98）
        end_pattern = r"END took\s+([\d\.]+)\s*ms:\s*(\S+)\s+-\s+iteration\s+1"

        # 用于暂存上一个 physical time 的值
        current_physical_time = None

        with open(file_path, 'r') as file:
            for line in file:
                # 检查是否是 physical time 行
                physical_match = re.search(physical_pattern, line)
                if physical_match:
                    current_physical_time = float(physical_match.group(1))
                    # 处理完当前行后，继续下一个循环（等待 END took 行）
                    continue

                # 检查是否是 END took 行
                end_match = re.search(end_pattern, line)
                if end_match:
                    end_time = float(end_match.group(1))
                    query = end_match.group(2)
                    # 如果没有找到对应的 physical time，则认为为 0
                    if current_physical_time is None:
                        current_physical_time = 0.0
                    total_time = current_physical_time + end_time
                    if query not in query_times:
                        query_times[query] = []
                    query_times[query].append(total_time)
                    # 重置 current_physical_time 以便下个查询正确匹配
                    current_physical_time = None

        return query_times
    result = extract_query_times(log_file)


    return config, result

# 0126
columns_num_tpcds_short = {
    'cr':1439749,
    'ctr':1439749,
    'c': 500000,
    'ca': 250000,
    'cc': 1439749,
    'cd': 24,
    'cp': 12000,
    'cs': 14329288,
    'd': 73049,
    'hd': 7200,
    'i': 102000,
    'ib': 20,
    'inv': 133110000,
    'p': 500,
    'r': 45,
    's': 102,
    'sm': 20,
    'sr': 2775158,
    'ss': 27504814,
    't': 86400,
    'w': 10,
    'web': 42,
    'wp': 200,
    'wr': 686665,
    'ws': 7195778,
}

def parse_value(value):
    if isinstance(value, list):
        return [parse_single_value(v.strip()) for v in value]
    return parse_single_value(value)

def parse_single_value(value):
    try:
        return int(value)
    except ValueError:
        value = value.strip()
        try:
            return float(value)
        except ValueError:
            try:
                return datetime.strptime(value, '%Y-%m-%d')
            except ValueError:
                return value

def parse_files(folder_path):
    result = {}
    for filename in os.listdir(folder_path):
        if filename.endswith('.txt'):
            with open(os.path.join(folder_path, filename), 'r') as file:
                current_column = None
                for line in file:
                    line = line.strip()
                    if line.startswith('Column:'):
                        current_column = line.split(':')[1].strip()
                        if current_column not in result:
                            result[current_column] = []
                    elif current_column:
                        match = re.search(r'(.*?):\s*.*?height\s*=\s*([\d.]+)', line)
                        if match:
                            value_range = match.group(1).strip('[]').strip('()')
                            height = float(match.group(2))
                            result[current_column].append((value_range, height))
                            # print((value_range, height))
    return result

def parse_tpcds_file(folder_path):
    result = {}

    return result

def query_data(data, column, value, operator, columns_num):
    if column not in data:
        return 0
    print(column)
    col_num = columns_num[column.split('_')[0]]
    total_height = 0
    parsed_values = parse_value(value)

    if isinstance(parsed_values, list):
        operator = '='  # 如果 value 是列表，强制使用 '=' 操作符
        for parsed_value in parsed_values:
            total_height += query_single_value(data[column], parsed_value, operator)
    else:
        total_height = query_single_value(data[column], parsed_values, operator)

    return total_height * col_num

def query_single_value(column_data, parsed_value, operator):
    total_height = 0
    for value_range, height in column_data:
        range_values = [parse_single_value(v.strip()) for v in value_range.split(',')]
        if len(range_values) == 2:
            start, end = range_values
            if (operator == '<' or operator == '<=') and parsed_value < start:
                total_height += height
            elif (operator == '>' or operator == '>=') and parsed_value > end:
                total_height += height
            elif operator == '=' and start <= parsed_value <= end:
                total_height += height
        elif len(range_values) == 1:
            if operator == '=' and parsed_value == range_values[0]:
                total_height += height
            elif (operator == '<' or operator == '<=') and parsed_value < range_values[0]:
                total_height += height
            elif (operator == '>' or operator == '>=') and parsed_value > range_values[0]:
                total_height += height
    return total_height

