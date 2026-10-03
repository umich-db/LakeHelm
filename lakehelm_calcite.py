"""Calcite-based plan generation for user queries (add-on; not used by run_all.sh).

Turns raw SQL into optimized logical plans with Apache Calcite and into LakeHelm
plan-tree features, so new workloads can be prepared without the original Spark
plan files.  Inputs:

  * a DDL file of CREATE TABLE statements (column names and types),
  * a JSON file of table row counts, e.g. {"orders": 15000000, ...} (drives Calcite's
    cost-based join ordering and the cardinality features),
  * SQL queries, and optionally a workload CSV of measured (config, latency) runs.

Commands:
  python lakehelm_calcite.py plan    --ddl schema.sql --rows rows.json --sql q1.sql q2.sql [--out plans.json] [--text]
  python lakehelm_calcite.py prepare --ddl schema.sql --rows rows.json --workload runs.csv \\
                                     --benchmark mybench --out prepared/

Workload CSV columns: query_name, sql_file, engine, datalake, conf, latency, sf
(sql_file is relative to the CSV; engine in spark/presto/trino; datalake in delta/iceberg/hudi;
latency in milliseconds, as in data/output).  `prepare` writes
  prepared/<benchmark>/sf<N>/<benchmark>_<engine>_<datalake>_sf<N>.csv   (repo latency format)
  prepared/plans.json   (Calcite plans)   prepared/trees.pt   (node features per query)

The Java planner (calcite_planner/) is built with Maven on first use.
"""
import argparse
import csv
import json
import math
import os
import re
import subprocess
import sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
JAR = os.path.join(HERE, 'calcite_planner', 'target', 'calcite-planner.jar')

# Unified operator vocabulary (names as in the Spark-plan featurizer, so the features
# line up with --tree-feat-norm agnostic); Calcite operators are mapped onto it.
OP_VOCAB = ['AND', 'Aggregate', 'Expand', 'GlobalLimit', 'Join ExistenceJoin', 'Join FullOuter',
            'Join Inner', 'Join LeftAnti', 'Join LeftOuter', 'Join LeftSemi', 'LocalLimit', 'OR',
            'Predicate', 'Project', 'Relation', 'Scan', 'Sort', 'Union', 'Window', 'exist', '<none>']
_CALCITE_TO_VOCAB = {
    'Scan': 'Scan', 'Filter': 'Predicate', 'Project': 'Project', 'Aggregate': 'Aggregate',
    'Sort': 'Sort', 'Limit': 'GlobalLimit', 'Union': 'Union', 'Window': 'Window',
    'Join Inner': 'Join Inner', 'Join Left': 'Join LeftOuter', 'Join Right': 'Join LeftOuter',
    'Join Full': 'Join FullOuter', 'Join Semi': 'Join LeftSemi', 'Join Anti': 'Join LeftAnti',
}
FEATURE_NAMES = ([f'op={o}' for o in OP_VOCAB]
                 + ['limit_digits', 'log_rows', 'pred_none', 'pred_and', 'pred_or',
                    'table_count', 'log_table_digits', 'max_table_digits', 'mean_table_digits'])


# ----------------------------------------------------------------------------- inputs

def parse_ddl(ddl_text, rows):
    """CREATE TABLE statements -> {table: {"rows": n, "columns": [{"name", "type"}]}}."""
    tables = {}
    for m in re.finditer(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`"]?([\w.]+)[`"]?\s*\((.*?)\)\s*;',
                         ddl_text, re.S | re.I):
        name = m.group(1).split('.')[-1]
        parts, depth, cur = [], 0, ''
        for ch in m.group(2):
            depth += ch == '('
            depth -= ch == ')'
            if ch == ',' and depth == 0:
                parts.append(cur); cur = ''
            else:
                cur += ch
        parts.append(cur)
        cols = []
        for p in map(str.strip, parts):
            if not p or re.match(r'(PRIMARY|FOREIGN|UNIQUE|CONSTRAINT|KEY|INDEX)\b', p, re.I):
                continue
            tok = p.split()
            cols.append({'name': tok[0].strip('`"'), 'type': ' '.join(tok[1:]) or 'VARCHAR'})
        if name not in rows:
            print(f"[warn] no row count for table '{name}', using 1000", file=sys.stderr)
        tables[name] = {'rows': float(rows.get(name, 1000)), 'columns': cols}
    if not tables:
        raise ValueError('no CREATE TABLE statements found in the DDL')
    return tables


def ensure_jar():
    if not os.path.exists(JAR):
        print('[info] building calcite_planner with Maven ...', file=sys.stderr)
        subprocess.run(['mvn', '-q', '-B', 'package', '-DskipTests'],
                       cwd=os.path.join(HERE, 'calcite_planner'), check=True)
    return JAR


def run_calcite(tables, queries):
    """queries: list of (name, sql) -> list of {"name", "plan"|"error", "text"}."""
    payload = json.dumps({'tables': tables,
                          'queries': [{'name': n, 'sql': s} for n, s in queries]})
    r = subprocess.run(['java', '-jar', ensure_jar()], input=payload, capture_output=True,
                       text=True, check=True)
    return json.loads(r.stdout)['plans']


# ----------------------------------------------------------------------------- features

def _slog(x):
    return math.copysign(math.log1p(abs(x)), x)


def node_features(node):
    """One plan node -> feature vector laid out as FEATURE_NAMES."""
    op = _CALCITE_TO_VOCAB.get(node['kind'], '<none>')
    onehot = [1.0 if o == op else 0.0 for o in OP_VOCAB]
    limit = float(len(str(node['fetch']))) if node.get('fetch') is not None else 0.0
    cond = node.get('condition', '')
    pred = [0.0, 0.0, 0.0]
    pred[1 if cond.startswith('AND(') else 2 if cond.startswith('OR(') else 0] = 1.0
    if node['kind'] == 'Scan':
        digits = float(len(str(int(node.get('table_rows') or 0))))
        tab = [1.0, math.log1p(digits), digits, digits]
    else:
        tab = [0.0, 0.0, 0.0, 0.0]
    return onehot + [limit, _slog(max(node.get('rows', 0.0), 0.0))] + pred + tab


def plan_to_tree(plan):
    """Calcite JSON plan -> (node_feats [N x F] list, children lists, root_id), the same
    structure TreeQueryEncoder consumes (pre-order, root = 0)."""
    feats, children = [], []

    def dfs(n):
        idx = len(feats)
        feats.append(node_features(n)); children.append([])
        for c in n.get('children', []):
            children[idx].append(dfs(c))
        return idx

    root = dfs(plan)
    return feats, children, root


# ----------------------------------------------------------------------------- commands

def cmd_plan(a):
    tables = parse_ddl(open(a.ddl).read(), json.load(open(a.rows)))
    queries = [(os.path.splitext(os.path.basename(p))[0], open(p).read()) for p in a.sql]
    plans = run_calcite(tables, queries)
    for p in plans:
        if 'error' in p:
            print(f"[error] {p['name']}: {p['error']}", file=sys.stderr)
        elif a.text:
            print(f"== {p['name']}\n{p['text']}")
    if a.out:
        json.dump(plans, open(a.out, 'w'), indent=1)
        print(f"wrote {len(plans)} plans to {a.out}")


def cmd_prepare(a):
    import torch
    tables = parse_ddl(open(a.ddl).read(), json.load(open(a.rows)))
    base = os.path.dirname(os.path.abspath(a.workload))
    runs = list(csv.DictReader(open(a.workload)))
    need = ['query_name', 'sql_file', 'engine', 'datalake', 'conf', 'latency', 'sf']
    missing = [c for c in need if c not in (runs[0].keys() if runs else [])]
    if missing:
        raise ValueError(f'workload CSV is missing columns {missing}')
    sqls = {}
    for r in runs:
        sqls.setdefault(r['query_name'], open(os.path.join(base, r['sql_file'])).read())
    plans = run_calcite(tables, sorted(sqls.items()))
    ok = {p['name']: p for p in plans if 'plan' in p}
    for p in plans:
        if 'error' in p:
            print(f"[error] {p['name']}: {p['error']} (its runs are skipped)", file=sys.stderr)

    os.makedirs(a.out, exist_ok=True)
    groups = defaultdict(list)
    for r in runs:
        if r['query_name'] in ok:
            groups[(r['engine'].lower(), r['datalake'].lower(), str(r['sf']))].append(r)
    for (eng, lake, sf), rs in groups.items():
        d = os.path.join(a.out, a.benchmark, f'sf{sf}')
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, f'{a.benchmark}_{eng}_{lake}_sf{sf}.csv'), 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(['datalake', 'engine', 'query name', 'conf', 'latency', 'benchmark', 'sf'])
            for r in rs:
                w.writerow([lake, eng, r['query_name'], r['conf'], r['latency'], a.benchmark, sf])
    json.dump(plans, open(os.path.join(a.out, 'plans.json'), 'w'), indent=1)
    trees = {}
    for name, p in ok.items():
        feats, children, root = plan_to_tree(p['plan'])
        trees[f'{a.benchmark}_{name}'] = (torch.tensor(feats, dtype=torch.float), children, root)
    torch.save({'trees': trees, 'feature_names': FEATURE_NAMES}, os.path.join(a.out, 'trees.pt'))
    print(f"prepared {len(ok)}/{len(sqls)} queries, {sum(map(len, groups.values()))} runs "
          f"in {len(groups)} (engine, datalake, sf) files -> {a.out}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest='cmd', required=True)
    p = sub.add_parser('plan', help='optimize SQL files with Calcite and print/save the plans')
    p.add_argument('--ddl', required=True); p.add_argument('--rows', required=True)
    p.add_argument('--sql', nargs='+', required=True)
    p.add_argument('--out'); p.add_argument('--text', action='store_true')
    p.set_defaults(fn=cmd_plan)
    q = sub.add_parser('prepare', help='plans + features + latency files for a measured workload')
    q.add_argument('--ddl', required=True); q.add_argument('--rows', required=True)
    q.add_argument('--workload', required=True); q.add_argument('--benchmark', required=True)
    q.add_argument('--out', required=True)
    q.set_defaults(fn=cmd_prepare)
    a = ap.parse_args()
    a.fn(a)


if __name__ == '__main__':
    main()
