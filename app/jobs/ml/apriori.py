"""
Apriori 关联规则 - 智能景区
=============================
作业要求: 使用关联规则算法（如Apriori）分析游客消费行为与景点的关联。

本脚本用纯 Python 实现 Apriori 算法（不依赖 mlxtend），与 FPGrowth 并行运行。
FPGrowth 是 Apriori 的优化版本（相同结果，更快），两者同时保留以展示对比。

执行方式（demo-backend 内）：
  python3 /opt/jobs/ml/apriori.py

输入：MySQL t_consumption（游客 × 景点）
输出：/shared/models/apriori_rules.json
"""
import json
import os
from collections import defaultdict
from itertools import combinations

import pymysql

MIN_SUPPORT = 0.02
MIN_CONFIDENCE = 0.3
MAX_ITEMSET_SIZE = 3

MYSQL_CONFIG = {
    "host": os.environ.get("MYSQL_HOST", "mysql"),
    "port": int(os.environ.get("MYSQL_PORT", 3306)),
    "user": "root",
    "password": os.environ.get("MYSQL_ROOT_PASSWORD", "root123"),
    "database": "scenic",
    "charset": "utf8mb4",
}
SHARED_OUT = "/shared/models/apriori_rules.json"


def load_transactions():
    conn = pymysql.connect(**MYSQL_CONFIG)
    try:
        sql = """
            SELECT 游客ID, 景点ID FROM t_consumption
            UNION
            SELECT 游客ID, 景点ID FROM t_visit_record
        """
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    finally:
        conn.close()

    txns = defaultdict(set)
    for visitor_id, attraction_id in rows:
        txns[str(visitor_id)].add(str(attraction_id))
    return list(txns.values())


def find_frequent_itemsets(transactions, min_support, max_k):
    n = len(transactions)
    min_count = min_support * n

    item_count = defaultdict(int)
    for t in transactions:
        for item in t:
            item_count[frozenset([item])] += 1

    frequent = {k: v for k, v in item_count.items() if v >= min_count}
    all_frequent = dict(frequent)

    for k in range(2, max_k + 1):
        if not frequent:
            break
        candidates = set()
        items_set = set()
        for fs in frequent:
            items_set.update(fs)
        for combo in combinations(sorted(items_set), k):
            candidates.add(frozenset(combo))

        cand_count = defaultdict(int)
        for t in transactions:
            for c in candidates:
                if c.issubset(t):
                    cand_count[c] += 1

        frequent = {k: v for k, v in cand_count.items() if v >= min_count}
        all_frequent.update(frequent)

    return all_frequent, n


def generate_rules(frequent_itemsets, n, min_confidence):
    rules = []
    for itemset, count in frequent_itemsets.items():
        if len(itemset) < 2:
            continue
        support = count / n
        for i in range(1, len(itemset)):
            for antecedent in combinations(itemset, i):
                ant_set = frozenset(antecedent)
                consequent = itemset - ant_set
                if not consequent:
                    continue
                ant_count = frequent_itemsets.get(ant_set, 0)
                if ant_count == 0:
                    continue
                confidence = count / ant_count
                if confidence >= min_confidence:
                    con_count = frequent_itemsets.get(consequent, 0)
                    lift = confidence / (con_count / n) if con_count > 0 else 0
                    rules.append({
                        "antecedent": sorted(list(ant_set)),
                        "consequent": sorted(list(consequent)),
                        "support": round(support, 4),
                        "confidence": round(confidence, 4),
                        "lift": round(lift, 4),
                    })
    rules.sort(key=lambda r: r["lift"], reverse=True)
    return rules


def main():
    print("[1/3] Loading transactions from MySQL ...", flush=True)
    transactions = load_transactions()
    print(f"    {len(transactions)} visitors with transactions", flush=True)

    print("[2/3] Running Apriori ...", flush=True)
    frequent, n = find_frequent_itemsets(transactions, MIN_SUPPORT, MAX_ITEMSET_SIZE)
    print(f"    {len(frequent)} frequent itemsets from {n} transactions", flush=True)

    print("[3/3] Generating rules ...", flush=True)
    rules = generate_rules(frequent, n, MIN_CONFIDENCE)
    print(f"    {len(rules)} rules (minSupport={MIN_SUPPORT}, minConfidence={MIN_CONFIDENCE})", flush=True)

    os.makedirs(os.path.dirname(SHARED_OUT), exist_ok=True)
    with open(SHARED_OUT, "w", encoding="utf-8") as f:
        json.dump(rules[:200], f, ensure_ascii=False, indent=2)
    print(f"    saved to {SHARED_OUT} ({min(len(rules), 200)} rules)", flush=True)
    print(f"\n=== Apriori Done: {len(rules)} rules ===", flush=True)


if __name__ == "__main__":
    main()
