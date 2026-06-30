"""
生成 22 万行原始数据 CSV（智能景区大数据平台数据扩容）

输出:
  data/raw_data/attractions.csv    10 行   (10 个景点)
  data/raw_data/visitors.csv       200 行  (200 个游客)
  data/raw_data/consumption.csv    100,000 行  (消费记录)
  data/raw_data/visit_records.csv  100,000 行  (游玩记录)
  data/raw_data/reviews.csv        20,000 行   (评论/扩展表)
  ---
  合计: 220,210 行

字段名严格对齐 mysql-init/01-init-business.sql 的中文 schema，
确保 app/backend/services/model_service.py 等 loader 不用改。

作业要求：6.3 数据处理与存储 - 多个 CSV 格式的初始数据集
用法:  python scripts/generate-raw-data.py
       python scripts/generate-raw-data.py --rows 500000  # 自定义行数
"""
from __future__ import annotations

import argparse
import csv
import os
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

# ============== 配置 ==============
PROJECT_DIR = Path(__file__).resolve().parent.parent
OUT_DIR = PROJECT_DIR / "data" / "raw_data"

# 数量配置
N_ATTRACTIONS = 10
N_VISITORS = 200
N_CONSUMPTION = 100_000
N_VISIT = 100_000
N_REVIEW = 20_000

# 时间范围（最近 365 天）
END_TIME = datetime(2025, 12, 31, 23, 59, 59)
START_TIME = END_TIME - timedelta(days=365)

# 随机种子（保证可复现）
SEED = 20250629
random.seed(SEED)


# ============== 主数据 ==============
ATTRACTION_TYPES = ["自然", "文化", "娱乐", "运动", "历史", "宗教", "主题"]
ATTRACTION_NAMES = [
    "西湖", "黄山", "张家界", "九寨沟", "故宫",
    "颐和园", "外滩", "鼓浪屿", "兵马俑", "莫高窟",
]
ATTRACTION_LOCATIONS = [
    "杭州-浙江", "黄山-安徽", "张家界-湖南", "九寨沟-四川", "北京-东城",
    "北京-海淀", "上海-黄浦", "厦门-思明", "西安-临潼", "敦煌-甘肃",
]
ATTRACTION_OPEN_TIMES = [
    "06:00-18:00", "全天", "07:00-19:00", "06:30-17:30", "08:30-17:00",
    "06:00-20:00", "全天24h", "07:30-18:30", "08:00-18:00", "08:00-17:30",
]

REGIONS = ["北京", "上海", "广州", "深圳", "杭州", "成都", "武汉", "西安", "南京", "重庆", "其他"]
GENDERS = ["男", "女"]

SURNAMES = list("赵钱孙李周吴郑王冯陈褚卫蒋沈韩杨朱秦尤许何吕施张孔曹严华金魏陶姜")
GIVEN_NAMES = list("伟芳娜秀英敏静丽强磊军洋勇艳杰娟涛明超秀兰霞平刚桂英文")

CONSUMPTION_TYPES = ["门票", "餐饮", "纪念品", "交通", "住宿", "娱乐项目"]
CONSUMPTION_AMOUNT_RANGE = {
    "门票":      (50, 500),
    "餐饮":      (30, 300),
    "纪念品":    (20, 500),
    "交通":      (10, 200),
    "住宿":      (200, 1500),
    "娱乐项目":  (50, 600),
}


# ============== 工具 ==============
def _gen_name() -> str:
    return random.choice(SURNAMES) + random.choice(GIVEN_NAMES) + random.choice(GIVEN_NAMES)


def _gen_age() -> int:
    # 偏态分布：18-30 多，60+ 少
    return int(random.gauss(35, 15))


def _gen_dt() -> str:
    delta = (END_TIME - START_TIME).total_seconds()
    offset = random.uniform(0, delta)
    dt = START_TIME + timedelta(seconds=offset)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_csv(path: Path, header: list, rows) -> int:
    """流式写 CSV，返回行数"""
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        n = 0
        for row in rows:
            w.writerow(row)
            n += 1
            if n % 10_000 == 0:
                print(f"  ...{n} rows", flush=True)
    return n


# ============== 各类数据生成器 ==============
def gen_attractions():
    """10 个景点"""
    header = ["景点ID", "景点名称", "类型", "位置", "开放时间", "门票价格"]
    print(f"[1/5] attractions.csv (n={N_ATTRACTIONS})")
    def rows():
        for i in range(1, N_ATTRACTIONS + 1):
            yield [
                f"A{i:03d}",
                ATTRACTION_NAMES[i - 1] if i <= len(ATTRACTION_NAMES) else f"景点{i}",
                random.choice(ATTRACTION_TYPES),
                ATTRACTION_LOCATIONS[i - 1] if i <= len(ATTRACTION_LOCATIONS) else f"城市{i}",
                random.choice(ATTRACTION_OPEN_TIMES),
                round(random.uniform(50, 500), 2),
            ]
    return header, rows()


def gen_visitors():
    """200 个游客"""
    header = ["游客ID", "姓名", "性别", "年龄", "地区", "偏好类型"]
    print(f"[2/5] visitors.csv (n={N_VISITORS})")
    def rows():
        for i in range(1, N_VISITORS + 1):
            age = _gen_age()
            yield [
                f"V{i:04d}",
                _gen_name(),
                random.choice(GENDERS),
                max(8, min(85, age)),
                random.choice(REGIONS),
                random.choice(ATTRACTION_TYPES),
            ]
    return header, rows()


def gen_consumption(visitor_ids, attraction_ids):
    """10 万条消费记录"""
    header = ["消费ID", "时间", "游客ID", "景点ID", "消费类型", "消费金额"]
    print(f"[3/5] consumption.csv (n={N_CONSUMPTION})")
    def rows():
        for i in range(1, N_CONSUMPTION + 1):
            ctype = random.choice(CONSUMPTION_TYPES)
            lo, hi = CONSUMPTION_AMOUNT_RANGE[ctype]
            yield [
                i,
                _gen_dt(),
                random.choice(visitor_ids),
                random.choice(attraction_ids),
                ctype,
                round(random.uniform(lo, hi), 2),
            ]
    return header, rows()


def gen_visit_records(visitor_ids, attraction_ids):
    """10 万条游玩记录"""
    header = ["记录ID", "时间", "游客ID", "景点ID", "游玩时长", "满意度"]
    print(f"[4/5] visit_records.csv (n={N_VISIT})")
    def rows():
        for i in range(1, N_VISIT + 1):
            yield [
                i,
                _gen_dt(),
                random.choice(visitor_ids),
                random.choice(attraction_ids),
                round(random.uniform(0.5, 12.0), 2),     # 0.5 - 12 小时
                random.randint(1, 5),                     # 满意度 1-5
            ]
    return header, rows()


def gen_reviews(visitor_ids, attraction_ids):
    """2 万条评论（扩展表，对应 t_review）"""
    header = ["评论ID", "时间", "游客ID", "景点ID", "评分", "评论内容"]
    print(f"[5/5] reviews.csv (n={N_REVIEW})")
    COMMENTS = [
        "风景很美，值得一去",
        "人太多了，体验一般",
        "服务态度好，下次还来",
        "门票有点贵",
        "设施完善，停车方便",
        "导游讲解很专业",
        "吃的不错，价格合理",
        "建议错峰出行",
        "夜景很棒，强烈推荐",
        "交通便利，地铁直达",
    ]
    def rows():
        for i in range(1, N_REVIEW + 1):
            yield [
                i,
                _gen_dt(),
                random.choice(visitor_ids),
                random.choice(attraction_ids),
                random.randint(1, 5),
                random.choice(COMMENTS),
            ]
    return header, rows()


# ============== 主程序 ==============
def main() -> int:
    parser = argparse.ArgumentParser(description="生成智能景区大数据平台原始数据")
    parser.add_argument("--consumption", type=int, default=N_CONSUMPTION,
                        help=f"消费记录行数 (默认 {N_CONSUMPTION:,})")
    parser.add_argument("--visit", type=int, default=N_VISIT,
                        help=f"游玩记录行数 (默认 {N_VISIT:,})")
    parser.add_argument("--review", type=int, default=N_REVIEW,
                        help=f"评论行数 (默认 {N_REVIEW:,})")
    parser.add_argument("--visitors", type=int, default=N_VISITORS,
                        help=f"游客数 (默认 {N_VISITORS})")
    args = parser.parse_args()

    _ensure_dir(OUT_DIR)
    print(f"=== 生成原始数据 ===")
    print(f"输出目录: {OUT_DIR}")
    print(f"种子: {SEED}")
    print()

    # 先生成主表（其他表依赖 ID）
    h1, r1 = gen_attractions()
    h2, r2 = gen_visitors()
    n1 = _write_csv(OUT_DIR / "attractions.csv", h1, r1)
    n2 = _write_csv(OUT_DIR / "visitors.csv", h2, r2)
    print(f"  ✓ attractions: {n1} rows")
    print(f"  ✓ visitors:    {n2} rows")

    # 重新读出 ID 列表（避免内存里同时存 10w+rows）
    attraction_ids = [f"A{i:03d}" for i in range(1, N_ATTRACTIONS + 1)]
    visitor_ids = [f"V{i:04d}" for i in range(1, args.visitors + 1)]

    h3, r3 = gen_consumption(visitor_ids, attraction_ids)
    n3 = _write_csv(OUT_DIR / "consumption.csv", h3, r3)
    print(f"  ✓ consumption: {n3:,} rows")

    h4, r4 = gen_visit_records(visitor_ids, attraction_ids)
    n4 = _write_csv(OUT_DIR / "visit_records.csv", h4, r4)
    print(f"  ✓ visit_records: {n4:,} rows")

    h5, r5 = gen_reviews(visitor_ids, attraction_ids)
    n5 = _write_csv(OUT_DIR / "reviews.csv", h5, r5)
    print(f"  ✓ reviews: {n5:,} rows")

    total = n1 + n2 + n3 + n4 + n5
    print()
    print(f"=== 完成 ===")
    print(f"总行数: {total:,}")
    print(f"文件:")
    for f in sorted(OUT_DIR.glob("*.csv")):
        size_mb = f.stat().st_size / 1024 / 1024
        print(f"  {f.name:30s}  {size_mb:6.2f} MB")

    return 0


if __name__ == "__main__":
    sys.exit(main())
