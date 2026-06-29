"""
Load CSV files from data/raw_data/ into MySQL scenic database.
Cross-platform (Windows / Linux / macOS), env-driven configuration.

Usage:
    # Local (defaults work - connects to Docker MySQL on localhost:13306)
    python scripts/load-csv-to-mysql.py

    # Custom paths / connection
    DATA_DIR=/srv/scenic/data/raw_data \\
    MYSQL_HOST=mysql MYSQL_USER=root MYSQL_PASS=root123 \\
    python scripts/load-csv-to-mysql.py
"""
import csv
import os
import sys
import time
from pathlib import Path

try:
    import pymysql
except ImportError:
    sys.exit("pymysql not installed. Run: pip install pymysql")


# ---------- Configuration (env-driven, sensible defaults) ----------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

DATA_DIR = Path(os.environ.get("DATA_DIR", PROJECT_ROOT / "data" / "raw_data"))
MYSQL_HOST = os.environ.get("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "13306"))
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASS = os.environ.get("MYSQL_PASS", "root123")
MYSQL_DB   = os.environ.get("MYSQL_DB", "scenic")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "5000"))

# CSV file -> (table, column order, type coercion)
# Column order MUST match MySQL table definition
LOAD_PLAN = [
    {
        "csv": "attractions.csv",
        "table": "t_attraction",
        "columns": ["景点ID", "景点名称", "类型", "位置", "开放时间"],
        "coerce": {},
    },
    {
        "csv": "visitors.csv",
        "table": "t_visitor",
        "columns": ["游客ID", "姓名", "性别", "年龄", "地区"],
        "coerce": {"年龄": int},
    },
    {
        "csv": "consumption.csv",
        "table": "t_consumption",
        "columns": ["消费ID", "时间", "游客ID", "景点ID", "消费金额"],
        "coerce": {"消费ID": int, "消费金额": float},
    },
    {
        "csv": "visit_records.csv",
        "table": "t_visit_record",
        "columns": ["记录ID", "时间", "游客ID", "景点ID", "游玩时长"],
        "coerce": {"记录ID": int, "游玩时长": float},
    },
]


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def connect_mysql():
    log(f"Connect MySQL {MYSQL_HOST}:{MYSQL_PORT} db={MYSQL_DB} user={MYSQL_USER}")
    return pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
        password=MYSQL_PASS, database=MYSQL_DB, charset="utf8mb4",
        autocommit=False, local_infile=False,
    )


def coerce_value(value, fn):
    """Convert empty string to None, apply type function."""
    if value is None or value == "":
        return None
    try:
        return fn(value)
    except (ValueError, TypeError):
        log(f"  WARN: cannot coerce {value!r} with {fn.__name__}, set to None")
        return None


def load_one(conn, plan: dict) -> int:
    csv_path = DATA_DIR / plan["csv"]
    table = plan["table"]
    cols = plan["columns"]
    coerce = plan["coerce"]

    if not csv_path.exists():
        log(f"  SKIP: {csv_path} not found")
        return 0

    log(f"Load {plan['csv']} -> {table}")
    placeholders = ", ".join(["%s"] * len(cols))
    col_list = ", ".join(f"`{c}`" for c in cols)
    sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
    log(f"  SQL: {sql[:120]}...")

    inserted = 0
    batch = []
    with conn.cursor() as cur, open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != cols:
            log(f"  WARN: CSV header {reader.fieldnames} != expected {cols}")

        for row in reader:
            values = []
            for c in cols:
                v = row.get(c, "")
                if c in coerce:
                    v = coerce_value(v, coerce[c])
                else:
                    v = None if v == "" else v
                values.append(v)
            batch.append(values)
            if len(batch) >= BATCH_SIZE:
                cur.executemany(sql, batch)
                conn.commit()
                inserted += len(batch)
                batch = []
        if batch:
            cur.executemany(sql, batch)
            conn.commit()
            inserted += len(batch)

    log(f"  Inserted {inserted} rows into {table}")
    return inserted


def verify(conn) -> None:
    log("Verify counts:")
    with conn.cursor() as cur:
        for t in ["t_attraction", "t_visitor", "t_consumption", "t_visit_record"]:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            log(f"  {t}: {cur.fetchone()[0]}")


def main():
    if not DATA_DIR.exists():
        sys.exit(f"DATA_DIR not found: {DATA_DIR}")
    log(f"DATA_DIR = {DATA_DIR}")

    conn = connect_mysql()
    try:
        # Use INSERT (not REPLACE) - data files are source of truth
        for plan in LOAD_PLAN:
            load_one(conn, plan)
        verify(conn)
        log("=== ALL DONE ===")
    except Exception as e:
        conn.rollback()
        log(f"ERROR: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
