#!/bin/bash
# Load data/raw_data/*.csv into MySQL scenic database.
# Idempotent: truncates target tables before load (re-runnable).
# Usage: ./scripts/load-csv-to-mysql.sh
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

# Defaults match docker-compose.yml; override via env
export DATA_DIR="${DATA_DIR:-$PROJECT_ROOT/data/raw_data}"
export MYSQL_HOST="${MYSQL_HOST:-localhost}"
export MYSQL_PORT="${MYSQL_PORT:-13306}"
export MYSQL_USER="${MYSQL_USER:-root}"
export MYSQL_PASS="${MYSQL_PASS:-root123}"
export MYSQL_DB="${MYSQL_DB:-scenic}"

echo ">>> Load CSV to MySQL"
echo "    DATA_DIR=$DATA_DIR"
echo "    MYSQL=$MYSQL_HOST:$MYSQL_PORT/$MYSQL_DB"

# Try to use project's venv if exists
if [ -f "$PROJECT_ROOT/app/backend/.venv/Scripts/activate" ]; then
    # Windows-style venv (local dev)
    source "$PROJECT_ROOT/app/backend/.venv/Scripts/activate"
elif [ -f "$PROJECT_ROOT/app/backend/.venv/bin/activate" ]; then
    # Linux/macOS-style venv
    source "$PROJECT_ROOT/app/backend/.venv/bin/activate"
elif [ -f "$PROJECT_ROOT/.venv/bin/activate" ]; then
    source "$PROJECT_ROOT/.venv/bin/activate"
fi

# Make sure pymysql is available; install on demand
if ! python -c "import pymysql" 2>/dev/null; then
    echo ">>> pip install pymysql"
    pip install pymysql
fi

# Truncate target tables for idempotent reload
echo ">>> Truncate target tables"
python -c "
import pymysql
conn = pymysql.connect(host='$MYSQL_HOST', port=$MYSQL_PORT, user='$MYSQL_USER',
                     password='$MYSQL_PASS', database='$MYSQL_DB', charset='utf8mb4')
with conn.cursor() as cur:
    cur.execute('SET FOREIGN_KEY_CHECKS = 0')
    for t in ['t_attraction','t_visitor','t_consumption','t_visit_record']:
        cur.execute(f'TRUNCATE TABLE {t}')
    cur.execute('SET FOREIGN_KEY_CHECKS = 1')
conn.commit()
conn.close()
print('Truncated 4 tables')
"

python "$SCRIPT_DIR/load-csv-to-mysql.py"
