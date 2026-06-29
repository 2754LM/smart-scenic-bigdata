"""
Hive service for analytics queries.
- Uses PyHive over HiveServer2 Thrift
- Falls back to MySQL aggregation if Hive not reachable
"""
from typing import List, Dict, Any, Optional
from loguru import logger

from config import get_settings
from .mysql_service import get_mysql


class HiveService:
    def __init__(self):
        self.s = get_settings()
        self._conn = None
        self._connect_attempted = False

    def _try_connect(self):
        """Lazy connection to HiveServer2. Return cursor or None."""
        if self._connect_attempted:
            return self._conn
        self._connect_attempted = True
        try:
            from pyhive import hive
            logger.info(f"Connecting to Hive {self.s.HIVE_HOST}:{self.s.HIVE_PORT}")
            self._conn = hive.Connection(
                host=self.s.HIVE_HOST, port=self.s.HIVE_PORT,
                database=self.s.HIVE_DB,
                username="hive",
            )
            logger.info("Hive connected")
        except Exception as e:
            logger.warning(f"Hive connection fail: {e}")
            self._conn = None
        return self._conn

    def health(self) -> bool:
        conn = self._try_connect()
        if not conn:
            return False
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            return True
        except Exception as e:
            logger.warning(f"Hive health fail: {e}")
            return False

    def _query(self, sql: str, fallback_sql: Optional[str] = None) -> List[Dict[str, Any]]:
        """Execute SQL on Hive, return list of dicts. If Hive unavailable, run fallback on MySQL."""
        conn = self._try_connect()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(sql)
                cols = [c[0] for c in cur.description] if cur.description else []
                rows = cur.fetchall()
                return [dict(zip(cols, r)) for r in rows]
            except Exception as e:
                logger.warning(f"Hive query fail, using fallback: {e}")

        # Fallback to MySQL
        if fallback_sql is None:
            fallback_sql = sql  # not ideal, but caller should provide compatible SQL
        return self._mysql_fallback(fallback_sql)

    def _mysql_fallback(self, sql: str) -> List[Dict[str, Any]]:
        """Run SQL on MySQL (works for aggregations of t_consumption / t_visit_record)."""
        try:
            mysql = get_mysql()
            with mysql._conn() as c, c.cursor() as cur:
                cur.execute(sql)
                return list(cur.fetchall())
        except Exception as e:
            logger.warning(f"MySQL fallback fail: {e}")
            return []

    # ---------- Public analytics methods ----------

    def daily_visitors(self, start: str = "2023-01-01", end: str = "2023-12-31") -> List[Dict[str, Any]]:
        sql = f"""
        SELECT date, year, month, day, weekday, is_weekend, is_holiday, visitors
        FROM ads_daily_visitors
        WHERE date BETWEEN '{start}' AND '{end}'
        ORDER BY date
        """
        fallback = f"""
        SELECT DATE(时间) AS date,
               YEAR(时间)  AS year,
               MONTH(时间) AS month,
               DAY(时间)   AS day,
               DAYNAME(时间) AS weekday,
               (DAYOFWEEK(时间) IN (1,7)) AS is_weekend,
               0 AS is_holiday,
               COUNT(DISTINCT 游客ID) AS visitors
        FROM t_visit_record
        WHERE DATE(时间) BETWEEN '{start}' AND '{end}'
        GROUP BY DATE(时间), YEAR(时间), MONTH(时间), DAY(时间), DAYNAME(时间), DAYOFWEEK(时间)
        ORDER BY date
        """
        rows = self._query(sql, fallback)
        for r in rows:
            if r.get("date") and hasattr(r["date"], "isoformat"):
                r["date"] = r["date"].isoformat()
            for k in ("year", "month", "day", "is_weekend", "is_holiday", "visitors"):
                if k in r:
                    r[k] = int(r[k]) if r[k] is not None else 0
        return rows

    def attraction_rank(self) -> List[Dict[str, Any]]:
        sql = "SELECT 景点ID, 景点名称, 类型, 游客数, 消费总额, 平均游玩时长, `rank` FROM ads_attraction_rank ORDER BY `rank`"
        fallback = """
        SELECT a.景点ID, a.景点名称, a.类型,
               COUNT(DISTINCT vr.游客ID) AS 游客数,
               IFNULL(SUM(c.消费金额), 0) AS 消费总额,
               IFNULL(AVG(vr.游玩时长), 0) AS 平均游玩时长,
               RANK() OVER (ORDER BY COUNT(DISTINCT vr.游客ID) DESC) AS `rank`
        FROM t_attraction a
        LEFT JOIN t_visit_record vr ON a.景点ID=vr.景点ID
        LEFT JOIN t_consumption  c  ON a.景点ID=c.景点ID
        GROUP BY a.景点ID, a.景点名称, a.类型
        ORDER BY `rank`
        """
        rows = self._query(sql, fallback)
        for r in rows:
            for k in ("游客数", "rank"):
                if k in r and r[k] is not None:
                    r[k] = int(r[k])
            for k in ("消费总额", "平均游玩时长"):
                if k in r and r[k] is not None:
                    r[k] = float(r[k])
        return rows

    def hourly_distribution(self) -> List[Dict[str, Any]]:
        """Hourly visitor / consumption distribution."""
        sql = """
        SELECT hour, SUM(visitors) AS visitors, SUM(consume) AS consume
        FROM (
          SELECT hour(时间) AS hour, COUNT(DISTINCT 游客ID) AS visitors, 0 AS consume
          FROM dwd_visit_record GROUP BY hour(时间)
        ) t
        GROUP BY hour ORDER BY hour
        """
        fallback = """
        SELECT HOUR(时间) AS hour,
               COUNT(DISTINCT 游客ID) AS visitors,
               IFNULL(SUM(消费金额), 0) AS consume
        FROM t_visit_record
        LEFT JOIN t_consumption USING (游客ID)
        GROUP BY HOUR(时间)
        ORDER BY hour
        """
        # For simplicity use MySQL fallback
        rows = self._mysql_fallback(fallback)
        for r in rows:
            for k in ("hour", "visitors"):
                if k in r and r[k] is not None:
                    r[k] = int(r[k])
            if "consume" in r and r["consume"] is not None:
                r["consume"] = float(r["consume"])
        return rows

    def age_group_distribution(self) -> List[Dict[str, Any]]:
        """Distribution by age group + gender."""
        fallback = """
        SELECT 年龄段, 性别, COUNT(*) AS n
        FROM t_visitor
        WHERE 年龄段 IS NOT NULL
        GROUP BY 年龄段, 性别
        ORDER BY 年龄段, 性别
        """
        rows = self._mysql_fallback(fallback)
        for r in rows:
            r["n"] = int(r["n"])
        return rows

    def region_distribution(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Top regions by visitor count."""
        fallback = f"""
        SELECT 地区, COUNT(*) AS visitors
        FROM t_visitor
        GROUP BY 地区
        ORDER BY visitors DESC
        LIMIT {limit}
        """
        rows = self._mysql_fallback(fallback)
        for r in rows:
            r["visitors"] = int(r["visitors"])
        return rows

    def type_summary(self) -> List[Dict[str, Any]]:
        """Per-type summary (景点类型)."""
        fallback = """
        SELECT a.类型,
               COUNT(DISTINCT a.景点ID) AS 景点数,
               COUNT(DISTINCT vr.游客ID) AS 游客数,
               IFNULL(SUM(c.消费金额), 0) AS 消费总额,
               IFNULL(AVG(vr.游玩时长), 0) AS 平均时长
        FROM t_attraction a
        LEFT JOIN t_visit_record vr ON a.景点ID=vr.景点ID
        LEFT JOIN t_consumption  c  ON a.景点ID=c.景点ID
        GROUP BY a.类型
        ORDER BY 游客数 DESC
        """
        rows = self._mysql_fallback(fallback)
        for r in rows:
            for k in ("景点数", "游客数"):
                if k in r and r[k] is not None:
                    r[k] = int(r[k])
            for k in ("消费总额", "平均时长"):
                if k in r and r[k] is not None:
                    r[k] = float(r[k])
        return rows


_hive_singleton: Optional[HiveService] = None


def get_hive() -> HiveService:
    global _hive_singleton
    if _hive_singleton is None:
        _hive_singleton = HiveService()
    return _hive_singleton
