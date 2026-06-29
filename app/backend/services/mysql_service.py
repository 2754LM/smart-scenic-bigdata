"""
MySQL service for scenic platform.
- Connection pool (per-request fresh connection to keep it simple)
- All queries return lists of dicts (easy to JSON serialize)
- Reads are SAFE (no SQL injection via parameterized queries)
"""
import pymysql
import pymysql.cursors
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
from loguru import logger

from config import get_settings


class MySQLService:
    def __init__(self):
        self.s = get_settings()

    @contextmanager
    def _conn(self):
        c = pymysql.connect(
            host=self.s.MYSQL_HOST,
            port=self.s.MYSQL_PORT,
            user=self.s.MYSQL_USER,
            password=self.s.MYSQL_PASS,
            database=self.s.MYSQL_DB,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=5,
        )
        try:
            yield c
        finally:
            c.close()

    def health(self) -> bool:
        try:
            with self._conn() as c, c.cursor() as cur:
                cur.execute("SELECT 1")
                return cur.fetchone() is not None
        except Exception as e:
            logger.warning(f"MySQL health fail: {e}")
            return False

    def list_attractions(self) -> List[Dict[str, Any]]:
        with self._conn() as c, c.cursor() as cur:
            cur.execute("SELECT 景点ID, 景点名称, 类型, 位置, 开放时间 FROM t_attraction ORDER BY 景点ID")
            return list(cur.fetchall())

    def get_attraction(self, id: int) -> Optional[Dict[str, Any]]:
        with self._conn() as c, c.cursor() as cur:
            cur.execute("SELECT 景点ID, 景点名称, 类型, 位置, 开放时间 FROM t_attraction WHERE 景点ID=%s", (id,))
            return cur.fetchone()

    def list_visitors(self, page: int = 1, page_size: int = 50,
                      gender: Optional[str] = None,
                      min_age: Optional[int] = None,
                      max_age: Optional[int] = None) -> Dict[str, Any]:
        offset = (page - 1) * page_size
        wheres, args = [], []
        if gender:
            wheres.append("性别=%s"); args.append(gender)
        if min_age is not None:
            wheres.append("年龄>=%s"); args.append(min_age)
        if max_age is not None:
            wheres.append("年龄<=%s"); args.append(max_age)
        where_sql = " WHERE " + " AND ".join(wheres) if wheres else ""
        with self._conn() as c, c.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS n FROM t_visitor{where_sql}", args)
            total = cur.fetchone()["n"]
            cur.execute(
                f"SELECT 游客ID, 姓名, 性别, 年龄, 地区 FROM t_visitor{where_sql} "
                f"ORDER BY 游客ID LIMIT %s OFFSET %s",
                args + [page_size, offset]
            )
            return {"total": total, "page": page, "page_size": page_size, "items": list(cur.fetchall())}

    def get_visitor(self, id: int) -> Optional[Dict[str, Any]]:
        with self._conn() as c, c.cursor() as cur:
            cur.execute("SELECT 游客ID, 姓名, 性别, 年龄, 地区 FROM t_visitor WHERE 游客ID=%s", (id,))
            return cur.fetchone()

    def list_consumption(self, page: int = 1, page_size: int = 50,
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         visitor_id: Optional[int] = None,
                         attraction_id: Optional[int] = None) -> Dict[str, Any]:
        offset = (page - 1) * page_size
        wheres, args = [], []
        if start_date:
            wheres.append("时间>=%s"); args.append(start_date)
        if end_date:
            wheres.append("时间<=%s"); args.append(end_date)
        if visitor_id is not None:
            wheres.append("游客ID=%s"); args.append(visitor_id)
        if attraction_id is not None:
            wheres.append("景点ID=%s"); args.append(attraction_id)
        where_sql = " WHERE " + " AND ".join(wheres) if wheres else ""
        with self._conn() as c, c.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS n FROM t_consumption{where_sql}", args)
            total = cur.fetchone()["n"]
            cur.execute(
                f"SELECT 消费ID, 时间, 游客ID, 景点ID, 消费金额 FROM t_consumption{where_sql} "
                f"ORDER BY 时间 DESC LIMIT %s OFFSET %s",
                args + [page_size, offset]
            )
            items = list(cur.fetchall())
            for it in items:
                if it.get("时间"):
                    it["时间"] = it["时间"].isoformat()
            return {"total": total, "page": page, "page_size": page_size, "items": items}

    def list_visit_records(self, page: int = 1, page_size: int = 50,
                           start_date: Optional[str] = None,
                           end_date: Optional[str] = None,
                           visitor_id: Optional[int] = None,
                           attraction_id: Optional[int] = None) -> Dict[str, Any]:
        offset = (page - 1) * page_size
        wheres, args = [], []
        if start_date:
            wheres.append("时间>=%s"); args.append(start_date)
        if end_date:
            wheres.append("时间<=%s"); args.append(end_date)
        if visitor_id is not None:
            wheres.append("游客ID=%s"); args.append(visitor_id)
        if attraction_id is not None:
            wheres.append("景点ID=%s"); args.append(attraction_id)
        where_sql = " WHERE " + " AND ".join(wheres) if wheres else ""
        with self._conn() as c, c.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS n FROM t_visit_record{where_sql}", args)
            total = cur.fetchone()["n"]
            cur.execute(
                f"SELECT 记录ID, 时间, 游客ID, 景点ID, 游玩时长 FROM t_visit_record{where_sql} "
                f"ORDER BY 时间 DESC LIMIT %s OFFSET %s",
                args + [page_size, offset]
            )
            items = list(cur.fetchall())
            for it in items:
                if it.get("时间"):
                    it["时间"] = it["时间"].isoformat()
            return {"total": total, "page": page, "page_size": page_size, "items": items}

    def attraction_summary(self) -> List[Dict[str, Any]]:
        """Per-attraction aggregate from t_consumption and t_visit_record."""
        sql = """
        SELECT
            a.景点ID, a.景点名称, a.类型,
            COUNT(DISTINCT vr.游客ID)   AS 游客数,
            COUNT(DISTINCT c.消费ID)   AS 消费笔数,
            IFNULL(SUM(c.消费金额), 0) AS 消费总额,
            IFNULL(AVG(vr.游玩时长), 0) AS 平均游玩时长
        FROM t_attraction a
        LEFT JOIN t_visit_record vr ON a.景点ID=vr.景点ID
        LEFT JOIN t_consumption  c  ON a.景点ID=c.景点ID
        GROUP BY a.景点ID, a.景点名称, a.类型
        ORDER BY 游客数 DESC
        """
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql)
            return list(cur.fetchall())

    def overall_kpi(self) -> Dict[str, Any]:
        with self._conn() as c, c.cursor() as cur:
            cur.execute("SELECT COUNT(*) n FROM t_visitor");        v = cur.fetchone()["n"]
            cur.execute("SELECT COUNT(*) n FROM t_attraction");     a = cur.fetchone()["n"]
            cur.execute("SELECT COUNT(*) n FROM t_consumption");    cn = cur.fetchone()["n"]
            cur.execute("SELECT COUNT(*) n FROM t_visit_record");   vn = cur.fetchone()["n"]
            cur.execute("SELECT IFNULL(SUM(消费金额),0) s, IFNULL(AVG(消费金额),0) a FROM t_consumption")
            row = cur.fetchone()
            cur.execute("SELECT IFNULL(AVG(游玩时长),0) a FROM t_visit_record")
            avg_dur = cur.fetchone()["a"]
            return {
                "游客总数": v, "景点总数": a, "消费笔数": cn, "游玩次数": vn,
                "消费总额": float(row["s"]), "平均消费": float(row["a"]),
                "平均游玩时长": float(avg_dur),
            }

    def time_series(self, metric: str = "consumption", start_date: str = "2023-01-01",
                    end_date: str = "2023-12-31") -> List[Dict[str, Any]]:
        """Daily aggregated series. metric: consumption | visit | visitors."""
        if metric == "consumption":
            sql = """
            SELECT DATE(时间) AS d,
                   SUM(消费金额) AS value,
                   COUNT(*) AS cnt
            FROM t_consumption
            WHERE DATE(时间) BETWEEN %s AND %s
            GROUP BY DATE(时间) ORDER BY d
            """
        elif metric == "visit":
            sql = """
            SELECT DATE(时间) AS d,
                   COUNT(*) AS value,
                   COUNT(DISTINCT 游客ID) AS cnt
            FROM t_visit_record
            WHERE DATE(时间) BETWEEN %s AND %s
            GROUP BY DATE(时间) ORDER BY d
            """
        else:  # visitors
            sql = """
            SELECT DATE(时间) AS d,
                   COUNT(DISTINCT 游客ID) AS value,
                   COUNT(*) AS cnt
            FROM t_visit_record
            WHERE DATE(时间) BETWEEN %s AND %s
            GROUP BY DATE(时间) ORDER BY d
            """
        with self._conn() as c, c.cursor() as cur:
            cur.execute(sql, (start_date, end_date))
            return [{"date": str(r["d"]), "value": float(r["value"]), "count": int(r["cnt"])}
                    for r in cur.fetchall()]

    def visitor_aggregates(self, visitor_id: int) -> Dict[str, Any]:
        with self._conn() as c, c.cursor() as cur:
            cur.execute("""SELECT COUNT(*) cnt, IFNULL(SUM(消费金额),0) total,
                                  IFNULL(AVG(消费金额),0) avg
                           FROM t_consumption WHERE 游客ID=%s""", (visitor_id,))
            c_row = cur.fetchone()
            cur.execute("""SELECT COUNT(*) cnt, IFNULL(AVG(游玩时长),0) avg
                           FROM t_visit_record WHERE 游客ID=%s""", (visitor_id,))
            v_row = cur.fetchone()
            cur.execute("""SELECT a.景点ID, a.景点名称, COUNT(*) cnt, SUM(c.消费金额) total
                           FROM t_consumption c JOIN t_attraction a ON c.景点ID=a.景点ID
                           WHERE c.游客ID=%s
                           GROUP BY a.景点ID, a.景点名称
                           ORDER BY cnt DESC LIMIT 10""", (visitor_id,))
            top = list(cur.fetchall())
            for r in top:
                if r.get("total") is not None:
                    r["total"] = float(r["total"])
            return {
                "游客ID": visitor_id,
                "消费笔数": c_row["cnt"], "消费总额": float(c_row["total"]), "平均消费": float(c_row["avg"]),
                "游玩次数": v_row["cnt"], "平均游玩时长": float(v_row["avg"]),
                "最常消费景点": top,
            }


_mysql_singleton: Optional[MySQLService] = None


def get_mysql() -> MySQLService:
    global _mysql_singleton
    if _mysql_singleton is None:
        _mysql_singleton = MySQLService()
    return _mysql_singleton
