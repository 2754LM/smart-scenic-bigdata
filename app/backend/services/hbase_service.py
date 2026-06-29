"""
HBase service via REST/Stargate.
- Read visitor profile by 游客ID
- Read attraction stat by 景点ID
- Read recent visit records (scan)
"""
import requests
from typing import List, Dict, Any, Optional
from loguru import logger

from config import get_settings


class HBaseService:
    def __init__(self):
        self.s = get_settings()
        self.base = self.s.HBASE_REST_URL.rstrip("/")

    def _url(self, table: str, key: str) -> str:
        # Stargate URL: /{table}/{rowkey}/{column_family:column}
        return f"{self.base}/{table}/{key}"

    def health(self) -> bool:
        try:
            r = requests.get(f"{self.base}/", timeout=3)
            return r.status_code in (200, 404)
        except Exception as e:
            logger.warning(f"HBase health fail: {e}")
            return False

    def get_visitor_profile(self, visitor_id: int) -> Optional[Dict[str, Any]]:
        key = f"V{visitor_id:08d}"
        try:
            r = requests.get(self._url(self.s.HBASE_TABLE_VISITOR_PROFILE, key), timeout=5)
            if r.status_code == 200:
                return {"row_key": key, "raw": r.text}
            return None
        except Exception as e:
            logger.warning(f"HBase get_visitor_profile fail: {e}")
            return None

    def get_attraction_stat(self, attraction_id: int) -> Optional[Dict[str, Any]]:
        key = f"A{attraction_id:04d}"
        try:
            r = requests.get(self._url(self.s.HBASE_TABLE_ATTRACTION_STAT, key), timeout=5)
            if r.status_code == 200:
                return {"row_key": key, "raw": r.text}
            return None
        except Exception as e:
            logger.warning(f"HBase get_attraction_stat fail: {e}")
            return None

    def scan_visit_recent(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Scan scenic_visit_rt for the most recent visits (row key is reverse ts)."""
        try:
            params = {"limit": limit}
            r = requests.get(
                f"{self.base}/{self.s.HBASE_TABLE_VISIT_RT}/*",
                params=params,
                timeout=5,
            )
            if r.status_code == 200:
                return [{"raw": r.text}]
            return []
        except Exception as e:
            logger.warning(f"HBase scan fail: {e}")
            return []


_hbase_singleton: Optional[HBaseService] = None


def get_hbase() -> HBaseService:
    global _hbase_singleton
    if _hbase_singleton is None:
        _hbase_singleton = HBaseService()
    return _hbase_singleton
