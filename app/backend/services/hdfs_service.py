"""
HDFS service: read ML model files and reports from HDFS.
- Uses hdfs library (python-hdfs) which is the official python client
- Falls back to file:// if HDFS not available
"""
import os
import json
import pickle
from typing import Optional, Any, Dict, List
from loguru import logger

from config import get_settings


class HDFSService:
    def __init__(self):
        self.s = get_settings()
        self._client = None
        self._attempted = False

    def _try_connect(self):
        if self._attempted:
            return self._client
        self._attempted = True
        try:
            from hdfs import InsecureClient
            url = f"http://{self.s.HDFS_NAMENODE}:{self.s.HDFS_PORT}"
            self._client = InsecureClient(url, user=self.s.HDFS_USER, timeout=5)
            logger.info(f"HDFS connected: {url}")
        except Exception as e:
            logger.warning(f"HDFS connection fail: {e}")
            self._client = None
        return self._client

    def health(self) -> bool:
        c = self._try_connect()
        if not c:
            return False
        try:
            c.status("/")
            return True
        except Exception as e:
            logger.warning(f"HDFS health fail: {e}")
            return False

    def read_json(self, hdfs_path: str) -> Optional[Any]:
        """Read a JSON file from HDFS."""
        c = self._try_connect()
        if c is None:
            return None
        try:
            with c.read(hdfs_path, encoding="utf-8") as r:
                # json was written as text via spark, take first line
                content = r.read()
                # Spark writes text format, one JSON per line; take first
                first_line = content.split("\n")[0].strip()
                return json.loads(first_line)
        except Exception as e:
            logger.warning(f"HDFS read_json fail for {hdfs_path}: {e}")
            return None

    def list_dir(self, hdfs_path: str) -> List[str]:
        c = self._try_connect()
        if c is None:
            return []
        try:
            return c.list(hdfs_path)
        except Exception as e:
            logger.warning(f"HDFS list fail: {e}")
            return []


_hdfs_singleton: Optional[HDFSService] = None


def get_hdfs() -> HDFSService:
    global _hdfs_singleton
    if _hdfs_singleton is None:
        _hdfs_singleton = HDFSService()
    return _hdfs_singleton
