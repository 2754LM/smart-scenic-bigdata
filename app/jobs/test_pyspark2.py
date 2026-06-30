import sys as _sys
for _p in ("/usr/local/lib/python3.8/dist-packages",):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)
from pyspark.sql import SparkSession
print("OK pyspark imported")