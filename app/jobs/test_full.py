import sys
print("path BEFORE:", sys.path[:5], flush=True)
for _p in ("/usr/local/lib/python3.8/dist-packages", "/usr/lib/python3/dist-packages"):
    if _p not in sys.path:
        sys.path.insert(0, _p)
print("path AFTER:", sys.path[:5], flush=True)
import pyspark
print("pyspark:", pyspark.__file__, flush=True)
import flask
print("flask:", flask.__version__, flush=True)
from pyspark.sql import SparkSession
print("SparkSession imported", flush=True)