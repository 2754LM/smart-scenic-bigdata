import sys
print("sys.path:", sys.path[:5], flush=True)
try:
    import pyspark
    print("pyspark:", pyspark.__file__, flush=True)
except Exception as e:
    print("FAIL:", e, flush=True)