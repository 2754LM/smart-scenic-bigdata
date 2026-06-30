import sys, os
os.environ['JAVA_HOME'] = '/opt/java/openjdk'
sys.path.insert(0, '/usr/local/lib/python3.8/dist-packages')
import pyspark
print("pyspark ok:", pyspark.__file__, flush=True)