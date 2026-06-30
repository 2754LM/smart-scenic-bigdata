import sys
for p in ("/usr/local/lib/python3.8/dist-packages",):
    sys.path.insert(0, p)
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

df = spark.createDataFrame([(1.0, 1.0), (2.0, 2.0), (3.0, 3.0)], ["y", "x"])
asm = VectorAssembler(inputCols=["x"], outputCol="raw")
sc = StandardScaler(inputCol="raw", outputCol="feat")
lr = LinearRegression(featuresCol="feat", labelCol="y")
p = Pipeline(stages=[asm, sc, lr])
m = p.fit(df)
print("type(m):", type(m).__name__)
print("m.stages:", [type(s).__name__ for s in m.stages])

# save & reload
m.write().overwrite().save("/tmp/test_pipeline_model")
from pyspark.ml import PipelineModel
m2 = PipelineModel.load("/tmp/test_pipeline_model")
print("loaded ok, type:", type(m2).__name__)
spark.stop()