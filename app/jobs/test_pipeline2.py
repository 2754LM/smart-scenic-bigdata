import sys
for p in ("/usr/local/lib/python3.8/dist-packages",):
    sys.path.insert(0, p)
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[2]").appName("t").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Build df_features equivalent
df = spark.createDataFrame([
    (1, 30, 5, 100, 3, 4.5, 2, 1000.0),
    (2, 40, 3, 200, 2, 3.0, 1, 500.0),
    (3, 25, 1, 50, 1, 2.0, 1, 100.0),
    (4, 50, 8, 300, 4, 5.0, 3, 2000.0),
], ["visitor_id", "age", "purchase_count", "avg_amount", "visit_count", "avg_duration", "unique_attractions", "total_amount"])

asm = VectorAssembler(inputCols=["age", "purchase_count", "avg_amount", "visit_count", "avg_duration", "unique_attractions"], outputCol="raw_features")
sc = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
pipe = Pipeline(stages=[asm, sc])
pm = pipe.fit(df)
print("pm type:", type(pm).__name__)
print("pm.stages:", [type(s).__name__ for s in pm.stages])

lr = LinearRegression(featuresCol="features", labelCol="total_amount")

# Try constructing full pipeline using prep_model.stages
p2 = Pipeline(stages=list(pm.stages) + [lr])
print("p2 type:", type(p2).__name__)
m2 = p2.fit(df)
print("m2 type:", type(m2).__name__)
print("m2.stages:", [type(s).__name__ for s in m2.stages])

m2.write().overwrite().save("/tmp/full_pipe")
m3 = PipelineModel.load("/tmp/full_pipe")
print("m3 loaded:", type(m3).__name__)
spark.stop()