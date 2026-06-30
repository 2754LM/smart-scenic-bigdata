import sys
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[2]").appName("d").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

from pyspark.ml.regression import LinearRegression, RandomForestRegressor, LinearRegressionModel
from pyspark.ml.clustering import KMeans

df = spark.createDataFrame([{"x": float(i), "y": float(i*2)} for i in range(20)])
from pyspark.ml.feature import VectorAssembler
asm = VectorAssembler(inputCols=["x"], outputCol="f")
df2 = asm.transform(df)

lr = LinearRegression(featuresCol="f", labelCol="y")
trained = lr.fit(df2)

with open("/tmp/debug.txt", "w") as f:
    f.write("trained type: " + type(trained).__name__ + "\n")
    f.write("isinstance(trained, LinearRegression): " + str(isinstance(trained, LinearRegression)) + "\n")
    f.write("isinstance(trained, LinearRegressionModel): " + str(isinstance(trained, LinearRegressionModel)) + "\n")
spark.stop()