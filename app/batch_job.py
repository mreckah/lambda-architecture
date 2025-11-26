from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

# ------------------------------
# 1. Create Spark Session
# ------------------------------
spark = SparkSession.builder \
    .appName("BatchLayerJob") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-lambda:8020") \
    .getOrCreate()

# Hide Spark INFO/DEBUG logs
spark.sparkContext.setLogLevel("ERROR")

# ------------------------------
# 2. Read dataset from HDFS
# ------------------------------
df = spark.read.json("hdfs://namenode-lambda:8020/data/transactions.json")

# ------------------------------
# 3. Aggregation: total amount per customer
# ------------------------------
totals = df.groupBy("customer").agg(
    spark_sum(col("amount")).alias("total_amount")
)

# ------------------------------
# 4. Show results (total per user)
# ------------------------------
print("=== Total amount per customer ===")
totals.show(truncate=False)

# ------------------------------
# 5. Save results to HDFS
# ------------------------------
totals.write.mode("overwrite").json("hdfs://namenode-lambda:8020/batch_view/")

spark.stop()
