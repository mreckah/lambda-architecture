from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

# Create Spark Session
spark = SparkSession.builder \
    .appName("BatchLayerJob") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-lambda:8020") \
    .getOrCreate()

print("=== Starting Batch Layer Job ===")

# ------------------------------
# 1. Read historical dataset from HDFS
# ------------------------------
print("Reading data from HDFS...")
df = spark.read.json("hdfs://namenode-lambda:8020/data/transactions.json")

print(f"Total records: {df.count()}")
df.show(5)

# ------------------------------
# 2. Aggregation: total per customer
# ------------------------------
print("Performing aggregation...")
result = df.groupBy("customer").agg(
    spark_sum(col("amount")).alias("total_amount")
)

result.show()

# ------------------------------
# 3. Save result to HDFS
# ------------------------------
print("Saving results to HDFS...")
result.write.mode("overwrite").json("hdfs://namenode-lambda:8020/batch_view/")

print("=== Batch job completed. Results saved to /batch_view/ ===")

spark.stop()