from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, current_timestamp

# ------------------------------
# 1. Create Spark Session
# ------------------------------
spark = SparkSession.builder \
    .appName("BatchLayerJob") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-lambda:8020") \
    .config("spark.cassandra.connection.host", "cassandra-lambda") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

# Hide Spark INFO/DEBUG logs
spark.sparkContext.setLogLevel("ERROR")

print("Spark session created successfully!")

# ------------------------------
# 2. Read dataset from HDFS
# ------------------------------
print("Reading data from HDFS...")
df = spark.read.json("hdfs://namenode-lambda:8020/data/transactions.json")

print(f"Total transactions read: {df.count()}")

# ------------------------------
# 3. Aggregation: total amount per customer
# ------------------------------
print("Aggregating data...")
totals = df.groupBy("customer").agg(
    spark_sum(col("amount")).alias("total_amount")
)

# Add timestamp for tracking when batch was processed
totals = totals.withColumn("updated_at", current_timestamp())

# ------------------------------
# 4. Show results (total per user)
# ------------------------------
print("\n=== Total amount per customer ===")
totals.show(truncate=False)

# ------------------------------
# 5. Save results to Cassandra
# ------------------------------
print("\nWriting results to Cassandra...")

try:
    totals.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="customer_totals", keyspace="lambda_batch") \
        .save()
    
    print("Successfully wrote results to Cassandra!")
    
    # ------------------------------
    # 6. Verify data in Cassandra
    # ------------------------------
    print("\n=== Verifying data in Cassandra ===")
    
    cassandra_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="customer_totals", keyspace="lambda_batch") \
        .load()
    
    cassandra_df.orderBy("customer").show(truncate=False)
    print(f"Total records in Cassandra: {cassandra_df.count()}")
    
except Exception as e:
    print(f"Error with Cassandra: {e}")
    print("\nFalling back to HDFS storage...")
    totals.write.mode("overwrite").json("hdfs://namenode-lambda:8020/batch_view/")
    print("Results saved to HDFS instead")

spark.stop()
print("\nBatch job completed!")