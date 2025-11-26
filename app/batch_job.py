from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg,
    current_timestamp, monotonically_increasing_id,
    to_date, year, month, dayofmonth
)

# ------------------------------
# 1. Create Spark Session
# ------------------------------
spark = SparkSession.builder \
    .appName("EcommerceBatchLayer_StarSchema") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode-lambda:8020") \
    .config("spark.cassandra.connection.host", "cassandra-lambda") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Spark session created successfully!")

# ------------------------------
# 2. Read dataset from HDFS
# ------------------------------
print("Reading e-commerce data from HDFS...")
df = spark.read.json("hdfs://namenode-lambda:8020/data/transactions.json")
print(f"Total transactions read: {df.count()}")
df.show(truncate=False)

# Convert purchase_date to pure date
df = df.withColumn("purchase_date_only", to_date(col("purchase_date")))

# ------------------------------
# 3. Create Dimension Tables
# ------------------------------
customer_dim = df.select("customer").distinct() \
    .withColumn("customer_id", monotonically_increasing_id()) \
    .withColumn("updated_at", current_timestamp())

category_dim = df.select("category").distinct() \
    .withColumn("category_id", monotonically_increasing_id()) \
    .withColumn("updated_at", current_timestamp())

region_dim = df.select("region").distinct() \
    .withColumn("region_id", monotonically_increasing_id()) \
    .withColumn("updated_at", current_timestamp())

product_dim = df.select("product").distinct() \
    .withColumn("product_id", monotonically_increasing_id()) \
    .withColumn("updated_at", current_timestamp())

date_dim = df.select(col("purchase_date_only").alias("date")).distinct() \
    .withColumn("date_id", monotonically_increasing_id()) \
    .withColumn("year", year("date")) \
    .withColumn("month", month("date")) \
    .withColumn("day", dayofmonth("date")) \
    .withColumn("updated_at", current_timestamp())

# ------------------------------
# 4. Create Fact Table (Normalized) with Customer Name
# ------------------------------
fact_table = df \
    .join(customer_dim, "customer") \
    .join(category_dim, "category") \
    .join(region_dim, "region") \
    .join(product_dim, "product") \
    .join(date_dim, df.purchase_date_only == date_dim.date) \
    .select(
        col("customer_id"),
        col("customer"),  # Include customer name in fact table
        col("category_id"),
        col("region_id"),
        col("product_id"),
        col("date_id"),
        col("sales").cast("double"),
        col("profit").cast("double"),
        current_timestamp().alias("updated_at")
    )

# ------------------------------
# 5. Save to Cassandra
# ------------------------------
def save_to_cassandra(table_name, df):
    df.write \
      .format("org.apache.spark.sql.cassandra") \
      .mode("append") \
      .options(table=table_name, keyspace="lambda_batch") \
      .save()
    print(f"✓ {table_name} saved to Cassandra")

try:
    save_to_cassandra("customer_dim", customer_dim)
    save_to_cassandra("category_dim", category_dim)
    save_to_cassandra("region_dim", region_dim)
    save_to_cassandra("product_dim", product_dim)
    save_to_cassandra("date_dim", date_dim)
    save_to_cassandra("sales_fact", fact_table)
except Exception as e:
    print(f"Error saving to Cassandra: {e}")
    print("Saving results to HDFS instead...")
    customer_dim.write.mode("overwrite").json("hdfs://namenode-lambda:8020/star_schema/customer_dim/")
    category_dim.write.mode("overwrite").json("hdfs://namenode-lambda:8020/star_schema/category_dim/")
    region_dim.write.mode("overwrite").json("hdfs://namenode-lambda:8020/star_schema/region_dim/")
    product_dim.write.mode("overwrite").json("hdfs://namenode-lambda:8020/star_schema/product_dim/")
    date_dim.write.mode("overwrite").json("hdfs://namenode-lambda:8020/star_schema/date_dim/")
    fact_table.write.mode("overwrite").json("hdfs://namenode-lambda:8020/star_schema/sales_fact/")

# ------------------------------
# 6. Aggregates - Include customer name in aggregates
# ------------------------------
customer_totals = fact_table.groupBy("customer_id", "customer").agg(  # Include customer name
    spark_sum("sales").alias("total_sales"),
    spark_sum("profit").alias("total_profit"),
    count("*").alias("order_count")
).withColumn("updated_at", current_timestamp())

category_totals = fact_table.groupBy("category_id").agg(
    spark_sum("sales").alias("total_sales"),
    spark_sum("profit").alias("total_profit"),
    count("*").alias("transaction_count")
).withColumn("updated_at", current_timestamp())

region_totals = fact_table.groupBy("region_id").agg(
    spark_sum("sales").alias("total_sales"),
    spark_sum("profit").alias("total_profit"),
    count("*").alias("transaction_count")
).withColumn("updated_at", current_timestamp())

save_to_cassandra("customer_totals", customer_totals)
save_to_cassandra("category_sales", category_totals)
save_to_cassandra("region_sales", region_totals)

# ------------------------------
# 7. KPIs
# ------------------------------
total_sales = df.agg(spark_sum("sales")).collect()[0][0]
total_profit = df.agg(spark_sum("profit")).collect()[0][0]
total_orders = df.count()
avg_order_value = total_sales / total_orders
profit_margin = (total_profit / total_sales) * 100

print("\n=== Key Performance Indicators ===")
print(f"Total Sales: ${total_sales:.2f}")
print(f"Total Profit: ${total_profit:.2f}")
print(f"Total Orders: {total_orders}")
print(f"Average Order Value: ${avg_order_value:.2f}")
print(f"Overall Profit Margin: {profit_margin:.2f}%")

spark.stop()
print("\n✅ E-commerce star schema batch job completed successfully!")