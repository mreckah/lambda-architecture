@echo off
echo ============================================
echo Lambda Architecture - E-commerce Batch Layer (Star Schema)
echo ============================================

echo.
echo [1/6] Dropping and recreating Cassandra tables...

docker exec cassandra-lambda cqlsh -e "DROP KEYSPACE IF EXISTS lambda_batch;"
docker exec cassandra-lambda cqlsh -e "CREATE KEYSPACE IF NOT EXISTS lambda_batch WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"

docker exec cassandra-lambda cqlsh -e "DROP TABLE IF EXISTS lambda_batch.customer_dim;"
docker exec cassandra-lambda cqlsh -e "DROP TABLE IF EXISTS lambda_batch.category_dim;"
docker exec cassandra-lambda cqlsh -e "DROP TABLE IF EXISTS lambda_batch.region_dim;"
docker exec cassandra-lambda cqlsh -e "DROP TABLE IF EXISTS lambda_batch.product_dim;"
docker exec cassandra-lambda cqlsh -e "DROP TABLE IF EXISTS lambda_batch.date_dim;"
docker exec cassandra-lambda cqlsh -e "DROP TABLE IF EXISTS lambda_batch.sales_fact;"
docker exec cassandra-lambda cqlsh -e "DROP TABLE IF EXISTS lambda_batch.customer_totals;"
docker exec cassandra-lambda cqlsh -e "DROP TABLE IF EXISTS lambda_batch.category_sales;"
docker exec cassandra-lambda cqlsh -e "DROP TABLE IF EXISTS lambda_batch.region_sales;"

docker exec cassandra-lambda cqlsh -e "CREATE TABLE lambda_batch.customer_dim (customer_id bigint PRIMARY KEY, customer text, updated_at timestamp);"

docker exec cassandra-lambda cqlsh -e "CREATE TABLE lambda_batch.category_dim (category_id bigint PRIMARY KEY, category text, updated_at timestamp);"

docker exec cassandra-lambda cqlsh -e "CREATE TABLE lambda_batch.region_dim (region_id bigint PRIMARY KEY, region text, updated_at timestamp);"

docker exec cassandra-lambda cqlsh -e "CREATE TABLE lambda_batch.product_dim (product_id bigint PRIMARY KEY, product text, updated_at timestamp);"

docker exec cassandra-lambda cqlsh -e "CREATE TABLE lambda_batch.date_dim (date_id bigint PRIMARY KEY, date date, year int, month int, day int, updated_at timestamp);"

docker exec cassandra-lambda cqlsh -e "CREATE TABLE lambda_batch.sales_fact (customer_id bigint, customer text, category_id bigint, region_id bigint, product_id bigint, date_id bigint, sales double, profit double, updated_at timestamp, PRIMARY KEY ((customer_id, date_id), category_id, region_id, product_id));"

docker exec cassandra-lambda cqlsh -e "CREATE TABLE lambda_batch.customer_totals (customer_id bigint PRIMARY KEY, customer text, total_sales double, total_profit double, order_count int, updated_at timestamp);"

docker exec cassandra-lambda cqlsh -e "CREATE TABLE lambda_batch.category_sales (category_id bigint PRIMARY KEY, total_sales double, total_profit double, transaction_count int, updated_at timestamp);"

docker exec cassandra-lambda cqlsh -e "CREATE TABLE lambda_batch.region_sales (region_id bigint PRIMARY KEY, total_sales double, total_profit double, transaction_count int, updated_at timestamp);"

echo.
echo [2/6] Creating HDFS directory...
docker exec namenode-lambda hadoop fs -mkdir -p /data

echo.
echo [3/6] Uploading enhanced e-commerce dataset to HDFS...
docker exec namenode-lambda hadoop fs -put -f /app/datasets/transactions.json /data/

echo.
echo [4/6] Running Spark E-commerce Batch Job...
docker exec spark-master-lambda /opt/spark/bin/spark-submit ^
  --master spark://spark-master-lambda:7077 ^
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 ^
  --conf spark.cassandra.connection.host=cassandra-lambda ^
  --conf spark.cassandra.connection.port=9042 ^
  --conf spark.driver.extraJavaOptions=-Duser.home=/tmp ^
  /app/batch_job.py

echo.
echo [5/6] Viewing results from Cassandra...

echo Customer Dimension:
docker exec cassandra-lambda cqlsh -e "SELECT * FROM lambda_batch.customer_dim;"

echo Category Dimension:
docker exec cassandra-lambda cqlsh -e "SELECT * FROM lambda_batch.category_dim;"

echo Region Dimension:
docker exec cassandra-lambda cqlsh -e "SELECT * FROM lambda_batch.region_dim;"

echo Product Dimension:
docker exec cassandra-lambda cqlsh -e "SELECT * FROM lambda_batch.product_dim;"

echo Date Dimension:
docker exec cassandra-lambda cqlsh -e "SELECT * FROM lambda_batch.date_dim;"

echo Sales Fact Table:
docker exec cassandra-lambda cqlsh -e "SELECT customer_id, customer, category_id, region_id, product_id, date_id, sales, profit, updated_at FROM lambda_batch.sales_fact;"

echo Customer Totals:
docker exec cassandra-lambda cqlsh -e "SELECT * FROM lambda_batch.customer_totals;"

echo Category Sales:
docker exec cassandra-lambda cqlsh -e "SELECT * FROM lambda_batch.category_sales;"

echo Region Sales:
docker exec cassandra-lambda cqlsh -e "SELECT * FROM lambda_batch.region_sales;"

echo.
echo [6/6] Calculating Profit Margins...
docker exec cassandra-lambda cqlsh -e "SELECT category_id, total_sales, total_profit, (total_profit/total_sales)*100 as profit_margin FROM lambda_batch.category_sales;"

echo.
echo ============================================
echo Batch job completed!
echo ============================================

pause