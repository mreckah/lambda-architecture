@echo off
echo ============================================
echo Lambda Architecture - Batch Layer
echo ============================================

echo.
echo [1/4] Setting up Cassandra...
docker exec cassandra-lambda cqlsh -e "CREATE KEYSPACE IF NOT EXISTS lambda_batch WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
docker exec cassandra-lambda cqlsh -e "CREATE TABLE IF NOT EXISTS lambda_batch.customer_totals (customer text PRIMARY KEY, total_amount double, updated_at timestamp);"

echo.
echo [2/4] Creating HDFS directory...
docker exec namenode-lambda hadoop fs -mkdir -p /data

echo.
echo [3/4] Uploading dataset to HDFS...
docker exec namenode-lambda hadoop fs -put -f /app/datasets/transactions.json /data/

echo.
echo [4/4] Running Spark Batch Job...
docker exec spark-master-lambda /opt/spark/bin/spark-submit ^
  --master spark://spark-master-lambda:7077 ^
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 ^
  --conf spark.cassandra.connection.host=cassandra-lambda ^
  --conf spark.cassandra.connection.port=9042 ^
  --conf spark.driver.extraJavaOptions=-Duser.home=/tmp ^
  /app/batch_job.py

echo.
echo ============================================
echo Batch job completed!
echo ============================================

echo.
echo Viewing results from Cassandra...
docker exec cassandra-lambda cqlsh -e "SELECT * FROM lambda_batch.customer_totals;"

pause
```

**Steps to run:**

1. First, run the setup (only once):
```
   setup_spark_cassandra.bat
```

2. Then run your batch job:
```
   app\run_batch.bat