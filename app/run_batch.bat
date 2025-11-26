@echo off

echo ============================================
echo Lambda Architecture - Batch Layer
echo ============================================

echo.
echo [1/3] Creating HDFS directory...
docker exec namenode-lambda hdfs dfs -mkdir -p /data

echo.
echo [2/3] Uploading dataset to HDFS...
docker exec namenode-lambda hdfs dfs -put -f /app/datasets/transactions.json /data/

echo.
echo [3/3] Running Spark Batch Job...
docker exec spark-master-lambda spark-submit ^
    --master spark://spark-master-lambda:7077 ^
    /app/batch_job.py

echo.
echo ============================================
echo Batch job completed!
echo ============================================
pause