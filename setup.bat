@echo off
echo ============================================
echo Downloading Cassandra Connector for Spark
echo ============================================

echo.
echo [1/5] Downloading Spark Cassandra Connector JAR...
docker exec spark-master-lambda wget -q -P /opt/spark/jars/ https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.4.1/spark-cassandra-connector_2.12-3.4.1.jar

echo.
echo [2/5] Downloading Spark Cassandra Connector Driver JAR...
docker exec spark-master-lambda wget -q -P /opt/spark/jars/ https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-driver_2.12/3.4.1/spark-cassandra-connector-driver_2.12-3.4.1.jar

echo.
echo [3/5] Downloading compatible Cassandra Java Driver (v4.14.1)...
docker exec spark-master-lambda wget -q -P /opt/spark/jars/ https://repo1.maven.org/maven2/com/datastax/oss/java-driver-core/4.14.1/java-driver-core-4.14.1.jar

echo.
echo [4/5] Downloading Shaded Guava dependency...
docker exec spark-master-lambda wget -q -P /opt/spark/jars/ https://repo1.maven.org/maven2/com/datastax/oss/java-driver-shaded-guava/25.1-jre-graal-sub-1/java-driver-shaded-guava-25.1-jre-graal-sub-1.jar

echo.
echo [5/5] Downloading Typesafe Config dependency...
docker exec spark-master-lambda wget -q -P /opt/spark/jars/ https://repo1.maven.org/maven2/com/typesafe/config/1.4.2/config-1.4.2.jar

echo.
echo ============================================
echo Download Complete!
echo ============================================
echo.
echo Now run: app\run_ba_
