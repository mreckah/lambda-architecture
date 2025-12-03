# E-commerce Lambda Architecture - Batch & Serving Layer

## Project Overview
Batch processing system for e-commerce data using Lambda Architecture. Processes historical data for business intelligence.

## Architecture

![alt text](screens/lambda-architecture.gif)

## Documentation Sections

### HDFS - Data Storage
![alt text](screens/image.png)

### Spark - Batch Processing
![alt text](screens/image-1.png)

### Cassandra - Data Storage
![alt text](screens/image-2.png)
![alt text](screens/image-3.png)
![alt text](screens/image-4.png)
![alt text](screens/image-5.png)
![alt text](screens/image-6.png)
![alt text](screens/image-7.png)

### Grafana - Analytics Dashboard
![alt text](screens/image-8.png)

## Batch Processing Results

### Key Business Metrics
- **Total Sales**: $6,305.00
- **Total Profit**: $1,869.60
- **Total Orders**: 46
- **Profit Margin**: 29.65%

## Access Points
- **Grafana**: http://localhost:3000
- **Spark UI**: http://localhost:8082
- **HDFS**: http://localhost:9870