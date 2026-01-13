## Architecture Diagram

Below is the detailed architecture of our stream analytics pipeline:

```text
┌──────────────────────┐
│ Kafka Producers      │       
│ (user_events) +      │
│ (transaction_events) │
└───────┬──────────────┘
        │
        ▼
┌───────────────┐
│ Kafka Topics  │
└───────┬───────┘
        │
        ▼
┌─────────────────┐
│ Kafka Consumers │
└───────┬─────────┘
        │
        ▼
┌────────────────────────────────┐
│ Raw JSON Files to Landing Zone │
└───────┬────────────────────────┘
        │
        ▼
┌──────────────────────────┐
│ Spark ETL Micro Batching │
│ DataFrames / SparkSQL    │
└───────┬──────────────────┘
        │
        ▼
┌────────────────────────┐
│ Cleaned Parquet Files  │
│ Land in Gold Zone      │
└────────────────────────┘
```

## Design Decisions

| Component | Technology | Reason for Selection |
|-----------|-----------|--------------------|
| **Event Streaming** | **Apache Kafka** | Kafka provides a distributed messaging system with producers, consumers, and topics. Kafka is also ideal for ingesting real-time user and transaction events. |
| **Batch Consumer** | **Python Script (Kafka Consumer → JSON files)** | Consumes messages from Kafka topics in short, bounded intervals and writes them as JSON files for Spark. This approach lets Airflow orchestrate batch tasks while still keeping data close to real-time. |
| **Data Processing** | **PySpark DataFrames / SparkSQL** | PySpark allows us to perform parallelized processing of large datasets. DataFrames and SparkSQL provide expressive APIs for complex joins, aggregations, window functions, and semi-structured JSON parsing. Spark’s cluster-aware design (master + workers) allows horizontal scaling and efficient resource utilization. |
| **Orchestration** | **Apache Airflow** | Airflow provides a DAG-based orchestration framework with scheduling, monitoring, and retry capabilities. By using BashOperator + `spark-submit`, Spark jobs are triggered reliably while maintaining full dependency control. Airflow’s UI gives visibility into pipeline health and logging. |
| **Containerization / Environment Isolation** | **Docker & Docker Compose** | Docker ensures consistent environments for Kafka, Spark, and Airflow. Compose enables easy orchestration of multiple services and volume mounts for persistent data/logs. |
| **Custom Airflow Image** | **Airflow + JDK + Spark Client** | Extending the official Airflow image allows `spark-submit` to be executed directly from DAG tasks without additional configuration, simplifying Spark integration and eliminating version mismatch issues. |
| **File Storage / Zones** | **Landing / Gold Zones (JSON → Parquet)** | Introducing a landing zone decouples ingestion from processing, enabling reprocessing and error recovery. The newly generated gold zone stores clean, transformed Parquet datasets ready for analytics. |

## Setup Instructions

### 1. Start everything fresh (with rebuild)
```
docker compose up -d --build
```

### 2. Start the Kafka producers

2.1 User Events Producer (new terminal)
```
python user_events_producer.py --bootstrap-servers localhost:9094 --topic user_events --interval 0.1
```

2.2 Transaction Events Producer (another terminal)
```
python transaction_events_producer.py --bootstrap-servers localhost:9094 --topic transaction_events --interval 0.1
```

### 3. Check Airflow accessibility
```
docker exec streamflow-airflow airflow dags list
```

If you see ModuleNotFoundError:
```
docker exec -it streamflow-airflow bash
```
```
su airflow
```
```
airflow dags list
```

### 4. Trigger the DAG manually
```
airflow dags trigger streamflow_main
```

### 5. Monitor in the Airflow UI
Open http://localhost:8082/
Watch the streamflow_main DAG run through all tasks
It should complete successfully with the fixes applied






