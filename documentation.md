┌──────────────────────┐
│ Kafka Producers      │       
│ (user_events) +      │
| (transaction_events) |
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

