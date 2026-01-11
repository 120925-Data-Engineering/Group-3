"""
StreamFlow ETL Job - PySpark Transformation Pipeline

Reads JSON from landing zone, applies transformations, writes Parquet to gold zone.

Pattern: ./data/landing/*.json -> (This Job) -> ./data/gold/
"""
import argparse
import sys
import os 
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Import factory
from spark_session_factory import create_spark_session

def run_etl(spark: SparkSession, input_path: str, output_path: str):
    """
    Main ETL pipeline: read -> transform -> write.

    Args:
        spark: Active SparkSession
        input_path: Landing zone path (e.g., '/opt/spark-data/landing/*.json')
        output_path: Gold zone path (e.g., '/opt/spark-data/gold')
    """
    # TODO: Implement
    # 1. Load Data Streams
    # Ingesting both types of JSON files from the landing zone
    user_df = spark.read.json(os.path.join(input_path, "user_events_*.json"))
    trans_df = spark.read.json(os.path.join(input_path, "transaction_events_*.json"))


    print("DEBUG: User Schema")
    user_df.printSchema()
    print("DEBUG: Transaction Schema")
    trans_df.printSchema()

    if user_df.count() == 0 or trans_df.count() == 0:
        print("Required data streams are missing or empty.")
        return

    # 2. Cleaning and Standardization
    # Standardize time and user IDs for joining
    user_cleaned = user_df.select(
        F.col("user_id").alias("u_id"),
        F.col("event_type"),
        F.to_timestamp(F.col("timestamp")).alias("activity_time")
    ).filter(F.col("u_id").isNotNull())

    trans_cleaned = trans_df.select(
        F.col("user_id").alias("t_id"),
        F.col("total").cast("double").alias("amount"),
        F.to_timestamp(F.col("timestamp")).alias("trans_time")
    ).fillna(0, subset=["amount"])

    # 3. Multi-Stage Transformations
    # Perform an Inner Join to find transactions associated with user activity
    enriched_df = user_cleaned.join(
        trans_cleaned, 
        user_cleaned.u_id == trans_cleaned.t_id, 
        "inner"
    ).drop("t_id")

    # 4. Window Functions: Ranking & Running Totals
    # Define window to track spend per user over time
    user_window = Window.partitionBy("u_id").orderBy("trans_time")
    
    transformed_df = enriched_df.withColumn(
        "transaction_rank", F.row_number().over(user_window)
    ).withColumn(
        "cumulative_spend", F.sum("amount").over(user_window)
    ).withColumn(
        "year", F.year("trans_time")
    ).withColumn(
        "month", F.month("trans_time")
    )

    # 5. Aggregations for Gold Zone
    gold_summary = transformed_df.groupBy("u_id", "year", "month") \
        .agg(
            F.count("event_type").alias("total_interactions"),
            F.sum("amount").alias("monthly_spend"),
            F.avg("amount").alias("avg_ticket_size")
        )

    # 6. Optimized Write
    # Save the detailed events as Parquet and summaries as CSV
    print(f"Writing to Gold Zone: {output_path}")
    
    transformed_df.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(os.path.join(output_path, "events_processed"))

    gold_summary.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(os.path.join(output_path, "user_summary"))

if __name__ == "__main__":
    # TODO: Create SparkSession, parse args, run ETL
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", default="/opt/spark-data/landing")
    parser.add_argument("--output_path", default="/opt/spark-data/gold")
    args = parser.parse_args()

    # Factory connects to spark://spark-master:7077
    spark = create_spark_session("StreamFlow_Enrichment_Job")

    try:
        run_etl(spark, args.input_path, args.output_path)
    finally:
        spark.stop()