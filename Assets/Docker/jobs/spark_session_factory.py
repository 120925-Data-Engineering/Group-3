"""
SparkSession Factory Module

Provides factory functions for creating SparkSession instances.
"""
from pyspark.sql import SparkSession
from typing import Optional


def create_spark_session(
    app_name: str,
    master: str = "spark://spark-master:7077",
    config_overrides: Optional[dict] = None
) -> SparkSession:
    """
    Create and return a configured SparkSession.

    Args:
        app_name: Name for the Spark application
        master: Spark master URL ("local[*]" or "spark://spark-master:7077")
        config_overrides: Optional dict of Spark configurations

    Returns:
        Configured SparkSession instance
    """
    builder = (
        SparkSession.builder 
        .appName(app_name) 
        .master(master) 
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
    )

    if config_overrides:
        for key, value in config_overrides.items():
            builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark