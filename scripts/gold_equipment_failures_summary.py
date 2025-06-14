import os
import re
import sys
from functions.env_config import get_jdbc_url, get_jdbc_opts
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, rank, col, first
from pyspark.sql.window import Window
import psycopg2
from psycopg2.extras import execute_values
from functions.env_config import get_jdbc_url, get_jdbc_opts

# base directory
BASE_DIR = Path(__file__).resolve().parent


# Spark session
spark = SparkSession.builder \
    .appName("gold_equipment_failures_summary") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "20") \
    .getOrCreate()

# JDBC settings
jdbc_url    = get_jdbc_url()
common_opts = get_jdbc_opts()

# Read source table
silver_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**common_opts) \
    .option("dbtable", "silver.equipment_failure_sensors") \
    .option("fetchsize", "1000") \
    .load()

#Cache
silver_df = silver_df.cache()

# Aggregations
equipment_totals = (
    silver_df
    .groupBy("equipment_id", "name", "group_name")
    .agg(count("*").alias("total_failures"))
)

group_avg = (
    equipment_totals
    .groupBy("group_name")
    .agg(avg("total_failures").alias("avg_failures_per_asset"))
)

sensor_totals = (
    silver_df
    .groupBy("equipment_id", "sensor_id",)
    .agg(count("*").alias("sensor_failures"),
		 first("group_name").alias("f_group_name"))
	
)

# Sensor rankings
window_by_equipment = Window.partitionBy("equipment_id") \
                            .orderBy(col("sensor_failures").desc())
window_by_group     = Window.partitionBy("f_group_name") \
                            .orderBy(col("sensor_failures").desc())

sensor_ranked = sensor_totals \
    .withColumn("sensor_rank_by_equipment", rank().over(window_by_equipment)) \
    .withColumn("sensor_rank_by_equipment_group", rank().over(window_by_group))

# Final DataFrame
gold_df = (
    equipment_totals
    .join(group_avg,    on="group_name", how="inner")
    .join(sensor_ranked, on="equipment_id", how="inner")
    .select(
        "equipment_id",
        "name",
        "group_name",
        "total_failures",
        "avg_failures_per_asset",
        "sensor_id",
        "sensor_failures",
        "sensor_rank_by_equipment",
		"sensor_rank_by_equipment_group"
    )
)

# Write results
gold_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "gold.equipment_failures_summary") \
    .options(**common_opts, batchsize="1000") \
    .mode("overwrite") \
    .option("truncate", "true") \
    .save()

print("Table gold.equipment_failures_summary updated.")

spark.stop()
sys.exit(0)
