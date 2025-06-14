import os
from functions.env_config import get_jdbc_url, get_jdbc_opts
import re
import sys
import glob
import uuid
import datetime
from pathlib import Path
import psycopg2
from functions.util import find_input_file

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, TimestampType
)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data"

TIMESTAMP_RE = re.compile(
    r"\[(\d{4})[-/](\d{1,2})[-/](\d{1,2})"             # date YYYY-M-D or YYYY/M/D
    r"(?:[ T](\d{1,2}):(\d{1,2}):(\d{1,2}))?\]"         # optional time H:M:S
)

# Allow override via ENV or use /data mounted by Compose
data_dir   = DATA_DIR
#data_dir   = "./data"
input_file = find_input_file(data_dir, "equipment_sensors", "csv")

spark = SparkSession.builder \
	.appName("bronze_ingest_equipment_sensors") \
    .config("spark.sql.shuffle.partitions", "20") \
	.getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Schema definition
schema = StructType([
    StructField("equipment_id", LongType(), False),
	StructField("sensor_id", LongType(), False)
])

# Read CSV file with header
df = spark.read.csv(input_file, schema=schema, header=True)

# JDBC URL for Postgres
jdbc_url    = get_jdbc_url()
common_opts = get_jdbc_opts()

# Load existing sensor_ids
existing = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**common_opts) \
    .option("dbtable", "bronze.equipment_sensors") \
    .load() \
    .select("sensor_id")

# Filter new sensors
new_sensors = df.join(existing, on="sensor_id", how="left_anti")

if new_sensors.rdd.isEmpty():
    print("No new sensors to insert.")
else:
    new_sensors.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "bronze.equipment_sensors") \
        .options(**common_opts) \
        .options(**common_opts, batchsize="1000") \
        .mode("append") \
        .save()
    print(f"{new_sensors.count()} sensor(s) inserted.")

new_sensors.show(50, truncate=False)
spark.stop()
sys.exit(0)