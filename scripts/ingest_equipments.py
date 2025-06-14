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
input_file = find_input_file(data_dir, "equipment", "json")

spark = SparkSession.builder \
	.appName("bronze_ingest_equipments") \
	.getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Schema definition
schema = StructType([
    StructField("equipment_id", LongType(), False),
    StructField("name", StringType(), False),
    StructField("group_name", StringType(), False)
])

# Read JSON file with schema
df = spark.read.json(input_file, schema=schema, multiLine=True)

# JDBC URL for Postgres
jdbc_url   = get_jdbc_url()
common_opts = get_jdbc_opts()

# Load existing equipment_ids
existing = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**common_opts) \
    .option("dbtable", "bronze.equipment") \
    .load() \
    .select("equipment_id")

# Filter new equipment
new_equip = df.join(existing, on="equipment_id", how="left_anti")

# Insert only if there are new records
if new_equip.rdd.isEmpty():
    print("No new equipment to insert.")
else:
    new_equip.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "bronze.equipment") \
        .options(**common_opts) \
        .mode("append") \
        .save()
    print(f"{new_equip.count()} equipment(s) inserted.")

new_equip.show(50, truncate=False)
spark.stop()
sys.exit(0)