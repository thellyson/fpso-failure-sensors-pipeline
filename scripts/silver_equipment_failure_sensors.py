import sys
from functions.env_config import (
    get_jdbc_url,
    get_jdbc_opts,
    PG_HOST,
    PG_PORT,
    PG_DB,
    PG_USER,
    PG_PASSWORD
)
from pathlib import Path
import psycopg2
from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parent

# Count total rows in failures table
conn = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    dbname=PG_DB,
    user=PG_USER,
    password=PG_PASSWORD
)

cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM bronze.equipment_failure_sensors;")
total_rows = cur.fetchone()[0]
cur.close()
conn.close()

if total_rows == 0:
    print("No rows in Bronze; skipping Silver ingestion.")
    sys.exit(0)

# Spark session
spark = SparkSession.builder \
    .appName("silver_ingest_equipment_failure_sensors") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "20") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

jdbc_url    = get_jdbc_url()
common_opts = get_jdbc_opts()

# Read bronze tables
equipments_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**common_opts) \
    .option("dbtable", "bronze.equipment") \
	.option("fetchsize", "1000") \
    .load()

sensors_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**common_opts) \
    .option("dbtable", "bronze.equipment_sensors") \
    .load()

#Cache
equipments_df = equipments_df.cache()
sensors_df    = sensors_df.cache()

# Read failures with row number partitioning
failures_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**common_opts) \
    .option(
        "dbtable",
        f"(SELECT *, row_number() OVER (ORDER BY ts_utc, id) AS rn "
        f"FROM bronze.equipment_failure_sensors) AS t"
    ) \
    .option("partitionColumn", "rn") \
    .option("lowerBound", "1") \
    .option("upperBound", str(total_rows)) \
    .option("numPartitions", "10") \
    .load()

# Filter ERROR messages
failures_df = failures_df.where("type_msg = 'ERROR'")

# Join and select final columns
silver_df = (failures_df
    .join(sensors_df.select("sensor_id","equipment_id"),
          on="sensor_id", how="left")
    .join(equipments_df.select("equipment_id","name","group_name"),
          on="equipment_id", how="left")
    .select(
        "id",
        "sensor_id",
        "equipment_id",
        "name",
        "group_name",
        "type_msg",
        "temperature",
        "vibration",
        "ts_utc"
    )
)

# Load existing silver data
existing_silver = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**common_opts) \
    .option("dbtable", "silver.equipment_failure_sensors") \
    .load() \
    .select("id")

# Filter new records
to_insert = silver_df.join(existing_silver, on="id", how="left_anti").dropDuplicates(["id"])

# Insert new records
if to_insert.rdd.isEmpty():
    print("No new rows for Silver.")
else:
    to_insert.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "silver.equipment_failure_sensors") \
        .options(**common_opts, batchsize="1000") \
        .mode("append") \
        .save()
    print(f"{to_insert.count()} row(s) inserted in Silver.")

# Show sample for verification
to_insert.show(50, truncate=False)

spark.stop()
sys.exit(0)
