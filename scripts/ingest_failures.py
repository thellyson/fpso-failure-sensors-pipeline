#!/usr/bin/env python3
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

def parse_line_to_row(line: str):
    line = line.strip()
    if not line:
        return None

    # Extract and parse timestamp
    m = TIMESTAMP_RE.match(line)
    if not m:
        print(f"[WARN] invalid timestamp: {line!r}", file=sys.stderr)
        return None

    yr, mo, dy, hh, mi, ss = m.groups()
    year, month, day = map(int, (yr, mo, dy))
    hour  = int(hh) if hh is not None else 0
    minute= int(mi) if mi is not None else 0
    second= int(ss) if ss is not None else 0
    ts = datetime.datetime(year, month, day, hour, minute, second)

    try:
        # Split into 4 parts by tab
        parts = line.split('\t', 3)
        if len(parts) < 4:
            print(f"[WARN] unexpected format (less than 4 columns): {line!r}", file=sys.stderr)
            return None

        level     = parts[1]            # ERROR or WARNING
        sensorseg = parts[2]            # ex: sensor[5820]:
        metrics   = parts[3].strip()    # ex: (temperature\t311.29, vibration\t6749.50)

        # Extract sensor_id
        sid = int(re.search(r"\[(\d+)\]", sensorseg).group(1))

        # Clean parentheses and split temperature/vibration
        if metrics.startswith('(') and metrics.endswith(')'):
            metrics = metrics[1:-1]
        temp_str, vib_str = [m.strip() for m in metrics.split(',')]
        temp_val = float(temp_str.split()[-1])
        vib_raw  = vib_str.split()[-1]
        if vib_raw.lower() == "err":
            return None
        vib_val  = float(vib_raw)

        # Create Row for Bronze schema
        return Row(
            id=f"{ts.isoformat()}_{sid}",
            type_msg    = level,
            sensor_id   = sid,
            temperature = temp_val,
            vibration   = vib_val,
            ts_utc      = ts
        )
    except Exception as e:
        print(f"[WARN] failed to parse line: {line!r} -> {e}", file=sys.stderr)
        return None

# Allow override via ENV or use /data mounted by Compose
data_dir   = DATA_DIR
#data_dir   = "./data"
input_file = find_input_file(data_dir, "equipment_failure_sensors", "txt")

spark = SparkSession.builder \
	.appName("bronze_ingest_failures") \
	.getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read raw text and apply parse_line_to_row in Python
rdd = (
	spark.read.text(input_file)
			.rdd
			.map(lambda r: parse_line_to_row(r.value))
			.filter(lambda row: row is not None)
)

# Schema identical to bronze table
schema = StructType([
	StructField("id",           StringType(),  False),
	StructField("type_msg",     StringType(),  False),
	StructField("sensor_id",    LongType(),    False),
	StructField("temperature",  DoubleType(),  False),
	StructField("vibration",    DoubleType(),  False),
	StructField("ts_utc",       TimestampType(), False),
])

final_df = spark.createDataFrame(rdd, schema)

final_df = final_df.dropDuplicates(["id"])

# Write via JDBC to Postgres
jdbc_url    = get_jdbc_url()
common_opts = get_jdbc_opts()

# Load existing ids
existing_ids = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**common_opts) \
    .option("dbtable", "bronze.equipment_failure_sensors") \
    .load() \
    .select("id")

# Filter new failures
delta = (
    final_df
    .join(existing_ids, on="id", how="left_anti")
    .dropDuplicates(["id"]) 
)

if delta.rdd.isEmpty():
    print("No new failures to insert.")
else:
    delta.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "bronze.equipment_failure_sensors") \
        .options(**common_opts) \
        .mode("append") \
        .save()
    print(f"{delta.count()} failure(s) inserted.")

delta.show(50, truncate=False)
spark.stop()
sys.exit(0)