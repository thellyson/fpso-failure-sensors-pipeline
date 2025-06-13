import os
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
    r"\[(\d{4})[-/](\d{1,2})[-/](\d{1,2})"             # data YYYY-M-D ou YYYY/M/D
    r"(?:[ T](\d{1,2}):(\d{1,2}):(\d{1,2}))?\]"         # opcionalmente hora H:M:S
)



# permite override via ENV ou usa /data montado pelo Compose
data_dir   = DATA_DIR
#data_dir   = "./data"
input_file = find_input_file(data_dir, "equipment_sensors", "csv")

spark = SparkSession.builder \
	.appName("bronze_ingest_equipment_sensors") \
	.getOrCreate()


# Definição do esquema
schema = StructType([
    StructField("equipment_id", LongType(), False),
	StructField("sensor_id", LongType(), False)
])


# Ler o arquivo CSV com cabeçalho
df = spark.read.csv(input_file, schema=schema, header=True)

# JDBC URL para Postgres
jdbc_url = "jdbc:postgresql://postgres:5432/fpso?stringtype=unspecified"

df.write \
	.format("jdbc") \
	.option("url",      jdbc_url) \
	.option("dbtable",  "bronze.equipment_sensors") \
	.option("user",     "shape") \
	.option("password", "shape") \
	.option("driver",   "org.postgresql.Driver") \
	.mode("overwrite") \
	.save()


df.show(50, truncate=False)
spark.stop()
sys.exit(0)