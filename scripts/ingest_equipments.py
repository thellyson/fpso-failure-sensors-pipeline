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
    r"\[(\d{4})[-/](\d{1,2})[-/](\d{1,2})"             # data YYYY-M-D ou YYYY/M/D
    r"(?:[ T](\d{1,2}):(\d{1,2}):(\d{1,2}))?\]"         # opcionalmente hora H:M:S
)



# permite override via ENV ou usa /data montado pelo Compose
data_dir   = DATA_DIR
#data_dir   = "./data"
input_file = find_input_file(data_dir, "equipment", "json")

spark = SparkSession.builder \
	.appName("bronze_ingest_equipments") \
	.getOrCreate()


# Definição do esquema
schema = StructType([
    StructField("equipment_id", LongType(), False),
    StructField("name", StringType(), False),
    StructField("group_name", StringType(), False)
])

# Ler o arquivo JSON com o esquema
df = spark.read.json(input_file, schema=schema, multiLine=True)

# JDBC URL para Postgres
jdbc_url   = get_jdbc_url()
common_opts = get_jdbc_opts()

df.write \
	.format("jdbc") \
	.option("url",      jdbc_url) \
	.option("dbtable",  "bronze.equipment") \
	.options(**common_opts) \
	.mode("overwrite") \
	.save()


df.show(50, truncate=False)
spark.stop()
sys.exit(0)