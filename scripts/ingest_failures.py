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
    r"\[(\d{4})[-/](\d{1,2})[-/](\d{1,2})"             # data YYYY-M-D ou YYYY/M/D
    r"(?:[ T](\d{1,2}):(\d{1,2}):(\d{1,2}))?\]"         # opcionalmente hora H:M:S
)

def parse_line_to_row(line: str):
    line = line.strip()
    if not line:
        return None

    # --- 1) Extrai e parseia o timestamp robustamente ---
    m = TIMESTAMP_RE.match(line)
    if not m:
        print(f"[WARN] timestamp inválido: {line!r}", file=sys.stderr)
        return None

    yr, mo, dy, hh, mi, ss = m.groups()
    year, month, day = map(int, (yr, mo, dy))
    hour  = int(hh) if hh is not None else 0
    minute= int(mi) if mi is not None else 0
    second= int(ss) if ss is not None else 0
    ts = datetime.datetime(year, month, day, hour, minute, second)

    try:
        # --- 2) Quebra em 4 partes por tab ---
        parts = line.split('\t', 3)
        if len(parts) < 4:
            print(f"[WARN] formato inesperado (menos de 4 colunas): {line!r}", file=sys.stderr)
            return None

        level     = parts[1]            # ERROR ou WARNING
        sensorseg = parts[2]            # ex: sensor[5820]:
        metrics   = parts[3].strip()    # ex: (temperature\t311.29, vibration\t6749.50)

        # --- 3) Extrai sensor_id ---
        sid = int(re.search(r"\[(\d+)\]", sensorseg).group(1))

        # --- 4) Limpa parênteses e separa temperatura/vibração ---
        if metrics.startswith('(') and metrics.endswith(')'):
            metrics = metrics[1:-1]
        temp_str, vib_str = [m.strip() for m in metrics.split(',')]
        temp_val = float(temp_str.split()[-1])
        vib_raw  = vib_str.split()[-1]
        if vib_raw.lower() == "err":
            return None
        vib_val  = float(vib_raw)

        # --- 5) Monta o Row para o schema Bronze ---
        return Row(
            id=f"{ts.isoformat()}_{sid}",
            type_msg    = level,
            sensor_id   = sid,
            temperature = temp_val,
            vibration   = vib_val,
            ts_utc      = ts
        )
    except Exception as e:
        print(f"[WARN] não parseou linha: {line!r} -> {e}", file=sys.stderr)
        return None


# permite override via ENV ou usa /data montado pelo Compose
data_dir   = DATA_DIR
#data_dir   = "./data"
input_file = find_input_file(data_dir, "equipment_failure_sensors", "txt")

spark = SparkSession.builder \
	.appName("bronze_ingest_failures") \
	.getOrCreate()

# lê texto bruto e aplica parse_line_to_row em Python
rdd = (
	spark.read.text(input_file)
			.rdd
			.map(lambda r: parse_line_to_row(r.value))
			.filter(lambda row: row is not None)
)

# schema idêntico ao da tabela bronze
schema = StructType([
	StructField("id",           StringType(),  False),
	StructField("type_msg",     StringType(),  False),
	StructField("sensor_id",    LongType(),    False),
	StructField("temperature",  DoubleType(),  False),
	StructField("vibration",    DoubleType(),  False),
	StructField("ts_utc",       TimestampType(), False),
])

final_df = spark.createDataFrame(rdd, schema)

# escreve via JDBC no Postgres
jdbc_url    = get_jdbc_url()
common_opts = get_jdbc_opts()


#Carrega ids já gravados
existing_ids = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**common_opts) \
    .option("dbtable", "bronze.equipment_failure_sensors") \
    .load() \
    .select("id")

#Filtra apenas as falhas novas
delta = final_df.join(existing_ids, on="id", how="left_anti")

if delta.rdd.isEmpty():
    print("Nenhuma falha nova para inserir.")
else:
    delta.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "bronze.equipment_failure_sensors") \
        .options(**common_opts) \
        .mode("append") \
        .save()
    print(f"{delta.count()} falha(s) inserida(s).")


delta.show(50, truncate=False)
spark.stop()
sys.exit(0)