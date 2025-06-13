#!/usr/bin/env python3
import os
import re
import sys
import glob
import uuid
import datetime
from pathlib import Path
import psycopg2

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, TimestampType
)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data"

def find_input_file(data_dir: str) -> str:
    pattern = os.path.join(data_dir, "*failure_sensors*.txt")
    matches = glob.glob(pattern)
    if len(matches) != 1:
        print(f"[Erro] Esperava um único arquivo em '{data_dir}' que case com '*failure_sensors*.txt', achei: {matches}")
        sys.exit(1)
    return matches[0]

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
input_file = find_input_file(data_dir)

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

jdbc_ur = ( "jdbc:postgresql://postgres:5432/fpso"
           "?stringtype=unspecified")

final_df.write \
	.format("jdbc") \
	.option("url",      jdbc_ur) \
	.option("dbtable",  "bronze.equipment_failure_sensors") \
	.option("user",     "shape") \
	.option("password", "shape") \
	.option("driver",   "org.postgresql.Driver") \
	.mode("overwrite") \
	.save()


final_df.show(50, truncate=False)
spark.stop()
sys.exit(0)