#!/usr/bin/env python3
import sys
from pathlib import Path
import psycopg2
from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parent

# 1) Conta o total de linhas na tabela de falhas
conn = psycopg2.connect(
    host="postgres",
    dbname="fpso",
    user="shape",
    password="shape"
)
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM bronze.equipment_failure_sensors;")
total_rows = cur.fetchone()[0]
cur.close()
conn.close()

# 2) Cria sessão Spark (aumente driver.memory se necessário)
spark = SparkSession.builder \
    .appName("silver_ingest_equipment_failure_sensors") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://postgres:5432/fpso?stringtype=unspecified"
common_opts = {
    "user": "shape",
    "password": "shape",
    "driver": "org.postgresql.Driver",
    "fetchsize": "1000"
}

# 3) Leitura das tabelas bronze sem particionamento pesado
equipments_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**common_opts) \
    .option("dbtable", "bronze.equipment") \
    .load()

sensors_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**common_opts) \
    .option("dbtable", "bronze.equipment_sensors") \
    .load()

# 4) Leitura particionada por número de linha (row_number)  
#    subconsulta adiciona coluna rn de 1 até total_rows
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


# 4.5) Filtra apenas as linhas com type_msg = 'ERROR'
failures_df = failures_df.where("type_msg = 'ERROR'")

# 5) Junta e seleciona colunas finais para silver
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

# 6) Grava no Postgres em silver.equipment_failure_sensors
silver_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**common_opts) \
    .option("dbtable", "silver.equipment_failure_sensors") \
    .mode("overwrite") \
    .save()

# 7) Mostra amostra para verificação
silver_df.show(50, truncate=False)

spark.stop()
sys.exit(0)
