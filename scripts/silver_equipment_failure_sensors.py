import sys
from functions.env_config import get_jdbc_url, get_jdbc_opts
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

# 2) Cria sessão Spark
spark = SparkSession.builder \
    .appName("silver_ingest_equipment_failure_sensors") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "20") \
    .getOrCreate()

jdbc_url    = get_jdbc_url()
common_opts = get_jdbc_opts()

# 3) Leitura das tabelas bronze sem particionamento pesado
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


# 6) Carrega silver existente
existing_silver = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**common_opts) \
    .option("dbtable", "silver.equipment_failure_sensors") \
    .load() \
    .select("id")


# 7) Filtra só registros novos
to_insert = silver_df.join(existing_silver, on="id", how="left_anti")

# 8) Insere registros novos
if to_insert.rdd.isEmpty():
    print("Nenhuma linha nova para Silver.")
else:
    to_insert.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "silver.equipment_failure_sensors") \
        .options(**common_opts, batchsize="1000") \
        .mode("append") \
        .save()
    print(f"{to_insert.count()} linha(s) inserida(s) em Silver.")

# 9) Mostra amostra para verificação
to_insert.show(50, truncate=False)

spark.stop()
sys.exit(0)
