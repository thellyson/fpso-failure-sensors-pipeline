import os
import re
import sys
from functions.env_config import get_jdbc_url, get_jdbc_opts
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, rank, col, first
from pyspark.sql.window import Window
import psycopg2
from psycopg2.extras import execute_values
from functions.env_config import get_jdbc_url, get_jdbc_opts

# diretório base (assume que o script ficará em dags/scripts)
BASE_DIR = Path(__file__).resolve().parent


# 1) Cria sessão Spark
spark = SparkSession.builder \
    .appName("gold_equipment_failures_summary") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "20") \
    .getOrCreate()

# 2) Opções JDBC comuns
jdbc_url    = get_jdbc_url()
common_opts = get_jdbc_opts()

# 3) Leitura da tabela silver.equipment_failure_sensors
silver_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**common_opts) \
    .option("dbtable", "silver.equipment_failure_sensors") \
    .option("fetchsize", "1000") \
    .load()

#Cache
silver_df = silver_df.cache()

# 4) Agregações:

# 4.1) Total de falhas por equipamento
equipment_totals = (
    silver_df
    .groupBy("equipment_id", "name", "group_name")
    .agg(count("*").alias("total_failures"))
)

# 4.2) Média de falhas por ativo em cada grupo
group_avg = (
    equipment_totals
    .groupBy("group_name")
    .agg(avg("total_failures").alias("avg_failures_per_asset"))
)

# 4.3) Total de falhas por sensor em cada equipamento
sensor_totals = (
    silver_df
    .groupBy("equipment_id", "sensor_id",)
    .agg(count("*").alias("sensor_failures"),
		 first("group_name").alias("f_group_name"))
	
)

# 4.4) Ranking dos sensores
window_by_equipment = Window.partitionBy("equipment_id") \
                            .orderBy(col("sensor_failures").desc())
window_by_group     = Window.partitionBy("f_group_name") \
                            .orderBy(col("sensor_failures").desc())

sensor_ranked = sensor_totals \
    .withColumn("sensor_rank_by_equipment", rank().over(window_by_equipment)) \
    .withColumn("sensor_rank_by_equipment_group", rank().over(window_by_group))

# 5) Monta o DataFrame final do Gold unindo tudo
gold_df = (
    equipment_totals
    .join(group_avg,    on="group_name", how="inner")
    .join(sensor_ranked, on="equipment_id", how="inner")
    .select(
        "equipment_id",
        "name",
        "group_name",
        "total_failures",
        "avg_failures_per_asset",
        "sensor_id",
        "sensor_failures",
        "sensor_rank_by_equipment",
		"sensor_rank_by_equipment_group"
    )
)

#Escrita na tabela
gold_df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "gold.equipment_failures_summary") \
    .options(**common_opts, batchsize="1000") \
    .mode("overwrite") \
    .option("truncate", "true") \
    .save()

print("Tabela gold.equipment_failures_summary atualizada (overwrite).")

spark.stop()
sys.exit(0)
