from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

with DAG("md_pipeline_fspo",
         start_date=days_ago(1),
         schedule_interval="@daily",
         catchup=False) as dag:

    # ─── Bronze: ingest raw files para Postgres ───
    ingest_failures = SparkSubmitOperator(
        task_id="bronze_ingest_failures",
        application="scripts/ingest_failures.py",
        name="bronze_ingest_failures",
        conf={"spark.jars": "/path/to/postgres-jdbc.jar"},
        application_args=["--target-table","bronze.equipment_failure_sensors"]
    )
    
'''
    ingest_sensors = SparkSubmitOperator(
        task_id="bronze_ingest_sensors",
        application="ingest_sensors.py",
        name="bronze_ingest_sensors",
        conf={"spark.jars": "/path/to/postgres-jdbc.jar"},
        application_args=["--target-table","bronze.equipment_sensors"]
    )

    ingest_equipment = SparkSubmitOperator(
        task_id="bronze_ingest_equipment",
        application="ingest_equipment.py",
        name="bronze_ingest_equipment",
        conf={"spark.jars": "/path/to/postgres-jdbc.jar"},
        application_args=["--target-table","bronze.equipment"]
    )


    # ─── Silver: limpeza e join ───
    transform_silver = SparkSubmitOperator(
        task_id="silver_transform",
        application="transform_to_silver.py",
        name="silver_transform",
        conf={"spark.jars": "/path/to/postgres-jdbc.jar"},
        application_args=["--src-bronze-failures","bronze.equipment_failure_sensors",
                          "--src-bronze-sensors","bronze.equipment_sensors",
                          "--src-bronze-eq","bronze.equipment",
                          "--target-table","silver.failures_enriched"]
    )


    # ─── Gold: agregações finais ───
    aggregate_gold_counts = SparkSubmitOperator(
        task_id="gold_aggregate_counts",
        application="aggregate_gold.py",
        name="gold_aggregate_counts",
        conf={"spark.jars": "/path/to/postgres-jdbc.jar"},
        application_args=["--silver-table","silver.failures_enriched",
                          "--target-counts","gold.equipment_failure_counts",
                          "--target-avg","gold.avg_failures_per_asset_group",
                          "--target-rankings","gold.sensor_failure_rankings"]
    )
'''

    #[ingest_failures, ingest_sensors, ingest_equipment] >> transform_silver >> aggregate_gold_counts
[ingest_failures]
