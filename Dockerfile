FROM apache/airflow:3.0.2-python3.12

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      openjdk-17-jdk-headless \
      wget \
 && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER root
RUN mkdir -p /opt/airflow/jars \
 && wget -q https://jdbc.postgresql.org/download/postgresql-42.7.6.jar \
       -O /opt/airflow/jars/postgresql-42.7.6.jar

ENV SPARK_HOME=/usr/local/lib/python3.10/site-packages/pyspark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*.zip:$PYTHONPATH

COPY spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf

USER airflow
RUN airflow db init