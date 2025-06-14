# Etapa única: Spark + Python + JDBC + pytest
FROM python:3.11-slim

ENV SPARK_VERSION=3.4.1 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    PATH=$SPARK_HOME/bin:$PATH

# 1) Instala Java, wget, tar e dependências de build
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless wget tar curl build-essential && \
    rm -rf /var/lib/apt/lists/*

# 2) Baixa e descompacta o Spark
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# 3) Instala PySpark, driver JDBC e pytest
RUN pip install --no-cache-dir \
      pyspark==${SPARK_VERSION} \
      psycopg2-binary \
      pytest && \
    wget -q -P ${SPARK_HOME}/jars https://jdbc.postgresql.org/download/postgresql-42.7.6.jar

#Instala dependencias adicionais
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4) Copia configurações e código
WORKDIR /app
COPY spark-defaults.conf ${SPARK_HOME}/conf/
COPY .env .env
COPY scripts/ ./scripts
COPY data/   ./data
COPY dash.py ./


# 5) Ponto de entrada padrão (pode ser sobrescrito no docker-compose)
ENTRYPOINT ["spark-submit", "--master", "local[*]", "--jars", "/opt/spark/jars/postgresql-42.7.6.jar"]
