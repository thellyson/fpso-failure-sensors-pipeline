# Dockerfile
FROM apache/airflow:3.0.2-python3.12
# imagem oficial com Python 3.12 e Airflow 3.0.2 :contentReference[oaicite:4]{index=4}

USER root

# Instala Java (JRE) necessário para rodar PySpark
RUN apt-get update \
    && apt-get install -y openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copia as dependências Python e instala
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

USER airflow

# Configurações de ambiente para o Spark
ENV SPARK_HOME=/usr/local/lib/python3.12/site-packages/pyspark
ENV PATH="$SPARK_HOME/bin:${PATH}"
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
