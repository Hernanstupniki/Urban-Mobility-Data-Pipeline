FROM apache/airflow:2.10.5

USER root

# 1. Instalar Java (OpenJDK 17) necesario para Apache Spark
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Configurar JAVA_HOME para que PySpark lo detecte automaticamente
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

ENV SPARK_HOME=/home/airflow/.local/lib/python3.12/site-packages/pyspark
ENV SPARK_IVY_DIR=/home/airflow/.ivy2
ENV PATH="${SPARK_HOME}/bin:${PATH}"

ARG POSTGRES_JDBC_VERSION=42.7.8
ARG POSTGRES_JDBC_SHA256=2a32a9dcbc42d67a50ad3a0de5efd102c8d2be46720045f2cbd6689f160ab7c7

# 3. Instalar PySpark, Delta Lake y conectores dentro del entorno de Airflow
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    delta-spark==3.1.0 \
    psycopg2-binary==2.9.11 \
    faker==40.1.0

# El mismo driver JDBC queda disponible en el driver y en los executors.
RUN python -c \
    "import hashlib, pathlib, urllib.request; \
p = pathlib.Path('${SPARK_HOME}/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar'); \
urllib.request.urlretrieve( \
    'https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar', p); \
actual = hashlib.sha256(p.read_bytes()).hexdigest(); \
assert actual == '${POSTGRES_JDBC_SHA256}', (actual, '${POSTGRES_JDBC_SHA256}')"

# 4. Resolver Delta durante el build para evitar carreras de Ivy en runtime.
RUN SPARK_LOCAL_IP=127.0.0.1 python -c \
    "from delta import configure_spark_with_delta_pip; \
from pyspark.sql import SparkSession; \
builder = SparkSession.builder.master('local[1]').appName('cache_delta_jars') \
.config('spark.jars.ivy', '${SPARK_IVY_DIR}'); \
spark = configure_spark_with_delta_pip(builder).getOrCreate(); \
spark.stop()" \
    && find "${SPARK_IVY_DIR}/jars" \
        -name 'io.delta_delta-spark_2.12-3.1.0.jar' -print -quit | grep -q . \
    && find "${SPARK_IVY_DIR}/jars" \
        -name 'io.delta_delta-storage-3.1.0.jar' -print -quit | grep -q . \
    && chmod -R g=u "${SPARK_IVY_DIR}"

USER root

ARG RUNTIME_UID=50000

# Spark executors run with the host UID so shared Delta files remain writable.
# Register that UID for Hadoop/JVM user lookup when it differs from `airflow`.
RUN getent passwd "${RUNTIME_UID}" >/dev/null \
    || echo "runtime:x:${RUNTIME_UID}:0:Runtime User:/home/airflow:/bin/bash" >> /etc/passwd

USER airflow
