# Guía de Operación y Arquitectura: Airflow + Apache Spark en WSL

Este documento detalla la arquitectura, cómo está configurado el entorno local (emulando Cloud), cómo operarlo diariamente y cómo continuar agregando tareas de ingeniería de datos.

---

## 1. Arquitectura del Entorno

El pipeline está montado sobre **Docker Compose en WSL 2 (Ubuntu 22.04)** y accesible desde el navegador de Windows.

```text
[ Windows Host ] ─── Navegador Web (http://localhost:8080)
       │
       ▼ (WSL 2 Port Forwarding)
[ WSL 2: Ubuntu 22.04 ]
       │
       ├── Docker Compose (`infra/airflow/`)
       │     ├── airflow-webserver (Puerto 8080)
       │     ├── airflow-scheduler (Monitorea y agenda DAGs)
       │     ├── airflow-worker (Ejecutor Celery: con Java 17 + PySpark 3.5.0 + Delta)
       │     ├── airflow-triggerer
       │     ├── postgres (Metadata Database de Airflow)
       │     └── redis (Broker de mensajería para Celery)
       │
       └── Repositorio Local (`/home/hernan/Urban-Mobility-Data-Pipeline`)
             ├── Montado adentro de los contenedores como `/opt/project`
             └── Sus DAGs se sincronizan en tiempo real desde `infra/airflow/dags/`
```

---

## 2. Credenciales y URLs de Acceso

- **URL de la Interfaz Web:** [http://localhost:8080](http://localhost:8080)
- **Usuario:** `admin`
- **Contraseña:** `admin`

*(Para monitorear los contenedores desde la terminal de WSL o Windows puedes usar `lazydocker` o `docker compose ps`).*

---

## 3. Comandos de Operación Diaria (Cheat Sheet)

Ejecutar siempre desde la carpeta `infra/airflow/`:

```bash
cd /home/hernan/Urban-Mobility-Data-Pipeline/infra/airflow
```

### A. Iniciar el entorno
```bash
docker compose --profile spark-cluster up -d
```

Este es el modo normal: Airflow envía los jobs a
`spark://spark-master:7077`.

### B. Detener el entorno (sin perder datos ni historial)
```bash
docker compose stop
```
*(O `docker compose down` si deseas remover los contenedores).*

### C. Ver estado y logs
```bash
docker compose ps
docker compose logs -f airflow-worker
docker compose logs -f airflow-scheduler
```

### D. Si modificas el `Dockerfile` (nuevas librerías de Python o paquetes del sistema)
```bash
docker compose build
docker compose up -d
```

---

## 4. Estructura del DAG de Producción (`urban_mobility_pipeline.py`)

Ubicación del archivo:
`infra/airflow/dags/urban_mobility_pipeline.py`
*(Desde Windows: `\\wsl.localhost\Ubuntu-22.04\home\hernan\Urban-Mobility-Data-Pipeline\infra\airflow\dags\urban_mobility_pipeline.py`)*

El DAG implementa la **Arquitectura Medallion** con los siguientes componentes clave:

1. **`spark_execution_check` (`PythonOperator`):**
   - Inicializa una `SparkSession` real adentro del worker.
   - Demuestra que el entorno tiene acceso a Spark y puede procesar DataFrames distribuidos.

2. **`TaskGroups` organizados:**
   - `bronze_ingestion`: Ingesta cruda de zonas, pasajeros, conductores y viajes en paralelo.
   - `silver_processing`: Limpieza, deduplicación y estructuración de tablas maestras y de hechos.
   - `gold_marts`: Cálculo de agregaciones analíticas y KPIs.

3. **Flujo de Dependencias:**
   ```python
   start >> run_spark_check >> bronze_group >> silver_group >> gold_group >> end
   ```

---

## 5. Cómo agregar y continuar codeando tus tareas

### Patrón 1: Tareas con PySpark nativo (`PythonOperator`)
Úsalo cuando quieras procesar DataFrames directamente dentro del pipeline:

```python
from airflow.operators.python import PythonOperator

def mi_tarea_spark(**context):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("MiProceso").getOrCreate()

    # Lectura de datos Delta en tu repositorio montado
    df = spark.read.format("delta").load("/opt/project/data/dev/bronze/zones")

    # Transformaciones
    df_clean = df.dropDuplicates(["zone_id"])

    # Escritura en Silver
    df_clean.write.format("delta").mode("append").save("/opt/project/data/dev/silver/zones")
    spark.stop()

tarea_spark = PythonOperator(
    task_id="transformar_zonas_silver",
    python_callable=mi_tarea_spark,
)
```

### Patrón 2: Ejecutar scripts existentes del repositorio (`BashOperator`)
Úsalo para correr directamente tus scripts de `src/` o `scripts/`:

```python
from airflow.operators.bash import BashOperator

tarea_script = BashOperator(
    task_id="ingesta_zones_oltp",
    bash_command="python3 /opt/project/src/bronze/zones_oltp_to_bronze.py",
)
```

---

## 6. Variables de Entorno y Conexiones

En `infra/airflow/.env` se encuentran configuradas las variables que consumen los scripts:
- `ENV`: `dev`
- `OLTP_DB_HOST`: `host.docker.internal`
- `DB_NAME`: `mobility_oltp`
- `DB_USER`: `postgres`
- `DB_PASSWORD`: Password de tu base de datos relacional OLTP.

Cualquier cambio que guardes en tus archivos `.py` dentro de `dags/` se refleja automáticamente en la web sin necesidad de reiniciar Docker.

---

## 7. Recursos y concurrencia para desarrollo local

La configuración predeterminada protege WSL de ejecuciones Spark simultáneas:

- `AIRFLOW__CELERY__WORKER_CONCURRENCY=2`.
- `max_active_runs=1` en el DAG principal.
- pool `spark_pool` con un slot para todas las tareas Spark.
- driver Spark con `1g`.
- dos workers Spark con un core y `768m` cada uno.
- límites Docker de `0.5 CPU/512m` para el master y
  `1 CPU/1280m` para cada worker.

Los valores pueden ajustarse con variables de entorno:

```text
AIRFLOW_WORKER_CONCURRENCY=2
AIRFLOW_DAG_MAX_ACTIVE_RUNS=1
AIRFLOW_SPARK_POOL=spark_pool
AIRFLOW_SPARK_POOL_SLOTS=1
SPARK_MASTER=spark://spark-master:7077
SPARK_DRIVER_MEMORY=1g
SPARK_EXECUTOR_MEMORY=768m
SPARK_EXECUTOR_CORES=1
SPARK_WORKER_CORES=1
SPARK_WORKER_MEMORY=768m
```

El driver se ejecuta dentro de `airflow-worker`. El Spark Master sólo
coordina recursos. Cada Spark Worker aloja executors de un core. Los JARs de
Delta 3.1.0 se resuelven durante el build de la imagen y el driver los entrega
a los executors, evitando descargas concurrentes durante el DAG.

Interfaces disponibles mientras el stack está activo:

- Airflow: `http://localhost:8080`.
- Spark Master: `http://localhost:8081`.
- Spark Worker 1: `http://localhost:8082`.
- Spark Worker 2: `http://localhost:8083`.
- Spark Driver: `http://localhost:4040` mientras se ejecuta un job.

### Modo distribuido normal

```bash
docker compose build
docker compose --profile spark-cluster up -d
```

### Fallback local para debugging

```bash
docker compose --profile spark-cluster stop \
  spark-worker-1 spark-worker-2 spark-master

SPARK_MASTER='local[2]' docker compose up -d --force-recreate \
  airflow-webserver airflow-scheduler airflow-worker airflow-triggerer
```

Para volver al modo distribuido:

```bash
docker compose --profile spark-cluster up -d --force-recreate
```

### Inicio automático con WSL

La unidad `urban-mobility-airflow.service` ejecuta el modo distribuido normal
después de que Docker esté disponible. Se instala una sola vez con:

```bash
sudo install -m 0644 urban-mobility-airflow.service \
  /etc/systemd/system/urban-mobility-airflow.service
sudo systemctl daemon-reload
sudo systemctl enable --now urban-mobility-airflow.service
```
