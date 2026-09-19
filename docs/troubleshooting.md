# Registro de errores y soluciones

Este documento conserva incidentes reales del proyecto. Debe consultarse antes de investigar un fallo y actualizarse cuando aparezca uno nuevo, incluso si todavía no tiene solución.

Estados permitidos:

- `DETECTADO`: existe evidencia del error, pero la causa todavía no está confirmada.
- `PENDIENTE`: la causa o corrección se conoce, pero falta completar la implementación o validación.
- `RESUELTO`: la corrección fue aplicada y verificada.

## Plantilla para incidentes nuevos

```text
### YYYY-MM-DD — Título breve

- Estado: DETECTADO | PENDIENTE | RESUELTO
- Componente:
- Síntoma:
- Evidencia:
- Causa confirmada:
- Archivos/configuración implicados:
- Solución aplicada:
- Validación:
- Seguimiento pendiente:
```

No guardar contraseñas, tokens ni valores secretos en este archivo.

## Incidentes

### 2026-09-16 — Reinicio de WSL por presión de recursos Spark/Airflow

- Estado: `RESUELTO`
- Componente: Airflow, Celery, PySpark y WSL.
- Síntoma: WSL se reinició después de ejecutar el DAG con varios jobs Spark.
- Evidencia: el stack permitía concurrencia Celery 16, tareas Spark paralelas y sesiones/JVM independientes sin límites locales suficientemente acotados.
- Causa confirmada: combinación de múltiples procesos Spark concurrentes y límites de ejecución demasiado amplios para desarrollo local.
- Archivos/configuración implicados: `infra/airflow/docker-compose.yaml`, `infra/airflow/dags/urban_mobility_pipeline.py`, `src/common/spark.py` y `infra/wsl/urban-mobility-airflow.service`.
- Solución aplicada: cluster principal con un Master y dos Workers; 1 core y 768 MB Spark por worker; driver de 1 GB; `max_active_runs=1`; pool Airflow `spark_pool` de un slot; Celery concurrency 2; límites Docker; fallback `local[2]`; sin `local[*]`, aumento de swap ni cambios de `.wslconfig`.
- Validación: smoke tests local y distribuido completados; suite de 11 pruebas aprobada.

### 2026-09-16 — UI de workers anunciada con IP interna

- Estado: `RESUELTO`
- Componente: Spark Worker UI.
- Síntoma: el enlace del worker apuntaba a una IP como `172.18.0.6:8081`, inaccesible desde Windows.
- Causa confirmada: Spark publicaba la dirección interna del contenedor.
- Archivos/configuración implicados: `infra/airflow/docker-compose.yaml`.
- Solución aplicada: `SPARK_PUBLIC_DNS=localhost` y puertos separados `8082`/`8083` publicados en el host.
- Validación: ambas UI respondieron HTTP 200 desde `localhost`.

### 2026-09-16 — DAG fallaba conectando PostgreSQL en localhost

- Estado: `RESUELTO`
- Componente: ingesta Bronze JDBC.
- Síntoma: `Connection to localhost:5432 refused` en las cuatro tareas Bronze.
- Evidencia: `Settings` leía `DB_HOST`, mientras Compose sólo proporcionaba inicialmente `OLTP_DB_HOST`.
- Causa confirmada: nombres de variables inconsistentes entre el código y el contenedor.
- Archivos/configuración implicados: `src/common/config.py` e `infra/airflow/docker-compose.yaml`.
- Solución aplicada: `Settings` prioriza `OLTP_DB_HOST`, `OLTP_DB_PORT`, `OLTP_DB_NAME`, `OLTP_DB_USER` y `OLTP_DB_PASSWORD`, conservando `DB_*` como fallback para ejecución fuera de Airflow.
- Validación: conexión TCP y autenticación PostgreSQL exitosas desde Airflow; Bronze zones alcanzó y leyó JDBC.

### 2026-09-16 — Colisión de DB_HOST con el entrypoint de Airflow

- Estado: `RESUELTO`
- Componente: Airflow worker y Redis.
- Síntoma: el worker mostraba `Maximum number of retries (20) reached` e intentaba Redis en `host.docker.internal:6379`; el driver Spark desaparecía durante el job.
- Evidencia: el log del contenedor mostró `DB_HOST=host.docker.internal`, `DB_PORT=6379` y conexión Redis rechazada.
- Causa confirmada: `DB_HOST` es usada internamente por el entrypoint de la imagen Airflow y no puede reutilizarse para el OLTP.
- Archivos/configuración implicados: `infra/airflow/docker-compose.yaml` y `src/common/config.py`.
- Solución aplicada: eliminar `DB_HOST`/`DB_*` genéricas del entorno común de Airflow y usar exclusivamente `OLTP_DB_*` para la aplicación.
- Validación: Airflow worker quedó `healthy`, con `RestartCount=0`, y un job Spark completó sin perder el driver.

### 2026-09-16 — PostgreSQL de WSL inaccesible desde Docker

- Estado: `RESUELTO`
- Componente: PostgreSQL 14 local y red Docker.
- Síntoma: `host.docker.internal` primero no resolvía y luego respondía `Connection refused`.
- Evidencia: PostgreSQL escuchaba sólo en `127.0.0.1:5432`; la red `airflow_default` usa `172.18.0.0/16` con gateway `172.18.0.1`.
- Causa confirmada: Docker Engine nativo en WSL no proveía el mismo mapeo que Docker Desktop, y PostgreSQL no escuchaba en el bridge privado.
- Archivos/configuración implicados: `infra/airflow/docker-compose.yaml`, `/etc/postgresql/14/main/pg_hba.conf` y la configuración efectiva de PostgreSQL creada por `ALTER SYSTEM`.
- Solución aplicada: mapear `host.docker.internal` a `${DOCKER_HOST_GATEWAY:-172.18.0.1}`; escuchar sólo en `localhost,172.18.0.1`; permitir `mobility_oltp` para el usuario `postgres` únicamente desde `172.18.0.0/16` con `scram-sha-256`. Se creó el respaldo `/etc/postgresql/14/main/pg_hba.conf.pre-urban-mobility`.
- Validación: `AIRFLOW_DB_TCP_OK`, `WORKER1_DB_TCP_OK`, `WORKER2_DB_TCP_OK` y `POSTGRES_AUTH_OK`.
- Nota: si cambia la subred Compose, actualizar juntos `DOCKER_HOST_GATEWAY`, `listen_addresses` y la regla `pg_hba`; no abrir PostgreSQL a `0.0.0.0` como atajo.

### 2026-09-16 — Contraseña OLTP desalineada

- Estado: `RESUELTO`
- Componente: PostgreSQL/JDBC.
- Síntoma: `FATAL: password authentication failed for user "postgres"`.
- Causa confirmada: la contraseña del rol PostgreSQL local no coincidía con `DB_PASSWORD` en `infra/airflow/.env`.
- Archivos/configuración implicados: `infra/airflow/.env` y el rol local `postgres`.
- Solución aplicada: sincronizar el rol con el secreto ya configurado, sin imprimir ni versionar su valor.
- Validación: autenticación `psycopg2` desde Airflow y lectura JDBC Spark exitosas.

### 2026-09-16 — Permiso denegado sobre Delta compartido

- Estado: `RESUELTO`
- Componente: ejecutores Spark y volumen `/opt/project/data`.
- Síntoma: `FileNotFoundException ... Permission denied` bajo `data/dev/_control/etl_control`.
- Causa confirmada: Airflow ejecutaba con UID 1000 y los workers Spark con UID 50000 sobre el mismo bind mount.
- Archivos/configuración implicados: `infra/airflow/docker-compose.yaml` e `infra/airflow/Dockerfile`.
- Solución aplicada: ejecutar los workers con `${AIRFLOW_UID}` y registrar ese UID como usuario válido dentro de la imagen mediante el build arg `RUNTIME_UID`.
- Validación: ambos workers resolvieron `getent passwd 1000`; Bronze escribió/leyó Delta sin error de permisos.

### 2026-09-16 — Daemon y executors Spark no resolvían UID 1000

- Estado: `RESUELTO`
- Componente: Hadoop UserGroupInformation.
- Síntoma: `KerberosAuthException` causado por `NullPointerException: invalid null input: name`.
- Evidencia: el daemon arrancó con `SPARK_USER`, pero los JVM executors siguieron fallando porque UID 1000 no existía en `/etc/passwd`.
- Causa confirmada: `SPARK_USER` no sustituye el lookup Unix realizado dentro de cada executor.
- Archivos/configuración implicados: `infra/airflow/Dockerfile` e `infra/airflow/docker-compose.yaml`.
- Solución aplicada: registrar `RUNTIME_UID` en `/etc/passwd` durante el build. `SPARK_USER=airflow` se mantiene para el daemon.
- Validación: executors iniciaron y procesaron stages distribuidos en ambos workers.

### 2026-09-16 — Worker Spark no podía crear su directorio de trabajo

- Estado: `RESUELTO`
- Componente: Spark standalone Worker.
- Síntoma: `AccessDeniedException` al crear `${SPARK_HOME}/work`.
- Causa confirmada: `${SPARK_HOME}` pertenece a la imagen y no es escribible por UID 1000.
- Archivos/configuración implicados: `infra/airflow/docker-compose.yaml`.
- Solución aplicada: configurar `SPARK_WORKER_DIR=/tmp/spark-work` y pasar `--work-dir` al daemon.
- Validación: los dos workers se registraron correctamente en el Master y lanzaron executors.

### 2026-09-16 — Mensaje engañoso “job has not accepted any resources”

- Estado: `RESUELTO`
- Componente: Spark scheduler.
- Síntoma: `Initial job has not accepted any resources` aun con dos workers visibles.
- Evidencia: el Master lanzaba executors repetidamente y éstos terminaban con código 1.
- Causa confirmada: el mensaje era consecuencia, no causa; primero falló el work dir y luego el lookup del UID de los executors.
- Solución aplicada: corregir `SPARK_WORKER_DIR` y registrar `RUNTIME_UID`; no aumentar cores ni memoria.
- Validación: Bronze zones completó con `exit 0` y `status=NO_DATA` usando dos executors.

### 2026-09-16 — Referencia Gold a un archivo inexistente

- Estado: `RESUELTO`
- Componente: DAG `urban_mobility_pipeline`.
- Síntoma: el DAG apuntaba a `src/gold/driver_payouts.py`, archivo inexistente que habría fallado al alcanzar Gold.
- Archivos/configuración implicados: `infra/airflow/dags/urban_mobility_pipeline.py`.
- Solución aplicada: reemplazar esa tarea por el job existente `scripts/run/gold/_marts/aggregates/run_agg_driver_daily.sh`, con task id `compute_driver_daily_kpis`.
- Validación: compilación Python correcta, Airflow carga ambas tareas Gold y `airflow dags list-import-errors` no reporta errores.

### 2026-09-16 — Validación completa del DAG corregido

- Estado: `PENDIENTE`
- Componente: Airflow DAG run `manual__2026-09-16T06:14:39+00:00`.
- Situación: el run se creó mientras el scheduler todavía conservaba la versión serializada anterior. Luego scheduler, webserver y triggerer fueron recreados; Airflow ya lista `gold_marts.compute_driver_daily_kpis`.
- Validación completada: `compileall` correcto, `11 passed`, sin errores de importación, Bronze zones distribuido exitoso.
- Seguimiento pendiente: confirmar el estado del run existente o iniciar un run nuevo si conservó el grafo anterior, y verificar todas las tareas hasta `success`.

### 2026-09-16 — Recreación Compose interrumpida dejó nombres temporales

- Estado: `RESUELTO`
- Componente: Docker Compose.
- Síntoma: conflicto de nombre de contenedor durante `--force-recreate` después de que el comando excediera la ventana de espera.
- Causa confirmada: Compose estaba a mitad de su estrategia de renombrado/recreación cuando la invocación fue interrumpida.
- Solución aplicada: dejar terminar la reconciliación y volver a ejecutar `docker compose ... up -d --no-build`; no se borraron volúmenes ni datos.
- Validación: servicios Airflow saludables, Master saludable y ambos workers registrados.

### 2026-09-16 — Resolución Ivy aparece en cada inicio de Spark

- Estado: `RESUELTO`
- Componente: Delta Lake/Ivy.
- Síntoma: el log muestra `resolving dependencies`, lo que puede parecer una descarga concurrente.
- Evidencia: el reporte indicó `0 artifacts copied, 3 already retrieved` y `0 downloaded`.
- Causa confirmada: `configure_spark_with_delta_pip()` sigue resolviendo coordenadas, pero usa el cache precargado de la imagen.
- Archivos/configuración implicados: `infra/airflow/Dockerfile` y `src/common/spark.py`.
- Solución aplicada: precargar Delta 3.1.0 durante el build y compartir el cache Ivy de sólo lectura funcional; se mantienen PySpark 3.5.0 y Delta 3.1.0.
- Validación: ejecuciones posteriores no descargaron artefactos.

### 2026-09-16 — Creación de SparkSession duplicada en Silver y Gold

- Estado: `RESUELTO`
- Componente: jobs PySpark/Delta y scripts `run`.
- Síntoma: varios jobs Silver y dimensiones Gold usaban `SparkSession.builder.getOrCreate()` y configuración Delta/tuning local; los wrappers ejecutaban `spark-submit --packages/--jars` antes de que el código alcanzara el factory.
- Causa confirmada: coexistían implementaciones antiguas con el factory común, por lo que master, memoria, cores, Delta, paralelismo y classpath podían divergir por job.
- Archivos/configuración implicados: `src/common/spark.py`, `scripts/run/_common.sh`, ocho archivos bajo `src/silver`, tres dimensiones bajo `src/gold/_conformed/static`, `tmp/check_scd2.py` y `tests/integration/test_scd3_immediate_predecessor.py`.
- Solución aplicada: todos los jobs llaman exclusivamente `build_spark(job_name)`; el factory aplica master, memoria, cores, UI, Delta extensions/catalog, shuffle, paralelismo, tamaño de partición, Ivy, JARs opcionales, auto-merge y validación de retención. Los wrappers ejecutan Python y ya no crean un SparkContext previo con `spark-submit`.
- Validación: auditoría global sin violaciones; `SparkSession.builder`, `getOrCreate()` y `configure_spark_with_delta_pip()` aparecen sólo en `src/common/spark.py`; no quedan `spark.conf.set()` fuera del factory ni `spark-submit`, `--packages` o `--jars` en shell. Compilación y sintaxis shell correctas; 10 pruebas unitarias aprobadas.

### 2026-09-16 — Pytest de integración host espera resolución Ivy

- Estado: `PENDIENTE`
- Componente: `tests/integration/test_scd3_immediate_predecessor.py` ejecutado desde el venv WSL.
- Síntoma: pytest quedó esperando después de migrar el test a `build_spark()`.
- Evidencia: el proceso Java local se inició con `spark.jars.packages=io.delta:delta-spark_2.12:3.1.0`; el venv host no comparte el cache Ivy precargado de la imagen Docker. Había 6,1 GiB de RAM disponible y 0 swap usada, por lo que no fue OOM.
- Causa confirmada: resolución de paquetes Delta sin cache disponible en el entorno host.
- Solución prevista: ejecutar la integración dentro de la imagen Docker precargada o configurar un cache Ivy host válido; no volver a construir una SparkSession directa para acelerar el test.
- Seguimiento pendiente: ejecutar el test de integración cuando Docker quede estable.

### 2026-09-16 — Wrappers requieren Git dentro de la imagen

- Estado: `RESUELTO`
- Componente: scripts bajo `scripts/run`.
- Síntoma: `run_zones_bronze.sh: line 4: git: command not found`.
- Causa confirmada: los wrappers usan `git rev-parse` como fallback para encontrar la raíz, pero la imagen no instala Git.
- Archivos/configuración implicados: `infra/airflow/docker-compose.yaml` y wrappers `scripts/run/**/*.sh`.
- Solución aplicada: Compose define `PROJECT_DIR=/opt/project`; los wrappers usan ese valor y conservan `git rev-parse` sólo como fallback para ejecución host.
- Validación: configuración Compose válida. Smoke runtime pendiente por el incidente Docker descrito a continuación.

### 2026-09-16 — Metadatos Docker inconsistentes durante recreación

- Estado: `RESUELTO`
- Componente: Docker Engine en WSL y Compose.
- Síntoma: conflictos con nombres temporales, referencias `No such container`, capas `RW layer ... not found` y daemon Docker que vuelve a estado `inactive` durante la restauración.
- Evidencia: `journalctl -u docker` registró capas faltantes para los antiguos webserver, scheduler y triggerer. El problema apareció después de que WSL interrumpiera una operación `--force-recreate`; una segunda invocación Compose/systemd concurrente agravó la reconciliación.
- Causa confirmada: metadatos de contenedores efímeros quedaron a mitad de eliminación/recreación. Los volúmenes persistentes no fueron eliminados.
- Acciones realizadas: `docker compose down --remove-orphans` sin `-v` retiró red y contenedores del proyecto; al recrear, el daemon todavía restauró referencias antiguas y volvió a detenerse.
- Seguimiento pendiente: ninguno. Cerrado 2026-09-19.
- Solución aplicada (2026-09-19): el `--force-recreate` dejó tres contenedores zombie en `Dead` que `docker rm -f` y `container prune` no podían eliminar (el daemon respondía `No such container` pero `docker ps -a` los listaba). Se resolvió deteniendo `docker`/`docker.socket` como root y eliminando a mano los directorios huérfanos `/var/lib/docker/containers/<id-full>`, luego `systemctl start docker`, un único `docker compose --profile spark-cluster up -d` sin operaciones concurrentes y verificación de `docker.service active`.
- Validación: tras la recreación, el worker vio `GDPR_HASH_KEY` (largo 64) y `dag_gdpr_compliance` completó `success` (`manual__2026-09-19T00:12:54+00:00`, `row_count=0`, `touched_tables=0`, `vacuum_hours=168`); `urban_mobility_pipeline` había completado `success` con los wrappers (`manual__2026-09-18T21:55:14+00:00`).

### 2026-09-18 — BashOperator trata comandos terminados en `.sh` como plantilla Jinja

- Estado: `RESUELTO`
- Componente: `infra/airflow/dags/urban_mobility_pipeline.py` (BashOperator).
- Síntoma: `jinja2.exceptions.TemplateNotFound: 'bash /opt/project/scripts/run/run_zones_bronze.sh' not found in search path: '/opt/airflow/dags'`; las cuatro tareas Bronze fallaron en segundos y Silver/Gold quedaron en `upstream_failed`.
- Evidencia: log `attempt` de `bronze_ingestion.ingest_zones`, corrida `manual__2026-09-18T21:48:11+00:00`. El fallo ocurría en `render_templates`, antes de ejecutar el comando.
- Causa confirmada: BashOperator renderiza `bash_command` con Jinja; cuando la cadena termina en una extensión de plantilla conocida (`.sh`), Jinja intenta cargarla como archivo de template en vez de tratarla como texto literal.
- Archivos/configuración implicados: `infra/airflow/dags/urban_mobility_pipeline.py`, ocho `bash_command` que invocan wrappers de `scripts/run`.
- Solución aplicada: agregar un espacio final a cada `bash_command` (`"...run_x.sh "`), truco documentado por Airflow para desactivar la resolución de plantilla; el espacio es inofensivo para bash.
- Validación: corrida `manual__2026-09-18T21:55:14+00:00` completó `success` de Bronze a Gold (15 min, pool `spark_pool` de 1 slot), usando los wrappers del repo.
