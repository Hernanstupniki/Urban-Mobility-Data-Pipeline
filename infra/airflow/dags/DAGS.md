Ran command: `wsl -d Ubuntu-22.04 -e ls -la /home/hernan/Urban-Mobility-Data-Pipeline/scripts/run`

**No, rotundamente NO se hace 1 solo DAG.** 
Hacer un único "mega-DAG" para todo el sistema es uno de los errores más comunes de los principiantes (se le conoce como el anti-patrón del *DAG Monolítico*).

Te explico **la teoría de cómo se divide en la industria** y **exactamente cuántos DAGs deberías tener en este proyecto**.

---

### 1. La Regla de Oro en Producción: ¿Cuándo separar en DAGs distintos?

Un DAG debe responder a una única unidad de negocio o ciclo de vida. Separas en distintos DAGs cuando:
1. **Diferente Frecuencia:** Una cosa corre cada hora, otra una vez al día y otra una vez al mes.
2. **Diferente Dominio/Responsabilidad:** Los datos transaccionales del negocio no se mezclan con tareas de mantenimiento, limpieza legal o migraciones.
3. **Manejo de Fallos (Blast Radius):** Si falla el cálculo mensual de finanzas, **no debería bloquear** la ingesta de viajes en tiempo real de cada hora.

---

### 2. En TU Proyecto: ¿Cuántos DAGs deberías tener?

Al mirar tu repositorio (`scripts/run/`, `retention/`, `gdpr/`, `migrations/`), este proyecto pide naturalmente **4 DAGs bien definidos**:

```text
                                  AIRFLOW
                                     │
       ┌─────────────────────────────┼─────────────────────────────┐
       ▼                             ▼                             ▼
┌──────────────┐              ┌──────────────┐              ┌──────────────┐
│ DAG 1 (Core) │              │ DAG 2 (GDPR) │              │ DAG 3 (Ops)  │
│  Movilidad   │              │  Privacidad  │              │  Retención   │
│  Frecuencia: │              │  Frecuencia: │              │  Frecuencia: │
│    Diaria    │              │   Semanal    │              │   Mensual    │
└──────────────┘              └──────────────┘              └──────────────┘
```

#### DAG 1: El Pipeline Principal de Datos (`dag_urban_mobility_daily`)
* **Qué hace:** Es el que acabamos de armar: Ingesta Bronze -> Limpieza Silver -> Agregaciones Gold.
* **Frecuencia:** Diario (`@daily` a las 02:00 AM).
* **Por qué separado:** Es el corazón del negocio (los viajes, los choferes y los cobros de cada día).

#### DAG 2: El Pipeline de Cumplimiento y Privacidad (`dag_gdpr_compliance`)
* **Qué hace:** En tu proyecto tienes una carpeta `scripts/run/gdpr/`. La regulación de privacidad (GDPR / protección de datos personales) exige anonimizar datos de pasajeros que solicitaron ser olvidados o enmascarar tarjetas de crédito después de X días.
* **Frecuencia:** Semanal (`@weekly` los domingos a la noche).
* **Por qué separado:** Es un proceso legal/compliance. No tiene sentido correrlo cada vez que procesas viajes diarios.

#### DAG 3: Mantenimiento y Retención (`dag_lakehouse_retention_vacuum`)
* **Qué hace:** En tu repo tienes `scripts/run/retention/` y usas **Delta Lake**.
  Delta Lake guarda el historial de cambios (Time Travel). Si no le pasas un `VACUUM` periódicamente para borrar archivos obsoletos de más de 30 días, el almacenamiento en disco crece infinitamente.
* **Frecuencia:** Mensual (`@monthly`).
* **Por qué separado:** Es una tarea de mantenimiento del Lakehouse (Data Engineering Ops).

#### DAG 4 (Opcional en desarrollo): Inyección de Datos Sintéticos (`dag_generate_mock_data`)
* **Qué hace:** Llama a tu script `generate_oltp_data.py` para simular que 500 personas hicieron viajes hoy en Postgres.
* **Frecuencia:** Manual (`schedule=None`).
* **Por qué separado:** Solo te sirve cuando quieres probar cosas nuevas sin ensuciar la lógica del pipeline real.

---

### 3. ¿Cómo se comunican los DAGs entre sí en la Nube?

Si tienes varios DAGs y el DAG B necesita que termine el DAG A, **no se meten en el mismo archivo**. Se usan mecanismos profesionales de Airflow:
1. **`Dataset` (Event-driven):** Cuando el DAG 1 actualiza la tabla `silver/trips`, Airflow detecta que el dataset cambió y despierta automáticamente al DAG de reportes.
2. **`ExternalTaskSensor`:** Una tarea en un DAG que "espera" a que una tarea de otro DAG se ponga en verde.

---

### Resumen para tu ruta de aprendizaje:
Empieza con **`urban_mobility_pipeline.py`** como tu DAG principal. 
Cuando ese esté corriendo completo de Bronze a Gold, creas un segundo archivo en `dags/` (por ejemplo `retention_pipeline.py`) para tus scripts de mantenimiento y verás cómo en tu panel web de Airflow aparecen como sistemas desacoplados, limpios y modulares.

