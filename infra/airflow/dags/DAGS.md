# Production DAG schedules

All times use `America/Argentina/Buenos_Aires`. The DAGs share one `spark_pool` slot, so Spark jobs do not overlap. `catchup=False` prevents historical automatic runs.

| DAG | Purpose | Schedule |
|---|---|---|
| `urban_mobility_pipeline` | Ingest OLTP data, build Bronze, Silver, and Gold, then publish reporting tables | Daily at 03:00 |
| `dag_gdpr_compliance` | Propagate erasure requests and republish affected data | Sunday at 23:00 |
| `dag_lakehouse_retention_vacuum` | Apply Bronze and Silver retention, then vacuum Gold files | First day of each month at 05:00 |

The synthetic data generator is available only in `develop`. Check the next run times in Airflow before activating these DAGs in a production environment.
