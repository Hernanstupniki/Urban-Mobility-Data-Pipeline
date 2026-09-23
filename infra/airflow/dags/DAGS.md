# DAG responsibilities

Airflow keeps business processing, erasure propagation, and retention in separate DAGs because they have different schedules and failure boundaries.

| DAG | Purpose | `develop` | Planned production frequency |
|---|---|---|---|
| `urban_mobility_pipeline` | Ingest OLTP data, build Bronze/Silver/Gold, publish reporting tables | Manual | Daily |
| `dag_gdpr_compliance` | Propagate erasure requests and republish affected data | Manual | Weekly |
| `dag_lakehouse_retention_vacuum` | Apply Bronze/Silver retention and vacuum Gold files | Manual | Monthly |
| `dag_generate_mock_data` | Append dirty synthetic OLTP data for tests | Manual | Never scheduled |

The operational DAGs use the same `spark_pool` slot so Spark jobs do not overlap. The pipeline publishes only after Gold succeeds. The GDPR DAG republishes after an erasure, so consumers do not wait for the next daily run. Retention stays separate from normal processing. `catchup=False` prevents historical automatic backfills when schedules are enabled.

The DAG files remain the source of truth for exact schedule expressions. Use timezone-aware dates in `America/Asuncion` on the production branch and verify the next run times in Airflow before unpausing a DAG.
