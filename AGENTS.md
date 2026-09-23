# Agent instructions

## Incident log

Read `docs/troubleshooting.md` before diagnosing a failure. For a new reproducible error, add an entry before ending the task. Use `DETECTED` when the cause is unknown, `PENDING` when a fix or its validation remains incomplete, and `RESOLVED` only after verification. Record the date, component, exact symptom, evidence, confirmed cause, affected files, fix, and validation. Keep secrets out of the log. Preserve past incidents.

## Language and comments

Write repository documentation, explanatory comments, docstrings, and user-facing text in English. Use short, plain sentences that explain what the code does or why a non-obvious constraint exists. In Python and shell files, use ordinary `#` comments; do not use decorative separator lines or mixed comment styles. Keep existing identifiers and data contracts stable unless the task requires a change. Do not rewrite historical data to translate it.

## Branches

`develop` is the test branch and keeps Airflow DAGs manual. `main` is the production branch and schedules the operational DAGs in `America/Asuncion`. Keep synthetic data generation manual and out of production execution. Do not switch the live development checkout to production code while Airflow is running from its bind mount.

## Urban Mobility skills

Load a relevant skill before changing its area. Project definitions live in `.config/opencode/skills/`; OpenCode discovers them through `.opencode/skills`. Codex also has global copies. Skills do not expand task scope or authorize data changes or deployment.

| Skill | Use it for |
|---|---|
| `powerbi-optimization` | Measured refresh, DAX, VertiPaq, query, or visual performance problems. |
| `power-bi-agentic-development` | Selecting the PBIP, PBIR, report design, theme, or audit specialist. |
| `pyspark-processing` | Spark and Delta transformations, joins, windows, and partitions. |
| `dirty-data-generation` | Synthetic data and controlled quality or GDPR anomalies. |
| `scd-modeling` | Dimension history, surrogate keys, and temporal joins. |
| `pbi-semantic-modeling` | Star-schema relationships, TMDL, DAX, and Gold SQL comparisons. |
| `geospatial-analysis` | Coordinates, zones, distances, and geometries. |
| `data-quality-testing` | Contracts, quality flags, and data tests. |
| `docker-infrastructure` | Compose, Airflow, Spark, PostgreSQL, and service health. |
| `airflow-dag-patterns` | DAG dependencies, retries, operators, and tests. Adapt patterns to existing DAGs. |
| `airflow-mcp-operations` | Live DAG, task, and log inspection only when an MCP connection exists. |
| `airflow-official-reference` | Version-specific Airflow 2.10.5 APIs and configuration. |

The PySpark, SCD, geospatial, and testing links in these skills are references; the engineering skills are defined for this repository. The data-goblin repository is a skill catalog. `bi/validation/gen_report.py` remains the source of truth for Power BI visuals.

`mcp-server-airflow` is an optional MCP server, not a skill. It is not configured here because this Airflow instance would require an API authentication change. Do not store credentials in Git or enable basic authentication with the development `admin/admin` account. Only trigger, pause, or resume DAGs when the task requests that operation.
