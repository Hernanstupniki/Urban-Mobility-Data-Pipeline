---
name: docker-infrastructure
description: Use for Docker Compose, Airflow, Spark cluster, PostgreSQL, service configuration, health checks, and local infrastructure in Urban Mobility.
---

# Docker and infrastructure

Apply to `infra/airflow`, Compose files, service scripts, and related configuration. Inspect actual services and volumes before changes; validate Compose configuration and service health afterward. Preserve persistent data and established Spark factory/container conventions. Never prune volumes or reset the lake/database as a routine test. Avoid introducing Kafka or Grafana merely because they appeared in a generic description; use only services actually required by the task and project.
