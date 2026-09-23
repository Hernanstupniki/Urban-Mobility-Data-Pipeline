---
name: airflow-mcp-operations
description: Use for monitoring live Urban Mobility Airflow DAG runs, task instances, and logs through the configured Airflow MCP server; use mutation tools only when the task requests them.
---

# Airflow MCP operations

The optional MCP server is based on https://github.com/tomnagengast/mcp-server-airflow . Check whether the server is configured and reachable before calling it. List DAGs, runs, tasks, and logs to diagnose an incident. For a new failure, follow the project's `docs/troubleshooting.md` rule.

Triggering a DAG, pausing or unpausing it, or changing a task's state alters orchestration. Do so only when the user requested that operation and after checking the current run state and likely downstream effects. Do not expose credentials or put them in the repository. If the MCP server is disabled or unavailable, use the project's existing read-only Docker/Airflow diagnostics and report the missing connection clearly.
