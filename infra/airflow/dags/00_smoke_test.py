from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="00_smoke_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bootstrap", "local"],
):
    check_airflow = BashOperator(
        task_id="check_airflow",
        bash_command="echo 'Airflow OK in Docker + WSL'",
    )

    check_project_mount = BashOperator(
        task_id="check_project_mount",
        bash_command="test -d /opt/project && ls -1 /opt/project | head -n 20",
    )

    check_airflow >> check_project_mount
