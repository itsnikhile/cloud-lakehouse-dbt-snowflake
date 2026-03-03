"""
Airflow DAG — Lakehouse Pipeline Orchestration
Runs incremental dbt every 15 mins, full refresh daily.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["alerts@company.com"],
}

# ── Incremental DAG (every 15 minutes) ───────────────────────────────────────
with DAG(
    dag_id="lakehouse_incremental",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/15 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["lakehouse", "incremental"],
    description="Incremental dbt run every 15 minutes",
) as incremental_dag:

    check_freshness = BashOperator(
        task_id="check_source_freshness",
        bash_command="cd /opt/dbt && dbt source freshness --target prod",
    )

    run_staging = BashOperator(
        task_id="run_staging_models",
        bash_command="cd /opt/dbt && dbt run --select staging --target prod",
    )

    test_staging = BashOperator(
        task_id="test_staging_models",
        bash_command="cd /opt/dbt && dbt test --select staging --target prod",
    )

    check_freshness >> run_staging >> test_staging


# ── Full Refresh DAG (daily at 02:00 UTC) ────────────────────────────────────
with DAG(
    dag_id="lakehouse_full_refresh",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["lakehouse", "full-refresh"],
    description="Full dbt refresh including marts and docs",
) as full_dag:

    run_all = BashOperator(
        task_id="run_all_models",
        bash_command="cd /opt/dbt && dbt run --target prod",
    )

    test_all = BashOperator(
        task_id="test_all_models",
        bash_command="cd /opt/dbt && dbt test --target prod",
    )

    generate_docs = BashOperator(
        task_id="generate_docs",
        bash_command="cd /opt/dbt && dbt docs generate --target prod",
    )

    run_all >> test_all >> generate_docs
