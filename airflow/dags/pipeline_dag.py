from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args ={"retries":2, "retry_delay":timedelta(minutes=2)}

def create_run_context(**kwargs):
    from pipeline.run_context import new_run_context
    ctx = new_run_context()
    kwargs['ti'].xcom_push(key='run_id', value=ctx.run_id)
    kwargs['ti'].xcom_push(key='load_date', value=ctx.load_date)
    print(f"[pipeline] run_id={ctx.run_id} load_date={ctx.load_date}")

def ingest_crm(**kwargs):
    from pipeline.ingest_crm_revenue import run
    run_id = kwargs['ti'].xcom_pull(task_ids='create_run_context', key='run_id')
    load_date = kwargs['ti'].xcom_pull(task_ids='create_run_context', key='load_date')
    run(run_id=run_id, load_date=load_date)

def ingest_api(**kwargs):
    from pipeline.ingest_api_google_ads import run
    run_id = kwargs['ti'].xcom_pull(task_ids='create_run_context', key='run_id')
    load_date = kwargs['ti'].xcom_pull(task_ids='create_run_context', key='load_date')
    run(run_id=run_id, load_date=load_date)

def ingest_export(**kwargs):
    from pipeline.ingest_export_facebook import run
    run_id = kwargs['ti'].xcom_pull(task_ids='create_run_context', key='run_id')
    load_date = kwargs['ti'].xcom_pull(task_ids='create_run_context', key='load_date')
    run(run_id=run_id, load_date=load_date)

with DAG(
    dag_id='bluealpha_data_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
) as dag:
    t0 = PythonOperator(task_id='create_run_context', python_callable=create_run_context)
    t1 = PythonOperator(task_id='ingest_crm', python_callable=ingest_crm)
    t2 = PythonOperator(task_id='ingest_api', python_callable=ingest_api)
    t3 = PythonOperator(task_id='ingest_export', python_callable=ingest_export)

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/bluealpha/dbt_project && dbt run',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/bluealpha/dbt_project && dbt test'
    )

    t0 >> [t1, t2, t3] >> dbt_run >> dbt_test