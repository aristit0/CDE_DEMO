from datetime import timedelta
from airflow import DAG
from airflow.utils import timezone
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

default_args = {
    'retry_delay': timedelta(seconds=10),
    'depends_on_past': False,
}

dag = DAG(
    dag_id='mobile_tx_datamart_etl',
    default_args=default_args,
    start_date=timezone.utcnow(),
    schedule_interval='@once',
    catchup=False,
    is_paused_upon_creation=False
)

load_to_stg = CDEJobRunOperator(
    task_id='gpu_oad_mysql_to_stg',
    connection_id='cde_runtime_api',
    job_name='gpu_load_mysql_to_stg',  # This should match your CDE job name
    dag=dag
)

load_to_dm = CDEJobRunOperator(
    task_id='gpu_stg_to_datamart',
    connection_id='cde_runtime_api',
    job_name='gpu_stg_to_datamart',  # This should match your second job name
    dag=dag
)

load_to_stg >> load_to_dm