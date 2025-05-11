from dateutil import parser
from datetime import timedelta
from dateutil import parser
from airflow.utils import timezone
from airflow import DAG
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator


default_args = {
    'retry_delay': timedelta(seconds=5),
    'depends_on_past': False,
}

example_dag = DAG(
    'example-cdeoperator',
    default_args=default_args,
    start_date=parser.isoparse("2022-05-11T20:20:04.268Z").replace(tzinfo=timezone.utc),
    # end_date=datetime.combine(datetime.today() + timedelta(1), datetime.min.time()),
    schedule_interval='@once',
    catchup=False,
    is_paused_upon_creation=False
)

ingest_step1 = CDEJobRunOperator(
    connection_id='cde_runtime_api',
    task_id='ingest',
    retries=3,
    dag=example_dag,
    job_name='example-scala-pi'
)

prep_step2 = CDEJobRunOperator(
    task_id='data_prep',
    dag=example_dag,
    job_name='example-scala-pi'
)

ingest_step1 >> prep_step2
