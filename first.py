from airflow import DAG, models
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator

from datetime import datetime, timedelta

five_days_ago = datetime.combine(
    datetime.today() - timedelta(5), datetime.min.time()
    )

now = datetime.now()
now_to_the_hour = (
    now - timedelta(0, 0, 0, 0, 0, 3)
).replace(minute=0, second=0, microsecond=0)

START_DATE = five_days_ago
# START_DATE = now_to_the_hour
DAGNAME= 'first'


default_args = {

    'owner': 'R.Duran',
    'depends_on_past': False,
    #'start_date': datetime(2018, 10, 28),
    'start_date': START_DATE,
    'email': ['airflow@insight.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'schedule_interval': '@daily',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }

with DAG(dag_id=DAGNAME, default_args=default_args, schedule_interval=timedelta(1)) as dag:
    run_this_last = DummyOperator(
	    task_id='run_this_last'
	)
    
    run_this_first = BashOperator(
	    task_id='run_this_first',
	    bash_command='echo 1'
	)
    
    run_this_first >> run_this_last