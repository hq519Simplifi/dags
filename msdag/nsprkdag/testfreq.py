"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.sensors.sql import SqlSensor
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.email_operator import EmailOperator
import pendulum
import os

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
SCHEDULE_INTERVAL='*/1 * * * *',

local_tz = pendulum.timezone("America/Chicago")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["quhai519@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(DAG_ID, 
          start_date=datetime(2023, 3, 8, 19, 15, 0, tzinfo=local_tz),
          default_args=default_args, schedule_interval=SCHEDULE_INTERVAL, catchup=False)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = SqlSensor(
  task_id="data_check",
  conn_id="MY_PROD_DB",
  #conn_id="pg_consul",
  poke_interval=60,
  timeout=14400,
  sql="select count(*) from test1",
  dag=dag)


#t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)

t2 = BashOperator(task_id="localtest", bash_command="date >> /usr/local/airflow/dags/ttt", retries=3, dag=dag)


t2.set_upstream(t1)
