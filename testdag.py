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


local_tz = pendulum.timezone("America/Chicago")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    'start_date': datetime(2023, 2, 13, 13, 46, 0, tzinfo=local_tz),
    "email": ["quhai519@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "catchup": True,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("testAll", default_args=default_args, schedule_interval=timedelta(1))

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = SqlSensor(
  task_id="data_check",
  #conn_id="MY_PROD_DB",
  conn_id="pg_consul",
  poke_interval=60,
  timeout=14400,
  sql="select count(*) from test1",
  dag=dag)


#t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)

t2 = BashOperator(task_id="localtest", bash_command="date >> /usr/local/airflow/dags/ttt", retries=3, dag=dag)

t3 = SSHOperator(
  task_id="remotessh",
  command="date >> ~/ttt",
  #ssh_conn_id="ssh_default",
  ssh_conn_id="ssh_ubuntusrv",
  do_xcom_push=True,
  timeout=12*3600, # 12 hour
  depends_on_past=True,
  dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t1)
