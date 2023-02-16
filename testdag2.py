"""
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
from airflow.sensors.sql import SqlSensor
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.email_operator import EmailOperator
import pendulum



local_tz = pendulum.timezone("America/Chicago")
file_csv = "/usr/local/airflow/dags/data_tst.csv"

def load_pg():
    conn = PostgresHook(postgres_conn_id='pg_consul').get_conn()
    cur = conn.cursor()
    SQL_STATEMENT = """
        COPY test1 FROM STDIN WITH (Delimiter ';')
        """

    with open(file_csv, 'r') as f:

        cur.copy_expert(SQL_STATEMENT, f)
        conn.commit()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    'start_date': datetime(2023, 2, 15, 13, 46, 0, tzinfo=local_tz),
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

dag = DAG("testpgload", default_args=default_args, schedule_interval=timedelta(1))

t1 = DummyOperator(
  task_id="Initialize",
  dag=dag)

t2 = PythonOperator(
  task_id="pg_load",
  python_callable=load_pg,
  provide_context=True,
  dag=dag)

t2.set_upstream(t1)
