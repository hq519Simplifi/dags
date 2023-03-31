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
from airflow.contrib.operators.sftp_operator import SFTPOperator
import pendulum
import os

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

local_tz = pendulum.timezone("America/Chicago")
file_csv = "/usr/local/airflow/data_tst.csv"

def load_pg():
    #conn = PostgresHook(postgres_conn_id='pg_consul').get_conn()
    conn = PostgresHook(postgres_conn_id='MY_PROD_DB').get_conn()
    cur = conn.cursor()
    SQL_STATEMENT = """
        COPY test1 FROM STDIN WITH (Delimiter ';', FORMAT csv, NULL '')
        """

    with open(file_csv, 'r') as f:

        cur.copy_expert(SQL_STATEMENT, f)
        conn.commit()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["quhai519@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(DAG_ID,
           start_date=datetime(2023, 3, 8, 19, 10, 0, tzinfo=local_tz),
           default_args=default_args, schedule_interval=timedelta(1), catchup=False)

t0 = DummyOperator(
  task_id="Initialize",
  dag=dag)

t1 = SFTPOperator(
    task_id="load_file",
    ssh_conn_id="ssh_ubuntusrv",
    local_filepath=file_csv,
    remote_filepath="/home/hai/tst.txt",
    operation="get",
    create_intermediate_dirs=True,
    dag=dag
)



t2 = PythonOperator(
  task_id="pg_load",
  python_callable=load_pg,
  provide_context=True,
  dag=dag)

t1.set_upstream(t0)
t2.set_upstream(t1)
