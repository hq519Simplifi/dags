"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.sensors.sql import SqlSensor
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.email_operator import EmailOperator
import pendulum
from airflow.operators.dummy_operator import DummyOperator
#from airflow.utils.email import send_email

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
mailto = ["quhai519@gmail.com"]
local_tz = pendulum.timezone("America/Chicago")

def on_success_email(h):
    """
    email 
    """
    any_content = False
    html_logs = "<body>\n  <ul>\n"
    for task_id in ["data_check", "remoteSparkJobEnv"]:
        task_log = h["ti"].xcom_pull(task_ids=task_id)
        if task_log:
            any_content = True
            html_logs += f"    <li>{task_id}<br>\n<pre>\n{task_log}\n</pre></li>\n"
    html_logs += "  </ul>\n</body>"
    if any_content:
        send_email(mailto, "testspark Processing", html_logs)



default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": mailto,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "task_concurrency" : 1,
    #'start_date': datetime(2023, 2, 14, 13, 46, 0, tzinfo=local_tz),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(DAG_ID,
           start_date=datetime(2023, 3, 9, 9, 46, 0, tzinfo=local_tz),
           default_args=default_args,
           schedule_interval=timedelta(1), 
           catchup=True)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t0 = DummyOperator(
  task_id="Initialize",
  dag=dag)

t1 = SqlSensor(
  task_id="data_check",
  conn_id="MY_PROD_DB",
  #conn_id="pg_consul",
  poke_interval=60,
  timeout=14400,
  sql="select count(*) from test1",
  dag=dag)


t3 = SSHOperator(
  task_id="remoteSparkJobEnv",
  #command="/opt/apache/spark/bin/spark-submit --master spark://localhost:7077 --class org.apache.spark.examples.SparkPi /opt/apache/spark/examples/jars/spark-examples_2.12-3.3.1.jar",
  command="/home/hai/test/testSprkJob/runtestSprk",
  #ssh_conn_id="ssh_default",
  ssh_conn_id="ssh_ubuntusrv",
  do_xcom_push=True,
  timeout=12*3600, # 12 hour
  depends_on_past=True,
  dag=dag)

t_final = DummyOperator(
    task_id="Finalize",
    on_success_callback=on_success_email,
    dag=dag)

t0 >> t1 >> t3 >> t_final
