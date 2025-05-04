# Package Import
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

with DAG(
    dag_id='dags_trigger_dag_run_operator',
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2025, 4, 1, tz='Asia/Seoul'),
    catchup=False,
) as dag:
    
    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "start!"'
    )

    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_dag_task',
        # dags_python_operator 라는 DAG을 trigger로 사용함
        trigger_dag_id='dags_python_operator',
        trigger_run_id=None,
        execution_date='{{ data_interval_start }}',
        reset_dag_run=True,
        # 트리거된 DAG이 완료될때까지 기다리지는 않음
        wait_for_completion=False,
        # 완료되었는지 관측하는 간격
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None
    )

    start_task >> trigger_dag_task
