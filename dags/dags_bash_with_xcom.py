from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='dags_bash_with_xcom',
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2025, 4, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    bash_push = BashOperator(
        task_id='bash_push',
        bash_command='echo START && '
                     'echo XCOM_PUSHED '
                     # bash 오퍼레이터에서는 템플릿 문법으로 작성해야함
                     # 파이썬에서는 args, kwargs로 작성함
                     '{{ ti.xcom_push(key="bash_pushed", value="first_bash_message") }} && '
                     'echo COMPLETE'
    )

    bash_pull = BashOperator(
        task_id='bash_pull',
        env= {'PUSHED_VALUE':"{{ ti.xcom_pull(key='bash_pushed') }}",
              'RETURN_VALUE':"{{ ti.xcom_pull(task_ids='bash_push') }}"},
        bash_command='echo $PUSHED_VALUE && echo $RETURN_VALUE ',

        # 마지막 bash_command가 xcom에 올라가지 않도록 코드를 줌
        do_xcom_push=False
    )

    bash_push >> bash_pull