from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_eg2",
    # 매월 2번째 주 토요일 오전 0시 10분
    schedule="10 0 * * 6#2",
    start_date=pendulum.datetime(2025, 3, 1, tz="Asia/Seoul"),
    catchup=True,
) as dag:

    # START_DATE: 2주전 월요일, END_DATE: 2주전 토요일
    bash_task_2 = BashOperator(
        task_id="bash_task_2",
        # 한국 시간대로 맞추기 위해서 .in_timezone("Asia/Seoul") 사용
        env={'START_DATE' : '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=19)) | ds }}',
             # 연산자가 -로 되어 있기 떄문에 days=1로 해야함. 또 pipe ds를 적용하려면 전체를 괄호로 감싸주어야 함
             'END_DATE' : '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=14)) | ds }}'
        },
        bash_command="echo 'START_DATE: $START_DATE' && echo 'END_DATE: $END_DATE'"
    )