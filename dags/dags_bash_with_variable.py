from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.models import Variable

with DAG(
    dag_id='dags_bash_with_variable',
    schedule="10 9 * * *",
    start_date=pendulum.datetime(2025, 4, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    var_value = Variable.get("sample_key")

    # 1안, Variable 이용
    bash_var_1 = BashOperator(
        task_id='bash_var_1',
        bash_command=f'echo varaible"{var_value}"'
    )

    # 2안, 템플릿 변수를 이용함
    bash_var_2 = BashOperator(
        task_id='bash_var_2',
        bash_command='echo variable"{{ var.value.sample_key }}"'
    )