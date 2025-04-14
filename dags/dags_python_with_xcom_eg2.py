from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task

with DAG(
    dag_id='dags_python_with_xcom_eg2',
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 4, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    @task(task_id='python_xcom_push_by_return')
    def xcom_push_result(**kwargs):
        return 'Success'
    
    @task(task_id='python_xcom_pull_1')
    def xcom_pull_1(**kwargs):
        ti = kwargs['ti']
        # xcom_pull에서 default로 task가 return 한 값을 가져옴
        value1 = ti.xcom_pull(task_ids='python_xcom_push_by_return')
        print('xcom_pull 메서드로 직접 찾은 리턴 값:' + value1)

    @task(task_id='python_xcom_pull_2')
    def xcom_pull_2(status, **kwargs):
        print('함수 입력값으로 받은 값:' + status)

    # python_xcom_pull_by_return은 airflow의 task객체이다.
    python_xcom_push_by_return = xcom_push_result()
    # 이 과정에서 입력값 status에 Success가 들어감
    xcom_pull_2(python_xcom_push_by_return)
    # 이 과정에서 리턴 값에 Success가 다시 들어감
    python_xcom_push_by_return >> xcom_pull_1()