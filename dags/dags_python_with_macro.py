from airflow import DAG
import pendulum
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_macro",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2025, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    @task(task_id='task_using_macros',
          templates_dict={'start_date': '{{ (data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months=-1, day=1))| ds }}',
                          'end_date': '{{ (data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1))| ds }}'
          }
    )
    def get_datetime_macro(**kwargs):
        templates_dict = kwargs.get('templates_dict') or {}
        if templates_dict:
            start_date = templates_dict.get('start_date') or 'start_date없음'
            end_date = templates_dict.get('end_date') or 'end_date없음'
            print(start_date)
            print(end_date)

    @task(task_id='task_direct_calc')
    def get_datetime_calc(**kwargs):
        # 파일에 맨 위에 작성하지 않고 task decorator 안에 작성한 이유
        # 스케쥴러 부하를 경감하기 위함임
        # 스케쥴러는 주기적으로 문법적으로 오류가 있는지 없는지 파싱하게 됨.
        # 따라서 파일 맨 위에 작성하면 임포트를 계속하게 되어 부하가 발생함.
        from dateutil.relativedelta import relativedelta

        date_interval_end = kwargs['data_interval_start']
        prev_month_day_first = date_interval_end.in_timezone('Asia/Seoul') + relativedelta(months=-1, day=1)
        prev_month_day_last = date_interval_end.in_timezone('Asia/Seoul').replace(day=1) + relativedelta(days=-1)
        print(prev_month_day_first.strftime('%Y-%m-%d'))
        print(prev_month_day_last.strftime('%Y-%m-%d'))

    # task decorator의 경우에는 별도로 operator를 생성하지 않고 함수로 해도 됨
    get_datetime_macro() >> get_datetime_calc()