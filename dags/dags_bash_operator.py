from airflow.models.dag import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    # dag_id와 파일명을 같게 설정하는 것이 좋음
    dag_id="dags_bash_operator",
    # 매일 마다 몇시 몇분으로 돌리는지 설정
    schedule="0 0 * * *",
    # 처음 실행 날짜
    # UTC는 세계 표준시로 한국은 UTC+9시간 차이가 남
    # 따라서 Asia/Seoul로 설정해야 한국시간으로 설정된다.
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    # 처음 실행 날짜부터 이전 날짜까지 실행되는지 여부를 설정함.
    # 예를 들어 처음 실행 날짜가 2021년 1월 1일이고 오늘이 2021년 1월 10일이면 1월 1일부터 1월 10일까지 실행된다.
    catchup=False,
    # 실행 시간 제한
    # dagrun_timeout=datetime.timedelta(minutes=60),

    # 태그 설정
    # tags=["example", "example2"],

    # DAG에 전달할 파라미터 설정
    # params={"example_key": "example_value"},
) as dag:
    
    # [START howto_operator_bash]
    bash_t1 = BashOperator(
        # 태스크 아이디는 그래프에서 태스크를 구분하는 식별자
        task_id="bash_t1",
        # 우리가 어떤 shell script를 실행할지 설정
        bash_command="echo whoami",
    )

    bash_t2 = BashOperator(
        # 태스크 아이디는 그래프에서 태스크를 구분하는 식별자
        task_id="bash_t2",
        # 우리가 어떤 shell script를 실행할지 설정
        # 현재 호스트 이름을 출력
        bash_command="echo $HOSTNAME",
    )

    # bash operator의 실행 순서를 파이프라인처럼 실행
    bash_t1 >> bash_t2