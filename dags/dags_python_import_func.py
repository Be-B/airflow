from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
# 기본적으로 최상위 디렉터리를 path로 잡아서 from plugins.common.common_func import get_sftp 이렇게 써야하는데
# WSL 환경에서는 아래처럼 써줘야 한다. 왜냐하면 airflow에서는 plugins까지 sys.path에 설정되어 있기 때문이다.
from common.common_func import get_sftp

with DAG(
    dag_id="dags_python_import_func",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    task_get_sftp = PythonOperator(
        task_id="task_get_sftp",
        python_callable=get_sftp,
    )
