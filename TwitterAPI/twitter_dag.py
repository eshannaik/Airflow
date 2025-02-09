from airflow import DAG # type: ignore
from datetime import datetime,timedelta
import sys
sys.path.append('/opt/airflow/dags/TwitterAPI')

from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore

from getData import getData,insertData

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025,2,7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'TwitterAPI',
    default_args = default_args,
    description = 'Getting data from Twitter API',
    #schedule_interval = timedelta(days=1), #can schedule if needed
)

# called directly from getData_Task
# connection_Task = PythonOperator(
#     task_id = 'Connect_to_the_account',
#     python_callable = connection,
#     dag=dag,
# )

getData_Task = PythonOperator(
    task_id = 'GetData_for_users',
    python_callable = getData,
    dag=dag,
)

insertData_Task = PythonOperator(
    task_id = 'Insert_user_info_into_the_table',
    python_callable = insertData,
    dag=dag,
)
 
#DAG
getData_Task >> insertData_Task