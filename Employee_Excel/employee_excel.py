from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
import pandas as pd
import os

#check for new file
def test_file_access(file_path):
    if os.path.exists(file_path):
        print(f"File {file_path} exists.")
    else:
        print(f"File {file_path} not found.")
        raise AirflowSkipException(f"Skipping task: File {file_path} not found.")

# extract data from new file
def excel(path,ti):
    try:
        # cp the file first into worker container using the command below
        # docker cp <local_path> <container_id>:/opt/airflow/<file>
        r = pd.read_excel(path, engine='openpyxl')

        df = pd.DataFrame(r)

        df['first_name'].fillna("KKKKKKKKKK", inplace=True)
        df['last_name'].fillna("KKKKKKKKK", inplace=True)
        df['number'].fillna(0, inplace=True)
        df['Employee'].fillna("No", inplace=True)

        print(df.head())
        ti.xcom_push(key='employee_fetch',value = df.to_dict('records'))

    except Exception as e:
        print(f"Error reading the CSV file: {e}")

# insert query
def insert_employee(ti):
    employ = ti.xcom_pull(key='employee_fetch', task_ids='fetch_employee_data')

    postgres_hook = PostgresHook(postgres_conn_id='amazon_books_connection') # connection name

    insert_query = """
        INSERT INTO employee (id,first_name,last_name,number,Employee)
        VALUES(%s, %s, %s, %s, %s)
    """
    if employ is not None:
        for e in employ:
            postgres_hook.run(insert_query, parameters=(e['id'], e['first_name'], e['last_name'], e['number'], e['Employee']))
    else:
        print("Error: 'employ' is None")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025,2,4),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Employee_Data',
    default_args = default_args,
    description = 'Employee data from excel sheet',
    schedule_interval = timedelta(days=1),
)

test_task = PythonOperator(
    task_id='test_file_access',
    python_callable=test_file_access,
    op_args=['/opt/airflow/Employee_data.xlsx'],
    provide_context=True,
    dag=dag
)

excel_task = PythonOperator(
    task_id = 'fetch_employee_data',
    python_callable = excel,
    op_args=['/opt/airflow/Employee_data.xlsx'],
    dag = dag
)

#create table
create_employee = PostgresOperator(
    task_id='create_employee_table',
    postgres_conn_id='amazon_books_connection',
    sql="""
        DROP TABLE employee;
        CREATE TABLE employee (
            id INT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            "number" INT,
            Employee TEXT
        );
    """,
    dag=dag,
)

#insert_data
insert_book_data_task = PythonOperator(
    task_id='insert_employee',
    python_callable=insert_employee,
    dag=dag,
)

#update_data
update_employee_task = PostgresOperator(
    task_id='update_employee_table',
    postgres_conn_id='amazon_books_connection',
    sql="""
        UPDATE employee
        SET "number" = 8967,last_name='mma'
        WHERE id = 4;

        UPDATE employee
        SET Employee = 'No'
        WHERE Employee = 'not known';
    """,
    dag=dag,
)

# dag
test_task >> excel_task >> create_employee >> insert_book_data_task >> update_employee_task