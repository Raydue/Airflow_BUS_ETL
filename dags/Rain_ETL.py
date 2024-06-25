import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd

# Constants
POSTGRES_CONN_ID = 'Rain_TEST'  # Replace with your Postgres connection ID in Airflow

def fetch_rain_data(ti):
    url = "https://wic.heo.taipei/OpenData/API/Rain/Get?stationNo=001&loginId=open_rain&dataKey=85452C1D"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        keys = ['stationNo', 'stationName', 'recTime', 'rain']        
        result_list = []
       
        for station_data in data['data']:
            for key in keys:
                result_list.append(station_data[key])
        
        if len(result_list) == 4:
            ti.xcom_push(key='stationNo', value=result_list[0])
            ti.xcom_push(key='stationName', value=result_list[1])
            ti.xcom_push(key='recTime', value=result_list[2])
            ti.xcom_push(key='rain', value=result_list[3]) 
            print(f"Pushed data: stationNo={result_list[0]}, stationName={result_list[1]}, recTime={result_list[2]}, rain={result_list[3]}")
        else:
            print("Format Error!")
        print(result_list) 
    else:
        print("Failed to fetch data. Status code:", response.status_code)

def transform_data(ti):
    stationNo = ti.xcom_pull(key='stationNo', task_ids='fetch_rain_data')
    stationName = ti.xcom_pull(key='stationName', task_ids='fetch_rain_data')
    recTime = ti.xcom_pull(key='recTime', task_ids='fetch_rain_data')
    rain = ti.xcom_pull(key='rain', task_ids='fetch_rain_data')
    
    print(f"Pulled data: stationNo={stationNo}, stationName={stationName}, recTime={recTime}, rain={rain}")
    
    if stationNo and stationName and recTime and rain is not None:
        data = {
            'stationNo': stationNo,
            'stationName': stationName,
            'recTime': recTime,
            'rain': rain
        }
        df = pd.DataFrame([data])
        df['recTime'] = pd.to_datetime(df['recTime'], format='%Y%m%d%H%M')
        df['recTime'] = df['recTime'].astype(str)  # Convert timestamps to strings
        transformed_data_list = df.to_dict(orient='records')
        ti.xcom_push(key='transformed_data', value=transformed_data_list)
        print("Transformed Data:", transformed_data_list)
    else:
        print("Error: Missing data for transformation")
        if not stationNo:
            print("Missing stationNo")
        if not stationName:
            print("Missing stationName")
        if not recTime:
            print("Missing recTime")
        if rain is None:
            print("Missing rain")

def read_db():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    query = '''SELECT * FROM rain_data;'''    
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    print(rows)

default_args = {
    'owner': 'Ray',
    'start_date': datetime(2024, 6, 23),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='rain_data_etl_dag',
    default_args=default_args,
    description='A customized ETL DAG to fetch, transform, and load rain data to PostgreSQL',
    schedule_interval='*/10 * * * *',  # Run every 10 minutes
    catchup=False,  # Disable catchup in the DAG definition
) as dag:

    fetch_rain_data_task = PythonOperator(
        task_id='fetch_rain_data',
        python_callable=fetch_rain_data,
    )

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    create_table_task = PostgresOperator(
        task_id='create_table',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS rain_data (
            recTime TIMESTAMP,
            stationNo VARCHAR(10),
            stationName VARCHAR(255),
            rain FLOAT,
            PRIMARY KEY (recTime, stationNo)
        );
        """,
    )

    insert_data_task = PostgresOperator(
        task_id='insert_data',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
        INSERT INTO rain_data (recTime, stationNo, stationName, rain)
        VALUES (
            '{{ ti.xcom_pull(task_ids="transform_data", key="transformed_data")[0]["recTime"] }}',
            '{{ ti.xcom_pull(task_ids="transform_data", key="transformed_data")[0]["stationNo"] }}',
            '{{ ti.xcom_pull(task_ids="transform_data", key="transformed_data")[0]["stationName"] }}',
            {{ ti.xcom_pull(task_ids="transform_data", key="transformed_data")[0]["rain"] }}
        )
        ON CONFLICT (recTime, stationNo) DO NOTHING;
        """,
    )

    read_db_task = PythonOperator(
        task_id='read_db',
        python_callable=read_db,
    )

    fetch_rain_data_task >> transform_data_task >> create_table_task >> insert_data_task >> read_db_task
