import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
import pandas as pd


POSTGRES_CONN_ID = 'Rain_TEST'  # Postgres connection id in airflow

# Collect bus data function
def fetch_bus_data(ti):
    url = "https://tdx.transportdata.tw/api/basic/v2/Bus/RealTimeNearStop/City/Taipei/617?%24top=30&%24format=JSON"
    headers = {
        'accept': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJER2lKNFE5bFg4WldFajlNNEE2amFVNm9JOGJVQ3RYWGV6OFdZVzh3ZkhrIn0.eyJleHAiOjE3MTk0ODkxNzcsImlhdCI6MTcxOTQwMjc3NywianRpIjoiMDQ3YjVhZWItNGVhNC00ZDNjLTk2NDktNzg0ODFjYjZhYjc3IiwiaXNzIjoiaHR0cHM6Ly90ZHgudHJhbnNwb3J0ZGF0YS50dy9hdXRoL3JlYWxtcy9URFhDb25uZWN0Iiwic3ViIjoiNjZkYmQyYzgtM2YyMS00ODIxLWI3MWItNGI0MzIzYzhlYTMxIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoicmF5ZHVlMzgtMDAzYTc5MjMtNzRjYS00ZWNiIiwiYWNyIjoiMSIsInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJzdGF0aXN0aWMiLCJwcmVtaXVtIiwicGFya2luZ0ZlZSIsIm1hYXMiLCJhZHZhbmNlZCIsImdlb2luZm8iLCJ2YWxpZGF0b3IiLCJ0b3VyaXNtIiwiaGlzdG9yaWNhbCIsImJhc2ljIl19LCJzY29wZSI6InByb2ZpbGUgZW1haWwiLCJ1c2VyIjoiOGE4ZGI2YzAifQ.NzOXL2ulukiPa1HvoFTQa9rLCV6Qlo-nf8DtEYHRULvGqCHGkqOb73lpm3vRJZ-krJW9srv7gb_vi0taa-HFNVJR-IBu2v5ZCYX4sNPcOpF9J2CWIqw7FHHZt6so3DJ0soMjlJ_eix02axAfMnLnumlXUZpZPDuOM3StPl2eHI-1iskfGSGLBglbpM0i0wWzxKf5qKmIXR9lVMkLv3UzXsg21L4HLBmvEYhLmpXhMAltKPb0qgPB90ClZRezfHSc7wDl7Ewstzd2QpwgamjqW6wZC47uXe9ABpMnOszfchB07FnIcb9QAGX-kFgtj2K_iD60QjErJvJKR0_DymNyBw'
    }
    response = requests.get(url, headers=headers)
    ti.xcom_push(key='status_code', value=response.status_code)
    if response.status_code == 200:
        data = response.json()
        ti.xcom_push(key='raw_bus_data', value=data)
    else:
        print("Failed to fetch bus data. Status code:", response.status_code)

# Branch function
def check_status_code(ti):
    status_code = ti.xcom_pull(key='status_code', task_ids='fetch_bus_data')
    raw_bus_data = ti.xcom_pull(key='raw_bus_data', task_ids='fetch_bus_data')
    if status_code == 200:
        for bus in raw_bus_data:
            if not all([bus.get("PlateNumb"), bus["RouteName"].get("Zh_tw"), bus.get("Direction"),      #Check if any of the value is empty.
                    bus["StopName"].get("Zh_tw"), bus.get("StopSequence"), bus.get("GPSTime")]):
                return 'send_email_alert'
        return 'transform_bus_data'
    else:
        return 'send_email_alert'

# Send email alert function
def send_email_alert(**kwargs):
    subject = f"{kwargs['dag'].dag_id} --- Bus Data Fetching Failed."
    body = "The bus data fetching failed due to unauthorized access. Please update the API token or check data missing."
    email = EmailOperator(
        task_id='send_email_alert',
        to='raydue38@gmail.com',
        subject=subject,
        html_content=body
    )
    email.execute(context={})

# Transform bus data function
def transform_bus_data(ti):
    raw_bus_data = ti.xcom_pull(key='raw_bus_data', task_ids='fetch_bus_data')
    if raw_bus_data:
        extracted_data = []
        for bus in raw_bus_data:
            relevant_info = {
                "PlateNumb": bus.get("PlateNumb"),
                "RouteName": bus["RouteName"].get("Zh_tw"),
                "Direction": bus.get("Direction"),
                "StopName": bus["StopName"].get("Zh_tw"),
                "StopSequence": bus.get("StopSequence"),
                "GPSTime": bus.get("GPSTime")
            }
            extracted_data.append(relevant_info)
        
        df = pd.DataFrame(extracted_data)
        df['GPSTime'] = pd.to_datetime(df['GPSTime'])
        df['GPSTime'] = df['GPSTime'].astype(str)  # Convert timestamps to strings
        transformed_data_list = df.to_dict(orient='records')
        ti.xcom_push(key='transformed_data', value=transformed_data_list)
        print("Transformed Data:", transformed_data_list)
    else:
        print("Error: No raw bus data available for transformation")

# Insert data to db function
def insert_data_to_db(ti):
    transformed_data = ti.xcom_pull(key='transformed_data', task_ids='transform_bus_data')
    if transformed_data:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO bus_data (PlateNumb, RouteName, Direction, StopName, StopSequence, GPSTime)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (PlateNumb, GPSTime) DO NOTHING;
        """
        for record in transformed_data:
            cursor.execute(insert_query, (
                record["PlateNumb"],
                record["RouteName"],
                record["Direction"],
                record["StopName"],
                record["StopSequence"],
                record["GPSTime"]
            ))
        conn.commit()
        cursor.close()
        print("Data inserted successfully")
    else:
        print("Error: No transformed data available for insertion")

# Read db function
def read_db():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    query = '''SELECT * FROM bus_data;'''    
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    print(rows)

default_args = {
    'owner': 'Ray',
    'start_date': datetime(2024, 6, 26, 20, 0, 0),
    'end_date' : datetime(2024, 6, 27, 20, 0, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='bus_data_etl_dag',
    default_args=default_args,
    description='A customized ETL DAG to fetch, transform, and load bus data to PostgreSQL',
    schedule_interval='*/10 * * * *',  # Run every 10 minutes
    catchup=False,  # Disable catchup in the DAG definition
) as dag:

    fetch_bus_data_task = PythonOperator(
        task_id='fetch_bus_data',
        python_callable=fetch_bus_data,
    )

    check_status_code_task = BranchPythonOperator(
        task_id='check_status_code',
        python_callable=check_status_code,
        provide_context=True
    )

    transform_bus_data_task = PythonOperator(
        task_id='transform_bus_data',
        python_callable=transform_bus_data,
    )

    send_email_alert_task = PythonOperator(
        task_id='send_email_alert',
        python_callable=send_email_alert,
    )

    create_table_task = PostgresOperator(
        task_id='create_table',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS bus_data_24h (
            PlateNumb VARCHAR(20),
            RouteName VARCHAR(255),
            Direction INTEGER,
            StopName VARCHAR(255),
            StopSequence INTEGER,
            GPSTime TIMESTAMP,
            PRIMARY KEY (PlateNumb, GPSTime)
        );
        """,
    )

    insert_data_task = PythonOperator(
        task_id='insert_data_to_db',
        python_callable=insert_data_to_db,
    )

    read_db_task = PythonOperator(
        task_id='read_db',
        python_callable=read_db,
    )

    fetch_bus_data_task >> check_status_code_task
    check_status_code_task >> transform_bus_data_task >> create_table_task >> insert_data_task >> read_db_task
    check_status_code_task >> send_email_alert_task
