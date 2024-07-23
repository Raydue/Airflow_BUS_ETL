import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils import timezone
import pandas as pd


POSTGRES_CONN_ID = 'Rain_TEST'  # Postgres connection id in airflow

# Collect bus data function
def fetch_bus_data(ti):
    url = "https://tdx.transportdata.tw/api/basic/v2/Bus/RealTimeNearStop/City/Taipei/617?%24top=30&%24format=JSON"     #617公車
    headers = {
        'accept': 'application/json',
        'Authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJER2lKNFE5bFg4WldFajlNNEE2amFVNm9JOGJVQ3RYWGV6OFdZVzh3ZkhrIn0.eyJleHAiOjE3MTk4MTk3NTAsImlhdCI6MTcxOTczMzM1MCwianRpIjoiMWUwMTljNWEtMGMxYS00NzkxLTg2N2EtMDZhZTUyNjI4YWQxIiwiaXNzIjoiaHR0cHM6Ly90ZHgudHJhbnNwb3J0ZGF0YS50dy9hdXRoL3JlYWxtcy9URFhDb25uZWN0Iiwic3ViIjoiNjZkYmQyYzgtM2YyMS00ODIxLWI3MWItNGI0MzIzYzhlYTMxIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoicmF5ZHVlMzgtMDAzYTc5MjMtNzRjYS00ZWNiIiwiYWNyIjoiMSIsInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJzdGF0aXN0aWMiLCJwcmVtaXVtIiwicGFya2luZ0ZlZSIsIm1hYXMiLCJhZHZhbmNlZCIsImdlb2luZm8iLCJ2YWxpZGF0b3IiLCJ0b3VyaXNtIiwiaGlzdG9yaWNhbCIsImJhc2ljIl19LCJzY29wZSI6InByb2ZpbGUgZW1haWwiLCJ1c2VyIjoiOGE4ZGI2YzAifQ.G_eVH_rPbBWe3-PYMnZnmB-2_88ilk958fw_4juPPQMOFOmtdNsresyVodI74GpiTGjOsAz-h6cZXInsqMoAUoRBz_FCLfUCnqZA1h1lNRXZ3Q73WwE4uzStDYG4lwMQW-E3wquxXbtGEPRzVC_yH4oBv6p6PlxwibH3oKXRqT_jBuvjV0Mc1EoVOGZydSWcY4wQ2SCZo-dYyHa_RHYtF5zFkwutm6Kp4a9Cez6L_GhcwwjiUIhsoBgQfWG-YTfvDid0soWpBYW0mtjAx7mQDMb1cy9BLfpi3HbMKqtw6YLiqK8XRppsqxB2fz793LXayoQ9cuNwRryLk3tU6Gzssg'
    }
    response = requests.get(url, headers=headers)
    ti.xcom_push(key='status_code', value=response.status_code)
    if response.status_code == 200:
        data = response.json()
        ti.xcom_push(key='raw_bus_data', value=data)    #將抓來的資料放上xcom 以便資料的傳輸
    else:
        print("Failed to fetch bus data. Status code:", response.status_code)

# Branch function
def check_status_code(ti):
    status_code = ti.xcom_pull(key='status_code', task_ids='fetch_bus_data')    
    raw_bus_data = ti.xcom_pull(key='raw_bus_data', task_ids='fetch_bus_data')  #將xcom上key name為raw_bus_data 的value抓下來
    if status_code == 200:
        for bus in raw_bus_data:                    #Check each Bus.
             if any(value is None for value in [    #check if any of the value == None.
                bus.get("PlateNumb"), 
                bus["RouteName"].get("Zh_tw"), 
                bus.get("Direction"), 
                bus["StopName"].get("Zh_tw"), 
                bus.get("StopSequence"), 
                bus.get("GPSTime")
            ]):
                return 'send_email_alert'           #資料有缺失，寄 email 提醒
        return 'transform_bus_data'                 #沒有問題，進到下一階段transform_bus_data
    else:
        return 'send_email_alert'                   #secret key 過期，寄email提醒

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
        for bus in raw_bus_data:        #將每一台公車的json資料重構並摘出需要的欄位資料。
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
        ti.xcom_push(key='transformed_data', value=transformed_data_list)   #再把transformed data放入xcom 方便後續寫入資料庫。
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
        INSERT INTO bus_data_24h (PlateNumb, RouteName, Direction, StopName, StopSequence, GPSTime)
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
    'start_date': timezone.datetime(2024, 6, 26, 13, 30, 0),
    'end_date' : timezone.datetime(2024, 6, 27, 13, 30, 0),
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
    #Schedule the sequence of the workflow.
    fetch_bus_data_task >> check_status_code_task
    check_status_code_task >> transform_bus_data_task >> create_table_task >> insert_data_task >> read_db_task
    check_status_code_task >> send_email_alert_task
