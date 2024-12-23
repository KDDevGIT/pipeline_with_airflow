from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json

default_args = {
    "owner": "Kyler Dana",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "accuweather_mesa_etl",
    default_args=default_args,
    description="ETL pipeline for Mesa, AZ weather data",
    schedule_interval="@hourly",  # Runs every hour
    start_date=datetime(2023, 12, 1),
    catchup=False,
) as dag:

    # Task 1: Fetch current weather data
    fetch_weather = SimpleHttpOperator(
        task_id="fetch_weather",
        http_conn_id="Accu_Weather",  # Connection ID from Airflow UI
        endpoint="currentconditions/v1/331799",  # Mesa, AZ Location Key
        method="GET",
        headers={"Content-Type": "application/json"},
        response_filter=lambda response: json.loads(response.text),
        log_response=True,
    )

    # Task 2: Process the fetched weather data
    def process_weather_data(**context):
        weather_data = context['ti'].xcom_pull(task_ids='fetch_weather')[0]
        print("Current Weather Data:", weather_data)
        # Extract specific details for further processing
        transformed_data = {
            "city": "Mesa",
            "temperature": weather_data["Temperature"]["Imperial"]["Value"],
            "unit": weather_data["Temperature"]["Imperial"]["Unit"],
            "weather": weather_data["WeatherText"],
            "date_time": weather_data["LocalObservationDateTime"],
        }
        print("Transformed Data:", transformed_data)
        # Push transformed data to XCom for further processing
        context['ti'].xcom_push(key="transformed_data", value=transformed_data)

    process_weather = PythonOperator(
        task_id="process_weather",
        python_callable=process_weather_data,
        provide_context=True,
    )

    fetch_weather >> process_weather
