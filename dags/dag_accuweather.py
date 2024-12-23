import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# AccuWeather API Configuration
API_KEY = "5fkKqjA9uHqyCydycrC1nYGxL5t6JYuu"  # Replace with your actual API key
CURRENT_CONDITIONS_URL = "http://dataservice.accuweather.com/currentconditions/v1/"
FORECAST_5DAY_URL = "http://dataservice.accuweather.com/forecasts/v1/daily/5day/"
FORECAST_12HOUR_URL = "http://dataservice.accuweather.com/forecasts/v1/hourly/12hour/"
LOCATION_KEY = "328169"  # Example LocationKey for Orlando, Florida

# Default arguments for the DAG
default_args = {
    "owner": "Airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    "weather_data_pipeline",
    default_args=default_args,
    description="DAG to fetch weather data",
    schedule="@hourly",  # Use the new schedule parameter
    start_date=datetime(2023, 12, 1),
    catchup=False,
) as dag:

    # Task 1: Fetch current weather data
    def fetch_current_weather():
        response = requests.get(f"{CURRENT_CONDITIONS_URL}{LOCATION_KEY}", params={"apikey": API_KEY})
        if response.status_code == 200:
            data = response.json()
            print("Current Weather Data:", data)
        else:
            print(f"Failed to fetch current weather data: {response.status_code}, {response.text}")

    fetch_current_weather_task = PythonOperator(
        task_id="fetch_current_weather", python_callable=fetch_current_weather
    )

    # Task 2: Fetch 5-day forecast
    def fetch_5day_forecast():
        response = requests.get(f"{FORECAST_5DAY_URL}{LOCATION_KEY}", params={"apikey": API_KEY})
        if response.status_code == 200:
            data = response.json()
            print("5-Day Forecast Data:", data)
        else:
            print(f"Failed to fetch 5-day forecast data: {response.status_code}, {response.text}")

    fetch_5day_forecast_task = PythonOperator(
        task_id="fetch_5day_forecast", python_callable=fetch_5day_forecast
    )

    # Task 3: Fetch 12-hour forecast
    def fetch_12hour_forecast():
        response = requests.get(f"{FORECAST_12HOUR_URL}{LOCATION_KEY}", params={"apikey": API_KEY})
        if response.status_code == 200:
            data = response.json()
            print("12-Hour Forecast Data:", data)
        else:
            print(f"Failed to fetch 12-hour forecast data: {response.status_code}, {response.text}")

    fetch_12hour_forecast_task = PythonOperator(
        task_id="fetch_12hour_forecast", python_callable=fetch_12hour_forecast
    )

    # Task dependencies
    fetch_current_weather_task >> fetch_5day_forecast_task >> fetch_12hour_forecast_task
