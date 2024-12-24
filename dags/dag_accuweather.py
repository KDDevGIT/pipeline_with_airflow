from current_weather import save_current_weather_to_db
from forecast_5day import save_5day_forecast_to_db
from historical_6hour import save_6hour_to_db
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import logging

# Current Conditions, 5 Day Forecast, and 6 Hour Historical Based on LOCATION CODE

# API Configuration
API_KEY = "API_KEY"
CURRENT_CONDITIONS_URL = "http://dataservice.accuweather.com/currentconditions/v1/"
FORECAST_5DAY_URL = "http://dataservice.accuweather.com/forecasts/v1/daily/5day/"
HISTORICAL_6_HOUR = "http://dataservice.accuweather.com/currentconditions/v1/"
LOCATION_KEY = "331799"  # For Mesa, AZ.

# Default args
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
with DAG(
    "pipeline_with_airflow",
    default_args=default_args,
    description="Fetch weather data and store in MySQL",
    schedule="@hourly",
    start_date=datetime(2024, 12, 24),
    catchup=False,
) as dag:

    def fetch_and_store_current_weather():
        try:
            response = requests.get(f"{CURRENT_CONDITIONS_URL}{LOCATION_KEY}", params={"apikey": API_KEY})
            response.raise_for_status()  # Raise an error for bad status codes
            data = response.json()
            save_current_weather_to_db(data)
        except Exception as e:
            logging.error(f"Error fetching current weather: {e}")
            raise

    def fetch_and_store_5day_forecast():
        try:
            response = requests.get(f"{FORECAST_5DAY_URL}{LOCATION_KEY}", params={"apikey": API_KEY})
            response.raise_for_status()
            data = response.json()
            save_5day_forecast_to_db(data)
        except Exception as e:
            logging.error(f"Error fetching 5-day forecast: {e}")
            raise

    def fetch_and_store_6_hour_historical():
        try:
            response = requests.get(f"{HISTORICAL_6_HOUR}{LOCATION_KEY}", params={"apikey": API_KEY})
            response.raise_for_status()
            data = response.json()
            save_6hour_to_db(data)
        except Exception as e:
            logging.error(f"Error fetching 5-day forecast: {e}")
            raise

    fetch_current_weather = PythonOperator(
        task_id="fetch_and_store_current_weather",
        python_callable=fetch_and_store_current_weather
    )

    fetch_5day_forecast = PythonOperator(
        task_id="fetch_and_store_5day_forecast",
        python_callable=fetch_and_store_5day_forecast
    )

    fetch_6hour_historical = PythonOperator(
        task_id="fetch_and_store_6_hour_historical",
        python_callable=fetch_and_store_6_hour_historical
    )

    fetch_current_weather >> fetch_5day_forecast >> fetch_6hour_historical
