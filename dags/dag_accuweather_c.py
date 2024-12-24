from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import logging
import mysql.connector

# API Configuration
API_KEY = "API_KEY"
CURRENT_CONDITIONS_URL = "http://dataservice.accuweather.com/currentconditions/v1/"
FORECAST_5DAY_URL = "http://dataservice.accuweather.com/forecasts/v1/daily/5day/"
HISTORICAL_6_HOUR_URL = "http://dataservice.accuweather.com/currentconditions/v1/"
LOCATION_KEY = "331799"  # For Mesa, AZ.

# Database Configuration
DB_CONFIG = {
    "host": "host.docker.internal",
    "user": "SQL_USERNAME",
    "password": "SQL_PASSWORD",
    "database": "weather_data"
}

# Default args
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Save Current Weather to DB
def save_current_weather_to_db(data):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    query = """
        INSERT INTO current_weather (timestamp, location_key, temperature, weather)
        VALUES (%s, %s, %s, %s)
    """
    cursor.execute(query, (
        data[0]["LocalObservationDateTime"],
        LOCATION_KEY,
        data[0]["Temperature"]["Imperial"]["Value"],
        data[0]["WeatherText"]
    ))
    conn.commit()
    cursor.close()
    conn.close()

# Save 5-Day Forecast to DB
def save_5day_forecast_to_db(data):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    query = """
        INSERT INTO forecast_5day (date, location_key, min_temp, max_temp, day_condition, night_condition)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    for day in data["DailyForecasts"]:
        cursor.execute(query, (
            day["Date"].split("T")[0],
            LOCATION_KEY,
            day["Temperature"]["Minimum"]["Value"],
            day["Temperature"]["Maximum"]["Value"],
            day["Day"]["IconPhrase"],
            day["Night"]["IconPhrase"]
        ))
    conn.commit()
    cursor.close()
    conn.close()

# Save 6-Hour Historical Data to DB
def save_6hour_to_db(data):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    query = """
        INSERT INTO historical_6hour (
            observation_time,
            epoch_time,
            weather_text,
            weather_icon,
            has_precipitation,
            precipitation_type,
            is_daytime,
            temperature_c,
            temperature_f
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for item in data:
        cursor.execute(query, (
            item["LocalObservationDateTime"],
            item["EpochTime"],
            item["WeatherText"],
            item["WeatherIcon"],
            item["HasPrecipitation"],
            item.get("PrecipitationType"),
            item["IsDayTime"],
            item["Temperature"]["Metric"]["Value"],
            item["Temperature"]["Imperial"]["Value"]
        ))
    conn.commit()
    cursor.close()
    conn.close()

# Fetch and store current weather
def fetch_and_store_current_weather():
    try:
        response = requests.get(f"{CURRENT_CONDITIONS_URL}{LOCATION_KEY}", params={"apikey": API_KEY})
        response.raise_for_status()
        data = response.json()
        save_current_weather_to_db(data)
    except Exception as e:
        logging.error(f"Error fetching current weather: {e}")
        raise

# Fetch and store 5-day forecast
def fetch_and_store_5day_forecast():
    try:
        response = requests.get(f"{FORECAST_5DAY_URL}{LOCATION_KEY}", params={"apikey": API_KEY})
        response.raise_for_status()
        data = response.json()
        save_5day_forecast_to_db(data)
    except Exception as e:
        logging.error(f"Error fetching 5-day forecast: {e}")
        raise

# Fetch and store 6-hour historical data
def fetch_and_store_6_hour_historical():
    try:
        response = requests.get(f"{HISTORICAL_6_HOUR_URL}{LOCATION_KEY}", params={"apikey": API_KEY})
        response.raise_for_status()
        data = response.json()
        save_6hour_to_db(data)
    except Exception as e:
        logging.error(f"Error fetching 6-hour historical data: {e}")
        raise

# DAG Definition
with DAG(
    "pipeline_with_airflow",
    default_args=default_args,
    description="Fetch weather data and store in MySQL",
    schedule="@hourly",
    start_date=datetime(2024, 12, 24),
    catchup=False,
) as dag:

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
