from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import mysql.connector
import logging

# Constants
API_URL = "https://waterservices.usgs.gov/nwis/iv/"
DB_CONFIG = {
    "host": "host.docker.internal",
    "user": "MYSQL_USERNAME",
    "password": "MYSQL_PASSWORD",
    "database": "water_data"
}
LOCATIONS = ["09504420", "09505800"] #Add or remove desired locations based on USGS
API_PARAMS_TEMPLATE = {
    "format": "json",
    "indent": "on",
    "period": "PT4H",  #Change Time Period Here
    "siteStatus": "active",
    "siteType": "ST,SP"
}

# Default args
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Function to fetch and store data
def fetch_and_store_current_data(location):
    try:
        # Prepare API parameters
        api_params = API_PARAMS_TEMPLATE.copy()
        api_params["sites"] = location

        # Fetch data from the API
        response = requests.get(API_URL, params=api_params)
        response.raise_for_status()
        data = response.json()

        # Ensure timeSeries exists in the response
        if not data["value"].get("timeSeries"):
            raise ValueError(f"No timeSeries data available for location {location}")

        # Extract site info
        time_series = data["value"]["timeSeries"]
        site_info = time_series[0]["sourceInfo"]
        site_name = site_info.get("siteName", "Unknown")
        site_code = site_info["siteCode"][0]["value"]
        srs = site_info["geoLocation"]["geogLocation"].get("srs", "Unknown")
        latitude = site_info["geoLocation"]["geogLocation"]["latitude"]
        longitude = site_info["geoLocation"]["geogLocation"]["longitude"]

        # Extract variable values
        temp_values = time_series[0]["values"][0].get("value", [])
        streamflow_values = time_series[1]["values"][0].get("value", [])
        gage_height_values = time_series[2]["values"][0].get("value", [])

        # Ensure there's at least one data point for each variable
        if not temp_values or not streamflow_values or not gage_height_values:
            raise ValueError(f"No current data available for location {location}")

        # Get the most recent data points
        current_temp = temp_values[-1]
        current_streamflow = streamflow_values[-1]
        current_gage_height = gage_height_values[-1]

        # Extract the datetime and values
        observation_time = current_temp["dateTime"]
        water_temperature = float(current_temp["value"])
        streamflow = float(current_streamflow["value"])
        gage_height = float(current_gage_height["value"])

        # Connect to MySQL
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Insert query
        insert_query = """
            INSERT INTO water_data_current (
                site_name, site_code, srs, latitude, longitude, observation_time, water_temperature, streamflow, gage_height
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            site_name, site_code, srs, latitude, longitude, observation_time, water_temperature, streamflow, gage_height
        ))

        # Commit and close connection
        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        logging.error(f"Error fetching or storing data for location {location}: {e}")
        raise

# DAG Definition
with DAG(
    "current_water_data_pipeline",
    default_args=default_args,
    description="Fetch current water data for multiple locations and store in MySQL",
    schedule_interval="@hourly",
    start_date=datetime(2024, 12, 24),
    catchup=False,
) as dag:

    # Create tasks for each location
    for location in LOCATIONS:
        task = PythonOperator(
            task_id=f"fetch_and_store_current_data_{location}",
            python_callable=fetch_and_store_current_data,
            op_args=[location],
        )
