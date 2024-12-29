from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pymysql
import logging

# Constants
API_URL = "https://waterservices.usgs.gov/nwis/iv/"
DB_CONFIG = {
    "host": "host.docker.internal",  
    "user": "MYSQL_USERNAME",
    "password": "MYSQL_PASSWORD",
    "database": "water_data"
}
LOCATIONS = {
    "09504420": "OAK CREEK NEAR SEDONA, AZ",
    "09505800": "WEST CLEAR CREEK NEAR CAMP VERDE, AZ",
    "09473000": "ARAVAIPA CREEK NEAR MAMMOTH, AZ",
    "09499000": "TONTO CREEK ABOVE GUN CREEK NEAR ROOSEVELT, AZ",
    "09507480": "FOSSIL CREEK NEAR STRAWBERRY, AZ",
    "09512800": "AGUA FRIA RIVER NEAR ROCK SPRINGS, AZ",
    "09501150": "FISH CREEK NEAR TORTILLA FLAT, AZ",
    "09510200": "SYCAMORE CREEK NEAR FORT MCDOWELL, AZ",
}
API_PERIODS = {
    "4hour": "PT4H",
    "1day": "P1D",
    "2day": "P2D",
    "3day": "P3D",
    "1week": "P7D"
}

# Default args
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Fetch and store data
def fetch_and_store_data(location, period, table_name):
    try:
        # Prepare API parameters
        params = {
            "format": "json",
            "sites": location,
            "period": API_PERIODS[period],
            "siteStatus": "active",
            "siteType": "ST,SP"
        }

        # Fetch data
        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        data = response.json()

        # Log raw API response for debugging
        logging.info(f"Raw API response for {location}, {period}: {data}")

        # Safeguard against missing time series
        time_series = data["value"].get("timeSeries", [])
        if not time_series:
            logging.warning(f"No timeSeries data available for site {location} and period {period}. Skipping.")
            return

        # Parse site information
        site_info = time_series[0]["sourceInfo"]
        site_name = LOCATIONS.get(location, "Unknown Site")
        site_code = site_info["siteCode"][0]["value"]
        srs = site_info["geoLocation"]["geogLocation"].get("srs", "Unknown")
        latitude = site_info["geoLocation"]["geogLocation"]["latitude"]
        longitude = site_info["geoLocation"]["geogLocation"]["longitude"]

        # Extract streamflow, gauge height, and precipitation values
        streamflow_data = next(
            (ts for ts in time_series if "Streamflow" in ts["variable"]["variableName"]), None
        )
        gage_height_data = next(
            (ts for ts in time_series if "Gage height" in ts["variable"]["variableName"]), None
        )
        precipitation_data = next(
            (ts for ts in time_series if "Precipitation" in ts["variable"]["variableName"]), None
        )

        # Insert data into MySQL
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        insert_query = f"""
            INSERT INTO {table_name} (
                site_name, site_code, srs, latitude, longitude, observation_time, streamflow, gage_height, precipitation
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Process Gauge Height values
        if gage_height_data:
            for record in gage_height_data["values"][0]["value"]:
                observation_time = record["dateTime"]
                gage_height = float(record["value"])

                # Attempt to match Streamflow and Precipitation values for the same observation time
                streamflow = next(
                    (float(r["value"]) for r in (streamflow_data["values"][0]["value"] if streamflow_data else []) 
                     if r["dateTime"] == observation_time),
                    None
                )
                precipitation = next(
                    (float(r["value"]) for r in (precipitation_data["values"][0]["value"] if precipitation_data else []) 
                     if r["dateTime"] == observation_time),
                    None
                )

                # Log the processed data for debugging
                logging.info(f"Processed data for {location}, {observation_time}: Streamflow={streamflow}, "
                             f"Gauge Height={gage_height}, Precipitation={precipitation}")

                # Insert the data into the database
                cursor.execute(
                    insert_query,
                    (
                        site_name, site_code, srs, latitude, longitude, observation_time,
                        streamflow, gage_height, precipitation
                    )
                )
            conn.commit()
        else:
            logging.warning(f"No Gauge Height data available for site {location} and period {period}. Skipping Gauge Height processing.")

        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error fetching or storing data for location {location}, period {period}: {e}")
        raise

# DAG Definition
with DAG(
    "store_water_data_intervals",
    default_args=default_args,
    description="Fetch water data and store it for various time intervals",
    schedule_interval="@hourly",
    start_date=datetime(2024, 12, 25),
    catchup=False,
) as dag:

    tasks = []
    for period, table_suffix in API_PERIODS.items():
        table_name = f"water_data_{period}"
        for location in LOCATIONS:
            task = PythonOperator(
                task_id=f"fetch_{location}_{period}",
                python_callable=fetch_and_store_data,
                op_args=[location, period, table_name],
            )
            tasks.append(task)
