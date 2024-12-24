from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import mysql.connector

# Define database connection details
DB_CONFIG = {
    'host': 'host.docker.internal',
    'user': 'MYSQL_USERNAME',
    'password': 'MYSQL_PASSWORD',
    'database': 'weather_meteostat',
}

# Define Meteostat API configuration
API_URL = "https://meteostat.p.rapidapi.com/point/monthly"
HEADERS = {
    'x-rapidapi-key': "API_KEY",
    'x-rapidapi-host': "meteostat.p.rapidapi.com"
}

LAT = 33.4245  # Update latitude as needed
LON = -111.8332  # Update longitude as needed
ALT = 1378       # Update altitude as needed (optional)
START_DATE = "2024-01-01"  # Adjust the start date
END_DATE = "2024-12-31"    # Adjust the end date

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Function to fetch data from the Meteostat API
def fetch_meteostat_data():
    try:
        # Make the API request
        response = requests.get(
            API_URL,
            headers=HEADERS,
            params={
                "lat": LAT,
                "lon": LON,
                "alt": ALT,
                "start": START_DATE,
                "end": END_DATE
            }
        )
        # Log the request details
        print(f"Request URL: {response.url}")
        print(f"Response Status: {response.status_code}")

        # Handle response
        if response.status_code == 200:
            print("Data fetched successfully.")
            data = response.json()
            if "data" in data:
                save_meteostat_data_to_db(data["data"])
            else:
                raise Exception("No 'data' field found in API response.")
        else:
            print(f"Error fetching data: {response.status_code} - {response.reason}")
            print(response.text)
            raise Exception(f"Failed to fetch data. Status: {response.status_code} - {response.reason}")
    except Exception as e:
        print(f"Exception: {e}")
        raise

# Function to save data into MySQL
def save_meteostat_data_to_db(data):
    try:
        # Connect to the MySQL database
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Insert data into the database
        query = """
            INSERT INTO monthly_weather (date, tavg, tmin, tmax, prcp, wspd, pres, tsun)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            tavg = VALUES(tavg), tmin = VALUES(tmin), tmax = VALUES(tmax),
            prcp = VALUES(prcp), wspd = VALUES(wspd), pres = VALUES(pres), tsun = VALUES(tsun)
        """
        for row in data:
            cursor.execute(query, (
                row["date"],
                row.get("tavg", None),
                row.get("tmin", None),
                row.get("tmax", None),
                row.get("prcp", None),
                row.get("wspd", None),
                row.get("pres", None),
                row.get("tsun", None)
            ))
        conn.commit()
        print(f"Inserted {cursor.rowcount} rows into the database.")
    except mysql.connector.Error as e:
        print(f"MySQL Error: {e}")
        raise
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# Define the DAG
with DAG(
    "meteostat_pipeline",
    default_args=default_args,
    description="Fetch monthly weather data from Meteostat and store in MySQL",
    schedule="@monthly",
    start_date=datetime(2023, 12, 1),
    catchup=False,
) as dag:

    fetch_data_task = PythonOperator(
        task_id="fetch_meteostat_data",
        python_callable=fetch_meteostat_data
    )
