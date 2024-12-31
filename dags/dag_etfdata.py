from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import mysql.connector
import logging
import pandas as pd  # Import pandas

# Database configuration
DB_CONFIG = {
    "host": "host.docker.internal",
    "user": "MYSQL_USERNAME",
    "password": "MYSQL_PASSWORD",
    "database": "etf_data"
}

# ETF Symbols
ETF_SYMBOLS = ["QYLD", "VGT", "VHT", "VIG", "VNQ", "VOO", "VPU", "VTC", "VTEB", "VTI", "VTV", "VUG", "VXUS",
               "VYM", "VYMI"]

# Time periods and corresponding table names
TIME_PERIODS = {
    "1d": "etf_data_1D",
    "5d": "etf_data_5D",
    "1mo": "etf_data_1M",
    "6mo": "etf_data_6M",
    "ytd": "etf_data_YTD",
    "1y": "etf_data_1Y",
    "5y": "etf_data_5Y",
    "max": "etf_data_All"
}

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Function to fetch and store data for each time period
def fetch_and_store_etf_data():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        for period, table_name in TIME_PERIODS.items():
            query = f"""
                INSERT INTO {table_name} (date, price, volume, symbol)
                VALUES (%s, %s, %s, %s)
            """

            for symbol in ETF_SYMBOLS:
                etf = yf.Ticker(symbol)
                data = etf.history(period=period)  # Fetch data for the specific time period
                if not data.empty:
                    for index, row in data.iterrows():
                        record = (
                            index.to_pydatetime(),
                            float(row["Close"]),
                            int(row["Volume"]) if not pd.isna(row["Volume"]) else None,
                            symbol
                        )
                        cursor.execute(query, record)

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error fetching or storing ETF data: {e}")
        raise

# DAG definition
with DAG(
    "etf_data_pipeline",
    default_args=default_args,
    description="Fetch and store ETF data for different time periods",
    schedule_interval="@daily",  # Adjust schedule as needed
    start_date=datetime(2023, 12, 1),
    catchup=False,
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_and_store_etf_data",
        python_callable=fetch_and_store_etf_data,
    )

    fetch_data
