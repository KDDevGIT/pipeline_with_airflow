from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import mysql.connector
import logging
import pandas as pd

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
                INSERT INTO {table_name} (short_name, date, price, volume, symbol)
                VALUES (%s, %s, %s, %s, %s)
            """

            for symbol in ETF_SYMBOLS:
                etf = yf.Ticker(symbol)
                data = etf.history(period=period)  # Fetch data for the specific time period
                short_name = etf.info.get("shortName")  # Get shortName

                if not data.empty:
                    for index, row in data.iterrows():
                        # Replace NaN with None or default values
                        record = (
                            short_name if pd.notna(short_name) else None,  # Handle missing short_name
                            index.to_pydatetime(),
                            float(row["Close"]) if pd.notna(row["Close"]) else 0.0,  # Default price to 0.0
                            int(row["Volume"]) if pd.notna(row["Volume"]) else None,  # Handle missing volume
                            symbol
                        )
                        cursor.execute(query, record)

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error fetching or storing ETF data: {e}")
        raise

# Function to fetch and store extra stats
def fetch_and_store_extra_stats():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        query = """
            INSERT INTO etf_data_extra_stats (
                symbol, longname, fund_family, category, yield,
                previous_close, open, day_low, day_high,
                52_wklow, 52_wkhigh, 50d_avg,
                ytd_return, 3yr_avg_ret, 5yr_avg_ret
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for symbol in ETF_SYMBOLS:
            etf = yf.Ticker(symbol)
            info = etf.info

            # Extract the desired data
            record = (
                symbol,
                info.get("longName"),
                info.get("fundFamily"),
                info.get("category"),
                info.get("yield"),
                info.get("previousClose"),
                info.get("open"),
                info.get("dayLow"),
                info.get("dayHigh"),
                info.get("fiftyTwoWeekLow"),
                info.get("fiftyTwoWeekHigh"),
                info.get("fiftyDayAverage"),
                info.get("ytdReturn"),
                info.get("threeYearAverageReturn"),
                info.get("fiveYearAverageReturn"),
            )
            cursor.execute(query, record)

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error fetching or storing ETF extra stats: {e}")
        raise

# DAG definition
with DAG(
    "etf_data_pipeline",
    default_args=default_args,
    description="Fetch and store ETF data and extra stats",
    schedule_interval="@daily",  # Adjust schedule as needed
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_and_store_etf_data",
        python_callable=fetch_and_store_etf_data,
    )

    fetch_extra_stats = PythonOperator(
        task_id="fetch_and_store_extra_stats",
        python_callable=fetch_and_store_extra_stats,
    )

    fetch_data >> fetch_extra_stats
