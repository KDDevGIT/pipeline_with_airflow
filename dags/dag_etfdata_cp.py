from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import mysql.connector
import logging

# Database configuration
DB_CONFIG = {
    "host": "host.docker.internal",
    "user": "MYSQL_USERNAME",
    "password": "MYSQL_PASSWORD",
    "database": "etf_data"
}

# ETF Symbols (Can Change)
ETF_SYMBOLS = ["QYLD", "VOO", "VTC"]

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Function to fetch and store current prices
def fetch_and_store_current_price():
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Query to insert the data
        query = """
            INSERT INTO etf_current_price (symbol, timestamp, current_price)
            VALUES (%s, %s, %s)
        """

        for symbol in ETF_SYMBOLS:
            etf = yf.Ticker(symbol)
            data = etf.history(period="1d", interval="5m").tail(1)  # Get the latest price
            if not data.empty:
                last_row = data.iloc[-1]
                cursor.execute(query, (
                    symbol,
                    last_row.name.to_pydatetime(),
                    float(last_row["Close"]),
                ))

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error fetching or storing current price data: {e}")
        raise

# DAG definition
with DAG(
    "etf_current_price_pipeline",
    default_args=default_args,
    description="Fetch and store current ETF prices every 5 minutes",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2023, 12, 1),
    catchup=False,
) as dag:

    fetch_prices = PythonOperator(
        task_id="fetch_and_store_current_price",
        python_callable=fetch_and_store_current_price,
    )

    fetch_prices
