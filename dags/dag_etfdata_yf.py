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

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Function to fetch and store data
def fetch_and_store_etf_data(symbol: str):
    try:
        # MySQL connection
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Fetch ETF data from yfinance
        etf = yf.Ticker(symbol)
        data = etf.history(period="1d", interval="5m")

        # Ensure data is non-empty
        if data.empty:
            logging.warning(f"No data fetched for symbol: {symbol}.")
            return

        # Insert each row into the database
        query = """
            INSERT INTO etf_stock_data (symbol, timestamp, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        for index, row in data.iterrows():
            # Convert Series to Python native types
            cursor.execute(query, (
                symbol,  # ETF symbol
                index.to_pydatetime(),  # Convert Timestamp to datetime
                float(row["Open"]),
                float(row["High"]),
                float(row["Low"]),
                float(row["Close"]),
                int(row["Volume"])
            ))

        conn.commit()
        logging.info(f"Data for symbol {symbol} successfully stored in the database.")
    except Exception as e:
        logging.error(f"Error fetching or storing data for symbol {symbol}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

# Define the DAG
with DAG(
    "etf_stock_data_pipeline",
    default_args=default_args,
    description="Fetch and store ETF/stock data into MySQL",
    schedule_interval="*/1 * * * *",  # Every minute
    start_date=datetime(2023, 12, 1),
    catchup=False,
) as dag:

    # Create tasks for each symbol
    tasks = []
    for symbol in ["QYLD", "VOO", "VTC"]:  # Add ETF symbols here
        task = PythonOperator(
            task_id=f"fetch_and_store_{symbol}",
            python_callable=fetch_and_store_etf_data,
            op_args=[symbol]
        )
        tasks.append(task)
