import streamlit as st
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt

# Database configuration
DB_CONFIG = {
    "host": "localhost",
    "user": "MYSQL_USERNAME",
    "password": "MYSQL_PASSWORD",
    "database": "etf_data"
}

# Function to fetch data from MySQL
def fetch_data(table_name, selected_etfs):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        placeholders = ', '.join(['%s'] * len(selected_etfs))
        query = f"""
            SELECT * FROM {table_name}
            WHERE symbol IN ({placeholders})
            ORDER BY date
        """
        data = pd.read_sql(query, conn, params=selected_etfs)
        conn.close()
        return data
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()

# Streamlit app
st.title("ETF Data Dashboard")
st.markdown("Visualize ETF data for different time periods and symbols.")

# Dropdown for time periods
time_periods = {
    "1D": "etf_data_1D",
    "5D": "etf_data_5D",
    "1M": "etf_data_1M",
    "6M": "etf_data_6M",
    "YTD": "etf_data_YTD",
    "1Y": "etf_data_1Y",
    "5Y": "etf_data_5Y",
    "All": "etf_data_All"
}
selected_period = st.selectbox("Select Time Period", options=list(time_periods.keys()))

# Multiselect for ETFs
etf_symbols = ["QYLD", "VGT", "VHT", "VIG", "VNQ", "VOO", "VPU", "VTC", "VTEB", "VTI", "VTV", "VUG", "VXUS",
               "VYM", "VYMI"]
selected_etfs = st.multiselect("Select ETFs", options=etf_symbols, default=etf_symbols[:3])

# Fetch and visualize data
if selected_period and selected_etfs:
    table_name = time_periods[selected_period]
    data = fetch_data(table_name, selected_etfs)

    if not data.empty:
        # Enable dark theme for the plot
        plt.style.use("dark_background")  # Use Matplotlib's dark mode style

        # Plot data
        st.subheader(f"Graph for {selected_period}")
        fig, ax = plt.subplots(figsize=(12, 6))  # Adjusted figure size
        for etf in selected_etfs:
            etf_data = data[data['symbol'] == etf]
            ax.plot(
                etf_data['date'], 
                etf_data['price'], 
                marker='o',  # Point marker style
                markersize=5,  # Adjust the size of the points
                label=etf
            )
        ax.set_xlabel('Date', color='white')  # Set x-axis label color
        ax.set_ylabel('Price', color='white')  # Set y-axis label color
        ax.set_title(f"ETF Prices Over {selected_period}", color='white')  # Set title color
        ax.tick_params(colors='white')  # Set tick colors
        ax.legend(loc="upper left", bbox_to_anchor=(1, 1), facecolor="black", edgecolor="white", labelcolor="white")  # Dark legend
        plt.tight_layout()  # Ensure the layout doesn't overlap
        st.pyplot(fig)

        # Display data table below the graph
        st.subheader("Data Table")
        st.write(data)
    else:
        st.warning(f"No data available for {selected_period} and selected ETFs.")
