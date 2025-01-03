import streamlit as st
import mysql.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

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

# Function to fetch extra stats
def fetch_extra_stats(selected_etfs):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        placeholders = ', '.join(['%s'] * len(selected_etfs))
        query = f"""
            SELECT 
                symbol, longname AS `ETF Name`, fund_family AS `Fund Family`, 
                category AS `Category`, yield AS `Yield (%)`, 
                previous_close AS `Previous Close`, `open` AS `Open`, 
                day_low AS `Day Low`, day_high AS `Day High`, 
                52_wklow AS `52-Week Low`, 52_wkhigh AS `52-Week High`, 
                50d_avg AS `50-Day Average`, ytd_return AS `YTD Return (%)`, 
                3yr_avg_ret AS `3-Year Avg Return (%)`, 5yr_avg_ret AS `5-Year Avg Return (%)` 
            FROM etf_data_extra_stats
            WHERE symbol IN ({placeholders})
        """
        data = pd.read_sql(query, conn, params=selected_etfs)
        conn.close()
        return data
    except Exception as e:
        st.error(f"Error fetching extra stats: {e}")
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
        # Display the ETF Data Table
        st.subheader(f"ETF Data Table")
        data = data[["symbol", "short_name", "date", "price", "volume"]].rename(columns={
            "symbol": "ETF Symbol",
            "short_name": "ETF Name",
            "date": "Date",
            "price": "Closing Price",
            "volume": "Volume Traded"
        })
        data.index = data.index + 1  # Start index at 1
        st.dataframe(data)

        # Dropdown to select the graph style
        graph_style = st.selectbox("Select Graph Style", options=["Line", "Bar", "Candlestick"])

        # Plotly chart with the selected graph style
        st.subheader(f"Graph for {selected_period}")

        if graph_style == "Line":
            fig = px.line(
                data,
                x="Date",
                y="Closing Price",
                color="ETF Symbol",
                markers=True,
                title=f"ETF Prices Over {selected_period}",
                labels={
                    "Closing Price": "Price",
                    "Date": "Date",
                    "ETF Symbol": "ETF"
                }
            )
            fig.update_traces(marker=dict(size=8))
        elif graph_style == "Bar":
            fig = px.bar(
                data,
                x="Date",
                y="Closing Price",
                color="ETF Symbol",
                title=f"ETF Prices Over {selected_period}",
                labels={
                    "Closing Price": "Price",
                    "Date": "Date",
                    "ETF Symbol": "ETF"
                }
            )
        elif graph_style == "Candlestick":
            fig = go.Figure()
            for etf in data["ETF Symbol"].unique():
                etf_data = data[data["ETF Symbol"] == etf]
                fig.add_trace(go.Candlestick(
                    x=etf_data["Date"],
                    open=etf_data["Closing Price"],  # Replace with actual open prices if available
                    high=etf_data["Closing Price"],  # Replace with actual high prices if available
                    low=etf_data["Closing Price"],   # Replace with actual low prices if available
                    close=etf_data["Closing Price"],
                    name=etf
                ))
            fig.update_layout(
                title=f"ETF Prices Over {selected_period}",
                xaxis_title="Date",
                yaxis_title="Price",
                template="plotly_dark",
                hovermode="x unified"
            )

        st.plotly_chart(fig, use_container_width=True)

# Fetch and display extra stats for the selected ETFs
st.subheader("Performance")
if selected_etfs:
    extra_stats = fetch_extra_stats(selected_etfs)
    if not extra_stats.empty:
        extra_stats.index = extra_stats.index + 1  # Start index at 1
        st.dataframe(
            extra_stats[[
                "symbol", "ETF Name", "Fund Family", "Category", "Yield (%)", 
                "Previous Close", "Open", "Day Low", "Day High", 
                "52-Week Low", "52-Week High", "50-Day Average", 
                "YTD Return (%)", "3-Year Avg Return (%)", "5-Year Avg Return (%)"
            ]]
        )
    else:
        st.warning("No extra stats available for the selected ETFs.")
else:
    st.warning("Please select at least one ETF to view extra stats.")
