import streamlit as st
import pandas as pd
import pymysql
import plotly.express as px

# Database configuration
DB_CONFIG = {
    "host": "localhost",
    "user": "MYSQL_USERNAME",
    "password": "MYSQL_PASSWORD",
    "database": "water_data"
}

# Streamlit layout
st.set_page_config(page_title="USGS Water Data", layout="wide")
st.title("Real-Time USGS Water Data AZ")

# Select locations and time period
locations = st.multiselect(
    "Select Location(s):",
    ["OAK CREEK NEAR SEDONA, AZ [09504420]", 
     "WEST CLEAR CREEK NEAR CAMP VERDE, AZ [09505800]",
     "ARAVAIPA CREEK NEAR MAMMOTH, AZ [09473000]", 
     "TONTO CREEK ABOVE GUN CREEK NEAR ROOSEVELT, AZ [09499000]",
     "FOSSIL CREEK NEAR STRAWBERRY, AZ [09507480]", 
     "AGUA FRIA RIVER NEAR ROCK SPRINGS, AZ [09512800]", 
     "FISH CREEK NEAR TORTILLA FLAT, AZ [09501150]", 
     "SYCAMORE CREEK NEAR FORT MCDOWELL, AZ [09510200]"],
    default=[]  # No default selection
)
time_period = st.selectbox("Select Time Period", ["4 Hours", "1 Day", "2 Days", "3 Days", "1 Week"])

# Map time period to table name
TIME_PERIOD_MAPPING = {
    "4 Hours": "water_data_4hour",
    "1 Day": "water_data_1day",
    "2 Days": "water_data_2day",
    "3 Days": "water_data_3day",
    "1 Week": "water_data_1week"
}
table_name = TIME_PERIOD_MAPPING[time_period]

# Fetch data from MySQL
@st.cache_data
def fetch_multi_data(locations, table_name):
    conn = pymysql.connect(**DB_CONFIG)
    placeholders = ', '.join(['%s'] * len(locations))
    query = f"""
        SELECT observation_time, site_name, latitude, longitude, streamflow, gage_height, precipitation
        FROM {table_name}
        WHERE site_name IN ({placeholders})
        ORDER BY observation_time
    """
    df = pd.read_sql(query, conn, params=locations)
    conn.close()
    df["observation_time"] = pd.to_datetime(df["observation_time"])
    df["streamflow"] = pd.to_numeric(df["streamflow"], errors="coerce")
    df["gage_height"] = pd.to_numeric(df["gage_height"], errors="coerce")
    df["precipitation"] = pd.to_numeric(df["precipitation"], errors="coerce")
    return df

if locations:
    data = fetch_multi_data(locations, table_name)

    # Display selected locations and time period
    st.subheader(f"Selected Location(s): {', '.join(locations)} - {time_period}")

    # Interactive map for one location
    if len(locations) == 1:  # Display the map only if one location is selected
        st.subheader("Site Map")
        if "latitude" in data.columns and "longitude" in data.columns:
            location_data = data.drop_duplicates(subset=["latitude", "longitude", "site_name"])
            
            if not location_data.empty:
                fig_map = px.scatter_mapbox(
                    location_data,
                    lat="latitude",
                    lon="longitude",
                    hover_name="site_name",
                    hover_data={"latitude": True, "longitude": True},
                    zoom=10,
                    height=500
                )
                fig_map.update_layout(mapbox_style="open-street-map")
                st.plotly_chart(fig_map, use_container_width=True)
            else:
                st.write("No geographic data available for the selected location.")
        else:
            st.write("Latitude and Longitude data is missing for the selected location.")

    # Streamflow plot
    if not data.empty:
        fig1 = px.line(
            data,
            x="observation_time",
            y="streamflow",
            color="site_name",
            title="Discharge",
            labels={
                "observation_time": "Date and Time",
                "streamflow": "Discharge (CFS)",
                "site_name": "Site Name"
            },
            markers=True
        )
        fig1.update_traces(marker=dict(size=3))  # Set marker size smaller
        st.plotly_chart(fig1, use_container_width=True)

        # Gage height plot
        fig2 = px.line(
            data,
            x="observation_time",
            y="gage_height",
            color="site_name",
            title="Gage Height",
            labels={
                "observation_time": "Date and Time",
                "gage_height": "Gage Height (FT)",
                "site_name": "Site Name"
            },
            markers=True
        )
        fig2.update_traces(marker=dict(size=3))  # Set marker size smaller
        st.plotly_chart(fig2, use_container_width=True)

        # Precipitation plot
        fig3 = px.line(
            data,
            x="observation_time",
            y="precipitation",
            color="site_name",
            title="Precipitation",
            labels={
                "observation_time": "Date and Time",
                "precipitation": "Precipitation (Inches)",
                "site_name": "Site Name"
            },
            markers=True
        )
        fig3.update_traces(marker=dict(size=3))  # Set marker size smaller
        st.plotly_chart(fig3, use_container_width=True)

         # Summary statistics
    if not data.empty:
        st.subheader("Summary Statistics")
        summary_stats = data.groupby("site_name").agg(
            Mean_Streamflow=("streamflow", "mean"),
            Median_Streamflow=("streamflow", "median"),
            Min_Streamflow=("streamflow", "min"),
            Max_Streamflow=("streamflow", "max"),
            Mean_Gage_Height=("gage_height", "mean"),
            Median_Gage_Height=("gage_height", "median"),
            Min_Gage_Height=("gage_height", "min"),
            Max_Gage_Height=("gage_height", "max"),
            Mean_Precipitation=("precipitation", "mean"),
            Median_Precipitation=("precipitation", "median"),
            Min_Precipitation=("precipitation", "min"),
            Max_Precipitation=("precipitation", "max")
        ).reset_index()
        st.dataframe(summary_stats)
    else:
        st.write("No data available for the selected locations and time period.")
else:
    st.write("Please select at least one location.")
