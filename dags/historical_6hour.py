def save_6hour_to_db(data):
    import mysql.connector

    # Database connection
    conn = mysql.connector.connect(
        host="host.docker.internal",
        user="SQL_USERNAME",
        password="SQL_PASSWORD",
        database="weather_data"
    )
    cursor = conn.cursor()

    # Insert query without `mobile_link` and `link`
    insert_query = """
        INSERT INTO historical_6hour (
            observation_time,
            epoch_time,
            weather_text,
            weather_icon,
            has_precipitation,
            precipitation_type,
            is_daytime,
            temperature_c,
            temperature_f
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Iterate through data and insert each record
    for item in data:
        cursor.execute(insert_query, (
            item["LocalObservationDateTime"],
            item["EpochTime"],
            item["WeatherText"],
            item["WeatherIcon"],
            item["HasPrecipitation"],
            item.get("PrecipitationType"),  # Use .get() to handle potential NULL values
            item["IsDayTime"],
            item["Temperature"]["Metric"]["Value"],
            item["Temperature"]["Imperial"]["Value"]
        ))

    # Commit and close connection
    conn.commit()
    cursor.close()
    conn.close()
