import mysql.connector

def save_5day_forecast_to_db(data):
    conn = mysql.connector.connect(
        host="host.docker.internal",
        user="SQL_USERNAME",
        password="SQL_PASSWORD",
        database="weather_data"
    )
    cursor = conn.cursor()
    query = """
        INSERT INTO forecast_5day (date, location_key, min_temp, max_temp, day_condition, night_condition)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    for day in data["DailyForecasts"]:
        cursor.execute(query, (
            day["Date"].split("T")[0],
            "328169",
            day["Temperature"]["Minimum"]["Value"],
            day["Temperature"]["Maximum"]["Value"],
            day["Day"]["IconPhrase"],
            day["Night"]["IconPhrase"]
        ))
    conn.commit()
    cursor.close()
    conn.close()
