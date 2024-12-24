import mysql.connector

def save_current_weather_to_db(data):
    conn = mysql.connector.connect(
        host="host.docker.internal",
        user="MYSQL_USERNAME",
        password="MYSQL_PASSWORD",
        database="weather_data"
    )
    cursor = conn.cursor()
    query = """
        INSERT INTO current_weather (timestamp, location_key, temperature, weather)
        VALUES (%s, %s, %s, %s)
    """
    cursor.execute(query, (
        data[0]["LocalObservationDateTime"],
        "328169",  # Example LocationKey
        data[0]["Temperature"]["Imperial"]["Value"],
        data[0]["WeatherText"]
    ))
    conn.commit()
    cursor.close()
    conn.close()
