import requests
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, engine, types

york_username = "ggotoo"
mmai_db_secret = {
    "database": "ggotoo_db",
    "drivername": "mssql+pyodbc",
    "host": "mmai2024-ms-sql-server.c1oick8a8ywa.ca-central-1.rds.amazonaws.com",
    "port": "1433",
    "username": york_username,
    "password": "2024!Schulich",
    "driver": "ODBC Driver 18 for SQL Server",
    "trust_cert": "yes"
}

def create_db_engine(db_config):
    connection_url = engine.URL.create(
        drivername=db_config.get("drivername"),
        username=db_config.get("username"),
        password=db_config.get("password"),
        host=db_config.get("host"),
        port=db_config.get("port"),
        database=db_config.get("database"),
        query={
            "driver": db_config.get("driver"),
            "TrustServerCertificate": db_config.get("trust_cert")
        }
    )
    return create_engine(connection_url)

mssql_engine = create_db_engine(mmai_db_secret)
geolocation_df = pd.read_sql_table(table_name='geolocation_postal_codes', con=mssql_engine, schema='uploads')

api_key='335c91e08a89bc332e8b00a996fa1359'
current_weather_api = 'https://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&appid={}'
current_date = datetime.today()
current_weather = []

for _, postal_code_data in geolocation_df.iterrows():
    current_weather_url = current_weather_api.format(postal_code_data.latitude, postal_code_data.longitude, api_key)
    response = requests.get(current_weather_url)
    data = response.json()

    current_weather.append({
        'date': current_date.strftime('%Y-%m-%d'),
        'time': datetime.fromtimestamp(data['dt']).strftime('%H:%M:%S'),
        'time_queried': current_date.strftime('%H:%M:%S'),
        'longitude': postal_code_data.longitude,
        'latitude': postal_code_data.latitude,
        'postal_code': postal_code_data.postal_code,
        'city': postal_code_data.city,
        'province': postal_code_data.province,
        'weather': data['weather'][0]['main'],
        'description': data['weather'][0]['description'],
        'temperature': data['main']['temp'],
        'feels_like': data['main']['feels_like'],
        'temperature_min': data['main']['temp_min'],
        'temperature_max': data['main']['temp_max'],
        'humidity': data['main']['humidity'],
        'visibility': data['visibility'],
        'wind_speed': data['wind']['speed'],
        'wind_gust': data['wind']['gust'] if 'gust' in data['wind'].keys() else None,
        'clouds': data['clouds']['all'],
        'rain_1h': data['rain']['1h'] if 'rain' in data.keys() else None,
        'rain_3h': data['rain']['3h'] if 'rain' in data.keys() else None,
        'snow_1h': data['snow']['1h'] if 'snow' in data.keys() else None,
        'snow_3h': data['snow']['3h'] if 'snow' in data.keys() else None,
    })

current_weather_df = pd.DataFrame(current_weather)

current_weather_df['temperature'] = current_weather_df['temperature'].apply(lambda x: x - 273.15)
current_weather_df['feels_like'] = current_weather_df['feels_like'].apply(lambda x: x - 273.15)
current_weather_df['temperature_min'] = current_weather_df['temperature_min'].apply(lambda x: x - 273.15)
current_weather_df['temperature_max'] = current_weather_df['temperature_max'].apply(lambda x: x - 273.15)
current_weather_df.fillna(value=np.nan, inplace=True)


current_weather_df.to_sql(
    name = 'current_weather',
    con = mssql_engine,
    schema = 'uploads',
    if_exists = 'append',
    index  = False,
    dtype  = {
        'date': types.DATE,
        'time': types.TIME,
        'time_queried': types.TIME,
        'longitude': types.DECIMAL(10,7),
        'latitude': types.DECIMAL(10,7),
        'postal_code': types.VARCHAR(6),
        'city': types.VARCHAR(30),
        'province': types.VARCHAR(30),
        'weather': types.VARCHAR(50),
        'description': types.VARCHAR(255),
        'humidity': types.INTEGER,
        'visibility': types.INTEGER,
        'wind_speed': types.FLOAT,
        'wind_gust': types.FLOAT,
        'clouds': types.INTEGER,
        'rain_1h': types.FLOAT,
        'rain_3h': types.FLOAT,
        'snow_1h': types.FLOAT,
        'snow_3h': types.FLOAT,
        'temperature': types.DECIMAL(10,2),
        'feels_like': types.DECIMAL(10,2),
        'temperature_min': types.DECIMAL(10,2),
        'temperature_max': types.DECIMAL(10,2)
    },
    method = 'multi'
)
