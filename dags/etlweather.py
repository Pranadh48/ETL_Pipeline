from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

import pendulum

# Latitude and longitude for London
LATITUDE = '51.5074'
LONGITUDE = '-0.1278'

POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2026, 11, 20, tz="UTC")
}


# DAG Definition
with DAG(
    dag_id='weather_etl_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:

    # EXTRACT
    @task()
    def extract_weather_data():

        http_hook = HttpHook(
            http_conn_id=API_CONN_ID,
            method='GET'
        )

        endpoint = (
            f'/v1/forecast?latitude={LATITUDE}'
            f'&longitude={LONGITUDE}'
            '&current=temperature_2m,wind_speed_10m,'
            'wind_direction_10m,weather_code'
        )

        response = http_hook.run(endpoint)

        response.raise_for_status()

        return response.json()


    # TRANSFORM
    @task()
    def transform_weather_data(weather_data):

        current_weather = weather_data['current']

        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature_2m'],
            'windspeed': current_weather['wind_speed_10m'],
            'winddirection': current_weather['wind_direction_10m'],
            'weathercode': current_weather['weather_code']
        }

        return transformed_data


    # LOAD
    @task()
    def load_weather_data(transformed_data):

        pg_hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                id SERIAL PRIMARY KEY,
                latitude VARCHAR(50),
                longitude VARCHAR(50),
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Insert transformed data
        cursor.execute("""
            INSERT INTO weather_data
            (latitude, longitude, temperature,
             windspeed, winddirection, weathercode)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))

        conn.commit()

        cursor.close()
        conn.close()


    # DAG WORKFLOW

    weather_data = extract_weather_data()

    transformed_data = transform_weather_data(weather_data)

    load_weather_data(transformed_data)