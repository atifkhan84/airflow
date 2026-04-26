from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook # New Import
from pendulum import datetime,duration
import requests

CITIES = {
    "New_York": {"lat": 40.71, "lon": -74.00},
    "London": {"lat": 51.50, "lon": -0.12},
    "Delhi": {"lat":28.68, "lon":77.07},
    "Hyderabad":{"lat":17.38, "lon":78.49}
}

@dag(
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=['weather_postgres'],
    default_args={
                    'retries': 3,
                    'retry_delay': duration(minutes=3)
                }
)
def weather_to_postgres():

    @task
    def fetch_weather(city: str, coords: dict):
        url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&current_weather=true"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()['current_weather']

    @task
    def load_to_postgres(city: str, weather_data: dict):
        """Uses PostgresHook to insert data"""
        # 1. Create the Hook
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # 2. Define the SQL (using %s for safety against SQL injection)
        insert_sql = """
            INSERT INTO weather_table (city, temperature, windspeed, timestamp)
            VALUES (%s, %s, %s, %s);
        """
        
        # 3. Execute
        pg_hook.run(insert_sql, parameters=(
            city, 
            weather_data['temperature'], 
            weather_data['windspeed'], 
            weather_data['time']
        ))

    # Orchestration logic
    for city, coords in CITIES.items():
        data = fetch_weather(city, coords)
        load_to_postgres(city, data)

weather_to_postgres()
