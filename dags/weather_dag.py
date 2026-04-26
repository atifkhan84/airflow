from airflow.decorators import dag, task
from pendulum import datetime, duration
import requests
import json

# Define our "Data Asset" - Cities with their coordinates
CITIES = {
    "New_York": {"lat": 40.71, "lon": -74.00},
    "London": {"lat": 51.50, "lon": -0.12},
    "Tokyo": {"lat": 35.68, "lon": 139.69}
}

@dag(
    start_date=datetime(2026, 1, 1),
    schedule="@hourly", # Runs every hour
    catchup=False,
    tags=['learning_airflow', 'weather_no_key']
)
def open_meteo_etl():

    @task(retries=3, retry_delay=duration(seconds=30), retry_exponential_backoff=True)
    def fetch_weather(city_name: str, coords: dict):
        """Extract: Call the API"""
        url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&current_weather=true"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    @task
    def transform_weather(city_name: str, raw_data: dict):
        """Transform: Clean the raw JSON"""
        current = raw_data['current_weather']
        cleaned_data = {
            "city": city_name,
            "temp": current['temperature'],
            "windspeed": current['windspeed'],
            "time": current['time']
        }
        print(f"Cleaned Data for {city_name}: {cleaned_data}")
        return cleaned_data

    @task
    def mock_load(all_weather_data: list):
        """Load: In a real project, this would go to a Database or S3"""
        print("Final Batch to be loaded to Data Warehouse:")
        for record in all_weather_data:
            print(f"Inserting record: {record}")

    # Orchestration Logic
    weather_info_list = []
    for city, coords in CITIES.items():
        raw = fetch_weather(city, coords)
        cleaned = transform_weather(city, raw)
        weather_info_list.append(cleaned)
    
    mock_load(weather_info_list)

# Instantiate the DAG
open_meteo_etl()
