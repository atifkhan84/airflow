from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pendulum import datetime, duration
import requests
import json

CITIES = [
    {"name": "New_York", "lat": 40.71, "lon": -74.00},
    {"name": "London", "lat": 51.50, "lon": -0.12},
    {"name": "Tokyo", "lat": 35.68, "lon": 139.69},
    {"name": "Berlin", "lat": 52.52, "lon": 13.41},
    {"name": "Paris", "lat": 48.85, "lon": 2.35}
]

@dag(
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False
)
def weather_batch_to_s3():

    @task(retries=3, retry_delay=duration(seconds=30), retry_exponential_backoff=True)
    def fetch_all_weather(cities_list):
        """Fetches data for all cities and returns a single list"""
        all_data = []
        for city in cities_list:
            url = f"https://api.open-meteo.com/v1/forecast?latitude={city['lat']}&longitude={city['lon']}&current_weather=true"
            res = requests.get(url)
            res.raise_for_status()
            
            # Add city name to the record so we know which is which
            record = res.json()['current_weather']
            record['city'] = city['name']
            all_data.append(record)
            
        return all_data

    @task
    def upload_batch_to_s3(batch_data: list, ds=None, logical_date=None):
        s3 = S3Hook(aws_conn_id='aws_default')
        
        # Use the execution date to name the folder (Partitioning)
        # Result: weather_data/2026-04-26/hourly_batch_12.json
        file_name = f"weather_data/{ds}/batch_{logical_date.hour}.json"
        
        s3.load_string(
            string_data=json.dumps(batch_data),
            key=file_name,
            bucket_name="atifweatherdata",
            replace=True
        )

    # Execution flow
    weather_list = fetch_all_weather(CITIES)
    upload_batch_to_s3(weather_list)

weather_batch_to_s3()
