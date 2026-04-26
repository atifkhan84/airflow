from airflow.decorators import dag, task
from datetime import datetime

@dag(start_date=datetime(2026, 1, 1), schedule="@daily", catchup=False)
def trend_watcher():
    
    @task
    def extract_data():
        return ["AI is taking over!", "Airflow 3.0 is amazing", "Market crashes"]

    @task
    def process_sentiment(headlines):
        # Imagine sentiment logic here
        return {h: "Positive" if "amazing" in h else "Neutral" for h in headlines}

    @task
    def load_to_storage(results):
        print(f"Saving to Data Lake: {results}")

    # Set dependencies using simple function calls
    data = extract_data()
    processed = process_sentiment(data)
    load_to_storage(processed)

trend_watcher()
