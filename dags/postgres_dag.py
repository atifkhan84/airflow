from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

@dag(dag_id="postgres_dag", schedule="@daily", start_date=datetime(2026,4,22))
# Airflow handles the login behind the scenes using the Conn ID
def pipeline():

	query_task = PostgresOperator(
    		task_id='run_query',
    		postgres_conn_id='my_postgres_db',
    		sql="SELECT * FROM users;"
	)

	query_task

pipeline()
