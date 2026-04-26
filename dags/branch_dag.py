from airflow.sdk import DAG, TriggerRule
import random
from datetime import datetime
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator

dag = DAG(dag_id="branch_dag",
	start_date=datetime(2026,4,15),
	schedule="0 0 * * *",
	catchup=False)

def random_value():
	val = random.randint(1,10)
	print(val)
	if val%2 == 0:	
		return 'This'
	else:
		return 'That'

def this_is_this():
	print("Inside this")

def this_is_that():
	print("Inside that")



start = EmptyOperator(task_id="start", dag=dag)

decide = BranchPythonOperator(task_id="decide", dag=dag, python_callable=random_value)

task1 = BashOperator(task_id="Hello",
			bash_command="echo hello!",
			trigger_rule=TriggerRule.NONE_FAILED)

task2 = PythonOperator(task_id="This",
			dag=dag,
			python_callable=this_is_this)

task3 = PythonOperator(task_id="That",
			dag=dag,
			python_callable=this_is_that)

end = EmptyOperator(task_id="end", dag=dag)



start >> decide >> [task3, task2] >> task1 >> end


