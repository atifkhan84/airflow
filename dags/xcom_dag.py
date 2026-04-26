from airflow.sdk import DAG
from datetime import datetime
from airflow.providers.standard.operators.python import PythonOperator

dag=DAG(dag_id="xcom_prac",
          start_date=datetime(2026, 4, 22),
        schedule="0 0 * * *",
        catchup=False
        )

def student_csv(**context):
        print(context)
        ti = context["ti"]
        ti.xcom_push("output_path", "/airflow/output_path/csv/student")
        print("Student data processed")

def school_csv(**context):
        ti = context["ti"]
        ti.xcom_push(key="output_path", value="airflow/output_path/csv/school")
        print("School data processed")

def merge_school_student(**context):
	ti=context['ti']
	value = ti.xcom_pull(task_ids=["ingest_student_csv", "ingest_school_csv"], key="output_path")
	print(value)

task1 = PythonOperator(task_id = "ingest_student_csv", dag = dag, python_callable=student_csv)
task2 = PythonOperator(task_id = "ingest_school_csv", dag = dag, python_callable=school_csv)
task3 = PythonOperator(task_id = "merge_school_student_data", dag=dag, python_callable=merge_school_student)

[task1, task2] >> task3
