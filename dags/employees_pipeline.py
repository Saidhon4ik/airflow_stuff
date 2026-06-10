# type: ignore

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

# шаг 1: читаем CSV файл
def extract():
    df = pd.read_csv("/usr/local/airflow/include/employees_5000.csv")
    print(f"Extracted {len(df)} rows")
    print(df.head())

# шаг 2: считаем среднюю зарплату по департаментам
def transform():
    df = pd.read_csv("/usr/local/airflow/include/employees_5000.csv")
    result = df.groupby("department")["salary"].mean().round(2)
    print("Average salary by department:")
    print(result)

# шаг 3: выводим итоговый отчёт
def report():
    df = pd.read_csv("/usr/local/airflow/include/employees_5000.csv")
    print(f"Total employees: {len(df)}")
    print(f"Highest salary: {df['salary'].max()}")
    print(f"Lowest salary: {df['salary'].min()}")
    print(f"Average salary: {df['salary'].mean().round(2)}")

with DAG(
    dag_id="employees_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    report_task = PythonOperator(
        task_id="report",
        python_callable=report
    )

    extract_task >> transform_task >> report_task