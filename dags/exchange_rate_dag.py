#type: ignore
import os
import csv
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
def extract():
        response = requests.get("https://api.exchangerate-api.com/v4/latest/USD")
        data = response.json()
        uzs = data['rates']['UZS']
        print(f"1 USD = {uzs} UZS")
        return uzs
def transform(uzs):
        uzs = round(uzs, 2)
        return uzs
def load(uzs):
        with open('rates.csv','a',newline='') as file:
             writer = csv.writer(file)
             writer.writerow([datetime.now().date(),uzs])
def get_chat_ids():
    token = os.environ.get("TELEGRAM_TOKEN")
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    response = requests.get(url)
    data = response.json()
    
    chat_ids = []
    for update in data['result']:
        chat_id = update['message']['chat']['id']
        if chat_id not in chat_ids:
            chat_ids.append(chat_id)
    
    return chat_ids
def notify(uzs):
    token = os.environ.get("TELEGRAM_TOKEN")
    message = f"1 USD = {uzs} UZS"
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    
    chat_ids = get_chat_ids()  # автоматически берём всех
    for chat_id in chat_ids:
        requests.post(url, data={"chat_id": chat_id, "text": message})
with DAG(
    dag_id="exchange_rate_dag",
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
        python_callable=transform,
        op_args=[extract_task.output]
    )
    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
        op_args=[transform_task.output]
    )
    notify_task = PythonOperator(
        task_id='notify',
        python_callable=notify,
        op_args=[transform_task.output]  # передаём uzs
    )
    extract_task >> transform_task >> load_task >> notify_task