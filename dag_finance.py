from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os


def start_venv():
    os.system('. ~/PycharmProjects/banking_venv/bin/activate')


def git_pull():
    os.system('git pull ~/PycharmProjects/banking_app')


def get_banks_data():
    os.system('python3 main_banking_app.py --source_type banks --mode_database append ' +\
              '--start_date ' + str(datetime.date(datetime.now())) + 'T00:00:00 ' +\
              '--end_date ' + str(datetime.date(datetime.now())) + 'T23:59:59')


dag = DAG('finance_data', description='Get data from finance source and write to database',
          schedule_interval='59 23 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)


start_venv_operator = PythonOperator(task_id='start_venv', python_callable=start_venv(), dag=dag)

git_pull_operator = PythonOperator(task_id='pull_from_git', python_callable=git_pull(), dag=dag)

get_banks_data_operator = PythonOperator(task_id='get_data_from_banks', python_callable=get_banks_data(), dag=dag)

start_venv_operator >> git_pull_operator
git_pull_operator >> get_banks_data_operator
