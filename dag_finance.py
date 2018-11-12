from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


dag = DAG('finance_data', description='Get data from finance source and write to database',
          schedule_interval='59 23 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

start_venv_operator = BashOperator(task_id='start_venv',
                                   bash_command='. ~/PycharmProjects/banking_venv/bin/activate', dag=dag)

git_pull_operator = BashOperator(task_id='pull_from_git',
                                 bash_command='cd ~/PycharmProjects/banking_app; git pull', dag=dag)

get_banks_data_operator = BashOperator(task_id='get_data_from_bank',
                                       bash_command='python3 ~/PycharmProjects/banking_app/main_banking_app.py\
                                       --source_type banks --mode_database append ' +\
                                       '--start_date ' + str(datetime.date(datetime.now())) + 'T00:00:00 ' +\
                                       '--end_date ' + str(datetime.date(datetime.now())) + 'T23:59:59', dag=dag)

start_venv_operator >> git_pull_operator
git_pull_operator >> get_banks_data_operator
