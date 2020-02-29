import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
# from telegram_plugin.operators.telegram_operator import notify_on_failure
# from telegram_plugin.operators.telegram_operator import TelegramOperator
# from support import notify_on_failure
# from customize.telegram_plugin.operators.telegram_operator import notify_on_failure


# Default Arguments
DEFAULT_DAG_ARGS = {
    'owner': 'linkaja_dataeng',
    'depends_on_past': False,
    'start_date': datetime.datetime(2020, 1, 16),
    'email': ['rio_h_pangihutan@linkaja.id'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
}

# Instantiate DAG
dag = DAG(
    'notify_failure_telegram',
    schedule_interval='@daily',
    on_failure_callback=notify_on_failure,
    default_args=DEFAULT_DAG_ARGS
)

# Define Tasks
task_start = BashOperator(
    task_id='start_task',
    bash_command='echo start',
    dag=dag
)

task_finish = BashOperator(
    task_id='finish_task',
    bash_command='echo finish',
    dag=dag
)

task_that_fails = BashOperator(
    task_id='fail_task',
    bash_command='exit 1',
    #on_failure_callback=notify_on_failure,
    provide_context=True,
    dag=dag)


# Set Task Dependencies
task_start >> task_that_fails >> task_finish
