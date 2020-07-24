import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.mongo_hook import MongoHook

from ynabdashetl.budgets import BudgetJob

log = logging.getLogger('airflowtools.dags.ynab.budgets')


def run_budget_job():
    """
    Runs YNAB budget API job
    """
    job = BudgetJob()
    job.run()


def load_budget_into_mongo(**context):
    """
    Loads budget data into Mongo
    """
    db = 'ynab'
    collection = 'budgets'

    hook = MongoHook(conn_id='ynab_budgets')

    budget_data = context['task_instance'].xcom_pull(task_ids='run_budget_job')
    log.info('Loading data into Mongo')
    hook.insert_one(collection, budget_data, db)
    log.info(f'Loaded budget data into {collection} successfully')
    hook.close_conn()
    return


default_args = {
    'owner': 'bljustice',
    'depends_on_past': False,
    'retries': 0,
}

dag = DAG(
    dag_id='ynab_budgets_dag',
    default_args=default_args,
    start_date=datetime(2020, 6, 20),
    schedule_interval='@daily'
)

task1 = PythonOperator(
    task_id='run_budget_job',
    python_callable=run_budget_job,
    dag=dag
)

task2 = PythonOperator(
    task_id='load_budget_into_mongo',
    provide_context=True,
    python_callable=load_budget_into_mongo,
    dag=dag
)

# Set task1 "upstream" of task2, i.e. task1 must be completed
# before task2 can be started.
task1 >> task2
