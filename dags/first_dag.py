from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
import random

args = {
    'owner' : 'tinkujo',
    'start_date' : days_ago(1)
}

dag = DAG(dag_id = '101_tinku_dag', default_args = args, schedule_interval = None)

def run_this_func1(**context):
    rand = random.randint(1, 100)
    context['ti'].xcom_push(key='random_val', value = rand)
    print("Hi first func")

def run_this_func2(**context):
    received_value = context['ti'].xcom_pull(key='random_val')
    print(f"received value is {received_value}, Func 2")

def run_this_func3(**context):
    received_value = context['ti'].xcom_pull(key='random_val')
    print(f"received value is {received_value}, Func 3")

def branch_func(**context):
    if context['ti'].xcom_pull(key='random_val') > 50:
        return 'func2'
    else:
        return 'func3'

with dag:
    run_this_last = PythonOperator(
        task_id='func1',
        python_callable = run_this_func1,
        provide_context= True
    )

    branch_op = BranchPythonOperator(
        task_id='branch_task',
        provide_context=True,
        python_callable = branch_func
    )

    run_this_last2 = PythonOperator(
        task_id='func2',
        python_callable = run_this_func2,
        provide_context= True
    )

    run_this_last3 = PythonOperator(
        task_id='func3',
        python_callable = run_this_func3,
        provide_context= True
    )

run_this_last >> branch_op >> [ run_this_last2, run_this_last3]