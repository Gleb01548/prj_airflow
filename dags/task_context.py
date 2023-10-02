from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="chapter4_print_context", start_date=days_ago(3), schedule_interval="@daily"
)


def _print_context(**kwargs):
    print(kwargs)


print_context = PythonOperator(
    task_id="print_context", python_callable=_print_context, dag=dag
)
# 2878