from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {"wait_for_downstream": True}

dag = DAG(
    dag_id="stocksense_bashoperator",
    start_date=days_ago(0),
    schedule_interval="@hourly",
    tags=["wikipedia_views"],
    default_args=default_args,
)

get_data = BashOperator(
    task_id="get_data",
    bash_command=(
        "curl -o /data/wikipedia/bash/wikipageviews_{{execution_date}}.gz "
        "https://dumps.wikimedia.org/other/pageviews/"
        "{{ execution_date.year }}/"
        "{{ execution_date.year }}-"
        "{{ '{:02}'.format(execution_date.month) }}/"
        "pageviews-{{ execution_date.year }}"
        "{{ '{:02}'.format(execution_date.month) }}"
        "{{ '{:02}'.format(execution_date.day) }}-"
        "{{ '{:02}'.format(execution_date.hour) }}0000.gz"
    ),
    dag=dag,
)
