import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import logging

# from airflow.utils.dates import days_ago

default_args = {
    "wait_for_downstream": True,
    "retry_delay": dt.timedelta(seconds=5),
    "retries": 1,
    "email": ["budnikgleb@mail.ru"],
    "email_on_failure": True,
}

dag = DAG(
    dag_id="06_daily_sheduler_templated",
    start_date=dt.datetime(2019, 1, 1),
    end_date=dt.datetime(2019, 1, 4),
    # schedule_interval="@daily",  # запуск каждый день
    # schedule_interval="0 0 * * 5-6",  # запуск каждую субботу и воскресенье,
    schedule_interval=dt.timedelta(days=1),  # запуск каждый 2-й день
    tags=["api_events"],
    default_args=default_args,
    # catchup=False,  # отключает обрабоку интервалов в прошлом
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "curl -o /data/events_api/events_{{ds}}.json http://192.168.0.124:5000/events?"
        "start_date={{ds}}"
        "&end_date={{next_ds}}"
    ),
    dag=dag,
)


def _calculate_stats(**context):
    print("рачитаем статистику по событиям")
    logging.info("{{ds}}")
    logging.info("{{next_ds}}")
    input_path = context["templates_dict"]["input_path"]
    output_path = context["templates_dict"]["output_path"]

    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index()
    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)


calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    templates_dict={
        "input_path": "/data/events_api/events_{{ds}}.json",
        "output_path": "/data/events_api/stats{{ds}}_{{next_ds}}.csv",
    },
    dag=dag,
)


fetch_events >> calculate_stats
