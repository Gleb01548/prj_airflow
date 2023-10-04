import os
import pathlib
from urllib import request

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {"wait_for_downstream": True}
dir_path = "/data/wikipedia/python_operator"
pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)

file_load_path = os.path.join(dir_path, "wikipageviews_{{ execution_date }}.gz")


dag = DAG(
    dag_id="stocksense_python",
    start_date=days_ago(2),
    end_date=days_ago(1),
    schedule_interval="@hourly",
    tags=["wikipedia_views"],
    default_args=default_args,
    template_searchpath=dir_path,  # путь для поиска sql-файлов, может быть список
)


def _get_data(output_path, data_interval_start, data_interval_end, **context):
    year, month, day, hour, *_ = context["execution_date"].timetuple()
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )

    request.urlretrieve(url, output_path)


get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={"output_path": file_load_path},
    dag=dag,
)


extract_gz = BashOperator(
    task_id="extract_gz", bash_command=f"gunzip --force {file_load_path}", dag=dag
)


def _fetch_pageviews(pagenames, dir_path, file_load_path, execution_date):
    result = dict.fromkeys(pagenames, 0)
    with open(file_load_path, "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts

    with open(os.path.join(dir_path, "postgres_query.sql"), "w") as f:
        for pagename, pageviewcount in result.items():
            f.write(
                "INSERT INTO pageview_counts (pagename, pageviewcount, datetime) VALUES ("
                f"'{pagename}', '{pageviewcount}', '{execution_date}'"
                ");\n"
            )


fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={
        "pagenames": {"Google", "Amazon", "Apple", "Microsoft", "Facebook"},
        "dir_path": dir_path,
        "file_load_path": file_load_path.removesuffix(".gz"),
    },
    dag=dag,
)

write_to_postgres = PostgresOperator(
    task_id="write_to_postgres",
    postgres_conn_id="posrgres_work",
    sql="postgres_query.sql",
    dag=dag,
)


get_data >> extract_gz >> fetch_pageviews >> write_to_postgres
