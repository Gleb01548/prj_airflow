import os
import pathlib
from urllib import request
import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {"wait_for_downstream": True}
dir_path = "/data/db_russian_wiki/"
pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)


load_data_script = "postgres_load_data.sql"
write_data_to_table_page_name = "to_table_page_name.sql"
write_data_to_stat_views = "to_stat_views.sql"

file_load_path = os.path.join(dir_path, "wikipageviews.gz")
postgres_load_data = os.path.join(dir_path, load_data_script)

dag = DAG(
    dag_id="russian_wikipedia",
    start_date=dt.datetime(2023, 1, 1),
    end_date=days_ago(1),
    schedule_interval="@hourly",
    tags=["russian_wikipedia"],
    default_args=default_args,
    template_searchpath=dir_path,  # путь для поиска sql-файлов, может быть список
)


def _get_data(output_path, **context):
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


def _fetch_pageviews(file_load_path, postgres_load_data, data_interval_start):
    result = {}
    with open(file_load_path, "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "ru":
                result[page_title] = view_counts

    with open(postgres_load_data, "w") as f:
        f.write(
            "insert into resource.data_views (page_name, page_view_count, datetime) values"
        )
        max_index = len(result) - 1
        for index, (pagename, pageviewcount) in enumerate(result.items()):
            symbol = ",\n" if max_index > index else ";"
            pagename = pagename.replace("'", "''")
            pagename = pagename[:1000] if len(pagename) > 1000 else pagename
            f.write(
                f"('{pagename}', '{pageviewcount}', '{data_interval_start}'){symbol}\n"
            )


fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={
        # "pagenames": {"Google", "Amazon", "Apple", "Microsoft", "Facebook"},
        "postgres_load_data": postgres_load_data,
        "file_load_path": file_load_path.removesuffix(".gz"),
    },
    dag=dag,
)

write_to_postgres = PostgresOperator(
    task_id="write_to_postgres",
    postgres_conn_id="russian_wiki",
    sql="postgres_load_data.sql",
    dag=dag,
)

write_to_table_page_name = PostgresOperator(
    task_id="write_to_table_page_name",
    postgres_conn_id="russian_wiki",
    sql=write_data_to_table_page_name,
    dag=dag,
)

write_to_stat_views = PostgresOperator(
    task_id="write_to_stat_views",
    postgres_conn_id="russian_wiki",
    sql=write_data_to_stat_views,
    dag=dag,
)


(
    get_data
    >> extract_gz
    >> fetch_pageviews
    >> write_to_postgres
    >> write_to_table_page_name
    >> write_to_stat_views
)
