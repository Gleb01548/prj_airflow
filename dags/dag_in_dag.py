import airflow.utils.dates
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ================================================ EXAMPLE 1 =================================================

example_2_dag_1 = DAG(
    dag_id="figure_6_17_example_2_dag_1",
    start_date=airflow.utils.dates.days_ago(10),
    schedule_interval="0 0 * * *",
)
example_2_dag_2 = DAG(
    dag_id="figure_6_17_example_2_dag_2",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 0 * * *",
)
example_2_dag_3 = DAG(
    dag_id="figure_6_17_example_2_dag_3",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 0 * * *",
)
example_2_dag_4 = DAG(
    dag_id="figure_6_17_example_2_dag_4",
    start_date=airflow.utils.dates.days_ago(2),
    end_date=airflow.utils.dates.days_ago(1),
    schedule_interval=None,
)

for dag_ in [example_2_dag_1, example_2_dag_2, example_2_dag_3]:
    DummyOperator(task_id="etl", dag=dag_) >> TriggerDagRunOperator(
        task_id="trigger_dag4",
        trigger_dag_id="figure_6_17_example_2_dag_4",
        dag=dag_,
        execution_date="{{ data_interval_start }}",
        # trigger_run_id=f"manual__{'{{ data_interval_start }}'}",
    )

re = PythonOperator(
    task_id="report", dag=example_2_dag_4, python_callable=lambda: print("hello")
)

re1 = DummyOperator(task_id="re1", dag=example_2_dag_4)

re >> re1
