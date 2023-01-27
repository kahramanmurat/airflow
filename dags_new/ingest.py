import os
from airflow import DAG
from datetime import datetime

from ingest_script import ingest_callable

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

local_workflow = DAG(
    "LocalIngestionDag", schedule_interval="0 6 2 * *", start_date=datetime(2021, 1, 1)
)

url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"

URL_PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green"
URL_TEMPLATE = (
    URL_PREFIX + "/green_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv.gz"
)
OUTPUT_FILE_TEMPLATE = (
    AIRFLOW_HOME + "/output_{{ execution_date.strftime('%Y-%m') }}.csv"
)

TABLE_NAME_TEMPLATE = "green_taxi_{{ execution_date.strftime('%Y_%m') }}"


PG_HOST = os.getenv("PG_HOST")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")

with local_workflow:
    wget_task = BashOperator(
        task_id="wget",
        bash_command=f"curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}",
    )

    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=ingest_callable,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DB,
            table_name=TABLE_NAME_TEMPLATE,
            csv_file=OUTPUT_FILE_TEMPLATE,
        ),
    )

    wget_task >> ingest_task
