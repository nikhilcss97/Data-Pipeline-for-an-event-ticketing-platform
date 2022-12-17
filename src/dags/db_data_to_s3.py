from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['nikhil.rulez18@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

TEMP_DATA_PATH = "/home/nikhil/toptal/temp_data"
ist_time_zone = pendulum.timezone("Asia/Calcutta")


def postgres_to_s3(table, date_column="created_date", **kwargs):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    print("PREVIOUS EXECUTION DATE: ", kwargs['prev_ds'])
    sql = f"COPY (SELECT * FROM {table} WHERE {date_column} >= '{kwargs['prev_ds']}') TO STDOUT WITH CSV DELIMITER ','"
    filename = f"{table}s_{kwargs['prev_ds']}.csv"

    # with open(f"{TEMP_DATA_PATH}/{filename}", "w") as file:
    with NamedTemporaryFile(mode="wb", suffix=filename) as f:
        cursor.copy_expert(sql, f)
        f.flush()
        cursor.close()
        conn.close()
        print(f"SAVED {table} in {table}_data.csv file.")

        s3_hook = S3Hook(aws_conn_id="aws_default_s3")
        s3_hook.load_file(
            filename=f.name,
            key=f"db_data/{table}s/{kwargs['prev_ds']}/{filename}",
            bucket_name="ticketing-platform-toptal",
            replace=True
        )
        print(f"{table} Data Loaded into S3")


with DAG(
        'db_data_to_s3',
        default_args=default_args,
        description='Put data from postgres DB into S3 bucket',
        schedule_interval="1 0 * * *",
        start_date=datetime(2022, 10, 9, tzinfo=ist_time_zone),
        catchup=False,
) as dag:
    get_event_data = PythonOperator(
        task_id="event_postgres_to_s3",
        python_callable=postgres_to_s3,
        op_kwargs={
            "table": "event",
        },
        provide_context=True
    )
    get_organizer_data = PythonOperator(
        task_id="organizer_postgres_to_s3",
        python_callable=postgres_to_s3,
        op_kwargs={
            "table": "organizer",
        },
        provide_context=True
    )
    get_reseller_data = PythonOperator(
        task_id="reseller_postgres_to_s3",
        python_callable=postgres_to_s3,
        op_kwargs={
            "table": "reseller",
        },
        provide_context=True
    )
    get_commission_data = PythonOperator(
        task_id="commission_postgres_to_s3",
        python_callable=postgres_to_s3,
        op_kwargs={
            "table": "commission",
        },
        provide_context=True
    )
    get_transaction_data = PythonOperator(
        task_id="transaction_postgres_to_s3",
        python_callable=postgres_to_s3,
        op_kwargs={
            "table": "transaction",
            "date_column": "transaction_date"
        },
        provide_context=True
    )
    dummy_task_start = DummyOperator(task_id='START_OF_DATA_LOAD')
    dummy_task_end = DummyOperator(
        task_id='END_OF_DATA_LOAD',
        trigger_rule='none_failed_or_skipped'
    )

    dummy_task_start >> get_organizer_data >> get_reseller_data >> get_event_data >> get_commission_data >> \
    get_transaction_data >> dummy_task_end
