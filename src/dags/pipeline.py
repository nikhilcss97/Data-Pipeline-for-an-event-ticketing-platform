from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator

PATH_TO_REPO_SRC = "/home/nikhil/toptal/Nikhil-Gupta2-2/src"
INGESTION_BASE_PATH = f"{PATH_TO_REPO_SRC}/sql_scripts/ingestion/"
TRANSFORMATION_BASE_PATH = f"{PATH_TO_REPO_SRC}/sql_scripts/transformation/"
EXECUTE_SQL_PATH = f"{PATH_TO_REPO_SRC}/helpers/execute_sql.py"

# sys.path.insert(0, f'{PATH_TO_REPO_SRC}')

# from src.python_scripts.execute_sql import execute_ingestion_sql_stmt

import snowflake.connector as sf

ist_time_zone = pendulum.timezone("Asia/Calcutta")


def execute_ingestion_sql_stmt(filepath, account, database, password, schema, username, warehouse):
    account_name = account
    username = username
    password = password
    warehouse_name = warehouse
    database_name = database
    schema_name = schema
    print("RUN DETAILS",
          account_name + "," + username + "," + password + "," + warehouse_name + "," + database_name + ","
          + schema_name)

    try:
        sf_connection = sf.connect(
            user=username,
            password=password,
            account=account_name,
            warehouse=warehouse_name,
            database=database_name,
            schema=schema_name
        )
        sfq = sf_connection.cursor()
        print("EXECUTING FILE: ", filepath)
        sql_file = filepath
        sql_file_r = open(sql_file)
        sql_file_s = sql_file_r.read()

        sql_strings = sql_file_s.split(';')

        print("SQL STRINGS: ", sql_strings)
        for sql in sql_strings:
            sql = sql.strip()
            if sql != '\n' and sql != "":
                print("Running SQL STATEMENT- " + sql)
                sfq.execute(sql)

        res_tuple = sfq.fetchall()[0]
        count = res_tuple[0]
        sfq.close()
        sf_connection.close()

        print('END OF SQL SCRIPTS')
        print("DataType of count variable", type(count))
        return count
    except Exception as e:
        print(e)
        print('Connection failed. Check credentials')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['nikhil.rulez18@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

account = Variable.get('account')
database = Variable.get('database')
password = Variable.get('password')
schema = Variable.get('schema')
username = Variable.get('username')
warehouse = Variable.get('warehouse')


def check_data_from_s3(path_to_ingestion_sql_script, table):
    row_count = execute_ingestion_sql_stmt(path_to_ingestion_sql_script, account, database, password, schema, username,
                                           warehouse)
    print("Inside DAG, ROW COUNT: ", row_count)
    return f'move_{table}_to_raw_layer'
    # if row_count > 0:
    #     pass
    # else:
    #     return f'no_{table}'


"""
    1. Get data from DB into Data Lake
    2. Get CSV Data into Staging CSV Data Table
    3. Get XML Data into Staging XML Data Table
    4. Get DB Tables into respective Staging Tables
    5. Update all the RAW Layer Tables using the MERGE INTO logic.
        NOTE: Take care of conflicting rows
    6. Finally update the Analytical Layer Sales table using the raw layer and staging layer tables
    7. Delete data from all staging tables
"""

with DAG(
        'main_pipeline',
        default_args=default_args,
        description='Get data from Data Lake into Snowflake',
        schedule_interval="0 5 * * *",
        start_date=datetime(2022, 10, 9, tzinfo=ist_time_zone),
        catchup=False,
) as dag:
    branch_csv_data = BranchPythonOperator(
        task_id='load_csv_data_from_s3',
        python_callable=check_data_from_s3,
        do_xcom_push=False,
        op_kwargs={
            "path_to_ingestion_sql_script":
                f"{INGESTION_BASE_PATH}ingest_csv_data.sql",
            "table": "csv_data"
        }
    )
    branch_xml_data = BranchPythonOperator(
        task_id='load_xml_data_from_s3',
        python_callable=check_data_from_s3,
        do_xcom_push=False,
        op_kwargs={
            "path_to_ingestion_sql_script":
                f"{INGESTION_BASE_PATH}ingest_xml_data.sql",
            "table": "xml_data"
        }
    )
    branch_event_data = BranchPythonOperator(
        task_id='load_event_data_from_s3',
        python_callable=check_data_from_s3,
        do_xcom_push=False,
        op_kwargs={
            "path_to_ingestion_sql_script":
                f"{INGESTION_BASE_PATH}ingest_event_data.sql",
            "table": "event_data"
        }
    )
    branch_organizer_data = BranchPythonOperator(
        task_id='load_organizer_data_from_s3',
        python_callable=check_data_from_s3,
        do_xcom_push=False,
        op_kwargs={
            "path_to_ingestion_sql_script":
                f"{INGESTION_BASE_PATH}ingest_organizer_data.sql",
            "table": "organizer_data"
        }
    )
    branch_reseller_data = BranchPythonOperator(
        task_id='load_reseller_data_from_s3',
        python_callable=check_data_from_s3,
        do_xcom_push=False,
        op_kwargs={
            "path_to_ingestion_sql_script":
                f"{INGESTION_BASE_PATH}ingest_reseller_data.sql",
            "table": "reseller_data"
        }
    )
    branch_transaction_data = BranchPythonOperator(
        task_id='load_transaction_data_from_s3',
        python_callable=check_data_from_s3,
        do_xcom_push=False,
        op_kwargs={
            "path_to_ingestion_sql_script":
                f"{INGESTION_BASE_PATH}ingest_transaction_data.sql",
            "table": "transaction_data"
        }
    )
    branch_commission_data = BranchPythonOperator(
        task_id='load_commission_data_from_s3',
        python_callable=check_data_from_s3,
        do_xcom_push=False,
        op_kwargs={
            "path_to_ingestion_sql_script":
                f"{INGESTION_BASE_PATH}ingest_commission_data.sql",
            "table": "commission_data"
        }
    )

    transform_csv_data = BashOperator(
        task_id='move_csv_data_to_raw_layer',
        bash_command=f'python3 {EXECUTE_SQL_PATH} '
                     f'{TRANSFORMATION_BASE_PATH}transform_csv_data.sql '
                     f'{account} {database} {password} {schema} {username} {warehouse} '
    )
    transform_xml_data = BashOperator(
        task_id='move_xml_data_to_raw_layer',
        bash_command=f'python3 {EXECUTE_SQL_PATH} '
                     f'{TRANSFORMATION_BASE_PATH}transform_xml_data.sql '
                     f'{account} {database} {password} {schema} {username} {warehouse} '
    )
    transform_event_data = BashOperator(
        task_id='move_event_data_to_raw_layer',
        bash_command=f'python3 {EXECUTE_SQL_PATH} '
                     f'{TRANSFORMATION_BASE_PATH}transform_event_data.sql '
                     f'{account} {database} {password} {schema} {username} {warehouse} '
    )
    transform_organizer_data = BashOperator(
        task_id='move_organizer_data_to_raw_layer',
        bash_command=f'python3 {EXECUTE_SQL_PATH} '
                     f'{TRANSFORMATION_BASE_PATH}transform_organizer_data.sql '
                     f'{account} {database} {password} {schema} {username} {warehouse} '
    )
    transform_reseller_data = BashOperator(
        task_id='move_reseller_data_to_raw_layer',
        bash_command=f'python3 {EXECUTE_SQL_PATH} '
                     f'{TRANSFORMATION_BASE_PATH}transform_reseller_data.sql '
                     f'{account} {database} {password} {schema} {username} {warehouse} '
    )
    transform_transaction_data = BashOperator(
        task_id='move_transaction_data_to_raw_layer',
        bash_command=f'python3 {EXECUTE_SQL_PATH} '
                     f'{TRANSFORMATION_BASE_PATH}transform_transaction_data.sql '
                     f'{account} {database} {password} {schema} {username} {warehouse} '
    )
    transform_commission_data = BashOperator(
        task_id='move_commission_data_to_raw_layer',
        bash_command=f'python3 {EXECUTE_SQL_PATH} '
                     f'{TRANSFORMATION_BASE_PATH}transform_commission_data.sql '
                     f'{account} {database} {password} {schema} {username} {warehouse} '
    )
    fill_sales_data = BashOperator(
        task_id='fill_sales',
        bash_command=f'python3 {EXECUTE_SQL_PATH} '
                     f'{TRANSFORMATION_BASE_PATH}transform_sales.sql '
                     f'{account} {database} {password} {schema} {username} {warehouse} '
    )
    # dummy_task_raw_csv = DummyOperator(task_id='no_csv_data')
    # dummy_task_raw_xml = DummyOperator(task_id='no_xml_data')
    # dummy_task_raw_event = DummyOperator(task_id='no_event_data')
    # dummy_task_raw_organizer = DummyOperator(task_id='no_organizer_data')
    # dummy_task_raw_reseller = DummyOperator(task_id='no_reseller_data')
    # dummy_task_raw_transaction = DummyOperator(task_id='no_transaction_data')
    # dummy_task_raw_commission = DummyOperator(task_id='no_commission_data')
    dummy_task_end = DummyOperator(
        task_id='END_OF_DATA_LOAD',
        trigger_rule='none_failed_or_skipped'
    )
    dummy_task_start = DummyOperator(task_id='START_OF_DATA_LOAD')

dummy_task_start >> \
branch_csv_data >> [transform_csv_data] >> \
branch_xml_data >> [transform_xml_data] >> \
branch_event_data >> [transform_event_data] >> \
branch_organizer_data >> [transform_organizer_data] >> \
branch_reseller_data >> [transform_reseller_data] >> \
branch_transaction_data >> [transform_transaction_data] >> \
branch_commission_data >> [transform_commission_data] >> \
fill_sales_data >> dummy_task_end
