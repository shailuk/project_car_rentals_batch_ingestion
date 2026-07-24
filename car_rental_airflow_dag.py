from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator 
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.param import Param
from airflow.hooks.base import BaseHook

# Fetch snowflake connection details programmatically from Airflow UI Connection Store
sf_conn = BaseHook.get_connection('snowflake_conn')
extra_params = sf_conn.extra_dejson

# Safely extract account identifier (from Extra fields or host attribute)
sf_account = extra_params.get('account') or sf_conn.host or ''

# Construct the full Snowflake URL safely
if sf_account and not sf_account.endswith('.snowflakecomputing.com'):
    sf_url = f"{sf_account}.snowflakecomputing.com"
else:
    sf_url = sf_account

# Parse extra JSON parameters
sf_warehouse = extra_params.get('warehouse', 'COMPUTE_WH')
sf_role = extra_params.get('role', 'ACCOUNTADMIN')
sf_database = extra_params.get('database') or sf_conn.schema or 'CAR_RENTALS'
sf_schema = sf_conn.schema or 'PUBLIC'

# Default settings for all tasks in this DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# Define Jinja snippet for dynamic execution date resolution
EXEC_DATE_MACRO = "{{ params.execution_date if params.execution_date != 'NA' else ds_nodash }}"

with DAG(
    'car_rental_data_pipeline',
    default_args=default_args,
    description='Car Rental Data Pipeline via AWS EMR Serverless',
    schedule=None,
    start_date=datetime(2026, 7, 9),
    catchup=False,
    tags=['dev'],
    params={
        'execution_date': Param(default='NA', type='string', description='Execution date in yyyymmdd format'),
    } 
) as dag:

    # Task 1: Invalidate existing records in Snowflake (SCD Type 2)
    merge_customer_dim = SQLExecuteQueryOperator(
        task_id='merge_customer_dim',
        conn_id='snowflake_conn',
        sql=f"""
            MERGE INTO car_rentals.PUBLIC.customer_dim AS target
            USING (
                SELECT
                    $1 AS customer_id,
                    $2 AS name,
                    $3 AS email,
                    $4 AS phone
                FROM @car_rentals.PUBLIC.car_rental_data_stg/customers_{EXEC_DATE_MACRO}.csv (FILE_FORMAT => 'csv_format') 
            ) AS source
            ON target.customer_id = source.customer_id AND target.is_current = TRUE
            WHEN MATCHED AND (
                target.name != source.name OR
                target.email != source.email OR
                target.phone != source.phone
            ) THEN
                UPDATE SET target.end_date = CURRENT_TIMESTAMP(), target.is_current = FALSE;
        """,
    )

    # Task 2: Insert new/updated customer records
    insert_customer_dim = SQLExecuteQueryOperator(
        task_id='insert_customer_dim',
        conn_id='snowflake_conn',
        sql=f"""
            INSERT INTO car_rentals.PUBLIC.customer_dim (customer_id, name, email, phone, effective_date, end_date, is_current)
            SELECT
                $1 AS customer_id,
                $2 AS name,
                $3 AS email,
                $4 AS phone,
                CURRENT_TIMESTAMP() AS effective_date,
                NULL AS end_date,
                TRUE AS is_current
            FROM @car_rentals.PUBLIC.car_rental_data_stg/customers_{EXEC_DATE_MACRO}.csv (FILE_FORMAT => 'csv_format');
        """,
    )

    # AWS EMR Serverless Constants
    EMR_SERVERLESS_APP_ID = '00g7esj8m4aqa809'
    EMR_SERVERLESS_ROLE_ARN = 'arn:aws:iam::060662064877:role/EMRServerlessExecutionRole'

    PYSPARK_JOB_FILE_PATH = 's3://rootshailesh1/car_rental_data/py_file/spark_job.py'
    SNOWFLAKE_JAR_1 = 's3://rootshailesh1/car_rental_data/jars/spark-snowflake_2.12-2.12.0-spark_3.4.jar'
    SNOWFLAKE_JDBC_JAR = 's3://rootshailesh1/car_rental_data/jars/snowflake-jdbc-3.13.30.jar'

    # Task 3: Submit PySpark job to AWS EMR Serverless
    submit_pyspark_job = EmrServerlessStartJobOperator(
        task_id='submit_pyspark_job',
        application_id=EMR_SERVERLESS_APP_ID,
        execution_role_arn=EMR_SERVERLESS_ROLE_ARN,
        aws_conn_id='aws_default',
        job_driver={
            "sparkSubmit": {
                "entryPoint": PYSPARK_JOB_FILE_PATH,
                "entryPointArguments": [
                    "--date", EXEC_DATE_MACRO,
                    "--sf_url", sf_url,
                    "--sf_user", sf_conn.login,
                    "--sf_password", sf_conn.password,
                    "--sf_database", sf_database,
                    "--sf_schema", sf_schema,
                    "--sf_warehouse", sf_warehouse,
                    "--sf_role", sf_role
                ],
                "sparkSubmitParameters": f"--jars {SNOWFLAKE_JAR_1},{SNOWFLAKE_JDBC_JAR} --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"
            }
        },
    )

    # Pipeline Dependencies
    merge_customer_dim >> insert_customer_dim >> submit_pyspark_job