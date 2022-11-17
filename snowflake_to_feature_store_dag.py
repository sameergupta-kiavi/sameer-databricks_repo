import json
from airflow import DAG
from datetime import datetime, timedelta
from contrib.operators.kv_snowflake_operator import KVSnowflakeOperator
from contrib.hooks.kv_databricks_hook import KVDatabricksHook
from tasks.borrower_over_estimation.config import Config
from tasks.borrower_over_estimation.snowflake_to_s3 import snowflake_to_s3
from tasks.borrower_over_estimation.snowflake_data_transformation import (
    snowflake_data_transformation,
)


config = Config()
databricks_hook = KVDatabricksHook(config.DATABRICKS_CONNECTION_ID)

# Default arguments for Dag
default_args = {
    "start_date": datetime(2022, 11, 16, 12, 00),
    "schedule_interval": timedelta(days=1),
    "max_active_runs": 1,
    "concurrency": 1,
}

# Dag with dag_id = snowflake_to_feature_store
dag = DAG(
    "snowflake_to_feature_store_borrower_information",
    default_args=default_args,
)

# Data Transformation
task1 = KVSnowflakeOperator(
    task_id="data_transformation",
    dag=dag,
    snowflake_conn_id=config.SNOWFLAKE_CONNECTION_NAME,
    sql=snowflake_data_transformation(config.SCHEMA),
)

# Snowflake to S3
task2 = KVSnowflakeOperator(
    task_id="snowflake_to_s3",
    dag=dag,
    snowflake_conn_id=config.SNOWFLAKE_CONNECTION_NAME,
    sql=snowflake_to_s3(
        config.S3_LOCATION_PREFIX, config.DATABASE, config.SCHEMA, config.TABLE
    ),
)

# S3 to Delta lake
# # notebook name: DATA - 3934 - Data Transform from csv to Delta format
task3 = databricks_hook.get_run_operator(
    task_id="s3_to_delta",
    dag=dag,
    job_name="s3_to_delta",
    notebook_params=config.S3_TO_DELTA_NOTEBOOK_PARAMS,
)

# Populate Feature Store
# notebook name: DATA - 3934 - Populate Feature Store
task4 = databricks_hook.get_run_operator(
    task_id="populate_feature_store",
    dag=dag,
    job_name="populate_feature_store",
    notebook_params=config.FS_NOTEBOOK_PARAMS,
)

task1 >> task2 >> task3 >> task4
