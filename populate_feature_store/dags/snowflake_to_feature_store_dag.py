from airflow import DAG
from datetime import datetime, timedelta
from util.airflow_utils import owner_default_args
from contrib.operators.kv_snowflake_operator import KVSnowflakeOperator
from contrib.operators.kv_databricks_operators import KVDatabricksRunNowOperator
from tasks.borrower_over_estimation.training_config import Config as TrainingConfig
from tasks.borrower_over_estimation.snowflake_to_s3 import snowflake_to_s3
from tasks.borrower_over_estimation.snowflake_data_transformation import (
    snowflake_historical_data_transformation,
)


config = TrainingConfig()

dag_owners = ['matias.marenchino']
owner_names, owner_emails = owner_default_args(dag_owners)

# Default arguments for Dag
default_args = {
    "owner": owner_names,
    "email": owner_emails,
    "start_date": datetime.strptime("2022-12-14 12:00:00", "%Y-%m-%d %H:%M:%S"),
    "catchup": False,
    "schedule_interval": "0 12 * * 1-5",
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "queue": "analytics",
    "execution_timeout": timedelta(minutes=120),
    "max_active_runs": 1,
    "concurrency": 1,
}


# Dag with dag_id = snowflake_to_feature_store
dag = DAG(
    "snowflake_to_feature_store_borrower_information_historical",
    default_args=default_args,
)

# Data Transformation
task1 = KVSnowflakeOperator(
    task_id="data_transformation",
    dag=dag,
    snowflake_conn_id=config.SNOWFLAKE_CONNECTION_NAME,
    sql=snowflake_historical_data_transformation(config.DATABASE, config.SCHEMA, config.TABLE),
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
task3 = KVDatabricksRunNowOperator(
    dag=dag,
    task_id="s3_to_delta",
    databricks_conn_id=config.DATABRICKS_CONNECTION_ID,
    job_name="s3_to_delta__ingestion",
    notebook_params=config.S3_TO_DELTA_NOTEBOOK_PARAMS,
)

# Populate Feature Store
task4 = KVDatabricksRunNowOperator(
    dag=dag,
    task_id="populate_feature_store",
    databricks_conn_id=config.DATABRICKS_CONNECTION_ID,
    job_name="feature_store__ingestion",
    notebook_params=config.FS_NOTEBOOK_PARAMS,
)

task1 >> task2 >> task3 >> task4
