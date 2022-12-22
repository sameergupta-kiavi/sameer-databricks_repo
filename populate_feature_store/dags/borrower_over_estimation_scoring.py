from airflow import DAG
from datetime import datetime, timedelta
from util.airflow_utils import owner_default_args
from contrib.operators.kv_snowflake_operator import KVSnowflakeOperator
from contrib.operators.kv_databricks_operators import KVDatabricksRunNowOperator
from tasks.borrower_over_estimation.config import Config
from tasks.borrower_over_estimation.snowflake_to_s3 import snowflake_to_s3
from tasks.borrower_over_estimation.snowflake_data_transformation import (
    snowflake_data_transformation,
)
from tasks.borrower_over_estimation.s3_to_snowflake_table_create import (
    s3_to_snowflake_table_create,
)
from tasks.borrower_over_estimation.s3_to_snowflake_data_ingestion import (
    s3_to_snowflake_data_ingestion,
)

config = Config()

dag_owners = ['matias.marenchino']
owner_names, owner_emails = owner_default_args(dag_owners)

# Default arguments for DAG
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
    "borrower_over_estimation_scoring",
    default_args=default_args,
)

# Data Transformation
task1 = KVSnowflakeOperator(
    task_id="data_transformation",
    dag=dag,
    snowflake_conn_id=config.SNOWFLAKE_CONNECTION_NAME,
    sql=snowflake_data_transformation(config.DATABASE, config.SCHEMA, config.TABLE),
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

# task5: for model inferencing
task5 = KVDatabricksRunNowOperator(
    dag=dag,
    task_id="bow_scoring",
    databricks_conn_id=config.DATABRICKS_CONNECTION_ID,
    job_name="inference__ml_m5a_large",
    notebook_params={"model_name": "borrower_over_estimation"},
)

# task6: for delta to s3.
task6 = KVDatabricksRunNowOperator(
    dag=dag,
    task_id="delta_to_s3",
    databricks_conn_id=config.DATABRICKS_CONNECTION_ID,
    job_name="delta_to_s3__ml_m5a_large",
    notebook_params=config.DELTA_TO_S3_NOTEBOOK_PARAMS,
)

# Create table in snowflake

task7 = KVSnowflakeOperator(
    task_id="s3_to_snowflake_table_create",
    dag=dag,
    snowflake_conn_id=config.SNOWFLAKE_CONNECTION_NAME,
    sql=s3_to_snowflake_table_create(
        config.FINAL_S3_LOCATION_PREFIX,
        config.DATABASE,
        config.MODEL_SCHEMA,
        config.PREDICTED_DATA_TABLE,
    ),
)

# Ingest data in snowflake table
task8 = KVSnowflakeOperator(
    task_id="s3_to_snowflake_predicted_data_ingestion",
    dag=dag,
    snowflake_conn_id=config.SNOWFLAKE_CONNECTION_NAME,
    sql=s3_to_snowflake_data_ingestion(
        config.FINAL_S3_LOCATION_PREFIX,
        config.DATABASE,
        config.MODEL_SCHEMA,
        config.PREDICTED_DATA_TABLE,
    ),
)

task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7 >> task8
