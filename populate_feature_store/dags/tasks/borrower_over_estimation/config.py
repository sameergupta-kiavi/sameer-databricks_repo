from global_config.global_config import (
    GlobalBaseConfig,
    GlobalTestConfig,
    GlobalProductionConfig,
)


class Config(GlobalBaseConfig):
    # Snowflake
    SNOWFLAKE_CONNECTION_NAME = "snowflake_data_2"
    DATABASE = "lh_prod_data_snowflake"
    SCHEMA = "scratch"
    MODEL_SCHEMA = "scratch"
    TABLE = "borrower_information"
    PREDICTED_DATA_TABLE = "borrower_over_estimation_scores"
    S3_LOCATION_PREFIX = "scratch.lh_base_stage"
    FINAL_S3_LOCATION_PREFIX = "lh_prod_data_snowflake.public.DATABRICKS_STAGE_DEV"

    # Databricks
    DATABRICKS_CONNECTION_ID = "databricks_conn"
    S3_TO_DELTA_NOTEBOOK_PARAMS = {
        "s3_data_location": "s3://lendinghome-data/dumps_test/databricks_tmp/borrower_information/",
        "delta_table_location": "dbfs:/Shared/data_engineering/snowflake_mirror/borrower_information",
        "primary_keys": "LOAN_ID",
        "timestamp_keys": "LOAN_SUBMITTED_AT",
    }
    FS_NOTEBOOK_PARAMS = {
        "src_table": "dbfs:/Shared/data_engineering/snowflake_mirror/borrower_information",
        "feature_store_table": "FEATURE_STORE.borrower_information",
        "feature_store_primary_keys": "LOAN_ID",
        "feature_store_timestamp_keys": "LOAN_SUBMITTED_AT",
    }
    DELTA_TO_S3_NOTEBOOK_PARAMS = {
        "delta_table_name": "MODELS.borrower_over_estimation",
        "s3_write_location": "s3://k17e-dev-dbricks-data/dumps/databricks_tmp/borrower_over_estimation/scores/",
    }


class TestConfig(GlobalTestConfig):
    # Snowflake
    SNOWFLAKE_CONNECTION_NAME = "snowflake_data_2"
    DATABASE = "lh_prod_data_snowflake"
    SCHEMA = "scratch"
    MODEL_SCHEMA = "scratch"
    TABLE = "borrower_information"
    PREDICTED_DATA_TABLE = "borrower_over_estimation_scores"
    S3_LOCATION_PREFIX = "scratch.lh_base_stage"
    FINAL_S3_LOCATION_PREFIX = "lh_prod_data_snowflake.public.DATABRICKS_STAGE_QA"

    # Databricks
    S3_TO_DELTA_NOTEBOOK_PARAMS = {
        "s3_data_location": "s3://lendinghome-data/dumps_test/databricks_tmp/borrower_information/",
        "delta_table_location": "dbfs:/Shared/data_engineering/snowflake_mirror/borrower_information",
        "primary_keys": "LOAN_ID",
        "timestamp_keys": "LOAN_SUBMITTED_AT",
    }
    FS_NOTEBOOK_PARAMS = {
        "src_table": "dbfs:/Shared/data_engineering/snowflake_mirror/borrower_information",
        "feature_store_table": "FEATURE_STORE.borrower_information",
        "feature_store_primary_keys": "LOAN_ID",
        "feature_store_timestamp_keys": "LOAN_SUBMITTED_AT",
        "feature_store_description": "Historical Borrower Information before October 2021",
    }
    DELTA_TO_S3_NOTEBOOK_PARAMS = {
        "delta_table_name": "MODELS.borrower_over_estimation",
        "s3_write_location": "s3://k17e-dev-dbricks-data/dumps/databricks_tmp/borrower_over_estimation/scores/",
    }


class ProductionConfig(GlobalProductionConfig):
    # Snowflake
    SNOWFLAKE_CONNECTION_NAME = "snowflake_data_2"
    DATABASE = "lh_prod_data_snowflake"
    TABLE = "borrower_information"
    SCHEMA = "public"
    MODEL_SCHEMA = "models"
    S3_LOCATION_PREFIX = "public.lh_base_stage"
    PREDICTED_DATA_TABLE = "borrower_over_estimation_scores"
    FINAL_S3_LOCATION_PREFIX = "lh_prod_data_snowflake.public.DATABRICKS_STAGE_PROD"

    # Databricks
    S3_TO_DELTA_NOTEBOOK_PARAMS = {
        "s3_data_location": "s3://lendinghome-data/dumps/databricks_tmp/borrower_information/",
        "delta_table_location": "dbfs:/Shared/data_engineering/snowflake_mirror/borrower_information",
        "primary_keys": "LOAN_ID",
        "timestamp_keys": "LOAN_SUBMITTED_AT",
    }
    FS_NOTEBOOK_PARAMS = {
        "src_table": "dbfs:/Shared/data_engineering/snowflake_mirror/borrower_information",
        "feature_store_table": "FEATURE_STORE.borrower_information",
        "feature_store_primary_keys": "LOAN_ID",
        "feature_store_timestamp_keys": "LOAN_SUBMITTED_AT",
        "feature_store_description": "Historical Borrower Information before October 2021",
    }
    DELTA_TO_S3_NOTEBOOK_PARAMS = {
        "delta_table_name": "MODELS.borrower_over_estimation",
        "s3_write_location": "s3://k17e-prod-dbricks-data/dumps/databricks_tmp/borrower_over_estimation/scores/",
    }
