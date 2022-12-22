from global_config.global_config import (
    GlobalBaseConfig,
    GlobalTestConfig,
    GlobalProductionConfig,
)


class Config(GlobalBaseConfig):
    # Snowflake
    SNOWFLAKE_CONNECTION_NAME = "snowflake_data"
    DATABASE = "lh_prod_data_snowflake"
    SCHEMA = "scratch"
    TABLE = "borrower_information_training"
    S3_LOCATION_PREFIX = "scratch.lh_base_stage"

    # Databricks
    DATABRICKS_CONNECTION_ID = "databricks_conn"
    S3_TO_DELTA_NOTEBOOK_PARAMS = {
        "s3_data_location": "s3://lendinghome-data/dumps_test/databricks_tmp/borrower_information_training/",
        "delta_table_location": "dbfs:/Shared/data_engineering/snowflake_mirror/borrower_information_training",
        "primary_keys": "LOAN_ID",
        "timestamp_keys": "LOAN_SUBMITTED_AT",
    }
    FS_NOTEBOOK_PARAMS = {
        "src_table": "dbfs:/Shared/data_engineering/snowflake_mirror/borrower_information_training",
        "feature_store_table": "FEATURE_STORE.borrower_information_training",
        "feature_store_primary_keys": "LOAN_ID",
        "feature_store_timestamp_keys": "LOAN_SUBMITTED_AT",
    }


class TestConfig(GlobalTestConfig):
    # Snowflake
    SCHEMA = "scratch"
    S3_LOCATION_PREFIX = "scratch.lh_base_stage"

    # Databricks
    S3_TO_DELTA_NOTEBOOK_PARAMS = {
        "s3_data_location": "s3://lendinghome-data/dumps_test/databricks_tmp/borrower_information_training/",
        "delta_table_location": "dbfs:/Shared/data_engineering/snowflake_mirror/borrower_information_training",
        "primary_keys": "LOAN_ID",
        "timestamp_keys": "LOAN_SUBMITTED_AT",
    }
    FS_NOTEBOOK_PARAMS = {
        "src_table": "dbfs:/Shared/data_engineering/snowflake_mirror/borrower_information_training",
        "feature_store_table": "FEATURE_STORE.borrower_information_training",
        "feature_store_primary_keys": "LOAN_ID",
        "feature_store_timestamp_keys": "LOAN_SUBMITTED_AT",
        "feature_store_description": "Historical Borrower Information before October 2021",
    }


class ProductionConfig(GlobalProductionConfig):
    # Snowflake
    DATABASE = "lh_prod_data_snowflake"
    TABLE = "borrower_information_training"
    SCHEMA = "public"
    S3_LOCATION_PREFIX = "public.lh_base_stage"

    # Databricks
    S3_TO_DELTA_NOTEBOOK_PARAMS = {
        "s3_data_location": "s3://lendinghome-data/dumps/databricks_tmp/borrower_information_training/",
        "delta_table_location": "dbfs:/Shared/data_engineering/snowflake_mirror/borrower_information_training",
        "primary_keys": "LOAN_ID",
        "timestamp_keys": "LOAN_SUBMITTED_AT",
    }
    FS_NOTEBOOK_PARAMS = {
        "src_table": "dbfs:/Shared/data_engineering/snowflake_mirror/borrower_information_training",
        "feature_store_table": "FEATURE_STORE.borrower_information_training",
        "feature_store_primary_keys": "LOAN_ID",
        "feature_store_timestamp_keys": "LOAN_SUBMITTED_AT",
        "feature_store_description": "Historical Borrower Information before October 2021",
    }
