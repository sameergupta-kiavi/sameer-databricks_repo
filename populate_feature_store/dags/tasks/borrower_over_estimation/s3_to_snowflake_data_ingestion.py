def s3_to_snowflake_data_ingestion(
    s3_location_prefix, database_name, schema_name, predicted_data_table
):
    return f"""
        COPY INTO {database_name}.{schema_name}.{predicted_data_table}
        FROM @{s3_location_prefix}/dumps/databricks_tmp/{predicted_data_table}
        file_format = (type=parquet)
        MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;
        """
