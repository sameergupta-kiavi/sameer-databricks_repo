def s3_to_snowflake_table_create(
    s3_location_prefix, database_name, schema_name, predicted_data_table
):
    return f"""
    create table IF NOT EXISTS {database_name}.{schema_name}.{predicted_data_table}
        using template (
            select array_agg(object_construct(*))
            from table(
                infer_schema(
                location=>'@{s3_location_prefix}/dumps/databricks_tmp/{predicted_data_table}',
                file_format=>'parquet'
                )
            ));
        """
