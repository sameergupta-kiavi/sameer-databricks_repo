def snowflake_to_s3(S3_LOCATION_PREFIX, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME):
    return f"""
        copy into @{S3_LOCATION_PREFIX}/databricks_tmp/{TABLE_NAME}/\
            from {DATABASE_NAME}.{SCHEMA_NAME}.{TABLE_NAME}
        file_format = (format_name='parquet')
        overwrite=true
        header=true;
    """
