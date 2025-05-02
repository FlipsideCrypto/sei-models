{{ config (
    materialized = 'view'
) }}
{# {{ streamline_external_table_query_v2(
    model = "sei_addresses",
    partition_function = "to_date(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1), 'YYYYMMDD')"
) }}
 #}


  WITH meta AS (
        SELECT
            job_created_time AS _inserted_timestamp,
            file_name,
            to_date(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1), 'YYYYMMDD') AS partition_key
        FROM
            TABLE(
                information_schema.external_table_file_registration_history(
                    start_time => DATEADD('day', -3, CURRENT_TIMESTAMP()),
                    table_name => 'streamline.SEI.sei_addresses')
                ) A
            )
        SELECT
            s.*,
            b.file_name,
            _inserted_timestamp
        FROM
             {{ source(
        'bronze_streamline',
        'sei_addresses'
    ) }}
            s
            JOIN meta b
            ON b.file_name = metadata$filename
            AND b.partition_key = s.partition_key
        WHERE
            b.partition_key = s.partition_key
    
 