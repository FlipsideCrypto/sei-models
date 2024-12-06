{{ config (
    materialized = 'view'
) }}
{{ streamline_external_table_query_v2(
    model = "sei_addresses",
    partition_function = "to_date(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1), 'YYYYMMDD')"
) }}
