{{ config (
    materialized = 'view'
) }}
{{ fsc_evm.streamline_external_table_query(
    model = "evm_blocks",
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 4), '_', 1) AS INTEGER )"
) }}
