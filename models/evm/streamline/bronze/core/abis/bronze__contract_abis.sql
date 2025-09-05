{{ config (
    materialized = 'view',
    tags = ['noncore']
) }}

{# Main query starts here #}
{{ streamline_external_table_query_v3(
    source_name = 'contract_abis',
    block_number = false,
    contract_address = true
) }}