{{ config (
    materialized = 'table',
    tags = ['streamline_core_complete']
) }}

SELECT
    live.udf_api(
        'POST',
        '{Service}/{Authentication}',{},{ 'method' :'eth_blockNumber',
        'params' :[],
        'id' :1,
        'jsonrpc' :'2.0' },
        'Vault/prod/sei/quicknode/arctic1'
    ) AS resp,
    utils.udf_hex_to_int(
        resp :data :result :: STRING
    ) AS block_number
