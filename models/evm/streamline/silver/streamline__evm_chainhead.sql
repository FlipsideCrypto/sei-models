{{ config (
    materialized = 'table',
    tags = ['streamline_core_evm_complete']
) }}

SELECT
    live.udf_api(
        'POST',
        '{Service}/{Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'fsc-quantum-state', 'livequery'
        ),
        OBJECT_CONSTRUCT(
            'id',
            0,
            'jsonrpc',
            '2.0',
            'method',
            'eth_blockNumber',
            'params',
            []
        ),
        'Vault/prod/sei/quicknode/mainnet'
    ) AS resp,
    utils.udf_hex_to_int(
        resp :data :result :: STRING
    )::INT - 500 AS block_number