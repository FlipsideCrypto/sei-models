{{ config (
    materialized = 'view',
    tags = ['recent_evm_test']
) }}

SELECT
    *
FROM
    {{ ref('silver_evm__transactions') }}
WHERE
    block_number > (
        SELECT
            block_number
        FROM
            {{ ref("_evm_block_lookback") }}
    )
    and inserted_timestamp < DATEADD('hour', -1, SYSDATE())