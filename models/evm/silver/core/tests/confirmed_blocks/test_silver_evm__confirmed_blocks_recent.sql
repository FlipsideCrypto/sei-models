{{ config (
    materialized = 'view',
    tags = ['recent_evm_test']
) }}

SELECT
    *
FROM
    {{ ref('silver_evm__confirmed_blocks') }}
WHERE
    block_number > (
        SELECT
            block_number
        FROM
            {{ ref("_evm_block_lookback") }}
    )