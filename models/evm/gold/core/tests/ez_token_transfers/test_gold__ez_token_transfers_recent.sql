{{ config (
    materialized = "view",
    tags = ['recent_test']
) }}

SELECT
    *
FROM
    {{ ref('core_evm__ez_token_transfers') }}
WHERE
    block_number > (
        SELECT
            block_number
        FROM
            {{ ref('_evm_block_lookback') }}
    )
