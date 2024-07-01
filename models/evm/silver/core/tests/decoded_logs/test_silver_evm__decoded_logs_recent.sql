-- depends_on: {{ ref('silver_evm__logs') }}
{{ config (
    materialized = 'view',
    tags = ['recent_test']
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_evm_block_lookback") }}
)
SELECT
    *
FROM
    {{ ref('silver_evm__decoded_logs') }}
WHERE
    block_number >= (
        SELECT
            block_number
        FROM
            last_3_days
    )
