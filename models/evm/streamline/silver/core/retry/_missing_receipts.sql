{{ config (
    materialized = "ephemeral"
) }}

WITH lookback AS (

    SELECT
        block_number
    FROM
        {{ ref("_evm_block_lookback") }}
)
SELECT
    DISTINCT t.block_number AS block_number
FROM
    {{ ref("core_evm__fact_transactions") }}
WHERE
    tx_succeeded is null
    and block_number >= (
        SELECT
            block_number
        FROM
            lookback
    )
    and block_timestamp >= DATEADD('hour', -84, SYSDATE())