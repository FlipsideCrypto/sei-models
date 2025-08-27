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
    DISTINCT cb.block_number AS block_number
FROM
    {{ ref("silver_evm__confirmed_blocks") }}
    cb
    LEFT JOIN {{ ref("core_evm__fact_transactions") }}
    txs USING (
        block_number,
        tx_hash
    )
WHERE
    txs.tx_hash IS NULL
    AND cb.block_number >= (
        SELECT
            block_number
        FROM
            lookback
    )
    AND cb._inserted_timestamp >= DATEADD('hour', -84, SYSDATE())
    AND (
        txs.inserted_timestamp >= DATEADD('hour', -84, SYSDATE())
        OR txs.inserted_timestamp IS NULL)
