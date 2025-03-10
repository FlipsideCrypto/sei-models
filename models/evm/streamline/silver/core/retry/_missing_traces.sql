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
    DISTINCT tx.block_number block_number
FROM
    {{ ref("silver_evm__transactions") }}
    tx
    LEFT JOIN {{ ref("core_evm__fact_traces") }}
    tr
    ON tx.block_number = tr.block_number
    AND tx.tx_hash = tr.tx_hash
WHERE
    tx.block_timestamp >= DATEADD('hour', -84, SYSDATE())
    AND tr.tx_hash IS NULL
    AND tx.block_number >= (
        SELECT
            block_number
        FROM
            lookback
    )
    AND tx.block_number NOT IN (
        81772279,
        81772251
    )
    AND tr.block_timestamp >= DATEADD('hour', -84, SYSDATE())
    AND tr.block_timestamp IS NOT NULL
