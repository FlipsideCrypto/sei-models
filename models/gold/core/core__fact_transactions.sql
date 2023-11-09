{{ config(
    materialized = 'view',
    tags = ['core']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_from,
    tx_succeeded,
    codespace,
    fee,
    gas_used,
    gas_wanted,
    tx_code,
    msgs
FROM
    {{ ref('silver__transactions_final') }}
