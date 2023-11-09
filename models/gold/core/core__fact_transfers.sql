{{ config(
    materialized = 'view',
    tags = ['core']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    transfer_type,
    sender,
    amount,
    currency,
    receiver
FROM
    {{ ref('silver__transfers') }}
UNION ALL
SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    transfer_type,
    sender,
    amount,
    currency,
    receiver
FROM
    {{ ref('silver__transfers_ibc') }}
