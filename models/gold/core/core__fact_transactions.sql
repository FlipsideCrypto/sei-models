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
    msgs,
    COALESCE (
        transactions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id']
        ) }}
    ) AS fact_transactions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__transactions_final') }}
