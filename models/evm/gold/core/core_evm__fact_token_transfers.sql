{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    from_address,
    to_address,
    raw_amount,
    raw_amount_precise,
    _log_id,
    transfers_id AS fact_token_transfers_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_evm__transfers') }}
