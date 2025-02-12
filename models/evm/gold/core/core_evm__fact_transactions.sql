{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    from_address,
    to_address,
    origin_function_signature,
    value,
    value_precise_raw,
    value_precise,
    tx_fee,
    tx_fee_precise,
    case when tx_status = 'SUCCESS' then true else false end as tx_succeeded,
    tx_type,
    nonce,
    position as tx_position,
    input_data,
    gas_price,
    effective_gas_price,
    gas AS gas_limit,
    gas_used,
    cumulative_gas_used,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    r,
    s,
    v,
    transactions_id AS fact_transactions_id,
    inserted_timestamp,
    modified_timestamp,
    tx_status AS status, -- deprecate
    position, -- deprecate
    block_hash -- deprecate
FROM
    {{ ref('silver_evm__transactions') }}
