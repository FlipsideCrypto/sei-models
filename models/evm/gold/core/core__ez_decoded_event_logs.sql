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
    contract_address,
    token_name AS contract_name,
    event_name,
    decoded_flat AS decoded_log,
    decoded_data AS full_decoded_log,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    topics,
    DATA,
    event_removed,
    tx_status as tx_succeeded,
    decoded_logs_id AS ez_decoded_event_logs_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__decoded_logs') }}
    l
    LEFT JOIN {{ ref('silver__contracts') }} C USING (contract_address)