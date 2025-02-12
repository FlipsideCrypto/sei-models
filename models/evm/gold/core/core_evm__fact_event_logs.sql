{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    {# tx_position, -- new column #}
    event_index,
    contract_address,
    topics,
    topics[0]::string as topic_0,
    topics[1]::string as topic_1,
    topics[2]::string as topic_2,
    topics[3]::string as topic_3,
    data,
    event_removed,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    CASE
        WHEN tx_status = 'SUCCESS' THEN TRUE
        ELSE FALSE
    END AS tx_succeeded,
    logs_id AS fact_event_logs_id,
    inserted_timestamp,
    modified_timestamp,
    tx_status, -- deprecate
    _log_id -- deprecate
FROM
    {{ ref('silver_evm__logs') }}
