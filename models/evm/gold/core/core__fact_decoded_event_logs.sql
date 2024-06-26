{{ config (
    materialized = "incremental",
    unique_key = ['block_number', 'event_index'],
    cluster_by = "block_timestamp::date",
    incremental_predicates = ["dynamic_range", "block_number"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['decoded_logs','reorg']
) }}    

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    event_name,
    decoded_flat AS decoded_log,
    decoded_data AS full_decoded_log,
    decoded_logs_id AS fact_decoded_event_logs_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__decoded_logs') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}