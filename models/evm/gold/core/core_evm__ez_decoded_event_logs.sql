{% set post_hook = 'ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(ez_decoded_event_logs_id, contract_name, contract_address)' %}

{{ config (
    materialized = "incremental",
    unique_key = ['ez_decoded_event_logs_id'],
    cluster_by = "block_timestamp::date",
    full_refresh = false,
    post_hook = post_hook,
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['gold_decoded_logs']
) }}

WITH base AS (

    SELECT
        tx_hash,
        block_number,
        event_index,
        event_name,
        contract_address,
        decoded_data as full_decoded_data,
        decoded_flat as decoded_log
    FROM
        {{ ref('silver_evm__decoded_logs') }}
    WHERE
        1=1

    {% if is_incremental() %}
    AND modified_timestamp > (
        SELECT
            COALESCE(
                MAX(modified_timestamp),
                '2000-01-01'::TIMESTAMP
            )
        FROM
            {{ this }}
    )
    {% endif %}
),
new_records as (
    SELECT
        b.block_number,
        fel.block_timestamp,
        b.tx_hash,
        ft.position as tx_position,
        b.event_index,
        b.contract_address,
        fel.topics,
        fel.topics[0] as topic_0,
        fel.topics[1] as topic_1,
        fel.topics[2] as topic_2,
        fel.topics[3] as topic_3,
        fel.DATA,
        fel.event_removed,
        fel.origin_from_address,
        fel.origin_to_address,
        fel.origin_function_signature,
        case when ft.status = 'SUCCESS' then TRUE else FALSE end as tx_succeeded,
        b.event_name, 
        b.full_decoded_data,
        b.decoded_log,
        dc.name as contract_name
    FROM base b 
    LEFT JOIN {{ ref('core_evm__fact_event_logs') }} fel
    ON b.block_number = fel.block_number 
    and b.event_index = fel.event_index
    LEFT JOIN {{ ref('core_evm__fact_transactions') }} ft
    ON b.tx_hash = ft.tx_hash
    LEFT JOIN {{ ref('core_evm__dim_contracts') }} dc
    ON b.contract_address = dc.address and dc.name IS NOT NULL
    WHERE 1=1 
    {% if is_incremental() %}
        and fel.inserted_timestamp > dateadd('day', -3, sysdate())
        and ft.inserted_timestamp > dateadd('day', -3, sysdate())
    {% endif %}
)
{% if is_incremental() %},
missing_tx_data AS (
    SELECT
        t.block_number,
        fel.block_timestamp,
        t.tx_hash,
        ft.position as tx_position,
        t.event_index,
        t.contract_address,
        fel.topics,
        fel.topics[0] as topic_0,
        fel.topics[1] as topic_1,
        fel.topics[2] as topic_2,
        fel.topics[3] as topic_3,
        fel.DATA,
        fel.event_removed,
        fel.origin_from_address,
        fel.origin_to_address,
        fel.origin_function_signature,
        case when ft.status = 'SUCCESS' then TRUE else FALSE end as tx_succeeded,
        t.event_name, 
        t.full_decoded_data,
        t.decoded_log,
        t.contract_name
    FROM {{ this }} t
    INNER JOIN {{ ref('core_evm__fact_event_logs') }} fel 
    USING (block_number, event_index)
    LEFT JOIN {{ ref('core_evm__fact_transactions') }} ft
    ON t.tx_hash = ft.tx_hash
    WHERE t.tx_succeeded IS NULL OR t.block_timestamp IS NULL and fel.block_timestamp IS NOT NULL
),
missing_contract_data AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        tx_position,
        event_index,
        contract_address,
        topics,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        event_removed,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_succeeded,
        event_name, 
        full_decoded_data,
        decoded_log,
        dc.name as contract_name
    FROM {{ this }} t
    INNER JOIN {{ ref('core_evm__dim_contracts') }} dc
    ON t.contract_address = dc.address and dc.name IS NOT NULL
    WHERE t.contract_name IS NULL
)
{% endif %}
, 
FINAL as (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        tx_position,
        event_index,
        contract_address,
        topics,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        event_removed,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_succeeded,
        event_name, 
        full_decoded_data,
        decoded_log,
        contract_name
    FROM
        new_records

    {% if is_incremental() %}
    UNION ALL 
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        tx_position,
        event_index,
        contract_address,
        topics,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        event_removed,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_succeeded,
        event_name, 
        full_decoded_data,
        decoded_log,
        contract_name
    FROM
        missing_tx_data
    UNION ALL 
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        tx_position,
        event_index,
        contract_address,
        topics,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        event_removed,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_succeeded,
        event_name, 
        full_decoded_data,
        decoded_log,
        contract_name
    FROM
        missing_contract_data
    {% endif %}
)
SELECT 
    block_number,
    block_timestamp,
    tx_hash,
    tx_position,
    event_index,
    contract_address,
    topics,
    topic_0,
    topic_1,
    topic_2,
    topic_3,
    DATA,
    event_removed,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_succeeded,
    event_name, 
    full_decoded_data,
    decoded_log,
    contract_name,
    {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
    ) }} AS ez_decoded_event_logs_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM FINAL
qualify ROW_NUMBER() over (
    PARTITION BY
        ez_decoded_event_logs_id
    ORDER BY
        block_timestamp DESC nulls last,
        tx_succeeded DESC nulls last,
        contract_name DESC nulls last
) = 1