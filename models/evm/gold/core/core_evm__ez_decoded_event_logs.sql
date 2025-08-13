{% set post_hook = 'ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(ez_decoded_event_logs_id, contract_name, contract_address)' %}
{{ config (
    materialized = "incremental",
    unique_key = ['ez_decoded_event_logs_id'],
    cluster_by = "block_timestamp::date",
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
        decoded_data AS full_decoded_data,
        decoded_flat AS decoded_log
    FROM
        {{ ref('silver_evm__decoded_logs') }}
    WHERE
        1 = 1

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        COALESCE(
            DATEADD('hour', -2, MAX(modified_timestamp)),
            '2000-01-01' :: TIMESTAMP)
            FROM
                {{ this }}
        )
    {% endif %}
),
new_records AS (
    SELECT
        b.block_number,
        fel.block_timestamp,
        b.tx_hash,
        ft.position AS tx_position,
        b.event_index,
        b.contract_address,
        fel.topics,
        fel.topics [0] :: STRING AS topic_0,
        fel.topics [1] :: STRING AS topic_1,
        fel.topics [2] :: STRING AS topic_2,
        fel.topics [3] :: STRING AS topic_3,
        fel.data,
        fel.event_removed,
        fel.origin_from_address,
        fel.origin_to_address,
        fel.origin_function_signature,
        CASE
            WHEN ft.status = 'SUCCESS' THEN TRUE
            ELSE FALSE
        END AS tx_succeeded,
        b.event_name,
        b.full_decoded_data,
        b.decoded_log,
        dc.name AS contract_name
    FROM
        base b
        LEFT JOIN {{ ref('core_evm__fact_event_logs') }}
        fel 
        on fel.block_number = b.block_number
        and fel.tx_hash = b.tx_hash
        and fel.event_index = b.event_index
        {% if is_incremental() %}
        AND fel.inserted_timestamp > DATEADD('day', -3, SYSDATE())
        {% endif %}
        LEFT JOIN {{ ref('core_evm__fact_transactions') }}
        ft 
        on ft.block_number = b.block_number
        and ft.tx_hash = b.tx_hash
        {% if is_incremental() %}
        AND ft.inserted_timestamp > DATEADD('day', -3, SYSDATE())
        {% endif %}
        LEFT JOIN {{ ref('core_evm__dim_contracts') }}
        dc
        ON b.contract_address = dc.address
        AND dc.name IS NOT NULL
    WHERE
        1 = 1
)

{% if is_incremental() %},
missing_tx_data AS (
    SELECT
        t.block_number,
        fel.block_timestamp,
        t.tx_hash,
        ft.position AS tx_position,
        t.event_index,
        t.contract_address,
        fel.topics,
        fel.topics [0] :: STRING AS topic_0,
        fel.topics [1] :: STRING AS topic_1,
        fel.topics [2] :: STRING AS topic_2,
        fel.topics [3] :: STRING AS topic_3,
        fel.data,
        fel.event_removed,
        fel.origin_from_address,
        fel.origin_to_address,
        fel.origin_function_signature,
        CASE
            WHEN ft.status = 'SUCCESS' THEN TRUE
            ELSE FALSE
        END AS tx_succeeded,
        t.event_name,
        t.full_decoded_data,
        t.decoded_log,
        t.contract_name
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('core_evm__fact_event_logs') }}
        fel USING (
            block_number,
            tx_hash,
            event_index
        )
        LEFT JOIN {{ ref('core_evm__fact_transactions') }}
        ft USING (
            block_number,
            tx_hash
        )
    WHERE
        t.tx_succeeded IS NULL
        OR t.block_timestamp IS NULL
        AND fel.block_timestamp IS NOT NULL
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
        dc.name AS contract_name
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('core_evm__dim_contracts') }}
        dc
        ON t.contract_address = dc.address
        AND dc.name IS NOT NULL
    WHERE
        t.contract_name IS NULL
)
{% endif %},
FINAL AS (
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
    full_decoded_data as full_decoded_log,
    decoded_log,
    contract_name,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS ez_decoded_event_logs_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    full_decoded_data -- deprecate
FROM
    FINAL qualify ROW_NUMBER() over (
        PARTITION BY ez_decoded_event_logs_id
        ORDER BY
            block_timestamp DESC nulls last,
            tx_succeeded DESC nulls last,
            contract_name DESC nulls last
    ) = 1
