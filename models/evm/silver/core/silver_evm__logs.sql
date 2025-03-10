{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = "block_timestamp::date, _inserted_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['core']
) }}

WITH base AS (

    SELECT
        block_number,
        tx_hash,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        tx_status,
        logs,
        _inserted_timestamp
    FROM
        {{ ref('silver_evm__receipts') }}
    WHERE
        ARRAY_SIZE(logs) > 0

{% if is_incremental() %}
AND _INSERTED_TIMESTAMP >= (
    SELECT
        DATEADD(
            'hour',
            {{ var('fill_missing_logs_hours', -6) }},
            MAX(
                _inserted_timestamp
            )
        )
    FROM
        {{ this }}
)
{% endif %}
),
flat_logs AS (
    SELECT
        block_number,
        tx_hash,
        origin_from_address,
        origin_to_address,
        tx_status,
        VALUE :address :: STRING AS contract_address,
        VALUE :blockHash :: STRING AS block_hash,
        VALUE :data :: STRING AS DATA,
        utils.udf_hex_to_int(
            VALUE :logIndex :: STRING
        ) :: INT AS event_index,
        VALUE :removed :: BOOLEAN AS event_removed,
        VALUE :topics AS topics,
        _inserted_timestamp
    FROM
        base,
        LATERAL FLATTEN(
            input => logs
        )
),
valid_txs as (
    SELECT DISTINCT
        b.block_number,
        t.value::string as tx_hash
    FROM
        flat_logs l 
        INNER JOIN {{ ref('silver_evm__blocks') }} b
            ON l.block_number = b.block_number,
        LATERAL FLATTEN(
            input => b.data:result:transactions
        ) t
),
new_records AS (
    SELECT
        l.block_number,
        txs.block_timestamp,
        l.tx_hash,
        l.origin_from_address,
        l.origin_to_address,
        txs.origin_function_signature,
        l.tx_status,
        l.contract_address,
        l.block_hash,
        l.data,
        l.event_index,
        l.event_removed,
        l.topics,
        l._inserted_timestamp,
        CASE
            WHEN txs.block_timestamp IS NULL
            OR txs.origin_function_signature IS NULL THEN TRUE
            ELSE FALSE
        END AS is_pending,
        CONCAT(
            l.tx_hash :: STRING,
            '-',
            l.event_index :: STRING
        ) AS _log_id
    FROM
        flat_logs l
        INNER JOIN valid_txs v
        ON l.block_number = v.block_number
        AND l.tx_hash = v.tx_hash
        LEFT JOIN {{ ref('silver_evm__transactions') }}
        txs
        ON l.block_number = txs.block_number
        AND l.tx_hash = txs.tx_hash

{% if is_incremental() %}
AND txs._INSERTED_TIMESTAMP >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
)

{% if is_incremental() %},
missing_data AS (
    SELECT
        t.block_number,
        txs.block_timestamp,
        t.tx_hash,
        t.origin_from_address,
        t.origin_to_address,
        txs.origin_function_signature,
        t.tx_status,
        t.contract_address,
        t.block_hash,
        t.data,
        t.event_index,
        t.event_removed,
        t.topics,
        GREATEST(
            t._inserted_timestamp,
            txs._inserted_timestamp
        ) AS _inserted_timestamp,
        _log_id,
        FALSE AS is_pending
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('silver_evm__transactions') }}
        txs USING (
            block_number,
            tx_hash
        )
    WHERE
        t.is_pending
)
{% endif %},
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_status,
        contract_address,
        block_hash,
        DATA,
        event_index,
        event_removed,
        topics,
        _inserted_timestamp,
        _log_id,
        is_pending
    FROM
        new_records

{% if is_incremental() %}
UNION
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_status,
    contract_address,
    block_hash,
    DATA,
    event_index,
    event_removed,
    topics,
    _inserted_timestamp,
    _log_id,
    is_pending
FROM
    missing_data
{% endif %}
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS logs_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over (PARTITION BY block_number, tx_hash, event_index
ORDER BY
    _inserted_timestamp DESC, is_pending ASC)) = 1
