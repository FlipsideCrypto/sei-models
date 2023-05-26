{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE', 'block_timestamp::DATE' ]
) }}

WITH base_table AS (

    SELECT
        block_id,
        tx_id,
        tx :tx_result :codespace :: STRING AS codespace,
        tx :tx_result :gas_used :: NUMBER AS gas_used,
        tx :tx_result :gas_wanted :: NUMBER AS gas_wanted,
        CASE
            WHEN tx :tx_result :code :: NUMBER IS NOT NULL THEN FALSE
            ELSE TRUE
        END AS tx_succeeded,
        tx :tx_result :code :: NUMBER AS tx_code,
        tx :tx_result :events AS msgs,
        TRY_PARSE_JSON(
            tx :tx_result :log
        ) AS tx_log,
        TRY_BASE64_DECODE_STRING(
            tx :tx_result :data
        ) AS tx_type,
        tx,
        _inserted_timestamp
    FROM
        {{ ref('bronze__transactions') }}
    WHERE
        tx_id IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY tx_id
ORDER BY
    _inserted_timestamp DESC)) = 1
)
SELECT
    b.block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    tx_code,
    codespace,
    gas_used,
    gas_wanted,
    tx_type,
    msgs,
    tx_log :: STRING AS tx_log,
    tx AS full_tx,
    b._inserted_timestamp
FROM
    base_table b
    LEFT OUTER JOIN {{ ref('silver__blocks') }}
    bb
    ON b.block_id = bb.block_id

{% if is_incremental() %}
WHERE
    bb._inserted_timestamp :: DATE >= (
        SELECT
            MAX(_inserted_timestamp) :: DATE - 2
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY tx_id
ORDER BY
    b._inserted_timestamp DESC)) = 1
