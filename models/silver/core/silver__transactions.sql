{{ config(
    materialized = 'view',
    tags = ['v2']
) }}

WITH base_table AS (

    SELECT
        block_number AS block_id,
        VALUE :hash :: STRING AS tx_id,
        VALUE :tx_result :codespace :: STRING AS codespace,
        VALUE :tx_result :gas_used :: NUMBER AS gas_used,
        VALUE :tx_result :gas_wanted :: NUMBER AS gas_wanted,
        CASE
            WHEN VALUE :tx_result :code :: NUMBER IS NOT NULL THEN FALSE
            ELSE TRUE
        END AS tx_succeeded,
        VALUE :tx_result :code :: NUMBER AS tx_code,
        VALUE :tx_result :events AS msgs,
        TRY_PARSE_JSON(
            VALUE :tx_result :log
        ) AS tx_log,
        TRY_BASE64_DECODE_STRING(
            VALUE :tx_result :data
        ) AS tx_type,
        _inserted_timestamp
    FROM
        {{ ref('streamline__transactions_realtime') }},
        LATERAL FLATTEN(
            DATA :data :result :txs
        )
    WHERE
        tx_id IS NOT NULL qualify(ROW_NUMBER() over (PARTITION BY tx_id
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
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id']
    ) }} AS transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    b._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base_table b
    LEFT OUTER JOIN {{ ref('silver__blocks') }}
    bb
    ON b.block_id = bb.block_id qualify(ROW_NUMBER() over (PARTITION BY tx_id
ORDER BY
    b._inserted_timestamp DESC)) = 1
