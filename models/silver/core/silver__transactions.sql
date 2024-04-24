{{ config(
    materialized = 'incremental',
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::DATE >= (select min(block_timestamp::DATE) from ' ~ generate_tmp_view_name(this) ~ ')'],
    unique_key = "tx_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp::DATE', 'block_timestamp::DATE' ],
    tags = ['core','full_test']
) }}
-- depends_on: {{ ref('bronze__streamline_transactions') }}
WITH base_table AS (

    SELECT
        DATA :height :: INT AS block_id,
        DATA :hash :: STRING AS tx_id,
        DATA :tx_result :codespace :: STRING AS codespace,
        DATA :tx_result :gas_used :: NUMBER AS gas_used,
        DATA :tx_result :gas_wanted :: NUMBER AS gas_wanted,
        CASE
            WHEN DATA :tx_result :code :: NUMBER IS NOT NULL THEN FALSE
            ELSE TRUE
        END AS tx_succeeded,
        DATA :tx_result :code :: NUMBER AS tx_code,
        DATA :tx_result :events AS msgs,
        TRY_PARSE_JSON(
            DATA :tx_result :log
        ) AS tx_log,
        TRY_BASE64_DECODE_STRING(
            DATA :tx_result :data
        ) AS tx_type,
        DATA AS tx,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_transactions') }}
{% else %}
    {{ ref('bronze__streamline_FR_transactions') }}
{% endif %}
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
