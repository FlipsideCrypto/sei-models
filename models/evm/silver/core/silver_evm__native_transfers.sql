{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['core']
) }}

WITH base AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address,
        to_address,
        VALUE,
        identifier,
        concat_ws(
            '-',
            block_number,
            tx_position,
            CONCAT(
                TYPE,
                '_',
                trace_address
            )
        ) AS _call_id,
        input,
        modified_timestamp AS _inserted_timestamp,
        value_precise_raw,
        value_precise,
        tx_position,
        trace_index
    FROM
        {{ ref('core_evm__fact_traces') }}
    WHERE
        VALUE > 0
        AND tx_succeeded
        AND trace_succeeded
        AND TYPE NOT IN (
            'DELEGATECALL',
            'STATICCALL'
        )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
tx_table AS (
    SELECT
        block_number,
        tx_hash,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        origin_function_signature
    FROM
        {{ ref('core_evm__fact_transactions') }}
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                base
        )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    tx_hash AS tx_hash,
    block_number AS block_number,
    block_timestamp AS block_timestamp,
    identifier AS identifier,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    from_address,
    to_address,
    VALUE AS amount,
    value_precise_raw AS amount_precise_raw,
    value_precise AS amount_precise,
    ROUND(
        VALUE * price,
        2
    ) AS amount_usd,
    _call_id,
    _inserted_timestamp,
    tx_position,
    trace_index,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'trace_index']
    ) }} AS native_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base A
    LEFT JOIN {{ ref('silver__complete_native_prices') }}
    ON DATE_TRUNC(
        'hour',
        A.block_timestamp
    ) = HOUR
    JOIN tx_table USING (
        tx_hash,
        block_number
    )
