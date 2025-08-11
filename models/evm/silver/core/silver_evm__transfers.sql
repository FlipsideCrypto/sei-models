{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "transfers_id",
    cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, origin_function_signature, origin_from_address, origin_to_address, contract_address, from_address, to_address, symbol)",
    tags = ['core']
) }}

WITH logs AS (

    SELECT
        concat(tx_hash, '-', event_index) AS _log_id,
        block_number,
        tx_hash,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address :: STRING AS contract_address,
        CONCAT('0x', SUBSTR(topics [1], 27, 40)) :: STRING AS from_address,
        CONCAT('0x', SUBSTR(topics [2], 27, 40)) :: STRING AS to_address,
        utils.udf_hex_to_int(SUBSTR(DATA, 3, 64)) AS raw_amount_precise,
        raw_amount_precise :: FLOAT AS raw_amount,
        event_index,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core_evm__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '36 hours'
    FROM
        {{ this }}
)
{% endif %}
),
token_transfers AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        t.contract_address,
        from_address,
        to_address,
        raw_amount_precise,
        raw_amount,
        IFF(
            C.token_decimals IS NOT NULL,
            utils.udf_decimal_adjust(
                raw_amount_precise,
                C.token_decimals
            ),
            NULL
        ) AS amount_precise,
        amount_precise :: FLOAT AS amount,
        C.token_decimals AS decimals,
        C.token_symbol AS symbol,
        CASE
            WHEN C.token_decimals IS NULL THEN 'false'
            ELSE 'true'
        END AS has_decimal,
        _log_id,
        _inserted_timestamp
    FROM
        logs t
        LEFT JOIN {{ ref('silver_evm__contracts') }} C USING (contract_address)
    WHERE
        raw_amount IS NOT NULL
        AND to_address IS NOT NULL
        AND from_address IS NOT NULL
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    from_address,
    to_address,
    raw_amount_precise,
    raw_amount,
    amount_precise,
    amount,
    amount * p.price as amount_usd,
    t.decimals,
    t.symbol,
    has_decimal,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    token_transfers t 
left join {{ ref('price__ez_prices_hourly') }} p
on date_trunc('hour', block_timestamp) = p.hour
and contract_address = p.token_address

{% if is_incremental() %}
union all
SELECT
    t.block_number,
    t.block_timestamp,
    t.tx_hash,
    t.event_index,
    t.origin_function_signature,
    t.origin_from_address,
    t.origin_to_address,
    t.contract_address,
    t.from_address,
    t.to_address,
    t.raw_amount_precise,
    t.raw_amount,
    t.amount_precise,
    t.amount,
    t.amount * p.price as amount_usd,
    t.decimals,
    t.symbol,
    t.has_decimal,
    t._log_id,
    t._inserted_timestamp,
    t.transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ this }} t 
    inner join {{ ref('price__ez_prices_hourly') }} p
    on date_trunc('hour', t.block_timestamp) = p.hour
    and t.contract_address = p.token_address
where t.amount_usd is null

{% endif %}

qualify row_number() over (partition by transfers_id order by _inserted_timestamp desc) = 1