{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, origin_function_signature, origin_from_address, origin_to_address, contract_address, from_address, to_address, symbol), SUBSTRING(origin_function_signature, contract_address, from_address, to_address, symbol)",
    tags = ['core']
) }}

WITH logs AS (

    SELECT
        _log_id,
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
        _inserted_timestamp
    FROM
        {{ ref('silver_evm__logs') }}
    WHERE
        topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
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
    decimals,
    symbol,
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
    token_transfers
