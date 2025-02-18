{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'sailorswap_swaps_id',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH pools AS (
    SELECT 
        pool_address,
        token0,
        token1
    FROM {{ ref('silver_evm_dex__sailorswap_pools') }}
),
swaps_base AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.event_index,
        'Swap' AS event_name,
        l.origin_function_signature,
        l.origin_from_address,
        l.origin_to_address,
        l.contract_address,
        CONCAT('0x', SUBSTR(topics[1] :: STRING, 27, 40)) AS sender,
        CONCAT('0x', SUBSTR(topics[2] :: STRING, 27, 40)) AS recipient,
        utils.udf_hex_to_int(
            's2c',
            regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}')[0] :: STRING
        ) :: FLOAT AS amount0,
        utils.udf_hex_to_int(
            's2c',
            regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}')[1] :: STRING
        ) :: FLOAT AS amount1,
        p.token0,
        p.token1,
        CASE WHEN p.pool_address IS NULL THEN TRUE ELSE FALSE END AS is_missing_pool
    FROM {{ ref('silver_evm__logs') }} l
    JOIN pools p
        ON LOWER(p.pool_address) = LOWER(l.contract_address)
    WHERE 
        topics[0] :: STRING = '0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83'
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
    AND l.modified_timestamp >= (
        SELECT
            MAX(modified_timestamp) - INTERVAL '5 minutes'
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address AS pool_address,
    sender,
    recipient,
    token0,
    token1,
    amount0,
    amount1,
    is_missing_pool,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS sailorswap_swaps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM swaps_base