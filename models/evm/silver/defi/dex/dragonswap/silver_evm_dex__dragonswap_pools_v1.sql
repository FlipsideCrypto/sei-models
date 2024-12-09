{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'pool_address',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['noncore']
) }}

WITH created_pools AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        decoded_log :token0 :: STRING AS token0,
        decoded_log :token1 :: STRING AS token1,
        decoded_log :pair :: STRING AS pool_address,
        decoded_log :length :: INT AS pool_id
    FROM
        {{ ref ('core_evm__ez_decoded_event_logs') }}
    WHERE
        tx_succeeded
        AND contract_address = LOWER('0x71f6b49ae1558357bbb5a6074f1143c46cbca03d') --DragonswapFactory
        AND event_name = 'PairCreated'

{% if is_incremental() %}
AND modified_timestamp >= (
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
    contract_address,
    token0,
    token1,
    pool_address,
    pool_id,
    {{ dbt_utils.generate_surrogate_key(
        ['pool_address']
    ) }} AS dragonswap_pools_v1_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    created_pools qualify(ROW_NUMBER() over (PARTITION BY pool_address
ORDER BY
    block_timestamp DESC)) = 1
