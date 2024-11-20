{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'dragonswap_swaps_decoded_id',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH pools AS (

    SELECT
        pool_address,
        token0,
        token1
    FROM
        {{ ref('silver_evm_dex__dragonswap_pools_v1') }}
    UNION ALL
    SELECT
        pool_address,
        token0,
        token1
    FROM
        {{ ref('silver_evm_dex__dragonswap_pools_v2') }}
),
swaps_base AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        decoded_log: amount0In :: bigint AS amount0In,
        decoded_log: amount1In :: bigint AS amount1In,
        decoded_log: amount0Out :: bigint AS amount0Out,
        decoded_log: amount1Out :: bigint AS amount1Out,
        decoded_log :sender :: STRING AS sender,
        decoded_log :to :: STRING AS tx_to,
        token0,
        token1
    FROM
        {{ ref('core_evm__ez_decoded_event_logs') }}
        l
        INNER JOIN pools p
        ON p.pool_address = l.contract_address
    WHERE
        event_name = 'Swap'
        AND tx_succeeded

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
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    sender,
    tx_to,
    amount0In,
    amount1In,
    amount0Out,
    amount1Out,
    token0,
    token1,
    CASE
        WHEN amount0In <> 0
        AND amount1In <> 0
        AND amount0Out <> 0 THEN amount1In
        WHEN amount0In <> 0 THEN amount0In
        WHEN amount1In <> 0 THEN amount1In
    END AS amount_in_unadj,
    CASE
        WHEN amount0Out <> 0 THEN amount0Out
        WHEN amount1Out <> 0 THEN amount1Out
    END AS amount_out_unadj,
    CASE
        WHEN amount0In <> 0
        AND amount1In <> 0
        AND amount0Out <> 0 THEN token1
        WHEN amount0In <> 0 THEN token0
        WHEN amount1In <> 0 THEN token1
    END AS token_in,
    CASE
        WHEN amount0Out <> 0 THEN token0
        WHEN amount1Out <> 0 THEN token1
    END AS token_out,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS dragonswap_swaps_decoded_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    swaps_base
WHERE
    token_in <> token_out
