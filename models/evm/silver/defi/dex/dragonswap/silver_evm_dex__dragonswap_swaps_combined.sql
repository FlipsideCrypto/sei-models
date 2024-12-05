{{ config(
    materialized = 'view',
    tags = ['noncore']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    tx_to,
    sender,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    dragonswap_swaps_decoded_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
    {{ ref('silver_evm_dex__dragonswap_swaps_decoded') }}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    tx_to,
    sender,
    CASE
        WHEN amount0_unadj < 0 THEN ABS(amount0_unadj)
        ELSE amount1_unadj
    END AS amount_in_unadj,
    CASE
        WHEN amount0_unadj < 0 THEN ABS(amount1_unadj)
        ELSE amount0_unadj
    END AS amount_out_unadj,
    CASE
        WHEN amount0_unadj < 0 THEN token0
        ELSE token1
    END AS token_in,
    CASE
        WHEN amount0_unadj < 0 THEN token1
        ELSE token0
    END AS token_out,
    dragonswap_swaps_undecoded_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
    {{ ref('silver_evm_dex__dragonswap_swaps_undecoded') }}
