{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }} },
    enabled = false
) }}

SELECT
    'astroport' AS platform,
    block_id,
    block_timestamp,
    tx_succeeded,
    tx_id,
    swapper,
    msg_group,
    msg_sub_group,
    msg_index,
    amount_in,
    currency_in,
    amount_out,
    currency_out,
    pool_address,
    pool_name,
    _inserted_timestamp
FROM
    {{ ref('silver__dex_swaps_astroport') }}
UNION ALL
SELECT
    'fuzio' AS platform,
    block_id,
    block_timestamp,
    tx_succeeded,
    tx_id,
    swapper,
    msg_group,
    msg_sub_group,
    msg_index,
    amount_in,
    currency_in,
    amount_out,
    currency_out,
    pool_address,
    pool_name,
    _inserted_timestamp
FROM
    {{ ref('silver__dex_swaps_fuzio') }}
UNION ALL
SELECT
    'seaswap' AS platform,
    block_id,
    block_timestamp,
    tx_succeeded,
    tx_id,
    swapper,
    msg_group,
    msg_sub_group,
    msg_index,
    amount_in,
    currency_in,
    amount_out,
    currency_out,
    pool_address,
    pool_name,
    _inserted_timestamp
FROM
    {{ ref('silver__dex_swaps_seaswap') }}
