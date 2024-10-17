{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }} },
    tags = ['noncore','recent_test']
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
    _inserted_timestamp,
    COALESCE (
        dex_swaps_astroport_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','msg_index']
        ) }}
    ) AS fact_dex_swaps_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
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
    _inserted_timestamp,
    COALESCE (
        dex_swaps_fuzio_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','msg_index']
        ) }}
    ) AS fact_dex_swaps_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
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
    _inserted_timestamp,
    COALESCE (
        dex_swaps_seaswap_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','msg_index']
        ) }}
    ) AS fact_dex_swaps_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ source(
        'silver',
        'dex_swaps_seaswap'
    ) }}
